# Assignment 2 Design Session

Tutoring session for designing the Assignment 2 architecture (message queues, consumers, load balancing).

---

**Tutor**: Before we even look at queues or consumers — what does your Assignment 1 server do when it receives a message from a client right now?

**Me**: it just echos the message back to the server with additional details like servertimestamp and status

**Tutor**: You mean back to the client, right? (the sender). So right now: one client sends a message → that same client gets it back. What should happen in a real chat room instead?

**Me**: it should echo to all the clients who are connected to that chat room

**Tutor**: Exactly. So what information does your server already have that would let it do that?

**Me**: there's a connectionMap, that I'm using to keep track of the users and the rooms they are connected to

**Tutor**: Right. So theoretically you could broadcast directly from the server without any queue at all — just iterate the room's clients and send. So why do you think the assignment is introducing a message queue in between? What problem does a queue solve that direct broadcast doesn't?

**Me**: to allow the server to process other requests instead of taking the responsibility to broadcast to all other connected clients

**Tutor**: That's a good instinct — offloading work from the server. That's related to a concept called decoupling. Let me push you a bit further though. Imagine your chat app gets really popular and you need to handle way more users. You spin up multiple server instances behind a load balancer. Now think about this scenario: User A connects to Server 1. User B connects to Server 2. They're both in Room 5. User A sends a message. What happens if Server 1 just tries to broadcast directly to Room 5?

**Me**: it only knows about the other clients in the connection map tahts on the server 1. It wouldn't know user b, since its connected to a different server

**Tutor**: Exactly. So each server only sees a partial view of the room. This is the core problem a message queue solves here — it's not just about offloading work, it's about coordinating across multiple servers. So with a queue in the middle, let's think about the roles. The server that receives a message from User A — what should its job be limited to?

**Me**: just publish the messages to the queue

**Tutor**: Right. And then something else reads from the queue and does the broadcasting. The assignment calls that the consumer. So here's the key question: the consumer needs to broadcast to all clients in Room 5 — including User B on Server 2. How does the consumer actually reach User B's WebSocket connection?

**Me**: the consumer can read the details from a global map, which lists the connectionMaps on each server, then broadcasts the messages to the ws connections that are connected to the room? Is it possible to have a global map? umm, else the consumer can read the messages from the server and broadcast it to all the servers. The servers can then bradcast the messages to all the clients connected to ther server?

**Tutor**: Good thinking — you're exploring two different approaches. On the "global map" idea: WebSocket connections are live objects in memory — they're not serializable. You can't store an active *websocket.Conn in a database or shared cache. So what does that rule out? On your second idea — consumer broadcasts to all servers, each server delivers to its local clients — that's actually a real pattern. But think about the mechanics: how would the consumer know which servers are running, and how would it talk to them?

**Me**: on the global map, can we have redis in place? on the second idea, one server writes messages to the queue, other server will broadcast messages to the clients connected. Since they need to have the context which server the client should connect to, the message should contain all the details. we can have one server doing both publish the message to queue and read from the queue on a different thered too

**Tutor**: On Redis — you're right that Redis can store shared state. But go back to what I said: a WebSocket connection is a live TCP connection in memory. Can you store that in Redis? What CAN you store in Redis that might be useful instead? On your third idea — one server both publishes AND consumes — you're onto something. Hold that thought, we'll come back to it. But first, let's focus on your second idea since it's the most interesting. You said "the message should contain all the details." What details exactly would the consumer need in the message to know where to deliver it?

**Me**: the serveIps of the rooms that are hosted on each server, and the clients connected to those rooms

**Tutor**: So you're saying the queue message itself carries routing information. That's interesting. But think about the timing — when User A sends a message, you'd need to embed the current state of who's connected where. What could go wrong with that approach?

**Me**: that would be a heavy message

**Tutor**: Yes, and also — what if a client disconnects between when the message was published and when the consumer reads it? The routing info in the message would be stale. So embedding connection state in the message is fragile. Let's go back to your Redis idea. You said you can't store a WebSocket connection in Redis. But what CAN you store there that would help the consumer know where to route a message for Room 5?

**Me**: the map[roomId]ServerRoomElement[] where serverRoomElement is a struct which contains serverId, userId, clientIp(from where the message is sent)

**Tutor**: Good. So Redis knows "Room 5 has users X and Y, X is on Server 1, Y is on Server 2." Now the consumer reads a message for Room 5 from the queue. It looks up Redis, finds Server 1 and Server 2 both have users in Room 5. Then what? How does the consumer actually deliver the message to those servers?

**Me**: loops through all the servers and broadcasts them to all the users connected to them

**Tutor**: Right, but HOW does it broadcast? The consumer is a separate process on a separate EC2. It can't just call a Go function on Server 1. What communication mechanism could the consumer use to tell Server 1 "hey, broadcast this message to your Room 5 clients"?

**Me**: maybe, we can hit an endpoint like /roomId/ to broadcast to all the users? maybe we can also establish a ws connection?

**Tutor**: Both are valid options. HTTP endpoint /broadcast/{roomId}: Consumer sends a POST to each server with the message, server receives it and delivers to its local Room 5 clients — simple, stateless. WebSocket connection: more complex to manage, but when would this be better than HTTP? But step back for a second. You now have a pretty complex system: Redis for shared state, Consumer looking up Redis, Consumer calling HTTP endpoints on each server, Servers maintaining local connection maps. This works, but it's a lot of moving pieces for a course assignment. Remember your earlier idea — "one server doing both publish AND consume on a different goroutine." Given what you now understand about the multi-server problem, when would that simpler approach actually be good enough?

**Me**: when the scale is less and the team is small. If we want to scale, we can just increase the nodes and we'll be fine with it.

**Tutor**: That's a reasonable business argument, but think more technically. With sticky sessions on the ALB — what does that guarantee about where a specific user's WebSocket connection lands, every time they connect?

**Me**: um, yeaa the user lands on the same server whenever a new connection is created. This makes sure that once user A connected to server 1, and then disconnects; if user A tries to re-connect after a while, ALB makes sure that it connects it to server 1.

**Tutor**: Close, but be careful — sticky sessions guarantee the same server for the same session, not necessarily across reconnects. It's cookie-based. But more importantly, think about the room level, not the user level. If User A and User B both join Room 5 fresh — does sticky sessions give any guarantee they'll land on the same server?

**Me**: no, it doesn't

**Tutor**: Right. So even with sticky sessions, users in the same room can be on different servers. So the "one server publishes and consumes" approach has a limitation — what is it?

**Me**: the user can be discconnected after the message is sent, but the broadcasted message might not reach them?

**Tutor**: That's a valid concern, but I'm thinking of something more fundamental. Think about it this way: Server 1 consumes a message from the queue for Room 5. It loops through its local connection map for Room 5. But User B is on Server 2. What happens to User B's delivery?

**Me**: ahh yeaa, the message once consumed, might not be delivered to other server.

**Tutor**: Exactly. User B simply never gets the message. It's silently dropped. So now you have two ends of a spectrum: Simple (each server publishes and consumes locally) vs Complex (Redis + consumer + HTTP callbacks). Before picking one, let me ask — what does the assignment actually test for? Go re-read the performance testing section. Does it test that User B on Server 2 actually receives User A's message?

**Me**: yea, it should reach user B

**Tutor**: Read the test scenarios carefully — specifically what the client is doing. It's sending 500K messages. What is it measuring?

**Me**: maximum throughputs?

**Tutor**: Right — throughput and latency. Now look at the client from Assignment 1. When it sends a message, what does it wait for before counting it as "done"?

**Me**: waits for a message from the server (Response) with a serverTimestamp

**Tutor**: Exactly. The client only cares about getting a response back to the sender. It's not verifying that User B on Server 2 actually received the broadcast. The test doesn't check cross-server delivery. So given that — which approach makes more sense for this assignment?

**Me**: single server with both producer and consumer

**Tutor**: Good call. Now let's think about WHY the queue still adds value even in this simpler setup. If the server just broadcasts directly without a queue, what happens to throughput when a room has 100 connected clients?

**Me**: but I want to make sure that the messages are delivered to ther users connected to the room

**Tutor**: That's a good instinct. But let's be precise — in your current test setup, all clients are load test bots from your client program. Do your load test clients actually join the same room and verify they received each other's messages? Or do they each just send messages and wait for their own echo back?

**Me**: leave the client from previous assignment, that bahaviour will change in the next assignment

**Tutor**: Good point. So for now, let's focus on what Assignment 2 actually requires. You've decided: single server with both producer and consumer. Now back to my question — what's the benefit of putting a queue in the middle even on a single server? Why not just broadcast directly from the read goroutine?

**Me**: too much resouce consumption

**Tutor**: Say more — what specific resource gets consumed when you broadcast directly to 100 clients from the same goroutine that's reading messages?

**Me**: it will write to the ws conn element that is connected to it?

**Tutor**: Think about it from the goroutine's perspective. The read goroutine is busy looping through 100 clients sending to their channels. Meanwhile, what is it NOT doing?

**Me**: it's not reading the incoming messages thats broadcated?

**Tutor**: Exactly. It's blocked doing broadcast work, so it can't read the next incoming message. The client on the other end is waiting. This is why you put a queue in between — the read goroutine just publishes to the queue and moves on immediately. It goes back to reading the next message right away. Then a separate consumer goroutine handles the slow work of broadcasting. That's the real benefit: decoupling the read speed from the broadcast speed. Now let's talk about the queue itself. The assignment gives you two options — RabbitMQ or SQS. What do you know about either of them?

**Me**: I dont have any preferencces, but I want to know more about them before I decide. RabbitMq - needs to be deployed on different EC2, should be scaled separately. has a better message retention time than SQS (>14 days) does this cost less? SQS - easy to deploy, but only queue. Need to have SNS if we want to subscribe to a topic

**Tutor**: Good start. Let me correct one thing — RabbitMQ doesn't have better retention than SQS by default. That's not really a differentiator here. On RabbitMQ: it supports something called a topic exchange. What do you think that means, and why might it be useful for a chat app with 20 rooms? On SQS: you mentioned you'd need SNS for pub/sub. For this assignment, do you actually need pub/sub, or do you just need a simple work queue?

**Me**: topic exchange can map to one or more queues based on some pattern. Um if we have a topic with roomId in it, it will automatically send it to those queues

**Tutor**: Exactly. So with RabbitMQ you'd have one exchange, and messages get routed to the right room queue automatically based on the routing key. With SQS you'd need... what instead?

**Me**: SNS. We need to make sure we configure an SNS, which will publish the topic to the right queue

**Tutor**: Right. So SQS alone isn't enough — you'd need SNS + SQS, which is more AWS services to configure and pay for. Now the other question — RabbitMQ needs its own EC2. What are the trade-offs of that?

**Me**: the scaling of EC2 is totally on us

**Tutor**: Yes. And the flip side — what does RabbitMQ give you that SQS doesn't, for debugging and monitoring during this assignment?

**Me**: automatic message routing, and monitoring

**Tutor**: Exactly — RabbitMQ has a built-in management UI at port 15672 where you can visually see queue depths, message rates, and consumer lag in real time. The assignment actually requires screenshots of this. With SQS you'd need to set up CloudWatch dashboards separately. So given all that — which are you leaning toward now?

**Me**: rabbit MQ

**Tutor**: Good choice. Now let's think about the queue topology. You have 20 rooms. What's your instinct on how many queues you should create?

**Me**: 20 queues

**Tutor**: Why 20? Could you get away with fewer?

**Me**: the number of total serves that are connected?

**Tutor**: Think about it from the consumer's perspective. If you have one queue for all rooms, what problem does that create when the consumer tries to route a message to the right room?

**Me**: the message procesing might be late

**Tutor**: That's one concern. But more fundamentally — if all messages from all 20 rooms go into one queue, what does the consumer have to do with every single message before it can broadcast? And think about ordering — if Room 1 is very busy and Room 2 is quiet, what happens to Room 2's messages in a single shared queue?

**Me**: it needs to wait

**Tutor**: Exactly — Room 2's messages get stuck behind Room 1's backlog. That's called head-of-line blocking. So 20 queues — one per room — makes sense. Each room's messages flow independently. Now, how do messages get from the producer to the right queue? What RabbitMQ concept routes messages to the correct queue? Here are the key RabbitMQ concepts: Exchange (producers publish to an exchange, not directly to queues), Queue (where messages sit waiting to be consumed), Binding (the link between exchange and queue), Routing Key (label the producer attaches to each message), Exchange Types: Direct (exact match), Topic (pattern matching), Fanout (sends to ALL bound queues). Which exchange type fits your use case, and what would your routing key look like?

**Me**: um, I think I'll write the messages from the server to a queue. The consumer then takes these messages and publish them to the exchange topic room5*. The server on the other hand, subscribes to this room topic and reads from the queue

**Tutor**: You're mixing up the roles a bit — in RabbitMQ, producers publish TO the exchange. The exchange then routes to queues. Consumers read FROM queues. The flow is strictly: Producer → Exchange → Queue(s) → Consumer. With that in mind, try again: when your server receives a message for Room 5, what does it publish to, and what does it include so RabbitMQ knows to route it to the Room 5 queue?

**Me**: it will publish to Rabbitmq's FIFO queue, with the topic as roomId. and the message. The server(which is the consumer) reads from the roomId queue and broadcasts it to all the clients connected to it

**Tutor**: Good, you've got the right idea. Now let's be precise: the server publishes to an exchange (not directly to a queue), it attaches a routing key like `room.5`, the exchange routes to the `room.5` queue based on that routing key, the consumer reads from the `room.5` queue. So what exchange type would you use here — direct, topic, or fanout?

**Me**: topic. so all the clients who subscribe to this get the message?

**Tutor**: Careful — you're mixing up two things. In RabbitMQ, clients don't subscribe to exchanges. Consumers subscribe to queues. The topic exchange just does the routing — it looks at the routing key and decides which queue to put the message in. So in your setup, how many consumers do you need, and what does each one subscribe to?

**Me**: if I have 500 users, and each user min connects to 10 rooms concurrently, one user should subscribe to all 10 queues

**Tutor**: Hold on — think about who the consumer is in your architecture. You decided earlier the consumer is part of the server, not the client. Clients connect via WebSocket to the server. They don't interact with RabbitMQ at all. So forget about the 500 users for a moment. How many queues does your server need to consume from?

**Me**: 20

**Tutor**: Right. So how many consumer goroutines do you need minimum?

**Me**: 20

**Tutor**: That works. One goroutine per room queue. But think about it from a resource perspective — in Go, goroutines are cheap, so 20 is fine. But what if you had 1000 rooms? Would "one goroutine per queue" still be a good approach?

**Me**: umm no, we need to pool it?

**Tutor**: Exactly. A worker pool — a fixed number of goroutines that each handle multiple queues. That's what the assignment calls a "multi-threaded consumer pool." For 20 rooms, either approach works. What are the trade-offs between the two?

**Me**: I want to try a worker pool concept in go. one go routine per queue, would lead to much storage space in the ram

**Tutor**: Good instinct on the worker pool. But let me push back on the RAM concern — in Go, a goroutine starts at about 2-4KB of stack space. So 20 goroutines is roughly 80KB. Is that actually a concern? More importantly, think about what happens inside each goroutine when it's idle (no messages in the queue). What does it do?

**Me**: this config is just the initial test. The assigment should have better throughput with atleast 80 consumer threads. the goroutine should be closed, since there are no messages on the topic

**Tutor**: The goroutine doesn't close — it blocks, waiting for the next message. RabbitMQ's consumer API delivers messages as they arrive, so the goroutine just sits idle consuming almost no CPU. So for 20 rooms, 20 goroutines is actually perfectly fine in Go. The worker pool pattern makes more sense when you have hundreds or thousands of queues. That said — the assignment specifically asks you to test with 10, 20, 40, 80 consumer threads. So you'll need to implement a configurable pool regardless. Let's think about the pool design. What does each worker goroutine in the pool need to do?

**Me**: a list of messages that are read from the client. Eg, it reads 80 messages at once and broadcasts it to all the channels it's connected

**Tutor**: That's batch processing — good thought for throughput. But let's back up one step. In a worker pool with say 10 workers and 20 room queues — how do you assign which worker handles which room? Does each worker own specific rooms, or is it more dynamic?

**Me**: Um. no, the worker doesn't own specific rooms. It just takes first 80 messages -> gets the clients connected on each message and broadcasts the messages

**Tutor**: So the workers pull from a shared pool of messages. That raises a question — where do those messages come from? You have 20 separate RabbitMQ queues. How do the worker goroutines get messages from all 20 queues without each worker having to know about every queue?

**Me**: ahh, okay. Let me clarify what I understand. The server get the message from the client, then server sends a message to the RabbitMQ queue. The RabbitMQ, routes this message to one of the 20 rooms its configured to based on the exchage.

**Tutor**: Good, that part is correct. Keep going — now what happens after the message lands in the room queue?

**Me**: it picks up first 80 messages then processes it. this is done by one worker

**Tutor**: Okay, but who picks up those 80 messages? You have 10 worker goroutines and 20 room queues. How does a worker actually get messages from the queues? Think about it in Go terms — what would the worker be reading from?

**Me**: umm, having 20 worker pools makes things easier?

**Tutor**: You're avoiding the question a bit — let me make it concrete. In Go, if you want multiple goroutines to share work, you typically use a channel. So what if you had a single Go channel that sits between the RabbitMQ queues and your worker pool? What would put messages INTO that channel, and what would read FROM it?

**Me**: the rabbitMq queues send a message to sendChan in the room. The room worker will then read the Queue message, and echos it to the clients connected

**Tutor**: You're very close. But let's be precise — RabbitMQ doesn't "send" to a Go channel directly. Something in your code has to bridge them. You'd have 20 goroutines (one per room queue) that just do one thing: read from RabbitMQ and forward to a shared Go channel. Then your worker pool reads from that shared channel and does the broadcasting. Architecture: `RabbitMQ queue room.1 → goroutine → | shared Go channel → worker 1 / worker 2 / worker N`. What's the advantage of this design over workers directly consuming from RabbitMQ queues?

**Me**: avoids idle goroutines. and here, we can process N messages at a same time

**Tutor**: Exactly — the workers stay busy as long as there are messages in the shared channel, regardless of which room they came from. Now, the 20 "bridge" goroutines that read from RabbitMQ — they're very lightweight, just forwarding messages. So having 20 of them is totally fine. Now let me ask you about the channel pool for the producer side. When your server wants to publish a message to RabbitMQ, it needs a RabbitMQ channel. In RabbitMQ, you have one connection but can have many channels. Why would you need a pool of channels rather than just one?

**Me**: we can multiplex on one connection, rather than creating one connection per user

**Tutor**: Right — one connection, many channels. That's RabbitMQ's multiplexing model. But why a pool of channels specifically? What happens if 500 concurrent goroutines all try to publish using the same single channel?

**Me**: the single channel becomes a bottleneck if the messages are over the buffer size of the channel

**Tutor**: Close — but the issue is more fundamental than buffer size. A RabbitMQ channel is not thread-safe. So if 500 goroutines share one channel, what do you need to do to use it safely?

**Me**: use mutex

**Tutor**: Exactly — and what does that mean for your 500 goroutines trying to publish simultaneously?

**Me**: all 500 go routines will be accessing the same channel

**Tutor**: And they'll be serialized — only one at a time can hold the mutex. So despite having 500 goroutines, you're publishing one message at a time. That's your bottleneck. So with a pool of say 20 channels, what changes?

**Me**: we're publishing 20 messages

**Tutor**: Exactly — 20 goroutines can publish simultaneously instead of 1. Much better throughput. So the channel pool in Go would look like what data structure?

**Me**: type RabbitMQ struct { conn *connection to RabbitMQ; ch chan(message) } where ch is instance type

**Tutor**: You're on the right track. But think about what the pool itself is. The pool holds multiple channels. A goroutine needs to borrow one, use it, then return it. In Go, what's the natural data structure for "take one when available, put it back when done"?

**Me**: semaphore?

**Tutor**: A semaphore controls access but doesn't hold the items themselves. Think simpler — you already used this pattern in your client for Assignment 1. What did you use to manage a pool of WebSocket connections?

**Me**: map

**Tutor**: Not quite — think about the blocking behavior you need. When all channels are in use, a goroutine should wait until one becomes available. What Go primitive does that naturally?

**Me**: channel

**Tutor**: Exactly! A buffered Go channel used as a pool: `type ChannelPool struct { pool chan *amqp.Channel; conn *amqp.Connection }`. When a goroutine wants to publish — it reads from pool (blocks if empty). When done — it writes back to pool. Why is this better than a mutex + slice approach?

**Me**: [image of RabbitMQ client code using amqp091-go] what about this? here the video mentions using RabbitMQ client and spanning a new channel for each Rmq instance. Help me understand how this is different from the channel pool struct

**Tutor**: Good question. Look at RabbitClient — it has one conn and one ch. So if you create 20 RabbitClient instances, what do you have?

**Me**: 20 RabbitClients

**Tutor**: Right. And each has its own connection. How is that different from a channel pool where you have one connection and 20 channels?

**Me**: wait, but it uses the same conn, to create multiple RabbitClients

**Tutor**: Look again carefully — NewRabbitMQClient takes a *amqp.Connection as a parameter. So yes, you could pass the same connection to create multiple clients, each with their own channel. So this pattern is essentially the same as a channel pool — one connection, many channels. The difference is just how you manage borrowing and returning. With this approach, how would you track which RabbitClient instances are available versus in use?

**Me**: umm yea, its difficuly

**Tutor**: Exactly. You'd need a mutex + slice or some other mechanism to track availability. Whereas with a buffered Go channel as a pool, borrowing and returning is built-in — reading blocks when empty, writing blocks when full. No extra bookkeeping needed. So which approach would you choose for your implementation?

**Me**: so the buffer size of this channelPool.pool is the no of consumer threads?

**Tutor**: Close — but be careful. The channel pool is on the producer side (publishing to RabbitMQ). The consumer threads are separate. So what should determine the buffer size of the channel pool?

**Me**: 500 as max messages in the queue?

**Tutor**: Think about it differently. The channel pool size determines how many goroutines can publish simultaneously. What's the maximum number of goroutines that could be trying to publish at the same time in your server?

**Me**: the no.of websocket connections, i.e no of clients

**Tutor**: Exactly. Which the assignment tests up to 512 threads. So your channel pool size should be tuned relative to that — not the number of rooms. But here's the thing — you don't need one channel per client. You just need enough channels so that clients rarely have to wait. What would happen if your pool size is too small? What if it's too large?

**Me**: if the pool size is too small, the messages get dropped. If its too large, we'll take up space

**Tutor**: Messages won't get dropped — remember, reading from an empty Go channel blocks. So goroutines would just wait for a channel to become available. But what's the problem with waiting?

**Me**: it the goroutine is idle

**Tutor**: Right — and what does that mean for the client waiting on the other end of that WebSocket connection?

**Me**: its blocked

**Tutor**: Exactly — increased latency. The client is waiting for a response while its goroutine is stuck waiting for a free RabbitMQ channel. So the pool size is a tuning parameter — too small causes latency, too large wastes memory. The assignment actually asks you to test different configurations to find the sweet spot. Now let's zoom out. You've made most of the key design decisions. [summary of decisions so far]. What's missing?

**Me**: writing the messages back to the client through the worker pool?

**Tutor**: Yes — after the worker pool broadcasts, it writes to each client.Send channel in the room. Your existing handleClientWrites goroutine per client then handles the actual WebSocket write. That part stays unchanged. What else? Think about failure scenarios. What happens if RabbitMQ goes down while your server is running?

**Me**: it needs to have a circuit breaker. Can you help me understand why?

**Tutor**: Sure. Imagine RabbitMQ goes down. Your server is still receiving WebSocket messages from clients. What happens when a goroutine tries to borrow a channel from the pool and publish?

**Me**: it can't publish it, since the Rabbit MQ server is down

**Tutor**: Right. And without a circuit breaker, what does that goroutine do?

**Me**: it just logs a message and moves on

**Tutor**: Maybe — but what if the publish call itself hangs waiting for a connection that never comes back? What happens to that goroutine and the client waiting for a response?

**Me**: it's idle and exhanusting the resources

**Tutor**: Exactly. And if hundreds of goroutines pile up waiting, what happens to your server?

**Me**: crashes

**Tutor**: Right — or becomes completely unresponsive. This is called cascading failure — one downstream service (RabbitMQ) takes down your entire server. A circuit breaker prevents this. Think of it like an electrical circuit breaker — what does it do when it detects a problem?

**Me**: it breakes the power flow in the curcuit

**Tutor**: Exactly. In software, a circuit breaker has 3 states. Can you guess what they might be?

**Me**: open, half-open, close

**Tutor**: Perfect. What does each state mean in terms of whether requests are allowed through? Think of it like a physical circuit breaker on a wall: Closed circuit = electricity flows (normal operation). Open circuit = electricity is cut off (failure state).

**Me**: closed - send all messages; open - no messages; half-open - half messages flow to the queue

**Tutor**: Close! Half-open isn't about "half the messages" — it's about probing to see if RabbitMQ has recovered. In half-open state, you let through a small number of test requests. If they succeed, what do you do? If they fail, what do you do?

**Me**: you open the circuit

**Tutor**: Which direction? If the test requests succeed, you want messages to flow again — so you move to which state?

**Me**: if the test succeed -> close the circuit; if they fail -> open the circuit

**Tutor**: Exactly. Now put it all together — what triggers the transition from closed to open?

**Me**: when couple of messages (30-50) get dropped or have an error publishing it to the RabbitMQ server

**Tutor**: Exactly — a failure threshold. After N consecutive failures, the circuit opens and you stop trying to publish, failing fast instead of hanging. So now you have a complete picture of the circuit breaker. Rather than building one from scratch, do you know if Go has any libraries for this?

**Me**: no, Idk

**Tutor**: That's worth a quick search when you start implementing. Look up sony/gobreaker or afex/hystrix-go — both are popular Go circuit breaker libraries. Now let's talk about one more important concept — message acknowledgment. When your consumer reads a message from RabbitMQ and broadcasts it to room clients, when should it acknowledge the message back to RabbitMQ?

**Me**: once it broadcasts it to all the users connected to that room

**Tutor**: Right. And why does the timing matter — what happens if you acknowledge before broadcasting?

**Me**: some broadcasts might be left unsent to other users in the room, but the RabbitMQ wouldn't know if the message needs to be re-tried

**Tutor**: Exactly. If you ack early and then the broadcast fails halfway through, the message is gone from the queue forever. That's a lost message. But if you ack after broadcasting, and your consumer crashes mid-broadcast, what does RabbitMQ do with that message?

**Me**: it retries

**Tutor**: Right — it redelivers to another consumer. That's at-least-once delivery. But what's the potential downside of at-least-once delivery?

**Me**: it can send this message to multiple servers

**Tutor**: Right — duplicate messages. Some clients might receive the same message twice. The assignment mentions handling duplicates. How would you detect if a message has already been delivered to a client?

**Me**: based on the message Id, that will be generated once we write to the RabbitMq

**Tutor**: Exactly. The messageId (UUID) in your QueueMessage struct serves this purpose. Where would you store the "already seen" message IDs to check against?

**Me**: I can have a councurrentHashMap to check against the messages

**Tutor**: Good. But think about memory — if you store every messageId forever, what happens over time with 500K messages?

**Me**: the concurrentHashmap takes up space

**Tutor**: Right. So you need some kind of eviction strategy. What are your options?

**Me**: umm, I can delete the messageId once I get an ack

**Tutor**: Think about the timing — by the time you get an ack, you've already used the messageId to check for duplicates. So deleting on ack doesn't help. Think about it differently — do you need to track messageIds forever, or just for a certain window of time?

**Me**: small window of time. I can have a TTL on them with 20 min

**Tutor**: Good thinking. And in Go, what data structure or approach could implement a TTL-based cache?

**Me**: time.Sleep + concurrent hashmap?

**Tutor**: That would work but it's crude — a goroutine that periodically sweeps the map and deletes expired entries. What's the downside of that approach?

**Me**: its blocking -> waits till the TTL is complete

**Tutor**: Right — and you'd be scanning the entire map on every sweep. For a simpler approach, look up patrickmn/go-cache — it's a Go library that handles TTL-based in-memory caching with automatic expiration. But honestly, for this assignment, duplicate messages are rare edge cases. Don't over-engineer it — a simple map with periodic cleanup is fine. You've now thought through all the major design decisions. What part of this design are you least confident about?

**Me**: I think the producer and consumer + rabbitMQ

**Tutor**: Good self-assessment. Let's break it down further — which part specifically? Is it: how to connect to RabbitMQ in Go, how the exchange/queue/binding setup works, how the channel pool works mechanically, or how the consumer reads messages and bridges to the worker pool?

**Me**: how channel pool works

**Tutor**: Okay. Let's build up the mental model step by step. Forget RabbitMQ for a moment. Imagine you have a pool of 5 database connections and 100 goroutines that need to query the database. How would you design that in Go using just a buffered channel?

**Me**: all 100 go routines will write to 5 buffered channels. Each db connection reads from these 5 channels and writes to db

**Tutor**: You've got it slightly inverted. The goroutines need a connection to do work — they don't write to the connections, they borrow one. Think of it like a library with 5 books. 100 students want to read. What's the process?

**Me**: first 5 studnets get the book. then once when one of the boook is returned, only then other student can access them

**Tutor**: Exactly. Now translate that to Go: the 5 books = buffered channel with capacity 5, pre-filled with connections. A student borrowing a book = reading from the channel. A student returning a book = writing back to the channel. What happens to the 6th goroutine that tries to read from an already-empty channel?

**Me**: it waits till the channel has messages

**Tutor**: Exactly — it blocks automatically. No mutex, no condition variables needed. The channel handles all the waiting and signaling for you. Now apply this to your RabbitMQ channel pool. Write out in pseudocode what Borrow() and Return() would look like.

**Me**: func (r *rmq) Borrow() { message := <-r.Channel; ...process message }; func (r rmq) Return() { create queueMessage from message; r.Channel <- QueueMessage }

**Tutor**: You're mixing two things — the pool manages *amqp.Channel objects (the publishing tools), not messages. Try again. The pool holds *amqp.Channel objects. Borrow() should return one so a goroutine can use it to publish. Return() should put it back.

**Me**: Borrow() { return pool.Channel }; Return() {}

**Tutor**: Close! But remember pool itself is a buffered Go channel. So how do you read from it and write to it?

**Me**: message <- pool.Channel - borrow; pool.Channel <- queueMessage - return

**Tutor**: You've got the arrow direction backwards for borrow. In Go, <- on the right side of := reads from a channel. The correct pattern: Borrow() returns <-p.pool (take a channel OUT of the pool). Return(ch) does p.pool <- ch (put the channel BACK into the pool). Now — how would a goroutine use this when it wants to publish a message?

**Me**: show me the pool struct

**Tutor**: You tell me — based on what you now understand, what fields does ChannelPool need?

**Me**: type ChannelPool strutc{ pool *amqp.Channel; ch chan(QueuMessage) }

**Tutor**: Almost — but two issues: pool should be a Go channel of *amqp.Channel, not a single pointer. And ch chan(QueueMessage) doesn't belong in the pool — the pool's only job is managing RabbitMQ channels. Try again with just what the pool needs.

**Me**: type ChannelPool strutc{ pool chan (*amqp.Channel) }

**Tutor**: That's it. Simple and clean. Now one more question — how do you initialize the pool? When your server starts up, what needs to happen before any goroutine can borrow a channel?

**Me**: it needs to initialize the pool with conn channels

**Tutor**: Right. So write the NewChannelPool function signature — what parameters does it need, and what does it do?

**Me**: func NewChannelPool(roomId, userId, concurrency){ return &ChannelPool{ pool: make(chan(*ampq.Channel), concurrency) } }

**Tutor**: You're overcomplicating it. The channel pool has nothing to do with rooms or users — it's just a generic pool of RabbitMQ channels for publishing. What does it actually need to create those channels?

**Me**: just a RabbitMQ connection

**Tutor**: Exactly. [Showed NewChannelPool with conn *amqp.Connection and size int, pre-filling pool in a loop]. Does this make sense? What does conn.Channel() do?

**Me**: gets one connection from the RabbitMq connection pool

**Tutor**: Close — conn is already a single RabbitMQ connection. conn.Channel() creates a new channel on top of that connection. Connection = TCP connection to RabbitMQ (expensive, one per app). Channel = lightweight virtual connection multiplexed over the TCP connection (cheap, many per connection). Now you understand the channel pool. Let's move to the consumer side. You have 20 room queues in RabbitMQ. How do you start consuming from a queue in Go using amqp091-go?

**Me**: QueuDeclare() -> check if the queue for current room exists then write the messages

**Tutor**: Right direction — but consuming is different from writing. After you declare the queue, what function do you call to start receiving messages from it?

**Me**: channel.Consume()

**Tutor**: Exactly. channel.Consume() returns a Go channel of amqp.Delivery. So what does your bridge goroutine (one per room) look like? It needs to read from that delivery channel and forward to the shared worker channel.

**Me**: let me summarize so far. server -> creates QueueMessage from message and publishes it to RabbitMQ's room[roomid] queue -> consumer reads from this RabbitMQ channel (first checks if it exists), gets the delivary channle to read messages from -> loops over delivary chan -> then broadcasts the messages

**Tutor**: Good summary. Now write it as a rough goroutine sketch.

**Me**: [first attempt - had worker broadcast logic instead of bridge forwarding]

**Tutor**: You're writing the worker logic, not the bridge goroutine. startRoomConsumer is the bridge — its only job is reading from RabbitMQ and forwarding to workerChan. What does that look like?

**Me**: [second attempt - delivaryChan := <- channel.Consume()]

**Tutor**: Good direction! But channel.Consume() returns a Go channel of deliveries, not a single message. The <- is wrong here. And where does this goroutine run? It needs to run forever in the background without blocking the caller.

**Me**: [third attempt - added unneeded doneCh and blocked on it]

**Tutor**: You're blocking on <-doneCh which defeats the purpose — the caller would wait forever. Also doneCh isn't needed here. Strip it down to the simplest version.

**Me**: func (s *Server) startRoomConsumer(roomId string, workerChan chan amqp.Delivery) { conn, channel := channel.QueueDeclare(roomId); delivaryChan, _ := channel.Consume(); go func() { for qmessage := range delivaryChan { workerChan <- qMessage } }() }

**Tutor**: That's it! Clean and correct. The goroutine runs in the background, forwarding messages from RabbitMQ to the worker pool. You'd call this for all 20 rooms at startup. Now write the worker goroutine — it reads from workerChan and broadcasts to room clients.

**Me**: func BroadcastWorker(workerChan chan ampq.Delivery){ for qm := range workerChan { go broadcastMessage(qm) } }

**Tutor**: Good structure! But why are you spawning a new goroutine with go broadcastMessage(qm) inside the worker? What's the downside?

**Me**: the worker wouldn't have control on the spawned goroutines

**Tutor**: Exactly — unbounded goroutine spawning. You could end up with thousands of goroutines and lose all the benefits of having a controlled worker pool. Just call it directly. Now write broadcastMessage — what does it need to do?

**Me**: it needs to get the message from the qm and broadcast it to the users connected to the room

**Tutor**: Right. Write the rough sketch — what information do you need from qm, and how do you get the room's clients?

**Me**: maybe we can have users subscribe to the room?

**Tutor**: You already have that — look at your existing server.go. The Server struct has a Rooms map with all connected clients. You don't need a separate subscription mechanism. So broadcastMessage needs access to the Server to look up the room. How would you give it that access?

**Me**: umm, make a rest end point to get a list of clients connected to the room from the server like url/room3 -> gives list of clientIps/userIds

**Tutor**: That's over-engineering it. The worker is running inside the same process as the server. It can access the server's state directly. How would you make broadcastMessage a method on Server instead of a standalone function?

**Me**: func(s *Server) broadcastMessage() { users := s.Rooms[roomId] }

**Tutor**: Exactly. And then what do you do with those users?

**Me**: func(s *Server) broadcastMessage(qm QueueMessage) { users := s.Rooms[roomId]; for _, u := range users { users.Send <- qm.message } }

**Tutor**: Great — that's the core logic. Two things: s.Rooms[roomId] gives you a *Room, not the users directly. How do you get the users from the room? And you're accessing s.Rooms without a lock. What should you do before reading from the rooms map?

**Me**: lock to room to access the clients

**Tutor**: Right. And which kind of lock — read lock or write lock? You're only reading here.

**Me**: read lock, since I'm just reading

**Tutor**: Exactly. [Showed broadcastMessage with RLock/RUnlock pattern]. Now fill in the iteration — what do you send to each client's Send channel?

**Me**: [completed iteration with client.Send <- qm.Message]

**Tutor**: Almost perfect! One issue — you're iterating users after releasing the lock. What could go wrong with that?

**Me**: umm, other threads can the userlist while the broadcast is taking place. But the user list can be updated

**Tutor**: Exactly — a user could disconnect and their Send channel could be closed while you're iterating. Writing to a closed channel in Go causes a panic. So what are your options to handle this safely?

**Me**: unlock only after broadcasting the messages

**Tutor**: That works — keep the lock for the entire iteration. But what's the downside of holding the lock while sending to each client's Send channel?

**Me**: it would lock alll users

**Tutor**: Right — no other goroutine can add or remove users from the room while you're broadcasting. That could cause latency spikes. The alternative is to copy the user list while holding the lock, release the lock, then iterate the copy. What's the trade-off of that approach?

**Me**: we can access the list of users, but allow other goroutines to access the rooms, while broadcasting

**Tutor**: Exactly. The trade-off is that your copy might be slightly stale — a user might disconnect between when you copied and when you send. So you could try to send to a closed channel. How would you handle that in Go safely?

**Me**: check if the channel is open, send the message only when the channel is open

**Tutor**: Right idea. In Go, you can't directly check if a channel is closed before sending. But you can use recover() to catch a panic, or use a select with a default case. But actually — look at your existing handleClientWrites function. How does it currently handle a closed Send channel?

**Me**: uses select

**Tutor**: Right — it checks ok when reading from the channel. But that's on the reading side. Your broadcast is on the writing side. The safest pattern for sending to a channel that might be closed is to use recover(). Or better — look at how your existing code closes the Send channel. Where does that happen?

**Me**: when a user exists a room, or the connection disconnects

**Tutor**: Right — in RemoveUserFromRoom. And it holds room.Mu.Lock() when closing the channel. So if your broadcast holds room.Mu.RLock() while sending, can RemoveUserFromRoom close the channel at the same time?

**Me**: yes

**Tutor**: Actually no — RLock and Lock are mutually exclusive. If you hold room.Mu.RLock(), RemoveUserFromRoom can't acquire room.Mu.Lock() until you release it. So holding the read lock during iteration actually prevents the channel from being closed mid-broadcast. That makes it safe to send. So which approach do you prefer now — hold the lock during iteration, or copy and release?

**Me**: umm, copy and relase it

**Tutor**: Good choice for throughput. But now you need to handle the potential panic when sending to a possibly-closed channel. The cleanest Go patterns: defer recover(), or non-blocking select with default case. Which do you prefer and why?

**Me**: non-blocking select

**Tutor**: Good choice. Why is non-blocking better here than recover()?

**Me**: it doesn't need to be defered. its clean

**Tutor**: Yes, and also — recover() catches panics which is expensive. The select with default is just a conditional, much cheaper. But there's a subtle difference — the default case triggers both when the channel is closed AND when the channel's buffer is full. Is that okay for your use case?

**Me**: um, when the buffer is full it needs to block till the queueu empties

**Tutor**: Good instinct for reliability. But think about what happens if one slow client's buffer is full — you'd block the entire broadcast for all other clients in the room. Is it better to skip one slow client and keep broadcasting to others, or block everyone waiting for that one client?

**Me**: um, yea its better to skip one

**Tutor**: Exactly. Skipping a slow client is better than blocking the entire room. This is a common pattern in real chat systems. So select with default is the right choice — if a client's buffer is full, skip them. They'll just miss that message.

---

## Final Design Decisions

1. **Architecture**: Single server acts as both producer and consumer
2. **Queue**: RabbitMQ, topic exchange `chat.exchange`, routing key `room.{roomId}`, 20 queues
3. **Producer**: Channel pool (`chan *amqp.Channel`, borrowed/returned per publish)
4. **Consumer**: 20 bridge goroutines → shared `chan amqp.Delivery` → configurable worker pool
5. **Broadcast**: Copy room users under read lock, release, iterate with non-blocking `select`
6. **New struct**: `QueueMessage` with `messageId`, `roomId`, `serverId`, `clientIp`
7. **Ack**: Manual, after broadcast attempt
8. **Circuit breaker**: Open after N failures, half-open probing, close on recovery (look up sony/gobreaker)
9. **Duplicate detection**: TTL-based map keyed on `messageId` (look up patrickmn/go-cache)
10. **Slow client handling**: Non-blocking send with `select/default` — skip full buffers

## Libraries to Investigate

- `github.com/rabbitmq/amqp091-go` — RabbitMQ client for Go
- `github.com/sony/gobreaker` — circuit breaker
- `github.com/patrickmn/go-cache` — TTL-based in-memory cache
