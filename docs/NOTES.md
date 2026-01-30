# NOTES

- Trying to access the websocket URI, through browser doesn't work
  - The browser ALWAYS makes a `http/s` request and doesn't "upgrade" the http connection
  - Using websocket client through a script(JS) would allow us to trigger the target ws endpoint
  - We can't have same URI for both ws connection and http connection
  - We can instead -
    - create a html page with a `/chat/${roomId}` to serve a http response
    - add a script in the html page to start a ws connection to `/ws/chat/${roomId}`
    - both the URI have different handlers where
      - `/chat/${roomId}` serves a html page
      - `/ws/chat/${roomId}` listens to the ws connection

- client -> which will that the ws req and add to the group of connections
- connPool(hub) -> map of all the connections -> registers/un the connections/client 
  - room -> []conn

- Pool should not read the messages from the wsClients. Each wsClient should have their own read goroutine.

```bash
# You started a new reader goroutine every time you call Pool.Get() (see pool.go: go c.Read()), so the same *websocket.Conn ends up with multiple concurrent readers.
# Gorilla WebSocket (and JSON decoding on top of it) is not safe for concurrent reads — concurrent ReadJSON calls can corrupt internal buffers and cause weird frame errors (RSV/bad opcode) and panics.

2026/01/30 01:40:33 read error: websocket: RSV1 set, RSV2 set, bad opcode 3
2026/01/30 01:40:33 read error: websocket: RSV1 set, RSV2 set, bad opcode 12
2026/01/30 01:40:33 read error: websocket: RSV2 set, continuation after FIN
2026/01/30 01:40:33 read error: websocket: RSV2 set, RSV3 set
2026/01/30 01:40:33 read error: websocket: RSV2 set, data before FIN
```

- **Root cause**: Multiple goroutines were calling ReadJSON on the same websocket connection. you started a new reader each time you called Pool.Get(). Gorilla WebSocket and json.Decoder aren’t safe for concurrent reads; concurrent reads corrupt buffers and cause those frame/parsing errors and panics.
- **Fix**:
  - start the read goroutine after initializing the wsClient
  - remove `go c.Read()` from `Pool.Get()`
