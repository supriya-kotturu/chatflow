// Package generate provides factory functions for creating test users,
// rooms, and messages used by the load-testing client.
package generate

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"supriyakotturu.github.com/chatflow/pkg/models"
)

var messages []string = []string{
	"Why don't scientists trust atoms? Because they make up everything!",
	"I told my wife she was drawing her eyebrows too high. She looked surprised.",
	"Why don't eggs tell jokes? They'd crack each other up!",
	"I'm reading a book about anti-gravity. It's impossible to put down!",
	"Did you hear about the mathematician who's afraid of negative numbers? He'll stop at nothing to avoid them!",
	"Why did the scarecrow win an award? He was outstanding in his field!",
	"I used to hate facial hair, but then it grew on me.",
	"What do you call a fake noodle? An impasta!",
	"Why don't skeletons fight each other? They don't have the guts!",
	"I'm terrified of elevators, so I'm going to start taking steps to avoid them.",
	"What's the best thing about Switzerland? I don't know, but the flag is a big plus!",
	"Why did the coffee file a police report? It got mugged!",
	"I haven't slept for ten days, because that would be too long.",
	"What do you call a bear with no teeth? A gummy bear!",
	"Why did the bicycle fall over? Because it was two-tired!",
	"I'm on a seafood diet. I see food and I eat it.",
	"What do you call a sleeping bull? A bulldozer!",
	"Why don't programmers like nature? It has too many bugs!",
	"I told a joke about UDP, but I'm not sure if you got it.",
	"There are only 10 types of people: those who understand binary and those who don't.",
	"Why do Java developers wear glasses? Because they can't C#!",
	"A SQL query goes into a bar, walks up to two tables and asks: 'Can I join you?'",
	"Why did the developer go broke? Because he used up all his cache!",
	"How many programmers does it take to change a light bulb? None, that's a hardware problem!",
	"Why do Python programmers prefer dark mode? Because light attracts bugs!",
	"I would tell you a joke about an infinite loop, but it would go on forever.",
	"Why did the function break up with the variable? It wasn't returning her calls!",
	"What's a programmer's favorite hangout place? Foo Bar!",
	"Why don't databases ever get lonely? They always have relations!",
	"I'm not lazy, I'm just on energy saving mode.",
	"Why did the cookie go to the doctor? Because it felt crumbly!",
	"What do you call a dinosaur that crashes his car? Tyrannosaurus Wrecks!",
	"Why don't scientists trust stairs? Because they're always up to something!",
	"I used to be addicted to soap, but I'm clean now.",
	"What do you call a fish wearing a crown? A king fish!",
	"Why did the math book look so sad? Because it had too many problems!",
	"I'm reading a book on the history of glue. I just can't seem to put it down!",
	"What do you call a cow with no legs? Ground beef!",
	"Why did the tomato turn red? Because it saw the salad dressing!",
	"I'm friends with 25 letters of the alphabet. I don't know Y.",
	"What do you call a belt made of watches? A waist of time!",
	"Why don't oysters share? Because they're shellfish!",
	"I used to play piano by ear, but now I use my hands.",
	"What's orange and sounds like a parrot? A carrot!",
	"Why did the golfer bring two pairs of pants? In case he got a hole in one!",
	"I'm writing a book about hurricanes. It's only a draft.",
	"What do you call a parade of rabbits hopping backwards? A receding hare-line!",
	"Why don't melons get married? Because they cantaloupe!",
	"I used to be a banker, but I lost interest.",
	"What do you call a group of disorganized cats? A cat-astrophe!",
}

// UserID returns a random numeric user ID between 1 and 100000.
func UserID() string {
	return strconv.Itoa(rand.Intn(100000) + 1)
}

// Username returns a username in the form "user-{userId}".
func Username(userId string) string {
	return string("user-" + userId)
}

// Message returns a random message from the predefined pool.
func Message() string {
	idx := rand.Intn(len(messages))
	return strconv.Itoa(idx) + "--" + messages[idx]
}

// TotalRooms is the size of the room ID space used by ChatRoomID.
const TotalRooms = 20

// ChatRoomID returns a random room ID between 1 and TotalRooms.
func ChatRoomID() string {
	return strconv.Itoa(rand.Intn(TotalRooms) + 1)
}

// NewJoinMessage creates a JOIN message for the given user and room.
func NewJoinMessage(userId string, chatRoom string) *models.Message {
	username := Username(userId)
	msg := models.NewMessage(userId, username, "", models.MessageTypeJoin)
	msg.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	return msg
}

// NewLeaveMessage creates a LEAVE message for the given user and room.
func NewLeaveMessage(userId string, chatRoom string) *models.Message {
	username := Username(userId)
	msg := models.NewMessage(userId, username, "", models.MessageTypeLeave)
	msg.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	return msg
}

// NewMessage creates a TEXT message with a random body for the given user.
func NewMessage(userId string) *models.Message {
	username := Username(userId)
	messageType := models.MessageTypeText
	message := Message()

	return models.NewMessage(userId, username, message, messageType)
}

// NewSessionMessages generates a message sequence for a user as repeated session
// cycles: JOIN → TEXT… → LEAVE. The distribution targets 5% JOIN, 90% TEXT,
// 5% LEAVE, which always yields 18 TEXT messages per session regardless of
// messageCount. The actual total may differ slightly from messageCount due to
// integer rounding of numSessions and textPerSession.
func NewSessionMessages(userId string, messageCount int) []*models.Message {
	numSessions := int(float64(messageCount) * 0.05)
	if numSessions < 1 {
		numSessions = 1
	}
	textPerSession := (messageCount - 2*numSessions) / numSessions
	if textPerSession < 0 {
		textPerSession = 0
	}

	messages := make([]*models.Message, 0, numSessions*(textPerSession+2))
	for i := 0; i < numSessions; i++ {
		messages = append(messages, NewJoinMessage(userId, ""))
		for j := 0; j < textPerSession; j++ {
			messages = append(messages, NewMessage(userId))
		}
		messages = append(messages, NewLeaveMessage(userId, ""))
	}
	return messages
}

// NewRooms returns a slice of unique random room IDs.
func NewRooms(size int) []string {
	seen := make(map[string]struct{})
	rooms := make([]string, 0, size)
	for len(rooms) < size {
		id := ChatRoomID()
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			rooms = append(rooms, id)
		}
	}
	return rooms
}

// NewUsers returns a slice of user IDs in the form "1", "2", etc.
func NewUsers(size int) []string {
	users := make([]string, size)
	for idx := range users {
		users[idx] = fmt.Sprintf("%d", idx+1)
	}
	return users
}
