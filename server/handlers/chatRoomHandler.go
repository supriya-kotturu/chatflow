package handlers

import (
	"log"
	"net/http"

	"supriyakotturu.github.com/chatflow/pkg/models"
)

func ChatRoomHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := WsUpgrader.Upgrade(w, r, nil)
	roomId := r.PathValue("roomId")

	log.Println("roomId: ", roomId)

	if err != nil {
		log.Printf("Failed to set websocket upgrade: %+v\n", err)
		return
	}
	defer conn.Close()

	for {
		var message models.Message
		err = conn.ReadJSON(&message)

		if err != nil {
			log.Println("error reading JSON: ", err)
			break
		}

		if err = conn.WriteJSON(&message); err != nil {
			log.Println("error writing JSON: ", err)
			break
		}
	}
}
