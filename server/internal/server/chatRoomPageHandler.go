package internal

import (
	"html/template"
	"net/http"
	"path/filepath"
)

func (s *Server) ChatRoomPageHandler(w http.ResponseWriter, r *http.Request) {
	roomId := r.PathValue("roomId")
	tmplPath := filepath.Join("server", "html", "index.html")
	tmpl := template.Must(template.ParseFiles(tmplPath))

	data := map[string]string{
		"Title": "Chat Room " + roomId,
	}

	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, "Template error", http.StatusInternalServerError)
	}
}
