package internal

import (
	"fmt"
	"html/template"
	"net/http"
	"path/filepath"
)

func (s *Server) HomeHandler(w http.ResponseWriter, r *http.Request) {
	tmplPath := filepath.Join("server", "html", "index.html")
	tmpl := template.Must(template.ParseFiles(tmplPath))
	data := map[string]string{
		"Title": "ChatFlow!",
	}

	if err := tmpl.Execute(w, data); err != nil {
		fmt.Println("template cannot be parsed.")
	}
}
