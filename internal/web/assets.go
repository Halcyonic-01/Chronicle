package web

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
)

//go:embed static/*
var files embed.FS

// Handler serves the Chronicle operations console.
func Handler() http.Handler {
	if dir := os.Getenv("CHRONICLE_UI_DIR"); dir != "" {
		return noCache(http.FileServer(http.Dir(dir)))
	}
	static, _ := fs.Sub(files, "static")
	return http.FileServer(http.FS(static))
}

type noCacheHandler struct{ next http.Handler }

func (h noCacheHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	h.next.ServeHTTP(w, r)
}

func noCache(next http.Handler) http.Handler { return noCacheHandler{next: next} }
