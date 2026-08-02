package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	port := getenv("PORT", "9000")
	dist := getenv("FASTSHARE_DIST", "../frontend/dist")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", handleHealth)
	mux.HandleFunc("/api/rooms", handleCreateRoom)
	mux.HandleFunc("/api/rooms/", handleRoomInfo)
	mux.HandleFunc("/ws", handleWS)
	mux.HandleFunc("/", spaHandler(dist))

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           withCommonHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Printf("fastshare listening on :%s (dist=%s)", port, dist)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("shutting down ...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hub.Shutdown(ctx)
	_ = srv.Shutdown(ctx)
	log.Println("bye.")
}

// spaHandler serves the built React app and falls back to index.html
// for client-side routes (/r/CODE).
func spaHandler(dist string) http.HandlerFunc {
	abs, err := filepath.Abs(dist)
	if err != nil {
		log.Fatalf("bad dist path: %v", err)
	}
	fs := http.FileServer(http.Dir(abs))
	index := filepath.Join(abs, "index.html")

	return func(w http.ResponseWriter, r *http.Request) {
		p := strings.TrimPrefix(filepath.Clean("/"+r.URL.Path), "/")
		if p != "" {
			full := filepath.Join(abs, filepath.FromSlash(p))
			if !strings.HasPrefix(full, abs) {
				http.NotFound(w, r)
				return
			}
			if fi, err := os.Stat(full); err == nil && !fi.IsDir() {
				fs.ServeHTTP(w, r)
				return
			}
		}
		http.ServeFile(w, r, index)
	}
}
