package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	db  *pgxpool.Pool
	ctx context.Context
	mux *http.ServeMux
}

func NewServer(ctx context.Context, db *pgxpool.Pool) *Server {
	return &Server{
		db:  db,
		ctx: ctx,
		mux: http.NewServeMux(),
	}
}

func (s *Server) Start(port string) {
	s.mux.HandleFunc("/metrics", s.MetricsHandler)

	httpServer := &http.Server{Addr: port, Handler: s.mux}

	go func() {
		<-s.ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(shutdownCtx)
	}()

	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("Error starting the server: %+v\n", err)
	}
}
