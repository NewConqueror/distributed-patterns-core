package main

import (
	"flag"
	"log"

	"geopulse/internal/buffer"
	"geopulse/internal/server"
)

func main() {
	port := flag.Int("port", 50051, "Port to listen on")
	bufferCap := flag.Int("buffer-cap", 10000, "Ring buffer capacity for ingestion backpressure")
	numWorkers := flag.Int("workers", 4, "Number of concurrent ingestion workers")
	flag.Parse()

	cfg := server.Config{
		Port:           *port,
		BufferCapacity: *bufferCap,
		NumWorkers:     *numWorkers,
		DropStrategy:   buffer.DropOldest,
	}

	if err := server.Run(cfg, nil); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
