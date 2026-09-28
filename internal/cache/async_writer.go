package cache

import (
	"context"
	"log"
	"sync"
	"time"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/internal/embedding"
)

// WriteTask represents an asynchronous cache insertion job.
type WriteTask struct {
	PromptText   string
	Embedding    []float32
	ResponseText string
	Metadata     map[string]any
}

// AsyncWriter manages background worker goroutines to insert items into VectorStore non-blockingly.
type AsyncWriter struct {
	store       VectorStore
	embedder    embedding.Embedder
	taskQueue   chan *WriteTask
	workerCount int
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

// NewAsyncWriter starts background workers to persist cache entries asynchronously.
func NewAsyncWriter(store VectorStore, embedder embedding.Embedder, queueSize int, workerCount int) *AsyncWriter {
	if queueSize <= 0 {
		queueSize = 500
	}
	if workerCount <= 0 {
		workerCount = 2
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &AsyncWriter{
		store:       store,
		embedder:    embedder,
		taskQueue:   make(chan *WriteTask, queueSize),
		workerCount: workerCount,
		ctx:         ctx,
		cancel:      cancel,
	}

	for i := 0; i < workerCount; i++ {
		w.wg.Add(1)
		go w.workerLoop()
	}

	return w
}

func (w *AsyncWriter) workerLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		case task, ok := <-w.taskQueue:
			if !ok {
				return
			}
			w.processTask(task)
		}
	}
}

func (w *AsyncWriter) processTask(task *WriteTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	emb := task.Embedding
	if len(emb) == 0 && w.embedder != nil {
		generated, err := w.embedder.Embed(ctx, task.PromptText)
		if err != nil {
			log.Printf("[AsyncWriter] Failed to generate embedding for async cache write: %v", err)
			return
		}
		emb = generated
	}

	if len(emb) == 0 {
		log.Printf("[AsyncWriter] Cannot store cache entry with empty embedding")
		return
	}

	if err := w.store.Store(ctx, task.PromptText, emb, task.ResponseText, task.Metadata); err != nil {
		log.Printf("[AsyncWriter] Failed to store response in vector cache: %v", err)
	}
}

// Enqueue puts a write task onto the queue non-blockingly. If the queue is full, it drops the write to preserve low latency.
func (w *AsyncWriter) Enqueue(task *WriteTask) bool {
	select {
	case w.taskQueue <- task:
		return true
	default:
		log.Printf("[AsyncWriter] Cache write queue is full; dropping task for prompt: %q", task.PromptText)
		return false
	}
}

// Stop gracefully shuts down workers and drains the queue.
func (w *AsyncWriter) Stop() {
	w.cancel()
	close(w.taskQueue)
	w.wg.Wait()
}
