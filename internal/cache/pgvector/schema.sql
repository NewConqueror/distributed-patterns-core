-- PostgreSQL Schema for Semantic Caching with pgvector

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS semantic_cache (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    prompt_text TEXT NOT NULL,
    prompt_embedding VECTOR(1536) NOT NULL,
    response_text TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- HNSW index for ultra-fast cosine similarity search
CREATE INDEX IF NOT EXISTS idx_semantic_cache_embedding_hnsw 
ON semantic_cache 
USING hnsw (prompt_embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);
