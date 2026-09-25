package transformer

import (
	"fmt"
	"sync"
)

// CategoryCache provides thread-safe category lookup for denormalizing product documents
type CategoryCache struct {
	mu         sync.RWMutex
	categories map[int]string
}

func NewCategoryCache() *CategoryCache {
	c := &CategoryCache{
		categories: make(map[int]string),
	}
	// Seed initial categories for lookup
	c.categories[101] = "Mice & Pointers"
	c.categories[102] = "Keyboards"
	c.categories[103] = "Monitors"
	c.categories[104] = "Laptops & PCs"
	c.categories[105] = "Audio & Headphones"
	return c
}

func (c *CategoryCache) Get(categoryID int) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if name, ok := c.categories[categoryID]; ok {
		return name
	}
	return fmt.Sprintf("Category #%d", categoryID)
}

func (c *CategoryCache) Set(categoryID int, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.categories[categoryID] = name
}
