package engine

import (
	"context"
	"fmt"

	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/model"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/repository"
	"Multi-TenantCoreEngine_with_PostgreSQLRLS_Relation-BasedAccessControlReBAC/internal/rebac/schema"

	"github.com/jackc/pgx/v5"
)

type memoryCheckEngine struct {
	schema *schema.Schema
	repo   repository.TupleRepository
}

// NewMemoryCheckEngine creates an in-memory BFS graph solver using the Schema DSL.
func NewMemoryCheckEngine(s *schema.Schema, repo repository.TupleRepository) CheckEngine {
	if s == nil {
		s = schema.DefaultSchema()
	}
	return &memoryCheckEngine{
		schema: s,
		repo:   repo,
	}
}

type searchNode struct {
	EntityType string
	EntityID   string
	Relation   string
}

func (n searchNode) Key() string {
	return fmt.Sprintf("%s:%s#%s", n.EntityType, n.EntityID, n.Relation)
}

// Check evaluates the ReBAC graph using Breadth-First Search (BFS) in Go memory,
// applying the Schema DSL rewrite rules and protecting against cycles.
func (e *memoryCheckEngine) Check(ctx context.Context, tx pgx.Tx, req CheckRequest) (bool, error) {
	queue := []searchNode{
		{
			EntityType: req.EntityType,
			EntityID:   req.EntityID,
			Relation:   req.Relation,
		},
	}

	visited := make(map[string]bool)

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		nodeKey := curr.Key()
		if visited[nodeKey] {
			continue // Cycle detected, skip node
		}
		visited[nodeKey] = true

		relDef, err := e.schema.GetRelation(curr.EntityType, curr.Relation)
		if err != nil {
			// If relation is not in schema, fallback to direct tuple match
			tuples, listErr := e.repo.ListAllForEntity(ctx, tx, curr.EntityType, curr.EntityID)
			if listErr != nil {
				return false, listErr
			}
			for _, t := range tuples {
				if t.Relation == curr.Relation {
					if t.SubjectType == req.SubjectType && t.SubjectID == req.SubjectID && t.SubjectRelation == nil {
						return true, nil
					}
					if t.SubjectRelation != nil {
						queue = append(queue, searchNode{
							EntityType: t.SubjectType,
							EntityID:   t.SubjectID,
							Relation:   *t.SubjectRelation,
						})
					}
				}
			}
			continue
		}

		// Fetch tuples for this entity
		tuples, err := e.repo.ListAllForEntity(ctx, tx, curr.EntityType, curr.EntityID)
		if err != nil {
			return false, fmt.Errorf("failed to fetch entity tuples: %w", err)
		}

		if e.evaluateRewrite(curr, relDef.Rewrite, tuples, req, &queue) {
			return true, nil
		}
	}

	return false, nil
}

func (e *memoryCheckEngine) evaluateRewrite(
	curr searchNode,
	rewrite *schema.UsersetRewrite,
	tuples []model.Tuple,
	req CheckRequest,
	queue *[]searchNode,
) bool {
	if rewrite == nil {
		return false
	}

	// 1. Direct assignment ("this")
	if rewrite.This {
		for _, t := range tuples {
			if t.Relation == curr.Relation {
				// Direct subject match (e.g. user:u-123)
				if t.SubjectType == req.SubjectType && t.SubjectID == req.SubjectID && t.SubjectRelation == nil {
					return true
				}
				// Userset match (e.g. workspace:ws-1#member)
				if t.SubjectRelation != nil && *t.SubjectRelation != "" {
					*queue = append(*queue, searchNode{
						EntityType: t.SubjectType,
						EntityID:   t.SubjectID,
						Relation:   *t.SubjectRelation,
					})
				}
			}
		}
	}

	// 2. Computed Userset (e.g. editor inherits owner on same object)
	if rewrite.ComputedUserset != "" {
		*queue = append(*queue, searchNode{
			EntityType: curr.EntityType,
			EntityID:   curr.EntityID,
			Relation:   rewrite.ComputedUserset,
		})
	}

	// 3. Tuple to Userset (e.g. parent#viewer)
	if rewrite.TupleToUserset != nil {
		ttu := rewrite.TupleToUserset
		for _, t := range tuples {
			if t.Relation == ttu.TuplesetRelation {
				*queue = append(*queue, searchNode{
					EntityType: t.SubjectType,
					EntityID:   t.SubjectID,
					Relation:   ttu.ComputedUserset,
				})
			}
		}
	}

	// 4. Union (OR)
	for _, childRewrite := range rewrite.Union {
		if e.evaluateRewrite(curr, &childRewrite, tuples, req, queue) {
			return true
		}
	}

	return false
}
