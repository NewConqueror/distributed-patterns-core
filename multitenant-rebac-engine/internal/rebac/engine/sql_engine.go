package engine

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type sqlCheckEngine struct{}

// NewSQLCheckEngine creates an engine that evaluates ReBAC queries using
// PostgreSQL Recursive Common Table Expressions (CTE) with built-in Cycle Detection.
func NewSQLCheckEngine() CheckEngine {
	return &sqlCheckEngine{}
}

// Check executes a Recursive CTE in PostgreSQL to traverse the authorization graph.
// Cycle Protection:
// - Keeps track of visited edges in an array: `path`
// - Uses `NOT (next_node = ANY(path))` to prevent infinite loops (A -> B -> A).
// - Limits recursion depth to at most 20 levels.
func (e *sqlCheckEngine) Check(ctx context.Context, tx pgx.Tx, req CheckRequest) (bool, error) {
	query := `
		WITH RECURSIVE graph_walk AS (
			-- Anchor / Base Step: Matching the initial object with direct relation or computed rewrites
			SELECT 
				entity_type, 
				entity_id, 
				relation, 
				subject_type, 
				subject_id, 
				subject_relation,
				ARRAY[entity_type || ':' || entity_id || '#' || relation] AS path
			FROM tuples
			WHERE entity_type = $1 
			  AND entity_id = $2 
			  AND (
				  relation = $3
				  OR ($3 = 'viewer' AND relation IN ('editor', 'owner'))
				  OR ($3 = 'editor' AND relation = 'owner')
				  OR ($3 = 'member' AND relation = 'admin')
				  OR relation = 'parent'
			  )

			UNION ALL

			-- Recursive Step: Traverse usersets and parent object hierarchies
			SELECT 
				t.entity_type, 
				t.entity_id, 
				t.relation, 
				t.subject_type, 
				t.subject_id, 
				t.subject_relation,
				gw.path || (t.entity_type || ':' || t.entity_id || '#' || t.relation) AS path
			FROM tuples t
			JOIN graph_walk gw ON (
				-- Case 1: Userset indirection (e.g., subject is workspace:ws-1#member)
				(gw.subject_relation IS NOT NULL 
				 AND t.entity_type = gw.subject_type 
				 AND t.entity_id = gw.subject_id 
				 AND t.relation = gw.subject_relation)
				OR
				-- Case 2: Parent hierarchy traversal (e.g., document#parent -> folder:f-1)
				(gw.relation = 'parent' 
				 AND t.entity_type = gw.subject_type 
				 AND t.entity_id = gw.subject_id 
				 AND (
					 t.relation = $3
					 OR ($3 = 'viewer' AND t.relation IN ('editor', 'owner'))
					 OR ($3 = 'editor' AND t.relation = 'owner')
					 OR t.relation = 'parent'
				 ))
			)
			-- Cycle Detection: terminate branch if node was already visited
			WHERE NOT (t.entity_type || ':' || t.entity_id || '#' || t.relation = ANY(gw.path))
			  AND array_length(gw.path, 1) < 20
		)
		SELECT EXISTS (
			SELECT 1 FROM graph_walk
			WHERE subject_type = $4 
			  AND subject_id = $5 
			  AND subject_relation IS NULL
			  AND (
				  relation = $3
				  OR ($3 = 'viewer' AND relation IN ('editor', 'owner'))
				  OR ($3 = 'editor' AND relation = 'owner')
				  OR ($3 = 'member' AND relation = 'admin')
			  )
		) AS allowed;
	`

	var allowed bool
	err := tx.QueryRow(
		ctx, query,
		req.EntityType,
		req.EntityID,
		req.Relation,
		req.SubjectType,
		req.SubjectID,
	).Scan(&allowed)

	if err != nil {
		return false, fmt.Errorf("sql recursive check failed: %w", err)
	}

	return allowed, nil
}
