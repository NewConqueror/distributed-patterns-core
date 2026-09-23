package schema

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrNamespaceNotFound = errors.New("namespace not found in schema")
	ErrRelationNotFound  = errors.New("relation not found in namespace")
)

// Schema represents the entire Zanzibar ReBAC authorization model.
type Schema struct {
	Namespaces map[string]*Namespace `json:"namespaces"`
}

// Namespace defines relations and rewrite rules for an entity type (e.g. "document", "folder").
type Namespace struct {
	Name      string               `json:"name"`
	Relations map[string]*Relation `json:"relations"`
}

// Relation defines how a relationship is computed.
type Relation struct {
	Name    string          `json:"name"`
	Rewrite *UsersetRewrite `json:"rewrite,omitempty"`
}

// UsersetRewrite describes how a permission/relation is expanded:
// - Direct ("this"): satisfied if a tuple exists directly.
// - ComputedUserset: check another relation on the same object (e.g. document#editor).
// - TupleToUserset: walk an edge (e.g. document#parent -> folder) and check a relation there.
// - Union (OR): satisfied if any of the child rewrites are satisfied.
// - Intersection (AND): satisfied only if all child rewrites are satisfied.
type UsersetRewrite struct {
	This            bool             `json:"this,omitempty"`
	ComputedUserset string           `json:"computed_userset,omitempty"`
	TupleToUserset  *TupleToUserset  `json:"tuple_to_userset,omitempty"`
	Union           []UsersetRewrite `json:"union,omitempty"`
	Intersection    []UsersetRewrite `json:"intersection,omitempty"`
}

// TupleToUserset specifies walking a tupleset relation to check a target relation.
// Example: TuplesetRelation: "parent", ComputedUserset: "viewer"
type TupleToUserset struct {
	TuplesetRelation string `json:"tupleset_relation"`
	ComputedUserset  string `json:"computed_userset"`
}

func NewSchema() *Schema {
	return &Schema{
		Namespaces: make(map[string]*Namespace),
	}
}

// AddNamespace registers a namespace in the schema.
func (s *Schema) AddNamespace(ns *Namespace) *Schema {
	s.Namespaces[ns.Name] = ns
	return s
}

// GetRelation retrieves a specific relation definition.
func (s *Schema) GetRelation(entityType, relationName string) (*Relation, error) {
	ns, exists := s.Namespaces[entityType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrNamespaceNotFound, entityType)
	}
	rel, exists := ns.Relations[relationName]
	if !exists {
		return nil, fmt.Errorf("%w: %s on entity %s", ErrRelationNotFound, relationName, entityType)
	}
	return rel, nil
}

// LoadFromJSON parses a JSON string into a Schema.
func LoadFromJSON(data []byte) (*Schema, error) {
	var s Schema
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema JSON: %w", err)
	}
	return &s, nil
}

// DefaultSchema returns the standard multi-tenant ReBAC schema:
// - workspace: admin, member
// - folder: owner, editor, viewer, parent
// - document: owner, editor, viewer, parent
func DefaultSchema() *Schema {
	return NewSchema().
		// 1. Workspace
		AddNamespace(&Namespace{
			Name: "workspace",
			Relations: map[string]*Relation{
				"admin": {
					Name:    "admin",
					Rewrite: &UsersetRewrite{This: true},
				},
				"member": {
					Name: "member",
					Rewrite: &UsersetRewrite{
						Union: []UsersetRewrite{
							{This: true},
							{ComputedUserset: "admin"},
						},
					},
				},
				"viewer": {
					Name: "viewer",
					Rewrite: &UsersetRewrite{
						Union: []UsersetRewrite{
							{This: true},
							{ComputedUserset: "member"},
						},
					},
				},
			},
		}).
		// 2. Folder
		AddNamespace(&Namespace{
			Name: "folder",
			Relations: map[string]*Relation{
				"parent": {
					Name:    "parent",
					Rewrite: &UsersetRewrite{This: true},
				},
				"owner": {
					Name:    "owner",
					Rewrite: &UsersetRewrite{This: true},
				},
				"editor": {
					Name: "editor",
					Rewrite: &UsersetRewrite{
						Union: []UsersetRewrite{
							{This: true},
							{ComputedUserset: "owner"},
							{
								TupleToUserset: &TupleToUserset{
									TuplesetRelation: "parent",
									ComputedUserset:  "editor",
								},
							},
						},
					},
				},
				"viewer": {
					Name: "viewer",
					Rewrite: &UsersetRewrite{
						Union: []UsersetRewrite{
							{This: true},
							{ComputedUserset: "editor"},
							{
								TupleToUserset: &TupleToUserset{
									TuplesetRelation: "parent",
									ComputedUserset:  "viewer",
								},
							},
						},
					},
				},
			},
		}).
		// 3. Document
		AddNamespace(&Namespace{
			Name: "document",
			Relations: map[string]*Relation{
				"parent": {
					Name:    "parent",
					Rewrite: &UsersetRewrite{This: true},
				},
				"owner": {
					Name:    "owner",
					Rewrite: &UsersetRewrite{This: true},
				},
				"editor": {
					Name: "editor",
					Rewrite: &UsersetRewrite{
						Union: []UsersetRewrite{
							{This: true},
							{ComputedUserset: "owner"},
							{
								TupleToUserset: &TupleToUserset{
									TuplesetRelation: "parent",
									ComputedUserset:  "editor",
								},
							},
						},
					},
				},
				"viewer": {
					Name: "viewer",
					Rewrite: &UsersetRewrite{
						Union: []UsersetRewrite{
							{This: true},
							{ComputedUserset: "editor"},
							{
								TupleToUserset: &TupleToUserset{
									TuplesetRelation: "parent",
									ComputedUserset:  "viewer",
								},
							},
						},
					},
				},
			},
		})
}
