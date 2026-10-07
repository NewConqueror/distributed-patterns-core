package model

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidObjectFormat  = errors.New("invalid object format, expected 'type:id'")
	ErrInvalidSubjectFormat = errors.New("invalid subject format, expected 'type:id' or 'type:id#relation'")
)

// Tuple represents a Zanzibar relation tuple:
// <entity_type>:<entity_id>#<relation>@<subject_type>:<subject_id>[#<subject_relation>]
type Tuple struct {
	ID              uuid.UUID  `json:"id"`
	TenantID        uuid.UUID  `json:"tenant_id"`
	EntityType      string     `json:"entity_type"`
	EntityID        string     `json:"entity_id"`
	Relation        string     `json:"relation"`
	SubjectType     string     `json:"subject_type"`
	SubjectID       string     `json:"subject_id"`
	SubjectRelation *string    `json:"subject_relation,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

// EntityString returns the formatted entity identifier: "type:id"
func (t Tuple) EntityString() string {
	return FormatEntity(t.EntityType, t.EntityID)
}

// SubjectString returns the formatted subject identifier: "type:id" or "type:id#relation"
func (t Tuple) SubjectString() string {
	if t.SubjectRelation != nil && *t.SubjectRelation != "" {
		return fmt.Sprintf("%s:%s#%s", t.SubjectType, t.SubjectID, *t.SubjectRelation)
	}
	return FormatEntity(t.SubjectType, t.SubjectID)
}

// String returns Zanzibar canonical string representation
// e.g. "document:doc-1#viewer@user:u-123"
func (t Tuple) String() string {
	return fmt.Sprintf("%s:%s#%s@%s", t.EntityType, t.EntityID, t.Relation, t.SubjectString())
}

// FormatEntity formats entity_type and entity_id into "type:id"
func FormatEntity(entityType, entityID string) string {
	return fmt.Sprintf("%s:%s", entityType, entityID)
}

// ParseEntity parses "type:id" into entityType and entityID
func ParseEntity(raw string) (string, string, error) {
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%w: %q", ErrInvalidObjectFormat, raw)
	}
	return parts[0], parts[1], nil
}

// ParseSubject parses "type:id" or "type:id#relation"
func ParseSubject(raw string) (string, string, *string, error) {
	if raw == "" {
		return "", "", nil, ErrInvalidSubjectFormat
	}

	hashIdx := strings.Index(raw, "#")
	var subjectRel *string
	entityPart := raw

	if hashIdx != -1 {
		rel := raw[hashIdx+1:]
		if rel == "" {
			return "", "", nil, fmt.Errorf("%w: empty relation in %q", ErrInvalidSubjectFormat, raw)
		}
		subjectRel = &rel
		entityPart = raw[:hashIdx]
	}

	sType, sID, err := ParseEntity(entityPart)
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: invalid subject %q", ErrInvalidSubjectFormat, raw)
	}

	return sType, sID, subjectRel, nil
}
