package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseEntity(t *testing.T) {
	eType, eID, err := ParseEntity("document:doc-123")
	require.NoError(t, err)
	assert.Equal(t, "document", eType)
	assert.Equal(t, "doc-123", eID)

	_, _, err = ParseEntity("invalid-entity")
	assert.ErrorIs(t, err, ErrInvalidObjectFormat)

	_, _, err = ParseEntity(":doc-123")
	assert.ErrorIs(t, err, ErrInvalidObjectFormat)
}

func TestParseSubject(t *testing.T) {
	// Simple subject
	sType, sID, sRel, err := ParseSubject("user:u-456")
	require.NoError(t, err)
	assert.Equal(t, "user", sType)
	assert.Equal(t, "u-456", sID)
	assert.Nil(t, sRel)

	// Userset subject
	sType, sID, sRel, err = ParseSubject("workspace:ws-1#member")
	require.NoError(t, err)
	assert.Equal(t, "workspace", sType)
	assert.Equal(t, "ws-1", sID)
	require.NotNil(t, sRel)
	assert.Equal(t, "member", *sRel)

	// Invalid
	_, _, _, err = ParseSubject("workspace:ws-1#")
	assert.Error(t, err)
}

func TestTupleString(t *testing.T) {
	tuple := Tuple{
		EntityType:  "document",
		EntityID:    "doc-1",
		Relation:    "viewer",
		SubjectType: "user",
		SubjectID:   "u-123",
	}
	assert.Equal(t, "document:doc-1#viewer@user:u-123", tuple.String())

	rel := "member"
	tupleWithRel := Tuple{
		EntityType:      "document",
		EntityID:        "doc-1",
		Relation:        "viewer",
		SubjectType:     "workspace",
		SubjectID:       "ws-1",
		SubjectRelation: &rel,
	}
	assert.Equal(t, "document:doc-1#viewer@workspace:ws-1#member", tupleWithRel.String())
}
