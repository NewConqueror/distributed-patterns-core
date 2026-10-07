package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultSchema(t *testing.T) {
	s := DefaultSchema()
	require.NotNil(t, s)

	// Document namespace checks
	docViewer, err := s.GetRelation("document", "viewer")
	require.NoError(t, err)
	assert.Equal(t, "viewer", docViewer.Name)
	assert.NotNil(t, docViewer.Rewrite)
	assert.NotEmpty(t, docViewer.Rewrite.Union)

	// Non-existent namespace check
	_, err = s.GetRelation("unknown_namespace", "viewer")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)

	// Non-existent relation check
	_, err = s.GetRelation("document", "unknown_rel")
	assert.ErrorIs(t, err, ErrRelationNotFound)
}

func TestSchemaJSONRoundtrip(t *testing.T) {
	orig := DefaultSchema()
	data, err := json.Marshal(orig)
	require.NoError(t, err)

	parsed, err := LoadFromJSON(data)
	require.NoError(t, err)

	docEditor, err := parsed.GetRelation("document", "editor")
	require.NoError(t, err)
	assert.Equal(t, "editor", docEditor.Name)
}
