package normalizer

import (
	"testing"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Trimming and lowercasing",
			input:    "   HELLO WORLD   ",
			expected: "hello world",
		},
		{
			name:     "Multiple spaces, tabs and newlines",
			input:    "What   is \t \n\n Go   language?  ",
			expected: "what is go language?",
		},
		{
			name:     "Already clean",
			input:    "golang gateway",
			expected: "golang gateway",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Normalize(tt.input)
			if got != tt.expected {
				t.Errorf("Normalize() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestExtractPromptFromMessages(t *testing.T) {
	messages := []models.Message{
		{Role: models.RoleSystem, Content: "You are a helpful assistant."},
		{Role: models.RoleUser, Content: "   How does   Go   concurrency work?  "},
		{Role: models.RoleAssistant, Content: "Go uses goroutines."},
		{Role: models.RoleUser, Content: "  Tell ME   more!  \n"},
	}

	extracted := ExtractPromptFromMessages(messages)
	expected := "tell me more!"

	if extracted != expected {
		t.Fatalf("ExtractPromptFromMessages() = %q, want %q", extracted, expected)
	}
}
