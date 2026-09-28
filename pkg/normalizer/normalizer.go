package normalizer

import (
	"regexp"
	"strings"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

var whitespaceRegex = regexp.MustCompile(`\s+`)

// Normalize cleans up raw prompt text by trimming, lowercasing, and collapsing whitespace.
func Normalize(text string) string {
	// 1. Lowercase
	cleaned := strings.ToLower(text)

	// 2. Collapse all consecutive whitespace (spaces, newlines, tabs) into a single space
	cleaned = whitespaceRegex.ReplaceAllString(cleaned, " ")

	// 3. Trim leading and trailing spaces
	return strings.TrimSpace(cleaned)
}

// ExtractPromptFromMessages extracts the semantic prompt from a slice of messages.
// It prioritizes the latest user query, optionally incorporating conversation context if present.
func ExtractPromptFromMessages(messages []models.Message) string {
	if len(messages) == 0 {
		return ""
	}

	// Find the last user message
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == models.RoleUser {
			return Normalize(messages[i].Content)
		}
	}

	// Fallback to the last message if no user message found
	return Normalize(messages[len(messages)-1].Content)
}
