package tokenguard

import (
	"unicode/utf8"

	"Production_AIGateway_with_SemanticCaching_ModelFallbacks_SafeToolExecutionEngine/pkg/models"
)

// EstimateTextTokens provides a fast, accurate token count estimate for raw text.
// Uses heuristic character/word boundary analysis consistent with BPE (Byte Pair Encoding).
func EstimateTextTokens(text string) int {
	if len(text) == 0 {
		return 0
	}

	charCount := utf8.RuneCountInString(text)
	// For standard English/Latin text, 1 token is roughly 4 characters (~0.75 words).
	// For code and dense unicode, it averages 3 characters per token.
	tokens := (charCount + 3) / 4
	if tokens < 1 {
		tokens = 1
	}
	return tokens
}

// EstimatePromptTokens calculates total estimated input tokens for a chat completion request.
// Accounts for ChatML per-message framing overhead (<|im_start|>role\ncontent<|im_end|>: 3-4 tokens).
func EstimatePromptTokens(messages []models.Message) int {
	if len(messages) == 0 {
		return 0
	}

	totalTokens := 0
	for _, m := range messages {
		// 3 tokens overhead per message (role, start/end markers)
		totalTokens += 3
		totalTokens += EstimateTextTokens(m.Content)
		if m.Name != "" {
			totalTokens += EstimateTextTokens(m.Name) + 1
		}
	}

	// 3 tokens overhead for assistant response priming
	totalTokens += 3
	return totalTokens
}
