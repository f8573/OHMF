package messages

import (
	"strings"
	"unicode"
)

// SearchQuery represents a normalized and parsed search query
type SearchQuery struct {
	Original       string
	Normalized     string
	Tokens         []string
	HasOperators   bool
	Operators      map[string]bool
	PhraseQueries  []string
	QueryLength    int
	IsEmpty        bool
	IsAllStopwords bool
}

// Common English stop words that often don't provide value in searches
var stopwords = map[string]bool{
	"a": true, "an": true, "and": true, "are": true, "as": true, "at": true,
	"be": true, "by": true, "for": true, "from": true, "in": true, "is": true,
	"of": true, "on": true, "or": true, "the": true, "to": true,
	"with": true, "i": true, "me": true, "my": true, "we": true, "you": true,
	"he": true, "she": true, "they": true, "this": true, "that": true,
	"what": true, "which": true, "who": true, "where": true, "when": true,
	"why": true, "how": true, "all": true, "each": true, "every": true,
	"both": true, "few": true, "more": true, "most": true, "some": true,
	"such": true, "no": true, "nor": true, "not": true, "only": true,
	"same": true, "so": true, "than": true, "too": true, "very": true,
	"just": true, "but": true, "can": true, "will": true,
	"do": true, "have": true, "has": true, "had": true, "does": true,
	"did": true, "could": true, "would": true, "should": true, "may": true,
	"might": true, "must": true, "shall": true, "ought": true,
}

// BooleanOperators that can be used in queries
var booleanOperators = map[string]bool{
	"AND": true,
	"OR":  true,
	"NOT": true,
	"&":   true,
	"|":   true,
	"!":   true,
	"-":   true,
}

// NormalizeQuery preprocesses user input for better matching
// Returns a SearchQuery struct with normalized and parsed information
func NormalizeQuery(q string) *SearchQuery {
	sq := &SearchQuery{
		Original:      q,
		Operators:     make(map[string]bool),
		PhraseQueries: make([]string, 0),
	}

	// Check if empty
	if strings.TrimSpace(q) == "" {
		sq.IsEmpty = true
		sq.Normalized = ""
		sq.QueryLength = 0
		return sq
	}

	// Extract phrase queries (quoted strings)
	phrases, remaining := extractPhrases(q)
	sq.PhraseQueries = phrases

	// Tokenize and normalize
	normalized := normalizeText(remaining)
	sq.Normalized = normalized
	sq.QueryLength = len(q)

	originalTokens := strings.Fields(strings.TrimSpace(remaining))
	normalizedTokens := strings.Fields(normalized)
	sq.Tokens = make([]string, 0, len(normalizedTokens))
	lexicalTokens := make([]string, 0, len(normalizedTokens))

	// Detect explicit operators, then keep only meaningful non-stopword terms.
	for idx, token := range normalizedTokens {
		originalToken := token
		if idx < len(originalTokens) {
			originalToken = originalTokens[idx]
		}
		if isExplicitBooleanOperator(originalToken) {
			sq.HasOperators = true
			sq.Operators[strings.ToUpper(strings.TrimSpace(originalToken))] = true
		} else if token != "" {
			lexicalTokens = append(lexicalTokens, token)
			if !stopwords[token] {
				sq.Tokens = append(sq.Tokens, token)
			}
		}
	}

	// Check if every lexical token was filtered as a stopword.
	if len(lexicalTokens) > 0 && len(sq.Tokens) == 0 && len(sq.PhraseQueries) == 0 {
		sq.IsAllStopwords = true
	}

	return sq
}

func isExplicitBooleanOperator(token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	switch token {
	case "AND", "OR", "NOT", "&", "|", "!", "-":
		return true
	default:
		return false
	}
}

// normalizeText performs unicode normalization, accent removal, and case normalization
func normalizeText(text string) string {
	// Convert to lowercase
	text = strings.ToLower(text)

	// Remove leading/trailing whitespace
	text = strings.TrimSpace(text)

	// Normalize spaces (multiple spaces -> single space)
	text = strings.Join(strings.Fields(text), " ")

	return text
}

// extractPhrases extracts quoted phrases from query and returns them plus remaining text
func extractPhrases(q string) ([]string, string) {
	phrases := make([]string, 0)
	remaining := strings.Builder{}
	inQuote := false
	escaped := false
	var currentPhrase strings.Builder

	for _, r := range q {
		if escaped {
			if inQuote {
				currentPhrase.WriteRune(r)
			}
			escaped = false
			continue
		}

		if r == '\\' {
			escaped = true
			continue
		}

		if r == '"' {
			if inQuote {
				// End of phrase
				phrase := strings.TrimSpace(currentPhrase.String())
				if phrase != "" {
					phrases = append(phrases, phrase)
				}
				currentPhrase.Reset()
				inQuote = false
			} else {
				// Start of phrase
				inQuote = true
			}
			continue
		}

		if inQuote {
			currentPhrase.WriteRune(r)
		} else {
			remaining.WriteRune(r)
		}
	}

	// If there's an unclosed phrase, treat it as regular text
	if inQuote {
		remaining.WriteString(currentPhrase.String())
	}

	return phrases, remaining.String()
}

// ValidateSearchQuality checks if a query is likely to be problematic
// Returns (isValid, shouldWarn, reason)
func ValidateSearchQuality(sq *SearchQuery) (bool, string) {
	if sq.IsEmpty {
		return false, "query is empty"
	}

	if len(sq.Tokens) == 0 && len(sq.PhraseQueries) == 0 {
		return false, "query contains only operators"
	}

	if sq.IsAllStopwords && len(sq.PhraseQueries) == 0 {
		return false, "query contains only common stop words"
	}

	if sq.QueryLength > 500 {
		return false, "query is too long (max 500 characters)"
	}

	// Warn on very short meaningful queries (but still valid)
	if len(sq.Tokens) == 1 && len(sq.Tokens[0]) == 1 {
		return true, "single character queries may return many results"
	}

	return true, ""
}

// removeSpecialChars removes problematic special characters while preserving quotes and operators
func removeSpecialChars(text string) string {
	return strings.Map(func(r rune) rune {
		// Keep alphanumeric, spaces, quotes, and operators
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsSpace(r) {
			return r
		}
		if r == '"' || r == '\'' || r == '-' || r == '_' {
			return r
		}
		// Remove other special chars
		return -1
	}, text)
}

// GetFirstToken returns the first non-operator token from the query
// Useful for fallback searches
func (sq *SearchQuery) GetFirstToken() string {
	if len(sq.Tokens) > 0 {
		return sq.Tokens[0]
	}
	if len(sq.PhraseQueries) > 0 {
		return sq.PhraseQueries[0]
	}
	return ""
}

// GetAllTokensJoined returns all tokens joined with spaces
// Useful for FTS queries
func (sq *SearchQuery) GetAllTokensJoined() string {
	if len(sq.Tokens) > 0 {
		return strings.Join(sq.Tokens, " ")
	}
	return sq.Normalized
}

// IsExactPhraseSearch returns true if query contains quoted phrases
func (sq *SearchQuery) IsExactPhraseSearch() bool {
	return len(sq.PhraseQueries) > 0
}

// GetExactPhrase returns the first exact phrase if available
func (sq *SearchQuery) GetExactPhrase() string {
	if len(sq.PhraseQueries) > 0 {
		return sq.PhraseQueries[0]
	}
	return ""
}

// TruncateForDB truncates query to safe database limits
func (sq *SearchQuery) TruncateForDB(maxLen int) string {
	if len(sq.Normalized) <= maxLen {
		return sq.Normalized
	}
	return sq.Normalized[:maxLen]
}

// ContainsOperator checks if query has boolean operators
func (sq *SearchQuery) ContainsOperator(op string) bool {
	return sq.Operators[strings.ToUpper(op)]
}

// TokenCount returns the number of meaningful tokens
func (sq *SearchQuery) TokenCount() int {
	return len(sq.Tokens)
}

// IsLikelyTypo checks if a token might be a typo (too many consonants, etc.)
// This is a simple heuristic check
func IsLikelyTypo(token string) bool {
	if len(token) < 3 {
		return false
	}

	normalized := strings.ToLower(strings.TrimSpace(token))
	if normalized == "" {
		return false
	}

	// Count vowels and consonants
	vowels := 0
	consonants := 0
	hasY := false
	for _, r := range normalized {
		switch {
		case isCoreVowel(r):
			vowels++
		case r == 'y':
			hasY = true
		case unicode.IsLetter(r):
			consonants++
		}
	}

	if vowels == 0 {
		return consonants >= 5 && !hasY
	}

	// If overwhelmingly consonant-heavy, likely a typo.
	if consonants >= 5 && consonants > vowels*3 {
		return true
	}

	return false
}

func isCoreVowel(r rune) bool {
	return strings.ContainsRune("aeiou", r)
}
