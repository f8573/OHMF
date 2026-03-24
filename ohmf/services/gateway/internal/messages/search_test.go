package messages_test

import (
	"testing"

	"ohmf/services/gateway/internal/messages"
)

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name              string
		query             string
		expectedTokens    []string
		expectedEmpty     bool
		expectedAllStops  bool
		expectedPhrases   int
		expectedOperators int
	}{
		{
			name:           "Basic text query",
			query:          "hello world",
			expectedTokens: []string{"hello", "world"},
		},
		{
			name:           "Query with extra spaces",
			query:          "  hello    world  ",
			expectedTokens: []string{"hello", "world"},
		},
		{
			name:          "Empty query",
			query:         "",
			expectedEmpty: true,
		},
		{
			name:          "Only whitespace",
			query:         "   ",
			expectedEmpty: true,
		},
		{
			name:           "Mixed case",
			query:          "HeLLo WoRLd",
			expectedTokens: []string{"hello", "world"},
		},
		{
			name:              "With boolean operators",
			query:             "hello AND world",
			expectedTokens:    []string{"hello", "world"},
			expectedOperators: 1,
		},
		{
			name:             "All stopwords",
			query:            "the and or",
			expectedAllStops: true,
		},
		{
			name:            "Quoted phrase",
			query:           `"exact phrase" search`,
			expectedPhrases: 1,
			expectedTokens:  []string{"search"},
		},
		{
			name:            "Multiple phrases",
			query:           `"first phrase" and "second phrase"`,
			expectedPhrases: 2,
		},
		{
			name:              "NOT operator",
			query:             "hello NOT world",
			expectedTokens:    []string{"hello", "world"},
			expectedOperators: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sq := messages.NormalizeQuery(test.query)

			if sq.IsEmpty != test.expectedEmpty {
				t.Errorf("IsEmpty: got %v, want %v", sq.IsEmpty, test.expectedEmpty)
			}

			if sq.IsAllStopwords != test.expectedAllStops {
				t.Errorf("IsAllStopwords: got %v, want %v", sq.IsAllStopwords, test.expectedAllStops)
			}

			if len(sq.Tokens) != len(test.expectedTokens) {
				t.Errorf("Token count: got %d, want %d", len(sq.Tokens), len(test.expectedTokens))
			} else {
				for i, expectedToken := range test.expectedTokens {
					if sq.Tokens[i] != expectedToken {
						t.Errorf("Token %d: got %q, want %q", i, sq.Tokens[i], expectedToken)
					}
				}
			}

			if len(sq.PhraseQueries) != test.expectedPhrases {
				t.Errorf("Phrase count: got %d, want %d", len(sq.PhraseQueries), test.expectedPhrases)
			}

			operatorCount := len(sq.Operators)
			if operatorCount != test.expectedOperators {
				t.Errorf("Operator count: got %d, want %d", operatorCount, test.expectedOperators)
			}
		})
	}
}

func TestValidateSearchQuality(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		expectedValid   bool
		expectedWarning bool
	}{
		{
			name:          "Valid query",
			query:         "hello world",
			expectedValid: true,
		},
		{
			name:          "Single character query",
			query:         "a",
			expectedValid: false,
		},
		{
			name:          "Empty query",
			query:         "",
			expectedValid: false,
		},
		{
			name:          "Only stopwords",
			query:         "the and or",
			expectedValid: false,
		},
		{
			name:          "Very long query",
			query:         "a" + string(make([]byte, 600)),
			expectedValid: false,
		},
		{
			name:          "Single character valid",
			query:         `"a"`,
			expectedValid: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sq := messages.NormalizeQuery(test.query)
			isValid, reason := messages.ValidateSearchQuality(sq)

			if isValid != test.expectedValid {
				t.Errorf("Valid: got %v, want %v (reason: %q)", isValid, test.expectedValid, reason)
			}
		})
	}
}

func TestGetFirstToken(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		expectedToken string
	}{
		{
			name:          "Single token",
			query:         "hello",
			expectedToken: "hello",
		},
		{
			name:          "Multiple tokens",
			query:         "hello world",
			expectedToken: "hello",
		},
		{
			name:          "Only phrase",
			query:         `"exact phrase"`,
			expectedToken: "exact phrase",
		},
		{
			name:          "Empty query",
			query:         "",
			expectedToken: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sq := messages.NormalizeQuery(test.query)
			token := sq.GetFirstToken()

			if token != test.expectedToken {
				t.Errorf("GetFirstToken: got %q, want %q", token, test.expectedToken)
			}
		})
	}
}

func TestIsExactPhraseSearch(t *testing.T) {
	tests := []struct {
		name             string
		query            string
		expectedIsPhrase bool
	}{
		{
			name:             "No phrase",
			query:            "hello world",
			expectedIsPhrase: false,
		},
		{
			name:             "With phrase",
			query:            `"exact phrase"`,
			expectedIsPhrase: true,
		},
		{
			name:             "Multiple phrases",
			query:            `"first" and "second"`,
			expectedIsPhrase: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sq := messages.NormalizeQuery(test.query)
			result := sq.IsExactPhraseSearch()

			if result != test.expectedIsPhrase {
				t.Errorf("IsExactPhraseSearch: got %v, want %v", result, test.expectedIsPhrase)
			}
		})
	}
}

func TestTruncateForDB(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		maxLen      int
		expectedLen int
	}{
		{
			name:        "No truncation needed",
			query:       "hello world",
			maxLen:      100,
			expectedLen: 11,
		},
		{
			name:        "Truncation needed",
			query:       "hello world test",
			maxLen:      5,
			expectedLen: 5,
		},
		{
			name:        "Exact length",
			query:       "hello",
			maxLen:      5,
			expectedLen: 5,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sq := messages.NormalizeQuery(test.query)
			result := sq.TruncateForDB(test.maxLen)

			if len(result) != test.expectedLen {
				t.Errorf("TruncateForDB: got length %d, want %d", len(result), test.expectedLen)
			}
		})
	}
}

func TestIsLikelyTypo(t *testing.T) {
	tests := []struct {
		name         string
		token        string
		expectedTypo bool
	}{
		{
			name:         "Common word",
			token:        "hello",
			expectedTypo: false,
		},
		{
			name:         "Consonant heavy",
			token:        "bbbccc",
			expectedTypo: true,
		},
		{
			name:         "Short token",
			token:        "ab",
			expectedTypo: false,
		},
		{
			name:         "With vowels",
			token:        "bcry",
			expectedTypo: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := messages.IsLikelyTypo(test.token)

			if result != test.expectedTypo {
				t.Errorf("IsLikelyTypo: got %v, want %v", result, test.expectedTypo)
			}
		})
	}
}

func BenchmarkNormalizeQuery(b *testing.B) {
	query := "hello world this is a test query"
	for i := 0; i < b.N; i++ {
		messages.NormalizeQuery(query)
	}
}

func BenchmarkValidateSearchQuality(b *testing.B) {
	sq := messages.NormalizeQuery("hello world test")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages.ValidateSearchQuality(sq)
	}
}
