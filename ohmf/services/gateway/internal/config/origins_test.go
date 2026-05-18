package config

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestGenerateAppOrigin_Determinism(t *testing.T) {
	// Same inputs should always produce same output
	origin1 := GenerateAppOrigin("app.ohmf.counter", "v1.0.0", "miniapp.local", 8)
	origin2 := GenerateAppOrigin("app.ohmf.counter", "v1.0.0", "miniapp.local", 8)
	if origin1 != origin2 {
		t.Errorf("Determinism failed: %s != %s", origin1, origin2)
	}
}

func TestGenerateAppOrigin_Uniqueness(t *testing.T) {
	tests := []struct {
		appID     string
		releaseID string
		wantUnique bool
	}{
		{"app.ohmf.counter", "v1.0.0", false},
		{"app.ohmf.eightball", "v1.0.0", false},
		{"app.ohmf.counter", "v2.0.0", false},
		{"different.app", "v1.0.0", false},
	}

	origins := make(map[string]struct{})
	for _, tt := range tests {
		origin := GenerateAppOrigin(tt.appID, tt.releaseID, "miniapp.local", 8)
		if _, exists := origins[origin]; exists {
			t.Errorf("Collision detected: %s and another combination produced %s", tt.appID, origin)
		}
		origins[origin] = struct{}{}
	}

	// Should have 4 unique origins
	if len(origins) != 4 {
		t.Errorf("Expected 4 unique origins, got %d", len(origins))
	}
}

func TestGenerateAppOrigin_Format(t *testing.T) {
	tests := []struct {
		name         string
		appID        string
		releaseID    string
		baseDomain   string
		subdomainLen int
		wantPattern  string
	}{
		{
			name:         "standard_format",
			appID:        "app.ohmf.counter",
			releaseID:    "v1.0.0",
			baseDomain:   "miniapp.local",
			subdomainLen: 8,
			wantPattern:  `^[a-f0-9]{8}\.miniapp\.local$`,
		},
		{
			name:         "custom_subdomain_len",
			appID:        "app.ohmf.counter",
			releaseID:    "v1.0.0",
			baseDomain:   "app.example.com",
			subdomainLen: 16,
			wantPattern:  `^[a-f0-9]{16}\.app\.example\.com$`,
		},
		{
			name:         "default_subdomain_len",
			appID:        "test.app",
			releaseID:    "v1",
			baseDomain:   "sandbox.io",
			subdomainLen: 0, // Should default to 8
			wantPattern:  `^[a-f0-9]{8}\.sandbox\.io$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			origin := GenerateAppOrigin(tt.appID, tt.releaseID, tt.baseDomain, tt.subdomainLen)
			matched, _ := regexp.MatchString(tt.wantPattern, origin)
			if !matched {
				t.Errorf("Origin %s does not match pattern %s", origin, tt.wantPattern)
			}
		})
	}
}

func TestGenerateAppOrigin_NormalizesInput(t *testing.T) {
	// Inputs with spaces should be normalized
	origin1 := GenerateAppOrigin("app.ohmf.counter", "v1.0.0", "miniapp.local", 8)
	origin2 := GenerateAppOrigin("  app.ohmf.counter  ", "  v1.0.0  ", "miniapp.local", 8)
	if origin1 != origin2 {
		t.Errorf("Normalization failed: %s != %s", origin1, origin2)
	}
}

func TestGenerateOriginConfig_Complete(t *testing.T) {
	params := OriginGenerationParams{
		AppID:       "app.ohmf.counter",
		ReleaseID:   "v1.0.0",
		BaseDomain:  "miniapp.local",
		SubdomainLen: 8,
	}

	config := GenerateOriginConfig(params)

	// Verify origin format
	if !regexp.MustCompile(`^[a-f0-9]{8}\.miniapp\.local$`).MatchString(config.AppOrigin) {
		t.Errorf("Invalid origin format: %s", config.AppOrigin)
	}

	// Verify CSP header is present
	if config.CSPHeader == "" {
		t.Error("CSP header is empty")
	}

	// Verify CSP contains required directives
	requiredDirectives := []string{
		"default-src 'none'",
		"script-src 'self'",
		"style-src 'self'",
		"connect-src 'self'",
		"frame-src 'none'",
		"object-src 'none'",
	}
	for _, directive := range requiredDirectives {
		if !strings.Contains(config.CSPHeader, directive) {
			t.Errorf("CSP header missing directive: %s", directive)
		}
	}

	// Verify CORS origins include self
	found := false
	for _, origin := range config.AllowedCORSOrigins {
		if origin == config.AppOrigin {
			found = true
			break
		}
	}
	if !found {
		t.Error("CORS origins does not include self")
	}
}

func TestGenerateCSPHeader_StrictPolicy(t *testing.T) {
	csp := generateCSPHeader("test.miniapp.local")

	// Verify CSP is strict (no unsafe-eval, limited inline)
	if strings.Contains(csp, "unsafe-eval") {
		t.Error("CSP contains unsafe-eval (should be forbidden)")
	}

	// Verify it allows only self or specific values
	if strings.Contains(csp, "'unsafe-script'") {
		t.Error("CSP contains unsafe-script (should be forbidden)")
	}

	// Verify frame-ancestors is set to self
	if !strings.Contains(csp, "frame-ancestors 'self'") {
		t.Error("CSP missing or incorrect frame-ancestors directive")
	}
}

func TestValidateOrigin_AcceptsValid(t *testing.T) {
	tests := []struct {
		origin     string
		baseDomain string
		wantValid  bool
	}{
		{"a7f3e1c5.miniapp.local", "miniapp.local", true},
		{"0123456789abcdef.miniapp.local", "miniapp.local", true},
		{"deadbeef.example.com", "example.com", true},
		{"ffffffff.sandbox.io", "sandbox.io", true},
	}

	for _, tt := range tests {
		got := ValidateOrigin(tt.origin, tt.baseDomain)
		if got != tt.wantValid {
			t.Errorf("ValidateOrigin(%s, %s) = %v, want %v", tt.origin, tt.baseDomain, got, tt.wantValid)
		}
	}
}

func TestValidateOrigin_RejectsInvalid(t *testing.T) {
	tests := []struct {
		origin     string
		baseDomain string
		wantValid  bool
		reason     string
	}{
		{"invalid", "miniapp.local", false, "no subdomain"},
		{"g1234567.miniapp.local", "miniapp.local", false, "invalid hex char (g)"},
		{"short.miniapp.local", "miniapp.local", false, "too short (4 chars)"},
		{"a7f3e1c5.wrong.domain", "miniapp.local", false, "wrong base domain"},
		{"example.com", "miniapp.local", false, "missing subdomain"},
		{"..miniapp.local", "miniapp.local", false, "empty subdomain"},
	}

	for _, tt := range tests {
		got := ValidateOrigin(tt.origin, tt.baseDomain)
		if got != tt.wantValid {
			t.Errorf("ValidateOrigin(%s, %s) = %v, want %v (%s)", tt.origin, tt.baseDomain, got, tt.wantValid, tt.reason)
		}
	}
}

func TestValidateOrigin_DefaultBaseDomain(t *testing.T) {
	// When baseDomain is empty, should default to "miniapp.local"
	got := ValidateOrigin("a7f3e1c5.miniapp.local", "")
	if !got {
		t.Error("ValidateOrigin should default to miniapp.local when baseDomain is empty")
	}
}

func TestIsSameOriginRequestValid(t *testing.T) {
	tests := []struct {
		requestOrigin    string
		expectedOrigin   string
		wantValid        bool
	}{
		{"a7f3e1c5.miniapp.local", "a7f3e1c5.miniapp.local", true},
		{"a7f3e1c5.miniapp.local", "b8g4f2d6.miniapp.local", false},
		{"http://a7f3e1c5.miniapp.local", "a7f3e1c5.miniapp.local", false},
		{"", "", true},
	}

	for _, tt := range tests {
		got := IsSameOriginRequestValid(tt.requestOrigin, tt.expectedOrigin)
		if got != tt.wantValid {
			t.Errorf("IsSameOriginRequestValid(%s, %s) = %v, want %v", tt.requestOrigin, tt.expectedOrigin, got, tt.wantValid)
		}
	}
}

func TestGetSameOriginCheckValue(t *testing.T) {
	checkValue := GetSameOriginCheckValue("a7f3e1c5.miniapp.local")
	if checkValue != "a7f3e1c5.miniapp.local" {
		t.Errorf("GetSameOriginCheckValue returned %s, want a7f3e1c5.miniapp.local", checkValue)
	}

	// Verify it's non-empty
	if checkValue == "" {
		t.Error("GetSameOriginCheckValue returned empty string")
	}
}

func TestGenerateAppOrigin_CollisionResistance(t *testing.T) {
	// Generate 100 origins with different app/release IDs
	origins := make(map[string]int)
	for i := 0; i < 100; i++ {
		appID := fmt.Sprintf("app.ohmf.test-%d", i)
		releaseID := fmt.Sprintf("v%d", i)
		origin := GenerateAppOrigin(appID, releaseID, "miniapp.local", 8)
		origins[origin]++
	}

	// All should be unique (no collisions)
	for origin, count := range origins {
		if count > 1 {
			t.Errorf("Collision detected: origin %s appeared %d times", origin, count)
		}
	}

	// Should have exactly 100 unique origins
	if len(origins) != 100 {
		t.Errorf("Expected 100 unique origins, got %d", len(origins))
	}
}

func TestGenerateOriginConfig_MultipleParams(t *testing.T) {
	// Generate configs for multiple apps and verify they have different origins
	configs := make([]*OriginConfig, 0, 5)
	appIDs := []string{"counter", "eightball", "todo", "notes", "chat"}

	for _, appID := range appIDs {
		params := OriginGenerationParams{
			AppID:        "app.ohmf." + appID,
			ReleaseID:    "v1.0.0",
			BaseDomain:   "miniapp.local",
			SubdomainLen: 8,
		}
		configs = append(configs, GenerateOriginConfig(params))
	}

	// Verify all origins are unique
	origins := make(map[string]struct{})
	for _, cfg := range configs {
		if _, exists := origins[cfg.AppOrigin]; exists {
			t.Errorf("Duplicate origin found: %s", cfg.AppOrigin)
		}
		origins[cfg.AppOrigin] = struct{}{}
	}

	if len(origins) != len(appIDs) {
		t.Errorf("Expected %d unique origins, got %d", len(appIDs), len(origins))
	}
}

func TestCSPHeader_NoWildcards(t *testing.T) {
	csp := generateCSPHeader("test.miniapp.local")

	// Verify no wildcard * is used (except in special cases like img-src data:)
	if strings.Contains(csp, "'*'") || strings.Contains(csp, " * ") {
		t.Error("CSP contains wildcard (should be specific)")
	}
}

func TestCSPHeader_NoExternalResources(t *testing.T) {
	csp := generateCSPHeader("test.miniapp.local")

	// For script-src and style-src, should only allow 'self', no external CDNs
	if strings.Contains(csp, "https://") || strings.Contains(csp, "http://") {
		t.Error("CSP contains external resources (should only allow self)")
	}
}

func TestOriginConfig_ParamDefaults(t *testing.T) {
	params := OriginGenerationParams{
		AppID:     "test.app",
		ReleaseID: "v1",
		// BaseDomain: omitted, should default
		// SubdomainLen: omitted, should default
	}

	config := GenerateOriginConfig(params)

	// Should have valid origin despite missing params
	if config.AppOrigin == "" {
		t.Error("Origin is empty with default params")
	}

	// Should end with default base domain
	if !strings.HasSuffix(config.AppOrigin, ".miniapp.local") {
		t.Errorf("Origin does not use default base domain: %s", config.AppOrigin)
	}
}

func BenchmarkGenerateAppOrigin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		GenerateAppOrigin("app.ohmf.counter", "v1.0.0", "miniapp.local", 8)
	}
}

func BenchmarkGenerateOriginConfig(b *testing.B) {
	params := OriginGenerationParams{
		AppID:        "app.ohmf.counter",
		ReleaseID:    "v1.0.0",
		BaseDomain:   "miniapp.local",
		SubdomainLen: 8,
	}
	for i := 0; i < b.N; i++ {
		GenerateOriginConfig(params)
	}
}

func BenchmarkValidateOrigin(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ValidateOrigin("a7f3e1c5.miniapp.local", "miniapp.local")
	}
}
