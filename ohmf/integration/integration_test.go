package integration

import (
    "bytes"
    "encoding/json"
    "fmt"
    "net/http"
    "os"
    "testing"
    "time"
)

func mustEnv(t *testing.T, k string) string {
    v := os.Getenv(k)
    if v == "" {
        t.Skipf("skipping integration test; env %s not set", k)
    }
    return v
}

func TestGatewayIntegration(t *testing.T) {
    if os.Getenv("INTEGRATION") == "" {
        t.Skip("INTEGRATION not set")
    }

    // Services are expected to be reachable on localhost; allow overrides via env
    gateway := os.Getenv("GATEWAY_ADDR")
    if gateway == "" {
		gateway = "http://localhost:18081"
    }
    contacts := os.Getenv("CONTACTS_ADDR")
    if contacts == "" {
        contacts = "http://localhost:18085"
    }

    client := &http.Client{Timeout: 10 * time.Second}

    // 1) Seed contacts service directly
    seed := map[string]interface{}{
        "pepper": "pepper-01",
        "contacts": []map[string]string{{
            "identifier": "alice@example.com",
            "user_id":    "user-alice",
            "display_name": "Alice",
        }},
    }
    b, _ := json.Marshal(seed)
    req, _ := http.NewRequest("POST", contacts+"/internal/seed", bytes.NewReader(b))
    req.Header.Set("X-Admin-Token", "test")
    resp, err := client.Do(req)
    if err != nil || resp.StatusCode >= 300 {
        t.Fatalf("seed failed: %v status=%v", err, resp.Status)
    }

    // 2) Discover via gateway
    discReq := map[string][]string{"identifiers": {"alice@example.com"}}
    db, _ := json.Marshal(discReq)
    resp, err = client.Post(gateway+"/v1/contacts/discover", "application/json", bytes.NewReader(db))
    if err != nil {
        t.Fatalf("gateway discover request failed: %v", err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        t.Fatalf("discover status: %v", resp.Status)
    }

    // 3) Register an app via gateway
    app := map[string]interface{}{
        "manifest": map[string]interface{}{
            "manifest_version": "1.0",
            "app_id": fmt.Sprintf("com.example.test.%d", time.Now().UnixNano()),
            "name": "Integration Test App",
            "version": "1.0.0",
            "entrypoint": map[string]any{
                "type": "url",
                "url":  "https://example.com/app",
            },
            "message_preview": map[string]any{
                "type": "static_image",
                "url":  "https://example.com/preview.png",
            },
            "permissions": []string{"conversation.read_context"},
            "capabilities": map[string]any{
                "turn_based": true,
            },
            "signature": map[string]any{
                "alg": "RS256",
                "kid": "integration",
                "sig": "placeholder",
            },
        },
    }
    ab, _ := json.Marshal(app)
    resp, err = client.Post(gateway+"/v1/apps/register", "application/json", bytes.NewReader(ab))
    if err != nil {
        t.Fatalf("apps register failed: %v", err)
    }
    if resp.StatusCode != http.StatusCreated {
        t.Fatalf("apps register status: %v", resp.Status)
    }

    // 4) Media upload init + complete via gateway
    resp, err = client.Post(gateway+"/v1/media/uploads", "application/json", nil)
    if err != nil {
        t.Fatalf("media create failed: %v", err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusCreated {
        t.Fatalf("media create status: %v", resp.Status)
    }
    var up struct{ UploadID string `json:"upload_id"` }
    if err := json.NewDecoder(resp.Body).Decode(&up); err != nil {
        t.Fatalf("decode upload response: %v", err)
    }

    // complete
    resp2, err := client.Post(gateway+"/v1/media/uploads/"+up.UploadID+"/complete", "application/json", nil)
    if err != nil {
        t.Fatalf("media complete failed: %v", err)
    }
    if resp2.StatusCode != http.StatusOK {
        t.Fatalf("media complete status: %v", resp2.Status)
    }
}

