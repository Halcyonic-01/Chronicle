package rca

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// OpenAICompatibleNarrator supports free-tier providers exposing Chat Completions.
// Configure RCA_API_KEY, optionally RCA_API_URL and RCA_MODEL.
type OpenAICompatibleNarrator struct {
	APIKey, URL, Model string
	Client             *http.Client
}

func NewOpenAICompatibleNarratorFromEnv() *OpenAICompatibleNarrator {
	return &OpenAICompatibleNarrator{APIKey: os.Getenv("RCA_API_KEY"), URL: valueOr(strings.TrimRight(os.Getenv("RCA_API_URL"), "/"), "https://api.groq.com/openai/v1/chat/completions"), Model: valueOr(os.Getenv("RCA_MODEL"), "llama-3.1-8b-instant"), Client: &http.Client{Timeout: 20 * time.Second}}
}
func valueOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
func (n *OpenAICompatibleNarrator) Narrate(ctx context.Context, result *Result) (string, error) {
	if n == nil || n.APIKey == "" {
		return "", fmt.Errorf("RCA_API_KEY is not configured")
	}
	type message struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	prompt := fmt.Sprintf("Explain this completed root-cause analysis in 3-5 sentences. Do not add causes. If confidence is below 0.5, explicitly say it is inconclusive.\n%s", mustJSON(result))
	body, err := json.Marshal(map[string]any{"model": n.Model, "temperature": 0.1, "messages": []message{{"system", "You are an SRE assistant. Explain only the supplied ranked evidence."}, {"user", prompt}}})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+n.APIKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("narrator returned HTTP %s", resp.Status)
	}
	var out struct {
		Choices []struct {
			Message message `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if len(out.Choices) == 0 || strings.TrimSpace(out.Choices[0].Message.Content) == "" {
		return "", fmt.Errorf("narrator returned no content")
	}
	return strings.TrimSpace(out.Choices[0].Message.Content), nil
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
