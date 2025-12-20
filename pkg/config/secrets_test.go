package config

import (
	"encoding/json/v2"
	"testing"
)

func TestSecretRef_RoundTrip(t *testing.T) {
	input := `{"aws_secret_arn":"arn:aws:secretsmanager:us-east-1:123456789:secret:my-secret","key":"password"}`

	var s SecretRef
	if err := json.Unmarshal([]byte(input), &s); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	got, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	if string(got) != input {
		t.Errorf("round-trip mismatch:\n  input:  %s\n  output: %s", input, string(got))
	}
}
