package config

import (
	"context"
	"encoding/json/v2"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
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

// MockSecretsManagerClient is a mock implementation of SecretsManagerClient for testing.
type MockSecretsManagerClient struct {
	// Secrets maps ARN to JSON secret string
	Secrets map[string]string
	// Errors maps ARN to error to return
	Errors map[string]error
	// Calls records all GetSecretValue calls for verification
	Calls []string
}

func NewMockSecretsManagerClient() *MockSecretsManagerClient {
	return &MockSecretsManagerClient{
		Secrets: make(map[string]string),
		Errors:  make(map[string]error),
	}
}

func (m *MockSecretsManagerClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	arn := *params.SecretId
	m.Calls = append(m.Calls, arn)

	if err, ok := m.Errors[arn]; ok {
		return nil, err
	}

	secret, ok := m.Secrets[arn]
	if !ok {
		return nil, errors.New("secret not found")
	}

	return &secretsmanager.GetSecretValueOutput{
		SecretString: &secret,
	}, nil
}

func TestSecretRef_Validate(t *testing.T) {
	tests := []struct {
		name    string
		ref     SecretRef
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty ref",
			ref:     SecretRef{},
			wantErr: true,
			errMsg:  "must have one of",
		},
		{
			name: "aws_secret_arn without key",
			ref: SecretRef{
				AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:test",
			},
			wantErr: true,
			errMsg:  "requires key",
		},
		{
			name: "aws_secret_arn with key",
			ref: SecretRef{
				AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:test",
				Key:          "password",
			},
			wantErr: false,
		},
		{
			name: "insecure_value",
			ref: SecretRef{
				InsecureValue: "secret123",
			},
			wantErr: false,
		},
		{
			name: "env_var",
			ref: SecretRef{
				EnvVar: "MY_SECRET",
			},
			wantErr: false,
		},
		{
			name: "multiple sources",
			ref: SecretRef{
				InsecureValue: "secret123",
				EnvVar:        "MY_SECRET",
			},
			wantErr: true,
			errMsg:  "only one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ref.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.errMsg != "" && err != nil {
				if !contains(err.Error(), tt.errMsg) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errMsg)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestSecretCache_GetAWSSecret(t *testing.T) {
	ctx := context.Background()

	mock := NewMockSecretsManagerClient()
	mock.Secrets["arn:aws:secretsmanager:us-east-1:123:secret:db-creds"] = `{"username":"dbuser","password":"dbpass123"}`

	cache := NewSecretCache(mock)

	tests := []struct {
		name    string
		ref     SecretRef
		want    string
		wantErr bool
		errMsg  string
	}{
		{
			name: "get username",
			ref: SecretRef{
				AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:db-creds",
				Key:          "username",
			},
			want: "dbuser",
		},
		{
			name: "get password",
			ref: SecretRef{
				AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:db-creds",
				Key:          "password",
			},
			want: "dbpass123",
		},
		{
			name: "missing key",
			ref: SecretRef{
				AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:db-creds",
				Key:          "nonexistent",
			},
			wantErr: true,
			errMsg:  "not found in secret",
		},
		{
			name: "missing secret",
			ref: SecretRef{
				AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:missing",
				Key:          "password",
			},
			wantErr: true,
			errMsg:  "secret not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cache.Get(ctx, tt.ref)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if tt.errMsg != "" && !contains(err.Error(), tt.errMsg) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.errMsg)
				}
				return
			}
			if got != tt.want {
				t.Errorf("Get() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSecretCache_Caching(t *testing.T) {
	ctx := context.Background()

	mock := NewMockSecretsManagerClient()
	mock.Secrets["arn:aws:secretsmanager:us-east-1:123:secret:db-creds"] = `{"username":"dbuser","password":"dbpass123"}`

	cache := NewSecretCache(mock)

	ref := SecretRef{
		AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:db-creds",
		Key:          "username",
	}

	// First call should hit AWS
	val1, err := cache.Get(ctx, ref)
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if val1 != "dbuser" {
		t.Errorf("first Get() = %q, want %q", val1, "dbuser")
	}

	// Second call should use cache
	val2, err := cache.Get(ctx, ref)
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	if val2 != "dbuser" {
		t.Errorf("second Get() = %q, want %q", val2, "dbuser")
	}

	// Should only have made one AWS call
	if len(mock.Calls) != 1 {
		t.Errorf("expected 1 AWS call, got %d", len(mock.Calls))
	}

	// Getting a different key from same secret should also use cache
	ref2 := SecretRef{
		AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:db-creds",
		Key:          "password",
	}
	val3, err := cache.Get(ctx, ref2)
	if err != nil {
		t.Fatalf("third Get() error = %v", err)
	}
	if val3 != "dbpass123" {
		t.Errorf("third Get() = %q, want %q", val3, "dbpass123")
	}

	// Still should only have one AWS call
	if len(mock.Calls) != 1 {
		t.Errorf("expected 1 AWS call after cache hit, got %d", len(mock.Calls))
	}
}

func TestSecretCache_AWSError(t *testing.T) {
	ctx := context.Background()

	mock := NewMockSecretsManagerClient()
	mock.Errors["arn:aws:secretsmanager:us-east-1:123:secret:forbidden"] = errors.New("AccessDeniedException: access denied")

	cache := NewSecretCache(mock)

	ref := SecretRef{
		AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:forbidden",
		Key:          "password",
	}

	_, err := cache.Get(ctx, ref)
	if err == nil {
		t.Fatal("expected error")
	}
	if !contains(err.Error(), "AccessDeniedException") {
		t.Errorf("error %q does not contain AccessDeniedException", err.Error())
	}
}

func TestSecretCache_InvalidJSON(t *testing.T) {
	ctx := context.Background()

	mock := NewMockSecretsManagerClient()
	mock.Secrets["arn:aws:secretsmanager:us-east-1:123:secret:invalid"] = `not valid json`

	cache := NewSecretCache(mock)

	ref := SecretRef{
		AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:invalid",
		Key:          "password",
	}

	_, err := cache.Get(ctx, ref)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !contains(err.Error(), "parse") {
		t.Errorf("error %q does not mention parsing", err.Error())
	}
}

func TestSecretCache_NonStringValue(t *testing.T) {
	ctx := context.Background()

	mock := NewMockSecretsManagerClient()
	mock.Secrets["arn:aws:secretsmanager:us-east-1:123:secret:typed"] = `{"count":42,"nested":{"foo":"bar"}}`

	cache := NewSecretCache(mock)

	ref := SecretRef{
		AwsSecretArn: "arn:aws:secretsmanager:us-east-1:123:secret:typed",
		Key:          "count",
	}

	_, err := cache.Get(ctx, ref)
	if err == nil {
		t.Fatal("expected error for non-string value")
	}
	if !contains(err.Error(), "not a string") {
		t.Errorf("error %q does not mention 'not a string'", err.Error())
	}
}
