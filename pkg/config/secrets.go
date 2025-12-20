package config

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// SecretRef identifies a secret value from one of several sources.
// Exactly one of AwsSecretArn, InsecureValue, or EnvVar must be set.
type SecretRef struct {
	// AwsSecretArn is the ARN of an AWS Secrets Manager secret.
	// Key must also be set to extract a specific field from the JSON secret.
	AwsSecretArn string `json:"aws_secret_arn,omitempty"`
	Key          string `json:"key,omitempty"`

	// InsecureValue is a plaintext secret value. Use only for development.
	InsecureValue string `json:"insecure_value,omitempty"`

	// EnvVar is the name of an environment variable containing the secret.
	EnvVar string `json:"env_var,omitempty"`
}

// Validate checks that exactly one secret source is configured.
func (r SecretRef) Validate() error {
	sources := 0
	if r.AwsSecretArn != "" {
		sources++
	}
	if r.InsecureValue != "" {
		sources++
	}
	if r.EnvVar != "" {
		sources++
	}

	if sources == 0 {
		return errors.New("secret ref must have one of: aws_secret_arn, insecure_value, or env_var")
	}
	if sources > 1 {
		return errors.New("secret ref must have only one of: aws_secret_arn, insecure_value, or env_var")
	}

	if r.AwsSecretArn != "" && r.Key == "" {
		return errors.New("aws_secret_arn requires key to be set")
	}

	return nil
}

// SecretsManagerClient is the interface for AWS Secrets Manager operations.
// This allows injecting a mock for testing.
type SecretsManagerClient interface {
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

// SecretCache caches secrets fetched from AWS Secrets Manager.
type SecretCache struct {
	mu     sync.RWMutex
	cache  map[string]map[string]any
	client SecretsManagerClient
}

// NewSecretCache creates a new SecretCache with the given Secrets Manager client.
func NewSecretCache(client SecretsManagerClient) *SecretCache {
	return &SecretCache{
		cache:  make(map[string]map[string]any),
		client: client,
	}
}

// NewSecretCacheFromEnv creates a new SecretCache using AWS config from the environment.
func NewSecretCacheFromEnv(ctx context.Context) (*SecretCache, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	client := secretsmanager.NewFromConfig(cfg)
	return NewSecretCache(client), nil
}

// Get retrieves the value for the given SecretRef.
// It handles aws_secret_arn, insecure_value, and env_var sources.
// Returns an error if the secret ref is invalid or the value cannot be retrieved.
func (sc *SecretCache) Get(ctx context.Context, ref SecretRef) (string, error) {
	if err := ref.Validate(); err != nil {
		return "", err
	}

	// Handle insecure_value
	if ref.InsecureValue != "" {
		return ref.InsecureValue, nil
	}

	// Handle env_var
	if ref.EnvVar != "" {
		val, ok := os.LookupEnv(ref.EnvVar)
		if !ok {
			return "", fmt.Errorf("environment variable %q not set", ref.EnvVar)
		}
		return val, nil
	}

	// Handle aws_secret_arn
	if secretData, ok := sc.getCached(ref.AwsSecretArn); ok {
		return extractStringKey(secretData, ref.Key)
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	// Double-check after acquiring write lock
	if secretData, ok := sc.cache[ref.AwsSecretArn]; ok {
		return extractStringKey(secretData, ref.Key)
	}

	// Fetch from AWS
	secretData, err := sc.fetchSecret(ctx, ref.AwsSecretArn)
	if err != nil {
		return "", err
	}

	sc.cache[ref.AwsSecretArn] = secretData
	return extractStringKey(secretData, ref.Key)
}

func (sc *SecretCache) getCached(arn string) (map[string]any, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	data, ok := sc.cache[arn]
	return data, ok
}

func (sc *SecretCache) fetchSecret(ctx context.Context, arn string) (map[string]any, error) {
	output, err := sc.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &arn,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get secret %s: %w", arn, err)
	}

	if output.SecretString == nil {
		return nil, fmt.Errorf("secret %s has no string value", arn)
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(*output.SecretString), &data); err != nil {
		return nil, fmt.Errorf("failed to parse secret %s as JSON: %w", arn, err)
	}

	return data, nil
}

func extractStringKey(data map[string]any, key string) (string, error) {
	val, ok := data[key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret", key)
	}

	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("value at key %q is not a string (got %T)", key, val)
	}

	return str, nil
}
