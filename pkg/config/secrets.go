package config

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// SecretRef identifies a specific key within an AWS Secrets Manager secret.
type SecretRef struct {
	AwsSecretArn string `json:"aws_secret_arn"`
	Key          string `json:"key"`
}

// SecretCache caches secrets fetched from AWS Secrets Manager.
type SecretCache struct {
	mu     sync.RWMutex
	cache  map[string]map[string]any
	client *secretsmanager.Client
}

// NewSecretCache creates a new SecretCache with the given Secrets Manager client.
func NewSecretCache(client *secretsmanager.Client) *SecretCache {
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

// Get retrieves the value for the given SecretRef. If the secret ARN is already
// cached, it returns the value at the specified key. Otherwise, it fetches the
// secret from AWS Secrets Manager, caches it, and returns the key value.
// Returns an error if the value at the given key is not a string.
func (sc *SecretCache) Get(ctx context.Context, ref SecretRef) (string, error) {
	sc.mu.RLock()
	if secretData, ok := sc.cache[ref.AwsSecretArn]; ok {
		sc.mu.RUnlock()
		return extractStringKey(secretData, ref.Key)
	}
	sc.mu.RUnlock()

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
