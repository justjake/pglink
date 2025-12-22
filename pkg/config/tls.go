package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"time"
)

// SSLMode represents the SSL mode for incoming client connections.
// These mirror PostgreSQL's sslmode settings but apply to the proxy as a server.
type SSLMode string

const (
	// SSLModeDisable means TLS is disabled entirely. Only plaintext connections are accepted.
	SSLModeDisable SSLMode = "disable"
	// SSLModeAllow means both TLS and plaintext connections are accepted from clients.
	SSLModeAllow SSLMode = "allow"
	// SSLModePrefer means TLS is preferred but plaintext connections are accepted if the client doesn't support TLS.
	SSLModePrefer SSLMode = "prefer"
	// SSLModeRequire means TLS is required for all connections. Plaintext connections are rejected.
	SSLModeRequire SSLMode = "require"
)

// JsonTLSConfig configures TLS for incoming client connections.
type JsonTLSConfig struct {
	// SSLMode controls whether TLS is required, preferred, or disabled.
	// See the SSLMode type for valid values.
	SSLMode SSLMode `json:"sslmode,omitzero"`

	// CertPath is the path to the TLS certificate file in PEM format.
	CertPath string `json:"cert_path,omitzero"`

	// CertPrivateKeyPath is the path to the TLS private key file in PEM format.
	CertPrivateKeyPath string `json:"cert_private_key_path,omitzero"`

	// GenerateCert enables automatic generation of a self-signed certificate.
	// If CertPath and CertPrivateKeyPath are also set, the certificate is
	// written to those paths (unless they already exist).
	GenerateCert bool `json:"generate_cert,omitzero"`
}

// Validate checks that the TLS configuration is valid.
// The fsys parameter is used to check if certificate files exist.
func (c *JsonTLSConfig) Validate(fsys fs.FS) error {
	// Default to disable if not specified
	mode := c.SSLMode
	if mode == "" {
		mode = SSLModeDisable
	}

	// Validate sslmode value
	switch mode {
	case SSLModeDisable, SSLModeAllow, SSLModePrefer, SSLModeRequire:
		// valid
	default:
		return fmt.Errorf("invalid sslmode %q: must be one of: disable, allow, prefer, require", c.SSLMode)
	}

	// If SSL is disabled, no other settings matter
	if mode == SSLModeDisable {
		return nil
	}

	// SSL is enabled in some form - need either cert paths or generate_cert
	hasCertPath := c.CertPath != ""
	hasKeyPath := c.CertPrivateKeyPath != ""
	hasPartialPaths := hasCertPath != hasKeyPath

	if hasPartialPaths {
		return errors.New("cert_path and cert_private_key_path must both be set or both be empty")
	}

	hasCertPaths := hasCertPath && hasKeyPath

	if !hasCertPaths && !c.GenerateCert {
		return errors.New("TLS enabled but no certificate configured: set cert_path and cert_private_key_path, or set generate_cert to true")
	}

	// If generate_cert is false, verify cert files exist
	if !c.GenerateCert && hasCertPaths {
		if _, err := fs.Stat(fsys, c.CertPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("cert_path %q does not exist", c.CertPath)
			}
			return fmt.Errorf("cert_path %q: %w", c.CertPath, err)
		}
		if _, err := fs.Stat(fsys, c.CertPrivateKeyPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("cert_private_key_path %q does not exist", c.CertPrivateKeyPath)
			}
			return fmt.Errorf("cert_private_key_path %q: %w", c.CertPrivateKeyPath, err)
		}
	}

	return nil
}

// Enabled returns true if TLS is enabled in any form (allow, prefer, or require).
func (c *JsonTLSConfig) Enabled() bool {
	switch c.SSLMode {
	case SSLModeAllow, SSLModePrefer, SSLModeRequire:
		return true
	default:
		return false
	}
}

// Required returns true if TLS is required for all connections.
func (c *JsonTLSConfig) Required() bool {
	return c.SSLMode == SSLModeRequire
}

// TLSResult contains the result of creating a TLS configuration.
type TLSResult struct {
	// Config is the TLS configuration, or nil if TLS is disabled.
	Config *tls.Config
	// WrittenFiles contains the paths of any certificate files that were written.
	WrittenFiles []string
}

// NewTLS creates a tls.Config based on the configuration.
// Returns a TLSResult with nil Config if TLS is disabled.
// If GenerateCert is true and CertPath/CertPrivateKeyPath are set,
// the generated certificate will be written to those paths if they don't exist.
//
// Parameters:
//   - fsys: filesystem for reading certificate files (paths are relative to fsys root)
//   - resolvePath: function to resolve relative paths to absolute paths for writing
//
// The caller should call Validate() before calling NewTLS().
func (c *JsonTLSConfig) NewTLS(fsys fs.FS, resolvePath func(string) string) (TLSResult, error) {
	if !c.Enabled() {
		return TLSResult{}, nil
	}

	var cert tls.Certificate
	var err error
	var writtenFiles []string

	if c.GenerateCert {
		// Check if we should write to paths
		hasCertPaths := c.CertPath != "" && c.CertPrivateKeyPath != ""
		certExists := hasCertPaths && fileExistsFS(fsys, c.CertPath)
		keyExists := hasCertPaths && fileExistsFS(fsys, c.CertPrivateKeyPath)

		if hasCertPaths && certExists && keyExists {
			// Both files exist, load them instead of generating
			cert, err = loadX509KeyPairFS(fsys, c.CertPath, c.CertPrivateKeyPath)
			if err != nil {
				return TLSResult{}, fmt.Errorf("failed to load certificate: %w", err)
			}
		} else {
			// Generate new cert
			cert, err = generateSelfSignedCert()
			if err != nil {
				return TLSResult{}, fmt.Errorf("failed to generate self-signed certificate: %w", err)
			}

			// Write to paths if configured and files don't exist
			if hasCertPaths && !certExists && !keyExists {
				certAbsPath := resolvePath(c.CertPath)
				keyAbsPath := resolvePath(c.CertPrivateKeyPath)
				if err := writeCertToFiles(cert, certAbsPath, keyAbsPath); err != nil {
					return TLSResult{}, fmt.Errorf("failed to write certificate to files: %w", err)
				}
				writtenFiles = []string{certAbsPath, keyAbsPath}
			}
		}
	} else {
		cert, err = loadX509KeyPairFS(fsys, c.CertPath, c.CertPrivateKeyPath)
		if err != nil {
			return TLSResult{}, fmt.Errorf("failed to load certificate: %w", err)
		}
	}

	return TLSResult{
		Config: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
		WrittenFiles: writtenFiles,
	}, nil
}

// fileExistsFS returns true if the file exists in the filesystem and is not a directory.
func fileExistsFS(fsys fs.FS, path string) bool {
	info, err := fs.Stat(fsys, path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// loadX509KeyPairFS loads a certificate and key from an fs.FS.
func loadX509KeyPairFS(fsys fs.FS, certPath, keyPath string) (tls.Certificate, error) {
	certPEM, err := fs.ReadFile(fsys, certPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to read cert file: %w", err)
	}

	keyPEM, err := fs.ReadFile(fsys, keyPath)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to read key file: %w", err)
	}

	return tls.X509KeyPair(certPEM, keyPEM)
}

// writeCertToFiles writes a certificate and its private key to the specified paths.
func writeCertToFiles(cert tls.Certificate, certPath, keyPath string) (err error) {
	// Write certificate
	certOut, err := os.Create(certPath)
	if err != nil {
		return fmt.Errorf("failed to create cert file: %w", err)
	}
	defer func() {
		if cerr := certOut.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("failed to close cert file: %w", cerr)
		}
	}()

	for _, certBytes := range cert.Certificate {
		if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes}); err != nil {
			return fmt.Errorf("failed to write cert: %w", err)
		}
	}

	// Write private key
	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create key file: %w", err)
	}
	defer func() {
		if kerr := keyOut.Close(); kerr != nil && err == nil {
			err = fmt.Errorf("failed to close key file: %w", kerr)
		}
	}()

	privKey, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return errors.New("private key is not ECDSA")
	}

	privDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return fmt.Errorf("failed to marshal private key: %w", err)
	}

	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER}); err != nil {
		return fmt.Errorf("failed to write key: %w", err)
	}

	return nil
}

// defaultTLSConfig returns the default TLS configuration when no TLS config is specified.
// It generates a self-signed certificate in memory without persistence.
func defaultTLSConfig() (TLSResult, error) {
	cert, err := generateSelfSignedCert()
	if err != nil {
		return TLSResult{}, fmt.Errorf("failed to generate default self-signed certificate: %w", err)
	}
	return TLSResult{
		Config: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		},
	}, nil
}

// generateSelfSignedCert creates a self-signed certificate for development use.
func generateSelfSignedCert() (tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"pglink"},
			CommonName:   "pglink",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
		DNSNames:              []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	privDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})

	return tls.X509KeyPair(certPEM, keyPEM)
}
