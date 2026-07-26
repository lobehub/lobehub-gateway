package gateway

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type authResolver struct {
	cfg Config

	keyMu sync.Mutex
	key   *rsa.PublicKey
}

func newAuthResolver(cfg Config) *authResolver {
	return &authResolver{cfg: cfg}
}

func (a *authResolver) resolve(ctx context.Context, storedPrincipal string, msg authMessage) (string, error) {
	if msg.Token == "" {
		return "", errors.New("Missing token")
	}

	if msg.TokenType == "apiKey" {
		if msg.ServerURL == "" {
			return "", errors.New("Missing serverUrl")
		}
		userID, err := verifyAPIKey(ctx, msg.ServerURL, msg.Token)
		if err != nil {
			return "", err
		}
		return resolvePrincipal(userID, ""), nil
	}

	if msg.Token == a.cfg.ServiceToken {
		if storedPrincipal == "" {
			return "", errors.New("Missing principal")
		}
		return storedPrincipal, nil
	}

	return a.verifyJWT(msg.Token)
}

func verifyAPIKey(ctx context.Context, serverURL string, token string) (string, error) {
	parsed, err := url.Parse(serverURL)
	if err != nil {
		return "", err
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/v1/users/me"
	parsed.RawQuery = "includeCount=0"
	parsed.Fragment = ""

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var body struct {
		Data *struct {
			ID     string `json:"id"`
			UserID string `json:"userId"`
		} `json:"data"`
		Error   string `json:"error"`
		Message string `json:"message"`
		Success *bool  `json:"success"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("Failed to parse response from %s.", parsed.String())
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 || (body.Success != nil && !*body.Success) {
		if body.Error != "" {
			return "", errors.New(body.Error)
		}
		if body.Message != "" {
			return "", errors.New(body.Message)
		}
		return "", fmt.Errorf("Request failed with status %d.", resp.StatusCode)
	}
	if body.Data == nil {
		return "", errors.New("Current user response did not include a user id.")
	}
	if body.Data.ID != "" {
		return body.Data.ID, nil
	}
	if body.Data.UserID != "" {
		return body.Data.UserID, nil
	}
	return "", errors.New("Current user response did not include a user id.")
}

func (a *authResolver) verifyJWT(tokenString string) (string, error) {
	key, err := a.publicKey()
	if err != nil {
		return "", err
	}
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid token")
	}

	var header struct {
		Alg string `json:"alg"`
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", err
	}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", err
	}
	if header.Alg != "RS256" {
		return "", fmt.Errorf("unexpected signing method: %s", header.Alg)
	}

	signed := []byte(parts[0] + "." + parts[1])
	sum := sha256.Sum256(signed)
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", err
	}
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return "", err
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", err
	}
	var claims map[string]any
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", err
	}
	now := float64(time.Now().Unix())
	if exp, ok := claims["exp"].(float64); ok && now >= exp {
		return "", errors.New(`"exp" claim timestamp check failed`)
	}
	if nbf, ok := claims["nbf"].(float64); ok && now < nbf {
		return "", errors.New(`"nbf" claim timestamp check failed`)
	}
	sub, _ := claims["sub"].(string)
	if sub == "" {
		return "", errors.New("Missing sub claim")
	}
	workspaceID, _ := claims["workspace_id"].(string)
	return resolvePrincipal(sub, workspaceID), nil
}

func (a *authResolver) publicKey() (*rsa.PublicKey, error) {
	a.keyMu.Lock()
	defer a.keyMu.Unlock()
	if a.key != nil {
		return a.key, nil
	}
	if a.cfg.JWKSPublicKey == "" {
		return nil, errors.New("JWKS_PUBLIC_KEY is required")
	}

	var jwks struct {
		Keys []struct {
			Alg string `json:"alg"`
			E   string `json:"e"`
			Kty string `json:"kty"`
			N   string `json:"n"`
		} `json:"keys"`
	}
	if err := json.Unmarshal([]byte(a.cfg.JWKSPublicKey), &jwks); err != nil {
		return nil, err
	}
	for _, key := range jwks.Keys {
		if key.Alg != "RS256" || key.Kty != "RSA" {
			continue
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, err
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, err
		}
		e := 0
		for _, b := range eBytes {
			e = e<<8 + int(b)
		}
		a.key = &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}
		return a.key, nil
	}
	return nil, errors.New("No RS256 key found in JWKS_PUBLIC_KEY")
}
