package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"distributed-kv-store/internal/config"
)

type RemoteClient interface {
	Forward(ctx context.Context, node config.Node, method string, key string, value string, consistency ConsistencyLevel) (RemoteResponse, error)
	Replicate(ctx context.Context, node config.Node, op Mutation) error
	Health(ctx context.Context, node config.Node) error
}

type RemoteResponse struct {
	Status int
	Body   []byte
}

type HTTPClient struct {
	client  *http.Client
	retries int
	logger  *slog.Logger
}

func NewClient(timeout time.Duration, retries int, logger *slog.Logger) *HTTPClient {
	return &HTTPClient{
		client:  &http.Client{Timeout: timeout},
		retries: retries,
		logger:  logger,
	}
}

func (c *HTTPClient) Forward(ctx context.Context, node config.Node, method string, key string, value string, consistency ConsistencyLevel) (RemoteResponse, error) {
	var body []byte
	if method == http.MethodPut {
		payload, err := json.Marshal(map[string]string{"value": value})
		if err != nil {
			return RemoteResponse{}, err
		}
		body = payload
	}

	url := fmt.Sprintf("http://%s/kv/%s", node.Address, key)
	if consistency != "" {
		url += "?consistency=" + string(consistency)
	}
	return c.do(ctx, method, url, body)
}

func (c *HTTPClient) Replicate(ctx context.Context, node config.Node, op Mutation) error {
	payload, err := json.Marshal(op)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("http://%s/internal/replicate", node.Address)
	resp, err := c.do(ctx, http.MethodPost, url, payload)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("replication to %s failed with status %d: %s", node.ID, resp.Status, strings.TrimSpace(string(resp.Body)))
	}
	return nil
}

func (c *HTTPClient) Health(ctx context.Context, node config.Node) error {
	url := fmt.Sprintf("http://%s/healthz", node.Address)
	resp, err := c.do(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if resp.Status < 200 || resp.Status >= 300 {
		return fmt.Errorf("health check to %s failed with status %d", node.ID, resp.Status)
	}
	return nil
}

func (c *HTTPClient) do(ctx context.Context, method string, url string, body []byte) (RemoteResponse, error) {
	var lastErr error
	attempts := c.retries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		var requestBody io.Reader
		if body != nil {
			requestBody = bytes.NewReader(body)
		}

		req, err := http.NewRequestWithContext(ctx, method, url, requestBody)
		if err != nil {
			return RemoteResponse{}, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(req)
		if err == nil {
			defer resp.Body.Close()
			responseBody, readErr := io.ReadAll(resp.Body)
			if readErr != nil {
				return RemoteResponse{}, readErr
			}
			if resp.StatusCode < 500 {
				return RemoteResponse{Status: resp.StatusCode, Body: responseBody}, nil
			}
			lastErr = fmt.Errorf("server returned status %d", resp.StatusCode)
		} else {
			lastErr = err
		}

		if attempt < attempts {
			c.logger.Warn("remote request failed, retrying", "method", method, "url", url, "attempt", attempt, "error", lastErr)
			time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
		}
	}
	return RemoteResponse{}, lastErr
}
