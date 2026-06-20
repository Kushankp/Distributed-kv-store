package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type Replication struct {
	VirtualNodes int    `json:"virtual_nodes"`
	Factor       int    `json:"factor"`
	Consistency  string `json:"consistency"`
}

type Config struct {
	NodeID            string      `json:"node_id"`
	Nodes             []Node      `json:"nodes"`
	Replication       Replication `json:"replication"`
	RequestTimeout    Duration    `json:"request_timeout"`
	RetryCount        int         `json:"retry_count"`
	DataDir           string      `json:"data_dir"`
	SnapshotInterval  Duration    `json:"snapshot_interval"`
	HeartbeatInterval Duration    `json:"heartbeat_interval"`
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = parsed
	return nil
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Self() (Node, bool) {
	for _, node := range c.Nodes {
		if node.ID == c.NodeID {
			return node, true
		}
	}
	return Node{}, false
}

func (c Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node_id is required")
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("at least one node is required")
	}
	if c.Replication.VirtualNodes <= 0 {
		return fmt.Errorf("replication.virtual_nodes must be positive")
	}
	if c.Replication.Factor <= 0 {
		return fmt.Errorf("replication.factor must be positive")
	}
	if c.Replication.Consistency == "" {
		return fmt.Errorf("replication.consistency is required")
	}
	switch c.Replication.Consistency {
	case "one", "quorum", "all":
	default:
		return fmt.Errorf("replication.consistency must be one, quorum, or all")
	}
	if c.RequestTimeout.Duration <= 0 {
		return fmt.Errorf("request_timeout must be positive")
	}
	if c.RetryCount < 0 {
		return fmt.Errorf("retry_count cannot be negative")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data_dir is required")
	}
	if c.SnapshotInterval.Duration < 0 {
		return fmt.Errorf("snapshot_interval cannot be negative")
	}
	if c.HeartbeatInterval.Duration <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}
	seen := map[string]bool{}
	for _, node := range c.Nodes {
		if node.ID == "" || node.Address == "" {
			return fmt.Errorf("node id and address are required")
		}
		if seen[node.ID] {
			return fmt.Errorf("duplicate node id %q", node.ID)
		}
		seen[node.ID] = true
	}
	if !seen[c.NodeID] {
		return fmt.Errorf("node_id %q is not present in nodes", c.NodeID)
	}
	return nil
}
