package agentroom

import "testing"

func TestNewClientTLSConfigServerName(t *testing.T) {
	t.Setenv("REDIS_TLS", "true")

	tests := []struct {
		name string
		addr string
		want string
	}{
		{name: "host port", addr: "redis.local:6379", want: "redis.local"},
		{name: "ipv6 host port", addr: "[::1]:6379", want: "::1"},
		{name: "no port fallback", addr: "redis.local", want: "redis.local"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(tt.addr)
			t.Cleanup(func() { _ = c.Close() })

			tlsConfig := c.Options().TLSConfig
			if tlsConfig == nil {
				t.Fatal("TLSConfig is nil")
			}
			if tlsConfig.ServerName != tt.want {
				t.Fatalf("ServerName = %q, want %q", tlsConfig.ServerName, tt.want)
			}
			if c.Options().Addr != tt.addr {
				t.Fatalf("Addr = %q, want %q", c.Options().Addr, tt.addr)
			}
		})
	}
}
