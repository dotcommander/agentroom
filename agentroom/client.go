package agentroom

import (
	"crypto/tls"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewClient is the single hardened constructor every agentchat process uses to
// obtain its redis client. It applies bounded dial/read/write timeouts so a hung
// (not down) Redis cannot stall a session, an explicit connection pool, and
// optional auth/TLS read from the environment (off by default):
//
//	REDIS_PASSWORD — sets AUTH; empty means no auth.
//	REDIS_TLS      — "1"/"true"/"yes" enables TLS (ServerName derived from addr).
//
// ReadTimeout applies only to non-blocking commands; go-redis extends the
// deadline for blocking reads (XReadGroup BLOCK) automatically, so the daemon's
// consumer loop is unaffected.
func NewClient(addr string) *redis.Client {
	opts := &redis.Options{
		Addr:         addr,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		PoolSize:     10,
		MinIdleConns: 1,
		MaxRetries:   3,
		Password:     os.Getenv("REDIS_PASSWORD"),
	}
	switch strings.ToLower(os.Getenv("REDIS_TLS")) {
	case "1", "true", "yes":
		host := addr
		if i := strings.LastIndex(addr, ":"); i >= 0 {
			host = addr[:i]
		}
		opts.TLSConfig = &tls.Config{ServerName: host}
	}
	return redis.NewClient(opts)
}
