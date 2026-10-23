package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"log/slog"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/pavan-dangeti/Conveyor/internal/metrics"
)

const requestIDKey = "x-request-id"

var readMethods = map[string]bool{"GetJob": true, "ListJobs": true, "ListQueues": true, "GetStats": true}

type InterceptorConfig struct {
	// Token is the required bearer token; empty disables authentication.
	Token string
	// PublicRead lets unauthenticated callers use read-only RPCs, for a
	// public dashboard whose writes still need the token.
	PublicRead bool
	// RatePerSec and Burst bound requests per client IP; 0 disables.
	RatePerSec float64
	Burst      float64
}

// Interceptor rate-limits and authenticates each call, assigns a request ID
// (reusing the caller's x-request-id), echoes it in the response header,
// and emits one JSON access log line and the RPC metrics.
func Interceptor(cfg InterceptorConfig, logger *slog.Logger) grpc.UnaryServerInterceptor {
	want := []byte("Bearer " + cfg.Token)
	limiter := newLimiter(cfg.RatePerSec, cfg.Burst)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		method := path.Base(info.FullMethod)
		md, _ := metadata.FromIncomingContext(ctx)
		reqID := first(md.Get(requestIDKey))
		if reqID == "" || len(reqID) > 64 {
			reqID = newID()
		}
		_ = grpc.SetHeader(ctx, metadata.Pairs(requestIDKey, reqID))
		client := clientIP(ctx, md)

		var resp any
		var err error
		authed := cfg.Token == "" || subtle.ConstantTimeCompare([]byte(first(md.Get("authorization"))), want) == 1
		publicRead := cfg.PublicRead && readMethods[method]
		switch {
		case !limiter.allow(client):
			err = status.Error(codes.ResourceExhausted, "rate limit exceeded")
		case !authed && !publicRead:
			err = status.Error(codes.Unauthenticated, "missing or invalid bearer token")
		default:
			resp, err = handler(ctx, req)
		}

		code := status.Code(err)
		elapsed := time.Since(start)
		metrics.RPCRequests.WithLabelValues(method, code.String()).Inc()
		metrics.RPCSeconds.WithLabelValues(method).Observe(elapsed.Seconds())
		level := slog.LevelInfo
		switch code {
		case codes.OK, codes.NotFound, codes.InvalidArgument, codes.FailedPrecondition, codes.Unauthenticated, codes.ResourceExhausted:
		default:
			level = slog.LevelError
		}
		logger.LogAttrs(ctx, level, "rpc",
			slog.String("request_id", reqID), slog.String("method", method), slog.String("client", client),
			slog.String("code", code.String()), slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000))
		return resp, err
	}
}

// clientIP prefers the first X-Forwarded-For hop, which the HTTP gateway
// (behind the TLS proxy) forwards, so limits apply to the browser rather
// than to the gateway's loopback connection. Direct gRPC callers can set
// the header themselves; the threat model notes this.
func clientIP(ctx context.Context, md metadata.MD) string {
	if v := first(md.Get("x-forwarded-for")); v != "" {
		hop := strings.TrimSpace(strings.Split(v, ",")[0])
		if host, _, err := net.SplitHostPort(hop); err == nil {
			return host
		}
		return hop
	}
	if p, ok := peer.FromContext(ctx); ok {
		if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
			return host
		}
	}
	return "unknown"
}

// limiter is a token bucket per client. Known limit: the map is dropped
// wholesale every 10 minutes instead of evicting idle clients; switch to an
// LRU if a deployment sees enough distinct clients for that to matter.
type limiter struct {
	rate, burst float64
	mu          sync.Mutex
	buckets     map[string]*bucket
	reset       time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(rate, burst float64) *limiter {
	return &limiter{rate: rate, burst: max(burst, 1), buckets: map[string]*bucket{}, reset: time.Now()}
}

func (l *limiter) allow(key string) bool {
	if l.rate <= 0 {
		return true
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.reset) > 10*time.Minute {
		l.buckets, l.reset = map[string]*bucket{}, now
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func first(v []string) string {
	if len(v) == 0 {
		return ""
	}
	return v[0]
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
