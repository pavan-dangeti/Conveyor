package api

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestClientIP(t *testing.T) {
	withPeer := peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 5555}})
	for name, c := range map[string]struct {
		ctx  context.Context
		md   metadata.MD
		want string
	}{
		"first forwarded hop": {context.Background(), metadata.Pairs("x-forwarded-for", "203.0.113.9, 10.0.0.1"), "203.0.113.9"},
		"forwarded with port": {context.Background(), metadata.Pairs("x-forwarded-for", "203.0.113.9:443"), "203.0.113.9"},
		"peer address":        {withPeer, metadata.MD{}, "10.0.0.7"},
		"nothing to go on":    {context.Background(), metadata.MD{}, "unknown"},
	} {
		if got := clientIP(c.ctx, c.md); got != c.want {
			t.Errorf("%s: got %q, want %q", name, got, c.want)
		}
	}
}

func TestLimiterRefillsAndIsPerClient(t *testing.T) {
	l := newLimiter(10, 2)
	for i, want := range []bool{true, true, false} {
		if got := l.allow("a"); got != want {
			t.Fatalf("request %d: allow = %v, want %v (burst of 2)", i+1, got, want)
		}
	}
	if !l.allow("b") {
		t.Fatal("clients must not share a bucket")
	}
	time.Sleep(150 * time.Millisecond)
	if !l.allow("a") {
		t.Fatal("bucket must refill at 10/s")
	}
	if !newLimiter(0, 0).allow("x") {
		t.Fatal("rate 0 disables limiting")
	}
}
