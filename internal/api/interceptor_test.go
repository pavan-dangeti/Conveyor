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
	from := func(ip net.IP) context.Context {
		return peer.NewContext(context.Background(), &peer.Peer{Addr: &net.TCPAddr{IP: ip, Port: 5555}})
	}
	gateway, proxy, public := from(net.IPv4(127, 0, 0, 1)), from(net.IPv4(172, 18, 0, 5)), from(net.IPv4(203, 0, 113, 7))
	xff := func(v string) metadata.MD { return metadata.Pairs("x-forwarded-for", v) }
	for name, c := range map[string]struct {
		ctx  context.Context
		md   metadata.MD
		want string
	}{
		"gateway forwards the first hop":   {gateway, xff("198.51.100.9, 172.18.0.5"), "198.51.100.9"},
		"proxy on the container network":   {proxy, xff("198.51.100.9:443"), "198.51.100.9"},
		"direct caller cannot forge a hop": {public, xff("10.9.9.9"), "203.0.113.7"},
		"direct caller without header":     {public, metadata.MD{}, "203.0.113.7"},
		"no peer at all":                   {context.Background(), xff("10.9.9.9"), "unknown"},
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
