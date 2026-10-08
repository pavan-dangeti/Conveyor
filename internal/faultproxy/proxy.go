// Package faultproxy is a TCP proxy that injects latency and connection
// drops between clients and Postgres. It is a deliberate simplification of
// Toxiproxy (no HTTP-awareness, no toxics), sufficient to exercise
// reconnect and fencing paths in the fault-injection harness.
package faultproxy

import (
	"math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
)

type Proxy struct {
	listener net.Listener
	target   string
	dropRate atomic.Uint64
	latency  atomic.Int64
}

func New(listenAddr, target string) (*Proxy, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	p := &Proxy{listener: l, target: target}
	go p.acceptLoop()
	return p, nil
}

func (p *Proxy) Addr() string { return p.listener.Addr().String() }

func (p *Proxy) SetDropRate(r float64)      { p.dropRate.Store(math.Float64bits(r)) }
func (p *Proxy) SetLatency(d time.Duration) { p.latency.Store(int64(d)) }
func (p *Proxy) Close() error               { return p.listener.Close() }

func (p *Proxy) acceptLoop() {
	for {
		c, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.proxy(c)
	}
}

func (p *Proxy) proxy(in net.Conn) {
	defer func() { _ = in.Close() }()
	if rand.Float64() < math.Float64frombits(p.dropRate.Load()) {
		return
	}
	latency := time.Duration(p.latency.Load())
	out, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		return
	}
	defer func() { _ = out.Close() }()
	done := make(chan struct{}, 2)
	go func() { p.pipe(in, out, latency); done <- struct{}{} }()
	go func() { p.pipe(out, in, latency); done <- struct{}{} }()
	<-done
}

func (p *Proxy) pipe(dst, src net.Conn, latency time.Duration) {
	buf := make([]byte, 16*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if latency > 0 {
				time.Sleep(latency)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
