// Package ui embeds the operator dashboard.
package ui

import (
	_ "embed"
	"net/http"
)

//go:embed dashboard.html
var page []byte

// Handler serves the dashboard; mount it on the exact root, "GET /{$}".
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Type", "text/html; charset=utf-8")
		h.Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		_, _ = w.Write(page)
	})
}
