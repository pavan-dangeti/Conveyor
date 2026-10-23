package main

import "testing"

func TestRefusePlaceholderSecretsOnAPublicDomain(t *testing.T) {
	t.Setenv("CONVEYOR_API_TOKEN", "change-me-token")
	t.Setenv("DOMAIN", "localhost")
	if err := refusePlaceholderSecrets(); err != nil {
		t.Fatalf("local runs may use placeholders: %v", err)
	}
	t.Setenv("DOMAIN", "conveyor.example.com")
	if err := refusePlaceholderSecrets(); err == nil {
		t.Fatal("a public domain with a placeholder token must refuse to start")
	}
	t.Setenv("CONVEYOR_API_TOKEN", "9f3c1a")
	t.Setenv("DATABASE_URL", "postgres://conveyor_app:change-me-app@postgres/conveyor")
	if err := refusePlaceholderSecrets(); err == nil {
		t.Fatal("a placeholder database password must refuse to start")
	}
	t.Setenv("DATABASE_URL", "postgres://conveyor_app:4b1d@postgres/conveyor")
	if err := refusePlaceholderSecrets(); err != nil {
		t.Fatalf("real secrets must start: %v", err)
	}
}
