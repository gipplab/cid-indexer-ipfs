package main

import "testing"

func TestLLMProviders(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("SAIA_API_KEY", "")
	s := NewStore(dir)
	cfg := PipelineConfig{DataDir: dir, Model: defaultModel, APIBase: defaultAPIBase}

	if _, err := resolveLLM(s, cfg); err == nil {
		t.Fatal("expected a missing Academic Cloud key")
	}
	if err := saveAPIKey(dir, "admin-key"); err != nil {
		t.Fatal(err)
	}
	got, err := resolveLLM(s, cfg)
	if err != nil || got.Provider != llmAcademic || got.APIKey != "admin-key" || got.Model != defaultModel {
		t.Fatalf("academic = %+v, %v", got, err)
	}

	if _, err := s.SaveLLM(cfg, LLMForm{Provider: llmGoogle}); err == nil {
		t.Fatal("google without a key should be rejected")
	}
	if got, err := resolveLLM(s, cfg); err != nil || got.Provider != llmAcademic {
		t.Fatalf("rejected save changed provider: %+v %v", got, err)
	}

	got, err = s.SaveLLM(cfg, LLMForm{Provider: llmGoogle, GoogleKey: "g-key", GoogleModel: "gemini-2.5-flash"})
	if err != nil || got.Provider != llmGoogle || got.APIKey != "g-key" {
		t.Fatalf("google = %+v, %v", got, err)
	}
	got, err = s.SaveLLM(cfg, LLMForm{Provider: llmGoogle, GoogleModel: "gemini-2.5-pro"})
	if err != nil || got.APIKey != "g-key" || got.Model != "gemini-2.5-pro" {
		t.Fatalf("blank key did not keep the saved secret: %+v, %v", got, err)
	}

	if _, err := s.SaveLLM(cfg, LLMForm{Provider: llmLocal, LocalBase: "notaurl", LocalModel: "llama"}); err == nil {
		t.Fatal("invalid local URL should be rejected")
	}
	got, err = s.SaveLLM(cfg, LLMForm{
		Provider:   llmLocal,
		LocalBase:  "http://127.0.0.1:11434/v1/",
		LocalModel: "llama3.1",
	})
	if err != nil || got.Provider != llmLocal || got.APIKey != "" || got.Model != "llama3.1" || got.APIBase != "http://127.0.0.1:11434/v1" {
		t.Fatalf("local = %+v, %v", got, err)
	}

	view := s.LLMViewForAdmin(cfg)
	if !view.Google.KeySet || view.Local.KeySet || view.Provider != llmLocal {
		t.Fatalf("view = %+v", view)
	}
}
