package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
)

const (
	llmAcademic = "academic"
	llmGoogle   = "google"
	llmLocal    = "local"

	defaultGoogleModel = "gemini-2.5-flash"
	defaultGoogleBase  = "https://generativelanguage.googleapis.com/v1beta"
	defaultLocalBase   = "http://127.0.0.1:11434/v1"

	settingLLMProvider      = "llm.provider"
	settingLLMAcademicKey   = "llm.academic.key"
	settingLLMAcademicModel = "llm.academic.model"
	settingLLMGoogleKey     = "llm.google.key"
	settingLLMGoogleModel   = "llm.google.model"
	settingLLMLocalBase     = "llm.local.base"
	settingLLMLocalModel    = "llm.local.model"
	settingLLMLocalKey      = "llm.local.key"
)

// LLMConfig is the provider used for keyword extraction.
type LLMConfig struct {
	Provider string
	APIKey   string
	APIBase  string
	Model    string
}

// LLMForm is an admin update. A blank secret keeps the value already saved.
type LLMForm struct {
	Provider      string
	AcademicKey   string
	AcademicModel string
	GoogleKey     string
	GoogleModel   string
	LocalBase     string
	LocalModel    string
	LocalKey      string
}

// LLMView is the admin page's provider settings with secrets omitted.
type LLMView struct {
	Provider string       `json:"provider"`
	Academic llmKeyFields `json:"academic"`
	Google   llmKeyFields `json:"google"`
	Local    llmKeyFields `json:"local"`
}

type llmKeyFields struct {
	Model  string `json:"model,omitempty"`
	Base   string `json:"base,omitempty"`
	KeySet bool   `json:"key_set"`
}

func (s *Store) setting(key string) string {
	var v string
	s.db.QueryRow("SELECT value FROM settings WHERE key=?", key).Scan(&v)
	return v
}

func (s *Store) setSettings(kv map[string]string) error {
	err := s.write(func(tx *sql.Tx) error {
		for k, v := range kv {
			if _, err := tx.Exec(`
INSERT INTO settings(key, value) VALUES(?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value`, k, v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("save settings failed", "error", err)
	}
	return err
}

// LLMViewForAdmin reports the saved provider choices without their secrets.
func (s *Store) LLMViewForAdmin(cfg PipelineConfig) LLMView {
	provider := s.setting(settingLLMProvider)
	if provider == "" {
		provider = llmAcademic
	}
	academicModel := s.setting(settingLLMAcademicModel)
	if academicModel == "" {
		academicModel = cfg.Model
	}
	googleModel := s.setting(settingLLMGoogleModel)
	if googleModel == "" {
		googleModel = defaultGoogleModel
	}
	localBase := s.setting(settingLLMLocalBase)
	if localBase == "" {
		localBase = defaultLocalBase
	}
	return LLMView{
		Provider: provider,
		Academic: llmKeyFields{
			Model:  academicModel,
			Base:   cfg.APIBase,
			KeySet: s.setting(settingLLMAcademicKey) != "",
		},
		Google: llmKeyFields{
			Model:  googleModel,
			Base:   defaultGoogleBase,
			KeySet: s.setting(settingLLMGoogleKey) != "",
		},
		Local: llmKeyFields{
			Model:  s.setting(settingLLMLocalModel),
			Base:   localBase,
			KeySet: s.setting(settingLLMLocalKey) != "",
		},
	}
}

// SaveLLM stores an admin update and returns the provider that will be used.
// The write happens only after the resulting choice is usable.
func (s *Store) SaveLLM(cfg PipelineConfig, form LLMForm) (LLMConfig, error) {
	next := s.llmSettings()
	put := func(key, value string) {
		if v := strings.TrimSpace(value); v != "" {
			next[key] = v
		}
	}
	put(settingLLMProvider, form.Provider)
	put(settingLLMAcademicKey, form.AcademicKey)
	put(settingLLMAcademicModel, form.AcademicModel)
	put(settingLLMGoogleKey, form.GoogleKey)
	put(settingLLMGoogleModel, form.GoogleModel)
	if v := strings.TrimSpace(form.LocalBase); v != "" {
		next[settingLLMLocalBase] = strings.TrimRight(v, "/")
	}
	put(settingLLMLocalModel, form.LocalModel)
	put(settingLLMLocalKey, form.LocalKey)

	got, err := llmFrom(next, cfg)
	if err != nil {
		return LLMConfig{}, err
	}
	if err := s.setSettings(next); err != nil {
		return LLMConfig{}, fmt.Errorf("could not save language model settings")
	}
	return got, nil
}

func (s *Store) llmSettings() map[string]string {
	keys := []string{
		settingLLMProvider,
		settingLLMAcademicKey, settingLLMAcademicModel,
		settingLLMGoogleKey, settingLLMGoogleModel,
		settingLLMLocalBase, settingLLMLocalModel, settingLLMLocalKey,
	}
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		out[k] = s.setting(k)
	}
	return out
}

// resolveLLM picks the saved provider, falling back to the process defaults.
func resolveLLM(s *Store, cfg PipelineConfig) (LLMConfig, error) {
	return llmFrom(s.llmSettings(), cfg)
}

func llmFrom(settings map[string]string, cfg PipelineConfig) (LLMConfig, error) {
	provider := strings.TrimSpace(settings[settingLLMProvider])
	if provider == "" {
		provider = llmAcademic
	}
	switch provider {
	case llmGoogle:
		key := settings[settingLLMGoogleKey]
		if key == "" {
			return LLMConfig{}, fmt.Errorf("Google AI Studio API key is not set")
		}
		model := settings[settingLLMGoogleModel]
		if model == "" {
			model = defaultGoogleModel
		}
		return LLMConfig{Provider: llmGoogle, APIKey: key, APIBase: defaultGoogleBase, Model: model}, nil
	case llmLocal:
		base := settings[settingLLMLocalBase]
		if base == "" {
			base = defaultLocalBase
		}
		if err := checkBaseURL(base); err != nil {
			return LLMConfig{}, err
		}
		model := settings[settingLLMLocalModel]
		if model == "" {
			return LLMConfig{}, fmt.Errorf("local model name is not set")
		}
		return LLMConfig{
			Provider: llmLocal,
			APIKey:   settings[settingLLMLocalKey],
			APIBase:  strings.TrimRight(base, "/"),
			Model:    model,
		}, nil
	case llmAcademic:
		key := settings[settingLLMAcademicKey]
		if key == "" {
			key = loadAPIKey(cfg.DataDir)
		}
		if key == "" {
			return LLMConfig{}, fmt.Errorf("Academic Cloud API key is not set")
		}
		model := settings[settingLLMAcademicModel]
		if model == "" {
			model = cfg.Model
		}
		base := cfg.APIBase
		if base == "" {
			base = defaultAPIBase
		}
		return LLMConfig{
			Provider: llmAcademic,
			APIKey:   key,
			APIBase:  strings.TrimRight(base, "/"),
			Model:    model,
		}, nil
	default:
		return LLMConfig{}, fmt.Errorf("unknown language model provider")
	}
}

func checkBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("local base URL must be an http or https address")
	}
	return nil
}

func applyLLM(p *Pipeline, llm LLMConfig) {
	p.Provider = llm.Provider
	p.APIKey = llm.APIKey
	p.APIBase = llm.APIBase
	p.Model = llm.Model
}
