package reliabilityhook

import (
	"os"
	"path/filepath"
	"testing"
)

func setHookEnv(t *testing.T, mode, token, directory string) {
	t.Helper()
	t.Setenv("RELIABILITY_ACCEPTANCE_MODE", mode)
	t.Setenv("RELIABILITY_FAULT_TOKEN", token)
	t.Setenv("RELIABILITY_CONTROL_DIR", directory)
}

func TestFromEnvDisabledModesRequireNoHookCredentials(t *testing.T) {
	for _, mode := range []string{"", "false", "FALSE", "0"} {
		t.Run("disabled/"+mode, func(t *testing.T) {
			setHookEnv(t, mode, "", "")
			hook, err := FromEnv()
			if err != nil {
				t.Fatalf("disabled mode returned an error: %v", err)
			}
			if hook == nil || hook.Enabled() {
				t.Fatalf("disabled mode returned an enabled hook: %v", hook)
			}
		})
	}
}

func TestFromEnvRejectsCredentialsWhenHookModeIsDisabled(t *testing.T) {
	for _, tc := range []struct{ mode, token, directory string }{
		{mode: "", token: testToken},
		{mode: "false", directory: t.TempDir()},
		{mode: "0", token: testToken, directory: t.TempDir()},
	} {
		setHookEnv(t, tc.mode, tc.token, tc.directory)
		if hook, err := FromEnv(); err == nil || hook != nil {
			t.Fatalf("disabled mode accepted a configured credential: mode=%q hook=%v err=%v", tc.mode, hook, err)
		}
	}
}

func TestFromEnvAcceptsEnabledModeOnlyInReliabilityE2EBuild(t *testing.T) {
	directory := t.TempDir()
	for _, mode := range []string{"true", "TRUE", "1"} {
		t.Run(mode, func(t *testing.T) {
			setHookEnv(t, mode, testToken, directory)
			hook, err := FromEnv()
			if acceptanceBuildAllowed {
				if err != nil || hook == nil || !hook.Enabled() {
					t.Fatalf("tagged acceptance mode failed to enable: hook=%v err=%v", hook, err)
				}
				return
			}
			if err == nil || hook != nil {
				t.Fatalf("ordinary build accepted enabled reliability mode: hook=%v err=%v", hook, err)
			}
		})
	}
}

func TestFromEnvRejectsWrongModeAndInvalidExplicitConfiguration(t *testing.T) {
	for _, mode := range []string{"yes", "on", "2"} {
		setHookEnv(t, mode, "", "")
		if hook, err := FromEnv(); err == nil || hook != nil {
			t.Fatalf("invalid mode %q was accepted: hook=%v err=%v", mode, hook, err)
		}
	}
	for _, tc := range []struct{ token, directory string }{
		{token: "short", directory: t.TempDir()},
		{token: testToken, directory: "relative-control"},
		{token: testToken, directory: filepath.Join(t.TempDir(), "missing")},
	} {
		if _, err := internalNew(Config{Enabled: true, Token: tc.token, Directory: tc.directory}); err == nil {
			t.Fatalf("internal test constructor accepted unsafe config: token=%q directory=%q", tc.token, tc.directory)
		}
	}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := internalNew(Config{Enabled: true, Token: testToken, Directory: file}); err == nil {
		t.Fatal("internal test constructor accepted a file as the control directory")
	}
}
