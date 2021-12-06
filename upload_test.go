package main

import (
	"fmt"
	"os/user"
	"path/filepath"
	"testing"
)

func TestExpandHomeDir(t *testing.T) {
	u, err := user.Current()
	if err != nil {
		t.Errorf("could not get current user: %s", err)
	}

	var tests = []struct {
		input string
		want  string
	}{
		{"", "."},
		{"/truc/truc/../muche", "/truc/muche"},
		{"./truc/muche", "truc/muche"},
		{"~/truc/muche/dir", filepath.Clean(filepath.Join(u.HomeDir, "/truc/muche/dir"))},
		{fmt.Sprintf("~%s/truc/muche", u.Username), filepath.Clean(filepath.Join(u.HomeDir, "/truc/muche"))},
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got, err := expandHomeDir(st.input)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if got != st.want {
				t.Errorf("got: %v, want %v", got, st.want)
			}
		})
	}
}
