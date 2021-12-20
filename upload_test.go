package main

import (
	"fmt"
	"os/user"
	"path/filepath"
	"runtime"
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

	if runtime.GOOS == "windows" {
		tests = []struct {
			input string
			want  string
		}{
			{"", "."},
			{"/truc/truc/../muche", "\\truc\\muche"},
			{"./truc/muche", "truc\\muche"},
			{"~/truc/muche/dir", filepath.Clean(filepath.Join(u.HomeDir, "/truc/muche/dir"))},
			{fmt.Sprintf("~%s/truc/muche", u.Username), filepath.Clean(filepath.Join(u.HomeDir, "/truc/muche"))},
		}
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

func TestRelPath(t *testing.T) {
	var tests = []struct {
		basedir string
		path    string
		want    string
	}{
		{"/var/truc/dir", "/var/truc/dir/dump.d/file", "dump.d/file"},
		{"/var/{dbname}/dir", "/var/b1/dir/b1.dump", "b1/dir/b1.dump"},
	}

	if runtime.GOOS == "windows" {
		tests = []struct {
			basedir string
			path    string
			want    string
		}{
			{"C:\\var\\truc\\dir", "C:\\var\\truc\\dir\\dump.d\\file", "dump.d\\file"},
			{"C:\\var\\{dbname}\\dir", "C:\\var\\b1\\dir\\b1.dump", "b1\\dir\\b1.dump"},
		}
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := relPath(st.basedir, st.path)
			if got != st.want {
				t.Errorf("got: %v, want %v", got, st.want)
			}
		})
	}
}

func TestForwardSlashes(t *testing.T) {
	var tests = []struct {
		path string
		want string
	}{
		{"/var/truc/dir", "/var/truc/dir"},
	}

	if runtime.GOOS == "windows" {
		tests = []struct {
			path string
			want string
		}{
			{"b1\\dir\\b1.dump", "b1/dir/b1.dump"},
		}
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			got := forwardSlashes(st.path)
			if got != st.want {
				t.Errorf("got: %v, want %v", got, st.want)
			}
		})
	}
}
