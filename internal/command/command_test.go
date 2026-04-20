package command

import (
	"fmt"
	"runtime"
	"testing"
)

func TestExecPath(t *testing.T) {
	var tests []struct {
		dir  string
		prog string
		want string
	}

	if runtime.GOOS != "windows" {
		tests = []struct {
			dir  string
			prog string
			want string
		}{
			{"", "pg_dump", "pg_dump"},
			{"/path/to/bin", "prog", "/path/to/bin/prog"},
		}
	} else {
		tests = []struct {
			dir  string
			prog string
			want string
		}{
			{"", "pg_dump", "pg_dump.exe"},
			{"C:\\path\\to\\bin", "prog", "C:\\path\\to\\bin\\prog.exe"},
		}
	}

	for i, st := range tests {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			binDir := st.dir
			got := ExecPath(binDir, st.prog)
			if got != st.want {
				t.Errorf("expected %q, got %q\n", st.want, got)
			}
		})
	}
}
