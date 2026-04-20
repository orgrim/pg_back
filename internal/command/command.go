package command

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"

	"github.com/orgrim/pg_back/internal/logger"
)

func ExecPath(binDir, prog string) string {
	binFile := prog
	if runtime.GOOS == "windows" {
		binFile = fmt.Sprintf("%s.exe", prog)
	}

	if binDir != "" {
		return filepath.Join(binDir, binFile)
	}

	return binFile
}

func PgToolVersion(logger *logger.LevelLog, binDir, tool string) int {
	vs, err := exec.Command(ExecPath(binDir, tool), "--version").Output()
	if err != nil {
		logger.Warnf("failed to retrieve version of %s: %s", tool, err)
		return 0
	}

	var maj, min, rev, numver int
	n, _ := fmt.Sscanf(string(vs), tool+" (PostgreSQL) %d.%d.%d", &maj, &min, &rev)

	switch n {
	case 3:
		// Before PostgreSQL 10, the format si MAJ.MIN.REV
		numver = (maj*100+min)*100 + rev
	case 2:
		// From PostgreSQL 10, the format si MAJ.REV, so the rev ends
		// up in min with the scan
		numver = maj*10000 + min
	default:
		// We have the special case of the development version, where the
		// format is MAJdevel
		fmt.Sscanf(string(vs), tool+" (PostgreSQL) %ddevel", &maj)
		numver = maj * 10000
	}

	logger.Verboseln(tool, "version is:", numver)

	return numver
}
