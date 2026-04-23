package helpers

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/orgrim/pg_back/internal/logger"
)

func WrappedClose(c io.Closer, err *error) {
	cErr := c.Close()
	if cErr != nil && *err == nil {
		*err = cErr
	}
}
func CleanDBName(dbname string) string {
	// We do not want a database name starting with a dot to avoid creating hidden files
	if strings.HasPrefix(dbname, ".") {
		dbname = "_" + dbname
	}

	// If there is a path separator in the database name, we do not want to
	// create the dump in a subdirectory or in a parent directory
	if strings.ContainsRune(dbname, os.PathSeparator) {
		dbname = strings.ReplaceAll(dbname, string(os.PathSeparator), "_")
	}

	// Always remove slashes to avoid issues with filenames on windows
	if strings.ContainsRune(dbname, '/') {
		dbname = strings.ReplaceAll(dbname, "/", "_")
	}

	return dbname
}

func FormatDumpPath(
	dir string,
	timeFormat string,
	suffix string,
	dbname string,
	when time.Time,
	compressLevel int,
) string {
	var f, s, d string

	// Avoid attacks on the database name
	dbname = CleanDBName(dbname)

	d = dir
	if dbname != "" {
		d = strings.ReplaceAll(dir, "{dbname}", dbname)
	}

	s = suffix
	if suffix == "" {
		s = "dump"
	}

	// Output is "dir(formatted)/dbname_date.suffix" when the
	// input time is not zero, otherwise do not include the date
	// and time. Reference time for time.Format(): "Mon Jan 2
	// 15:04:05 MST 2006"
	if when.IsZero() {
		f = fmt.Sprintf("%s.%s", dbname, s)
	} else {
		f = fmt.Sprintf("%s_%s.%s", dbname, when.Format(timeFormat), s)
	}

	if suffix == "sql" && compressLevel > 0 {
		f = f + ".gz"
	}

	return filepath.Join(d, f)
}

func RelPath(logger *logger.LevelLog, basedir, path string) string {
	target, err := filepath.Rel(basedir, path)
	if err != nil {
		logger.Warnf("could not get relative path from %s: %s\n", path, err)
		target = path
	}

	prefix := fmt.Sprintf("..%c", os.PathSeparator)
	for strings.HasPrefix(target, prefix) {
		target = strings.TrimPrefix(target, prefix)
	}

	return target
}
