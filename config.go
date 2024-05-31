// pg_back
//
// Copyright 2011-2021 Nicolas Thauvin and contributors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  1. Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//  2. Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in the
//     documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHORS ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package main

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/anmitsu/go-shlex"
	"github.com/spf13/pflag"
	"gopkg.in/ini.v1"
)

var defaultCfgFile = "/etc/pg_back/pg_back.conf"

//go:embed pg_back.conf
var defaultCfg string

// options struct holds command line and configuration file options
type options struct {
	NoConfigFile      bool
	BinDirectory      string
	Directory         string
	Host              string
	Port              int
	Username          string
	ConnDb            string
	ExcludeDbs        []string
	Dbnames           []string
	WithTemplates     bool
	Format            rune
	DirJobs           int
	CompressLevel     int
	Jobs              int
	PauseTimeout      int
	PurgeInterval     time.Duration
	PurgeKeep         int
	SumAlgo           string
	PreHook           string
	PostHook          string
	PgDumpOpts        []string
	PerDbOpts         map[string]*dbOpts
	CfgFile           string
	TimeFormat        string
	Verbose           bool
	Quiet             bool
	Encrypt           bool
	EncryptKeepSrc    bool
	CipherPassphrase  string
	CipherPublicKey   string
	CipherPrivateKey  string
	Decrypt           bool
	WithRolePasswords bool
	DumpOnly          bool

	Upload       string // values are none, s3, sftp, gcs
	Download     string // values are none, s3, sftp, gcs
	ListRemote   string // values are none, s3, sftp, gcs
	PurgeRemote  bool
	S3Region     string
	S3Bucket     string
	S3EndPoint   string
	S3Profile    string
	S3KeyID      string
	S3Secret     string
	S3ForcePath  bool
	S3DisableTLS bool

	SFTPHost             string
	SFTPPort             string
	SFTPUsername         string
	SFTPPassword         string
	SFTPDirectory        string
	SFTPIdentityFile     string // path to private key
	SFTPIgnoreKnownHosts bool

	GCSBucket          string
	GCSEndPoint        string
	GCSCredentialsFile string

	AzureContainer string
	AzureAccount   string
	AzureKey       string
	AzureEndpoint  string
}

func defaultOptions() options {
	timeFormat := time.RFC3339
	if runtime.GOOS == "windows" {
		timeFormat = "2006-01-02_15-04-05"
	}

	return options{
		NoConfigFile:      false,
		Directory:         "/var/backups/postgresql",
		Format:            'c',
		DirJobs:           1,
		CompressLevel:     -1,
		Jobs:              1,
		PauseTimeout:      3600,
		PurgeInterval:     -30 * 24 * time.Hour,
		PurgeKeep:         0,
		SumAlgo:           "none",
		CfgFile:           defaultCfgFile,
		TimeFormat:        timeFormat,
		WithRolePasswords: true,
		Upload:            "none",
		Download:          "none",
		ListRemote:        "none",
		AzureEndpoint:     "blob.core.windows.net",
	}
}

// parseCliResult is use to handle utility flags like help, version, that make
// the program end early
type parseCliResult struct {
	ShowHelp     bool
	ShowVersion  bool
	LegacyConfig string
	ShowConfig   bool
}

func (*parseCliResult) Error() string {
	return "please exit now"
}

func validateDumpFormat(s string) error {
	for _, format := range []string{"plain", "custom", "tar", "directory"} {
		// PostgreSQL tools allow the full name of the format and the
		// first letter
		if s == format || s == string([]rune(format)[0]) {
			return nil
		}
	}
	return fmt.Errorf("invalid dump format %q", s)
}

func validatePurgeKeepValue(k string) (int, error) {
	// returning -1 means keep all dumps
	if k == "all" {
		return -1, nil
	}

	keep, err := strconv.ParseInt(k, 10, 0)
	if err != nil {
		// return -1 too when the input is not convertible to an int
		return -1, fmt.Errorf("Invalid input for keep: %w", err)
	}

	if keep < 0 {
		return -1, fmt.Errorf("Invalid input for keep: negative value: %d", keep)
	}

	return int(keep), nil
}

func validatePurgeTimeLimitValue(i string) (time.Duration, error) {
	if days, err := strconv.ParseInt(i, 10, 0); err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return 0, errors.New("Invalid input for purge interval, number too big")
		}
	} else {
		return time.Duration(-days*24) * time.Hour, nil
	}

	d, err := time.ParseDuration(i)
	if err != nil {
		return 0, err
	}
	return -d, nil

}

func validateYesNoOption(s string) (bool, error) {
	ls := strings.TrimSpace(strings.ToLower(s))
	if ls == "y" || ls == "yes" {
		return true, nil
	}

	if ls == "n" || ls == "no" {
		return false, nil
	}

	return false, fmt.Errorf("value must be \"yes\" or \"no\"")
}

func validateEnum(s string, candidates []string) error {
	found := false
	ls := strings.TrimSpace(strings.ToLower(s))
	for _, v := range candidates {
		if v == ls {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("value not found in %v", candidates)
	}

	return nil
}

func validateDirectory(s string) error {
	fi, err := os.Stat(s)
	if err != nil {
		return err
	}

	if !fi.IsDir() {
		return fmt.Errorf("not a directory")
	}

	return nil
}

func parseCli(args []string) (options, []string, error) {
	var format, purgeKeep, purgeInterval string

	opts := defaultOptions()
	pce := &parseCliResult{}

	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "pg_back dumps some PostgreSQL databases\n\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  pg_back [OPTION]... [DBNAME]...\n")
		fmt.Fprintf(os.Stderr, "\nOptions:\n")
		pflag.CommandLine.SortFlags = false
		pflag.PrintDefaults()
	}

	pflag.BoolVar(&opts.NoConfigFile, "no-config-file", false, "skip reading config file\n")
	pflag.StringVarP(&opts.BinDirectory, "bin-directory", "B", "", "PostgreSQL binaries directory. Empty to search $PATH")
	pflag.StringVarP(&opts.Directory, "backup-directory", "b", "/var/backups/postgresql", "store dump files there")
	pflag.StringVarP(&opts.CfgFile, "config", "c", defaultCfgFile, "alternate config file")
	pflag.StringSliceVarP(&opts.ExcludeDbs, "exclude-dbs", "D", []string{}, "list of databases to exclude")
	pflag.BoolVarP(&opts.WithTemplates, "with-templates", "t", false, "include templates")
	WithoutTemplates := pflag.Bool("without-templates", false, "force exclude templates")
	pflag.BoolVar(&opts.WithRolePasswords, "with-role-passwords", true, "dump globals with role passwords")
	WithoutRolePasswords := pflag.Bool("without-role-passwords", false, "do not dump passwords of roles")
	pflag.BoolVar(&opts.DumpOnly, "dump-only", false, "only dump databases, excluding configuration and globals")
	pflag.IntVarP(&opts.PauseTimeout, "pause-timeout", "T", 3600, "abort if replication cannot be paused after this number\nof seconds")
	pflag.IntVarP(&opts.Jobs, "jobs", "j", 1, "dump this many databases concurrently")
	pflag.StringVarP(&format, "format", "F", "custom", "database dump format: plain, custom, tar or directory")
	pflag.IntVarP(&opts.DirJobs, "parallel-backup-jobs", "J", 1, "number of parallel jobs to dumps when using directory format")
	pflag.IntVarP(&opts.CompressLevel, "compress", "Z", -1, "compression level for compressed formats")
	pflag.StringVarP(&opts.SumAlgo, "checksum-algo", "S", "none", "signature algorithm: none sha1 sha224 sha256 sha384 sha512")
	pflag.StringVarP(&purgeInterval, "purge-older-than", "P", "30", "purge backups older than this duration in days\nuse an interval with units \"s\" (seconds), \"m\" (minutes) or \"h\" (hours)\nfor less than a day.")
	pflag.StringVarP(&purgeKeep, "purge-min-keep", "K", "0", "minimum number of dumps to keep when purging or 'all' to keep\neverything")
	pflag.StringVar(&opts.PreHook, "pre-backup-hook", "", "command to run before taking dumps")
	pflag.StringVar(&opts.PostHook, "post-backup-hook", "", "command to run after taking dumps\n")

	pflag.BoolVar(&opts.Encrypt, "encrypt", false, "encrypt the dumps")
	NoEncrypt := pflag.Bool("no-encrypt", false, "do not encrypt the dumps")
	pflag.BoolVar(&opts.EncryptKeepSrc, "encrypt-keep-src", false, "keep original files when encrypting")
	NoEncryptKeepSrc := pflag.Bool("no-encrypt-keep-src", false, "do not keep original files when encrypting")
	pflag.BoolVar(&opts.Decrypt, "decrypt", false, "decrypt files in the backup directory instead of dumping. DBNAMEs become\nglobs to select files")
	pflag.StringVar(&opts.CipherPassphrase, "cipher-pass", "", "cipher passphrase for encryption and decryption\n")
	pflag.StringVar(&opts.CipherPublicKey, "cipher-public-key", "", "AGE public key for encryption; in Bech32 encoding starting with 'age1'\n")
	pflag.StringVar(&opts.CipherPrivateKey, "cipher-private-key", "", "AGE private key for decryption; in Bech32 encoding starting with 'AGE-SECRET-KEY-1'\n")

	pflag.StringVar(&opts.Upload, "upload", "none", "upload produced files to target (s3, gcs,..) use \"none\" to override\nconfiguration file and disable upload")
	pflag.StringVar(&opts.Download, "download", "none", "download files from target (s3, gcs,..) instead of dumping. DBNAMEs become\nglobs to select files")
	pflag.StringVar(&opts.ListRemote, "list-remote", "none", "list the remote files on s3, gcs, sftp, azure instead of dumping. DBNAMEs become\nglobs to select files")
	purgeRemote := pflag.String("purge-remote", "no", "purge the file on remote location after upload, with the same rules\nas the local directory")

	pflag.StringVar(&opts.S3Region, "s3-region", "", "S3 region")
	pflag.StringVar(&opts.S3Bucket, "s3-bucket", "", "S3 bucket")
	pflag.StringVar(&opts.S3Profile, "s3-profile", "", "AWS client profile name to get credentials")
	pflag.StringVar(&opts.S3KeyID, "s3-key-id", "", "AWS Access key ID")
	pflag.StringVar(&opts.S3Secret, "s3-secret", "", "AWS Secret access key")
	pflag.StringVar(&opts.S3EndPoint, "s3-endpoint", "", "S3 endpoint URI")
	S3ForcePath := pflag.String("s3-force-path", "no", "force path style addressing instead of virtual hosted bucket\naddressing")
	S3UseTLS := pflag.String("s3-tls", "yes", "enable or disable TLS on requests")

	pflag.StringVar(&opts.SFTPHost, "sftp-host", "", "Remote hostname for SFTP")
	pflag.StringVar(&opts.SFTPPort, "sftp-port", "", "Remote port for SFTP")
	pflag.StringVar(&opts.SFTPUsername, "sftp-user", "", "Login for SFTP when different than the current user")
	pflag.StringVar(&opts.SFTPPassword, "sftp-password", "", "Password for SFTP or passphrase when identity file is set")
	pflag.StringVar(&opts.SFTPDirectory, "sftp-directory", "", "Target directory on the remote host")
	pflag.StringVar(&opts.SFTPIdentityFile, "sftp-identity", "", "Path to a private key")
	SFTPIgnoreHostKey := pflag.String("sftp-ignore-hostkey", "no", "Check the target host key against local known hosts")

	pflag.StringVar(&opts.GCSBucket, "gcs-bucket", "", "GCS bucket name")
	pflag.StringVar(&opts.GCSEndPoint, "gcs-endpoint", "", "GCS endpoint URL")
	pflag.StringVar(&opts.GCSCredentialsFile, "gcs-keyfile", "", "path to the GCS credentials file")

	pflag.StringVar(&opts.AzureContainer, "azure-container", "", "Azure Blob Container")
	pflag.StringVar(&opts.AzureAccount, "azure-account", "", "Azure Blob Storage account")
	pflag.StringVar(&opts.AzureKey, "azure-key", "", "Azure Blob Storage shared key")
	pflag.StringVar(&opts.AzureEndpoint, "azure-endpoint", "blob.core.windows.net", "Azure Blob Storage endpoint")

	pflag.StringVarP(&opts.Host, "host", "h", "", "database server host or socket directory")
	pflag.IntVarP(&opts.Port, "port", "p", 0, "database server port number")
	pflag.StringVarP(&opts.Username, "username", "U", "", "connect as specified database user")
	pflag.StringVarP(&opts.ConnDb, "dbname", "d", "", "connect to database name\n")
	pflag.StringVar(&pce.LegacyConfig, "convert-legacy-config", "", "convert a pg_back v1 configuration file")
	pflag.BoolVar(&pce.ShowConfig, "print-default-config", false, "print the default configuration\n")
	pflag.BoolVarP(&opts.Quiet, "quiet", "q", false, "quiet mode")
	pflag.BoolVarP(&opts.Verbose, "verbose", "v", false, "verbose mode\n")
	pflag.BoolVarP(&pce.ShowHelp, "help", "?", false, "print usage")
	pflag.BoolVarP(&pce.ShowVersion, "version", "V", false, "print version")

	// Do not use the default pflag.Parse() that use os.Args[1:],
	// but pass it explicitly so that unit-tests can feed any set
	// of flags
	pflag.CommandLine.Parse(args)

	// Record the list of flags set on the command line to allow
	// overriding the configuration later, if an alternate
	// configuration file has been provided
	changed := make([]string, 0)
	pflag.Visit(func(f *pflag.Flag) {
		changed = append(changed, f.Name)
	})

	// To override with_templates = true on the command line and
	// make it false, we have to ensure MergeCliAndConfigOptions()
	// use the cli value
	if *WithoutTemplates {
		opts.WithTemplates = false
		changed = append(changed, "with-templates")
	}

	// Same for dump_role_passwords
	if *WithoutRolePasswords {
		opts.WithRolePasswords = false
		changed = append(changed, "with-role-passwords")
	}

	// To override encrypt = true from the config file on the command line,
	// have MergeCliAndConfigOptions() use the false value
	if *NoEncrypt {
		opts.Encrypt = false
		changed = append(changed, "encrypt")
	}

	// Same for encrypt_keep_source = true in the config file
	if *NoEncryptKeepSrc {
		opts.EncryptKeepSrc = false
		changed = append(changed, "encrypt-keep-src")
	}

	// When --help or --version is given print and tell the caller
	// through the error to exit
	if pce.ShowHelp {
		pflag.Usage()
		return opts, changed, pce
	}

	if pce.ShowVersion {
		fmt.Printf("pg_back version %v\n", version)
		return opts, changed, pce
	}

	if len(pce.LegacyConfig) > 0 {
		return opts, changed, pce
	}

	if pce.ShowConfig {
		fmt.Print(defaultCfg)
		return opts, changed, pce
	}

	opts.Dbnames = pflag.Args()

	// When a list of databases have been provided ensure it will
	// override the one from the configuration when options are
	// merged
	if len(opts.Dbnames) > 0 {
		changed = append(changed, "include-dbs")
	}

	// Validate purge keep and time limit
	keep, err := validatePurgeKeepValue(purgeKeep)
	if err != nil {
		return opts, changed, err
	}
	opts.PurgeKeep = keep

	interval, err := validatePurgeTimeLimitValue(purgeInterval)
	if err != nil {
		return opts, changed, err
	}
	opts.PurgeInterval = interval

	if opts.CompressLevel < -1 || opts.CompressLevel > 9 {
		return opts, changed, fmt.Errorf("compression level must be in range 0..9")
	}

	if opts.Jobs < 1 {
		return opts, changed, fmt.Errorf("concurrent jobs (-j) cannot be less than 1")
	}

	if err := validateDumpFormat(format); err != nil {
		return opts, changed, err
	}

	opts.Format = []rune(format)[0]

	if opts.Encrypt && opts.Decrypt {
		return opts, changed, fmt.Errorf("options --encrypt and --decrypt are mutually exclusive")
	}

	if opts.CipherPassphrase != "" && opts.CipherPublicKey != "" {
		return opts, changed, fmt.Errorf("only one of --cipher-pass or --cipher-public-key allowed")
	}

	if opts.CipherPassphrase != "" && opts.CipherPrivateKey != "" {
		return opts, changed, fmt.Errorf("only one of --cipher-pass or --cipher-private-key allowed")
	}

	if opts.BinDirectory != "" {
		if err := validateDirectory(opts.BinDirectory); err != nil {
			return opts, changed, fmt.Errorf("bin directory (-B) must be an existing directory")
		}
	}

	// Validate upload and download options
	stores := []string{"none", "s3", "sftp", "gcs", "azure"}
	if err := validateEnum(opts.Upload, stores); err != nil {
		return opts, changed, fmt.Errorf("invalid value for --upload: %s", err)
	}

	if err := validateEnum(opts.Download, stores); err != nil {
		return opts, changed, fmt.Errorf("invalid value for --download: %s", err)
	}

	if err := validateEnum(opts.ListRemote, stores); err != nil {
		return opts, changed, fmt.Errorf("invalid value for --list-remote: %s", err)
	}

	opts.PurgeRemote, err = validateYesNoOption(*purgeRemote)
	if err != nil {
		return opts, changed, fmt.Errorf("invalid value for --purge-remote: %s", err)
	}

	for _, o := range []string{opts.Upload, opts.Download, opts.ListRemote} {
		switch o {
		case "s3":
			// Validate S3 options
			opts.S3ForcePath, err = validateYesNoOption(*S3ForcePath)
			if err != nil {
				return opts, changed, fmt.Errorf("invalid value for --s3-force-path: %s", err)
			}

			S3WithTLS, err := validateYesNoOption(*S3UseTLS)
			if err != nil {
				return opts, changed, fmt.Errorf("invalid value for --s3-tls: %s", err)
			}
			opts.S3DisableTLS = !S3WithTLS

		case "sftp":
			opts.SFTPIgnoreKnownHosts, err = validateYesNoOption(*SFTPIgnoreHostKey)
			if err != nil {
				return opts, changed, fmt.Errorf("invalid value for --sftp-ignore-hostkey: %s", err)
			}
		}
	}

	return opts, changed, nil
}

func validateConfigurationFile(cfg *ini.File) error {
	s, _ := cfg.GetSection(ini.DefaultSection)
	known_globals := []string{
		"bin_directory", "backup_directory", "timestamp_format", "host", "port", "user",
		"dbname", "exclude_dbs", "include_dbs", "with_templates", "format",
		"parallel_backup_jobs", "compress_level", "jobs", "pause_timeout",
		"purge_older_than", "purge_min_keep", "checksum_algorithm", "pre_backup_hook",
		"post_backup_hook", "encrypt", "cipher_pass", "cipher_public_key", "cipher_private_key",
		"encrypt_keep_source", "upload", "purge_remote", "s3_region", "s3_bucket", "s3_endpoint",
		"s3_profile", "s3_key_id", "s3_secret", "s3_force_path", "s3_tls", "sftp_host",
		"sftp_port", "sftp_user", "sftp_password", "sftp_directory", "sftp_identity",
		"sftp_ignore_hostkey", "gcs_bucket", "gcs_endpoint", "gcs_keyfile",
		"azure_container", "azure_account", "azure_key", "azure_endpoint", "pg_dump_options",
		"dump_role_passwords", "dump_only",
	}

gkLoop:
	for _, v := range s.KeyStrings() {
		for _, c := range known_globals {
			if v == c {
				continue gkLoop
			}
		}

		return fmt.Errorf("unknown parameter in configuration file: %s", v)
	}

	subs := cfg.Sections()
	knonw_perdb := []string{
		"format", "parallel_backup_jobs", "compress_level", "checksum_algorithm",
		"purge_older_than", "purge_min_keep", "schemas", "exclude_schemas", "tables",
		"exclude_tables", "pg_dump_options", "with_blobs",
	}

	for _, sub := range subs {
		if sub.Name() == ini.DefaultSection {
			continue
		}

	dbkLoop:
		for _, v := range sub.KeyStrings() {
			for _, c := range knonw_perdb {
				if v == c {
					continue dbkLoop
				}
			}

			return fmt.Errorf("unknown parameter in configuration file for db %s: %s", sub.Name(), v)
		}
	}

	return nil
}

func loadConfigurationFile(path string) (options, error) {
	var format, purgeKeep, purgeInterval string

	opts := defaultOptions()

	cfg, err := ini.Load(path)
	if err != nil {
		if path == defaultCfgFile && errors.Is(err, os.ErrNotExist) {
			// Fallback on defaults when the default configuration does not exist
			l.Verbosef("default configuration file %s does not exist, skipping\n", defaultCfgFile)
			return opts, nil
		}

		return opts, fmt.Errorf("Could load configuration file: %v", err)
	}

	if err := validateConfigurationFile(cfg); err != nil {
		return opts, fmt.Errorf("could not validate %s: %w", path, err)
	}

	s, _ := cfg.GetSection(ini.DefaultSection)

	// Read all configuration parameters ensuring the destination
	// struct member has the same default value as the commandline
	// flags
	opts.BinDirectory = s.Key("bin_directory").MustString("")
	opts.Directory = s.Key("backup_directory").MustString("/var/backups/postgresql")
	timeFormat := s.Key("timestamp_format").MustString("rfc3339")
	opts.Host = s.Key("host").MustString("")
	opts.Port = s.Key("port").MustInt(0)
	opts.Username = s.Key("user").MustString("")
	opts.ConnDb = s.Key("dbname").MustString("")
	opts.ExcludeDbs = s.Key("exclude_dbs").Strings(",")
	opts.Dbnames = s.Key("include_dbs").Strings(",")
	opts.WithTemplates = s.Key("with_templates").MustBool(false)
	opts.WithRolePasswords = s.Key("dump_role_passwords").MustBool(true)
	opts.DumpOnly = s.Key("dump_only").MustBool(false)
	format = s.Key("format").MustString("custom")
	opts.DirJobs = s.Key("parallel_backup_jobs").MustInt(1)
	opts.CompressLevel = s.Key("compress_level").MustInt(-1)
	opts.Jobs = s.Key("jobs").MustInt(1)
	opts.PauseTimeout = s.Key("pause_timeout").MustInt(3600)
	purgeInterval = s.Key("purge_older_than").MustString("30")
	purgeKeep = s.Key("purge_min_keep").MustString("0")
	opts.SumAlgo = s.Key("checksum_algorithm").MustString("none")
	opts.PreHook = s.Key("pre_backup_hook").MustString("")
	opts.PostHook = s.Key("post_backup_hook").MustString("")
	opts.Encrypt = s.Key("encrypt").MustBool(false)
	opts.CipherPassphrase = s.Key("cipher_pass").MustString("")
	opts.CipherPublicKey = s.Key("cipher_public_key").MustString("")
	opts.CipherPrivateKey = s.Key("cipher_private_key").MustString("")
	opts.EncryptKeepSrc = s.Key("encrypt_keep_source").MustBool(false)

	opts.Upload = s.Key("upload").MustString("none")
	opts.PurgeRemote = s.Key("purge_remote").MustBool(false)

	opts.S3Region = s.Key("s3_region").MustString("")
	opts.S3Bucket = s.Key("s3_bucket").MustString("")
	opts.S3EndPoint = s.Key("s3_endpoint").MustString("")
	opts.S3Profile = s.Key("s3_profile").MustString("")
	opts.S3KeyID = s.Key("s3_key_id").MustString("")
	opts.S3Secret = s.Key("s3_secret").MustString("")
	opts.S3ForcePath = s.Key("s3_force_path").MustBool(false)
	opts.S3DisableTLS = !s.Key("s3_tls").MustBool(true)

	opts.SFTPHost = s.Key("sftp_host").MustString("")
	opts.SFTPPort = s.Key("sftp_port").MustString("")
	opts.SFTPUsername = s.Key("sftp_user").MustString("")
	opts.SFTPPassword = s.Key("sftp_password").MustString("")
	opts.SFTPDirectory = s.Key("sftp_directory").MustString("")
	opts.SFTPIdentityFile = s.Key("sftp_identity").MustString("")
	opts.SFTPIgnoreKnownHosts = s.Key("sftp_ignore_hostkey").MustBool(false)

	opts.GCSBucket = s.Key("gcs_bucket").MustString("")
	opts.GCSEndPoint = s.Key("gcs_endpoint").MustString("")
	opts.GCSCredentialsFile = s.Key("gcs_keyfile").MustString("")

	opts.AzureContainer = s.Key("azure_container").MustString("")
	opts.AzureAccount = s.Key("azure_account").MustString("")
	opts.AzureKey = s.Key("azure_key").MustString("")
	opts.AzureEndpoint = s.Key("azure_endpoint").MustString("blob.core.windows.net")

	// Validate purge keep and time limit
	keep, err := validatePurgeKeepValue(purgeKeep)
	if err != nil {
		return opts, err
	}
	opts.PurgeKeep = keep

	interval, err := validatePurgeTimeLimitValue(purgeInterval)
	if err != nil {
		return opts, err
	}
	opts.PurgeInterval = interval

	if opts.CompressLevel < -1 || opts.CompressLevel > 9 {
		return opts, fmt.Errorf("compression level must be in range 0..9")
	}

	if opts.Jobs < 1 {
		return opts, fmt.Errorf("jobs cannot be less than 1")
	}

	if err := validateDumpFormat(format); err != nil {
		return opts, err
	}
	opts.Format = []rune(format)[0]

	if opts.BinDirectory != "" {
		if err := validateDirectory(opts.BinDirectory); err != nil {
			return opts, fmt.Errorf("bin_directory must be an existing directory")
		}
	}

	// Validate upload option
	stores := []string{"none", "s3", "sftp", "gcs", "azure"}
	if err := validateEnum(opts.Upload, stores); err != nil {
		return opts, fmt.Errorf("invalid value for upload: %s", err)
	}

	// Validate the value of the timestamp format. Force the use of legacy
	// on windows to avoid failure when creating filenames with the
	// timestamp
	if runtime.GOOS == "windows" {
		timeFormat = "legacy"
	}

	switch timeFormat {
	case "legacy":
		opts.TimeFormat = "2006-01-02_15-04-05"
	case "rfc3339":
	default:
		return opts, fmt.Errorf("unknown timestamp format: %s", timeFormat)
	}

	// Parse the pg_dump options as a list of args
	words, err := shlex.Split(s.Key("pg_dump_options").String(), true)
	if err != nil {
		return opts, fmt.Errorf("unable to parse pg_dump_options: %w", err)
	}
	opts.PgDumpOpts = words

	// Process all sections with database specific configuration,
	// fallback on the values of the global section
	subs := cfg.Sections()
	opts.PerDbOpts = make(map[string]*dbOpts, len(subs))

	for _, s := range subs {
		if s.Name() == ini.DefaultSection {
			continue
		}

		var dbFormat, dbPurgeInterval, dbPurgeKeep string

		o := dbOpts{}
		dbFormat = s.Key("format").MustString(format)
		o.Jobs = s.Key("parallel_backup_jobs").MustInt(opts.DirJobs)
		o.CompressLevel = s.Key("compress_level").MustInt(opts.CompressLevel)
		o.SumAlgo = s.Key("checksum_algorithm").MustString(opts.SumAlgo)
		dbPurgeInterval = s.Key("purge_older_than").MustString(purgeInterval)
		dbPurgeKeep = s.Key("purge_min_keep").MustString(purgeKeep)

		// Validate purge keep and time limit
		keep, err := validatePurgeKeepValue(dbPurgeKeep)
		if err != nil {
			return opts, err
		}
		o.PurgeKeep = keep

		interval, err := validatePurgeTimeLimitValue(dbPurgeInterval)
		if err != nil {
			return opts, err
		}
		o.PurgeInterval = interval

		if o.CompressLevel < -1 || o.CompressLevel > 9 {
			return opts, fmt.Errorf("compression level must be in range 0..9")
		}

		if err := validateDumpFormat(dbFormat); err != nil {
			return opts, err
		}
		o.Format = []rune(dbFormat)[0]

		o.Schemas = s.Key("schemas").Strings(",")
		o.ExcludedSchemas = s.Key("exclude_schemas").Strings(",")
		o.Tables = s.Key("tables").Strings(",")
		o.ExcludedTables = s.Key("exclude_tables").Strings(",")

		if s.HasKey("pg_dump_options") {
			words, err := shlex.Split(s.Key("pg_dump_options").String(), true)
			if err != nil {
				return opts, fmt.Errorf("unable to parse pg_dump_options for %s: %w", s.Name(), err)
			}
			o.PgDumpOpts = words
		} else {
			o.PgDumpOpts = opts.PgDumpOpts
		}

		if s.HasKey("with_blobs") {
			if wb, err := s.Key("with_blobs").Bool(); err != nil {
				return opts, fmt.Errorf("unable to parse with_blobs for %s: %w", s.Name(), err)
			} else if wb {
				o.WithBlobs = 1
			} else {
				o.WithBlobs = 2
			}
		}

		opts.PerDbOpts[s.Name()] = &o
	}

	return opts, nil
}

func mergeCliAndConfigOptions(cliOpts options, configOpts options, onCli []string) options {
	opts := configOpts

	// Command line values take precedence on everything, including per
	// database options
	for _, o := range onCli {
		switch o {
		case "bin-directory":
			opts.BinDirectory = cliOpts.BinDirectory
		case "backup-directory":
			opts.Directory = cliOpts.Directory
		case "exclude-dbs":
			opts.ExcludeDbs = cliOpts.ExcludeDbs
		case "include-dbs":
			opts.Dbnames = cliOpts.Dbnames
		case "with-templates":
			opts.WithTemplates = cliOpts.WithTemplates
		case "with-role-passwords":
			opts.WithRolePasswords = cliOpts.WithRolePasswords
		case "dump-only":
			opts.DumpOnly = cliOpts.DumpOnly
		case "pause-timeout":
			opts.PauseTimeout = cliOpts.PauseTimeout
		case "jobs":
			opts.Jobs = cliOpts.Jobs
		case "format":
			opts.Format = cliOpts.Format
			for _, dbo := range opts.PerDbOpts {
				dbo.Format = cliOpts.Format
			}
		case "parallel-backup-jobs":
			opts.DirJobs = cliOpts.DirJobs
			for _, dbo := range opts.PerDbOpts {
				dbo.Jobs = cliOpts.DirJobs
			}
		case "compress":
			opts.CompressLevel = cliOpts.CompressLevel
			for _, dbo := range opts.PerDbOpts {
				dbo.CompressLevel = cliOpts.CompressLevel
			}
		case "checksum-algo":
			opts.SumAlgo = cliOpts.SumAlgo
			for _, dbo := range opts.PerDbOpts {
				dbo.SumAlgo = cliOpts.SumAlgo
			}
		case "purge-older-than":
			opts.PurgeInterval = cliOpts.PurgeInterval
			for _, dbo := range opts.PerDbOpts {
				dbo.PurgeInterval = cliOpts.PurgeInterval
			}
		case "purge-min-keep":
			opts.PurgeKeep = cliOpts.PurgeKeep
			for _, dbo := range opts.PerDbOpts {
				dbo.PurgeKeep = cliOpts.PurgeKeep
			}
		case "pre-backup-hook":
			opts.PreHook = cliOpts.PreHook
		case "post-backup-hook":
			opts.PostHook = cliOpts.PostHook
		case "encrypt":
			opts.Encrypt = cliOpts.Encrypt
		case "encrypt-keep-src":
			opts.EncryptKeepSrc = cliOpts.EncryptKeepSrc
		case "cipher-pass":
			opts.CipherPassphrase = cliOpts.CipherPassphrase
		case "cipher-public-key":
			opts.CipherPublicKey = cliOpts.CipherPublicKey
		case "cipher-private-key":
			opts.CipherPrivateKey = cliOpts.CipherPrivateKey
		case "decrypt":
			opts.Decrypt = cliOpts.Decrypt

		case "upload":
			opts.Upload = cliOpts.Upload
		case "download":
			opts.Download = cliOpts.Download
		case "list-remote":
			opts.ListRemote = cliOpts.ListRemote
		case "purge-remote":
			opts.PurgeRemote = cliOpts.PurgeRemote

		case "s3-region":
			opts.S3Region = cliOpts.S3Region
		case "s3-bucket":
			opts.S3Bucket = cliOpts.S3Bucket
		case "s3-profile":
			opts.S3Profile = cliOpts.S3Profile
		case "s3-key-id":
			opts.S3KeyID = cliOpts.S3KeyID
		case "s3-secret":
			opts.S3Secret = cliOpts.S3Secret
		case "s3-endpoint":
			opts.S3EndPoint = cliOpts.S3EndPoint
		case "s3-force-path":
			opts.S3ForcePath = cliOpts.S3ForcePath
		case "s3-tls":
			opts.S3DisableTLS = cliOpts.S3DisableTLS

		case "sftp-host":
			opts.SFTPHost = cliOpts.SFTPHost
		case "sftp-port":
			opts.SFTPPort = cliOpts.SFTPPort
		case "sftp-user":
			opts.SFTPUsername = cliOpts.SFTPUsername
		case "sftp-password":
			opts.SFTPPassword = cliOpts.SFTPPassword
		case "sftp-directory":
			opts.SFTPDirectory = cliOpts.SFTPDirectory
		case "sftp-identity":
			opts.SFTPIdentityFile = cliOpts.SFTPIdentityFile
		case "sftp-ignore-hostkey":
			opts.SFTPIgnoreKnownHosts = cliOpts.SFTPIgnoreKnownHosts

		case "gcs-bucket":
			opts.GCSBucket = cliOpts.GCSBucket
		case "gcs-endpoint":
			opts.GCSEndPoint = cliOpts.GCSEndPoint
		case "gcs-keyfile":
			opts.GCSCredentialsFile = cliOpts.GCSCredentialsFile

		case "azure-container":
			opts.AzureContainer = cliOpts.AzureContainer
		case "azure-account":
			opts.AzureAccount = cliOpts.AzureAccount
		case "azure-key":
			opts.AzureKey = cliOpts.AzureKey
		case "azure-endpoint":
			opts.AzureEndpoint = cliOpts.AzureEndpoint

		case "host":
			opts.Host = cliOpts.Host
		case "port":
			opts.Port = cliOpts.Port
		case "username":
			opts.Username = cliOpts.Username
		case "dbname":
			opts.ConnDb = cliOpts.ConnDb
		}
	}

	return opts
}
