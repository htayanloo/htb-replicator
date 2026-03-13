package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/destfactory"
	"github.com/htb/htb-replicator/internal/metadata"
	"github.com/htb/htb-replicator/internal/sourcefactory"
)

var healthFullStats bool

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Run a full system health check and print a status report",
	Long: `Checks every component and prints a formatted status table:

  • Config    — validates all required fields and the cron schedule
  • Service   — detects whether the daemon is registered + running
  • Source    — connects to the S3 source bucket (lightweight ping)
  • Metadata  — opens the SQLite metadata database
  • Destinations — calls Ping() on every configured destination

With --full-stats the source bucket is fully enumerated (object count,
total size, newest/oldest object). This may be slow on large buckets.

Exit code 0 = all checks passed.
Exit code 1 = one or more checks failed.`,
	RunE: runHealth,
}

func init() {
	healthCmd.Flags().BoolVar(&healthFullStats, "full-stats", false,
		"Enumerate all source objects to show count and total size (slow on large buckets)")
}

// ── check result ─────────────────────────────────────────────────────────────

type checkStatus int

const (
	statusOK   checkStatus = iota
	statusWarn             // non-fatal
	statusFail             // fatal — exits 1
)

type checkResult struct {
	name    string
	status  checkStatus
	detail  string   // one-line summary shown in the table
	extra   []string // additional lines printed below the table
}

func ok(name, detail string, extra ...string) checkResult {
	return checkResult{name: name, status: statusOK, detail: detail, extra: extra}
}
func warn(name, detail string, extra ...string) checkResult {
	return checkResult{name: name, status: statusWarn, detail: detail, extra: extra}
}
func fail(name, detail string, extra ...string) checkResult {
	return checkResult{name: name, status: statusFail, detail: detail, extra: extra}
}

func (s checkStatus) label() string {
	switch s {
	case statusOK:
		return "  OK  "
	case statusWarn:
		return " WARN "
	default:
		return " FAIL "
	}
}

func (s checkStatus) badge() string {
	switch s {
	case statusOK:
		return green("[ OK ]")
	case statusWarn:
		return yellow("[WARN]")
	default:
		return red("[FAIL]")
	}
}

// ── ANSI colour helpers ───────────────────────────────────────────────────────

func green(s string) string  { return "\033[32m" + s + "\033[0m" }
func yellow(s string) string { return "\033[33m" + s + "\033[0m" }
func red(s string) string    { return "\033[31m" + s + "\033[0m" }
func bold(s string) string   { return "\033[1m" + s + "\033[0m" }
func dim(s string) string    { return "\033[2m" + s + "\033[0m" }

// ── main handler ─────────────────────────────────────────────────────────────

func runHealth(cmd *cobra.Command, _ []string) error {
	timeout := 15 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var results []checkResult

	// 1. Config validation
	results = append(results, checkConfig())

	// 2. Service installation / running status
	results = append(results, checkService())

	// 3. Metadata DB
	results = append(results, checkMetadata(ctx))

	// 4. Source connectivity
	results = append(results, checkSource(ctx))

	// 5. Destinations
	results = append(results, checkDestinations(ctx)...)

	// ── Print report ─────────────────────────────────────────────────────────
	printReport(results)

	// Exit 1 if any check failed.
	for _, r := range results {
		if r.status == statusFail {
			return fmt.Errorf("one or more health checks failed")
		}
	}
	return nil
}

// ── individual checks ─────────────────────────────────────────────────────────

func checkConfig() checkResult {
	if err := cfg.Validate(); err != nil {
		lines := strings.Split(err.Error(), "\n")
		return fail("Config", "invalid — see details below", lines...)
	}

	scheduleInfo := ""
	if cfg.Schedule != "" {
		scheduleInfo = fmt.Sprintf("cron: %q", cfg.Schedule)
	} else {
		scheduleInfo = fmt.Sprintf("interval: %ds", cfg.IntervalSecs)
	}

	detail := fmt.Sprintf("OK  workers=%d  %s  destinations=%d",
		cfg.Workers, scheduleInfo, len(cfg.Destinations))
	return ok("Config", detail)
}

func checkService() checkResult {
	switch runtime.GOOS {
	case "linux":
		return checkSystemd()
	case "darwin":
		return checkLaunchd()
	case "windows":
		return checkWindowsService()
	default:
		return warn("Service", fmt.Sprintf("service check not supported on %s", runtime.GOOS))
	}
}

func checkSystemd() checkResult {
	// Is unit file installed?
	unitPath := "/etc/systemd/system/htb-replicator.service"
	if _, err := os.Stat(unitPath); os.IsNotExist(err) {
		return warn("Service (systemd)", "unit file not found — run: replicator service install")
	}

	// Is it enabled?
	enabledOut, _ := execOutput("systemctl", "is-enabled", "htb-replicator")
	enabled := strings.TrimSpace(enabledOut) == "enabled"

	// Is it active?
	activeOut, _ := execOutput("systemctl", "is-active", "htb-replicator")
	running := strings.TrimSpace(activeOut) == "active"

	switch {
	case enabled && running:
		return ok("Service (systemd)", "enabled + active (running)", unitPath)
	case enabled && !running:
		return warn("Service (systemd)", "enabled but NOT running — run: systemctl start htb-replicator", unitPath)
	default:
		return warn("Service (systemd)", "installed but not enabled — run: systemctl enable htb-replicator", unitPath)
	}
}

func checkLaunchd() checkResult {
	home, _ := os.UserHomeDir()
	paths := []string{
		home + "/Library/LaunchAgents/com.htb.htb-replicator.plist",
		"/Library/LaunchDaemons/com.htb.htb-replicator.plist",
	}
	installed := ""
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			installed = p
			break
		}
	}
	if installed == "" {
		return warn("Service (launchd)", "plist not found — run: replicator service install")
	}

	out, _ := execOutput("launchctl", "list", "com.htb.htb-replicator")
	running := strings.Contains(out, "com.htb.htb-replicator")

	if running {
		return ok("Service (launchd)", "loaded + running", installed)
	}
	return warn("Service (launchd)", "plist installed but not loaded — run: launchctl start com.htb.htb-replicator", installed)
}

func checkWindowsService() checkResult {
	out, err := execOutput("sc", "query", "htb-replicator")
	if err != nil {
		return warn("Service (Windows)", "service not found — run: replicator service install")
	}
	running := strings.Contains(strings.ToUpper(out), "RUNNING")
	if running {
		return ok("Service (Windows)", "running")
	}
	return warn("Service (Windows)", "installed but not running — run: replicator service start")
}

func checkMetadata(ctx context.Context) checkResult {
	start := time.Now()
	store, err := metadata.NewSQLiteStore(ctx, cfg.MetadataDB)
	if err != nil {
		return fail("Metadata DB", fmt.Sprintf("cannot open: %v", err),
			"path: "+cfg.MetadataDB)
	}
	defer store.Close()

	stats, err := store.GetStats(ctx)
	latency := time.Since(start)

	if err != nil {
		return fail("Metadata DB", fmt.Sprintf("query failed: %v", err),
			"path: "+cfg.MetadataDB)
	}

	detail := fmt.Sprintf("OK  path=%s  latency=%s  total=%d  synced=%d  failed=%d",
		cfg.MetadataDB, latency.Round(time.Millisecond),
		stats.TotalObjects, stats.SyncedObjects, stats.FailedObjects)

	if stats.FailedObjects > 0 {
		return warn("Metadata DB", detail,
			fmt.Sprintf("warning: %d object(s) in failed state — run: replicator status", stats.FailedObjects))
	}
	return ok("Metadata DB", detail)
}

func checkSource(ctx context.Context) checkResult {
	name := "Source (" + cfg.Source.Type + ")"
	addr := sourceAddress(cfg.Source)

	src, err := sourcefactory.New(cfg.Source)
	if err != nil {
		return fail(name,
			fmt.Sprintf("failed to build client: %v", err),
			fmt.Sprintf("type    : %s", cfg.Source.Type),
			fmt.Sprintf("address : %s", addr),
			fmt.Sprintf("error   : %v", err),
		)
	}
	defer src.Close()

	start := time.Now()
	err = src.Ping(ctx)
	latency := time.Since(start)

	extra := []string{
		fmt.Sprintf("type    : %s", cfg.Source.Type),
		fmt.Sprintf("address : %s", addr),
		fmt.Sprintf("latency : %s", latency.Round(time.Millisecond)),
	}

	if err != nil {
		extra = append(extra, fmt.Sprintf("error   : %v", err))
		return fail(name,
			fmt.Sprintf("UNREACHABLE  %s", addr),
			extra...,
		)
	}

	if healthFullStats {
		objects, listErr := src.ListAll(ctx)
		if listErr != nil {
			extra = append(extra, fmt.Sprintf("list error: %v", listErr))
		} else {
			var totalBytes int64
			var newest, oldest time.Time
			for _, obj := range objects {
				totalBytes += obj.Size
				if newest.IsZero() || obj.LastModified.After(newest) {
					newest = obj.LastModified
				}
				if oldest.IsZero() || obj.LastModified.Before(oldest) {
					oldest = obj.LastModified
				}
			}
			extra = append(extra,
				fmt.Sprintf("objects : %d", len(objects)),
				fmt.Sprintf("size    : %s", formatBytes(totalBytes)),
			)
			if !newest.IsZero() {
				extra = append(extra,
					fmt.Sprintf("newest  : %s", newest.Format(time.RFC3339)),
					fmt.Sprintf("oldest  : %s", oldest.Format(time.RFC3339)),
				)
			}
			detail := fmt.Sprintf("OK  %s  latency=%s  objects=%d  size=%s",
				addr, latency.Round(time.Millisecond), len(objects), formatBytes(totalBytes))
			return ok(name, detail, extra...)
		}
	}

	detail := fmt.Sprintf("OK  %s  latency=%s", addr, latency.Round(time.Millisecond))
	return ok(name, detail, extra...)
}

// sourceAddress formats a human-readable address string for the source.
func sourceAddress(scfg config.SourceConfig) string {
	get := func(key string) string {
		v, _ := scfg.Opts[key]
		s, _ := v.(string)
		return s
	}
	getInt := func(key string, def int) int {
		v, ok := scfg.Opts[key]
		if !ok {
			return def
		}
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
		return def
	}

	switch scfg.Type {
	case "s3":
		ep := get("endpoint")
		bucket := get("bucket")
		if ep != "" {
			return ep + "/" + bucket
		}
		region := get("region")
		return fmt.Sprintf("s3://%s (%s)", bucket, region)
	case "sftp":
		host := get("host")
		port := getInt("port", 22)
		base := get("base_path")
		return fmt.Sprintf("sftp://%s:%d%s", host, port, base)
	case "ftp":
		host := get("host")
		port := getInt("port", 21)
		base := get("base_path")
		return fmt.Sprintf("ftp://%s:%d%s", host, port, base)
	case "local":
		if p := get("path"); p != "" {
			return "local://" + p
		}
	}
	return scfg.Type
}

func checkDestinations(ctx context.Context) []checkResult {
	var results []checkResult

	for _, dcfg := range cfg.Destinations {
		results = append(results, checkDestination(ctx, dcfg))
	}
	return results
}

func checkDestination(ctx context.Context, dcfg config.DestinationConfig) checkResult {
	name := fmt.Sprintf("Dest %-8s [%s]", "("+dcfg.Type+")", dcfg.ID)

	// Extract address info from opts for display.
	addr := destAddress(dcfg)

	d, err := destfactory.New(dcfg)
	if err != nil {
		return fail(name,
			fmt.Sprintf("FAILED to build  %s", addr),
			fmt.Sprintf("id      : %s", dcfg.ID),
			fmt.Sprintf("type    : %s", dcfg.Type),
			fmt.Sprintf("address : %s", addr),
			fmt.Sprintf("error   : %v", err),
		)
	}
	defer d.Close()

	start := time.Now()
	err = d.Ping(ctx)
	latency := time.Since(start)

	extra := []string{
		fmt.Sprintf("id      : %s", dcfg.ID),
		fmt.Sprintf("type    : %s", dcfg.Type),
		fmt.Sprintf("address : %s", addr),
		fmt.Sprintf("latency : %s", latency.Round(time.Millisecond)),
	}
	if err != nil {
		extra = append(extra, fmt.Sprintf("error   : %v", err))
		return fail(name,
			fmt.Sprintf("UNREACHABLE  %s", addr),
			extra...,
		)
	}

	return ok(name,
		fmt.Sprintf("OK  %s  latency=%s", addr, latency.Round(time.Millisecond)),
		extra...,
	)
}

// ── report rendering ─────────────────────────────────────────────────────────

func printReport(results []checkResult) {
	now := time.Now().Format("2006-01-02 15:04:05")
	fmt.Println()
	fmt.Println(bold("┌─────────────────────────────────────────────────────────────────┐"))
	fmt.Printf( bold("│")+" HTB-Replicator health report — %-33s"+bold("│")+"\n", now)
	fmt.Println(bold("└─────────────────────────────────────────────────────────────────┘"))
	fmt.Println()

	// Table header
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, dim("  STATUS\tCOMPONENT\tDETAIL"))
	fmt.Fprintln(w, dim("  ──────\t─────────\t──────"))

	anyFail := false
	anyWarn := false

	for _, r := range results {
		fmt.Fprintf(w, "  %s\t%s\t%s\n", r.status.badge(), r.name, r.detail)
		if r.status == statusFail {
			anyFail = true
		}
		if r.status == statusWarn {
			anyWarn = true
		}
	}
	_ = w.Flush()

	// Extra details (indented below the table)
	hasExtra := false
	for _, r := range results {
		if len(r.extra) == 0 {
			continue
		}
		if !hasExtra {
			fmt.Println()
			fmt.Println(dim("  ── Details ────────────────────────────────────────────────────────"))
			hasExtra = true
		}
		fmt.Printf("\n  %s  %s\n", r.status.badge(), bold(r.name))
		for _, line := range r.extra {
			fmt.Printf("       %s\n", dim(line))
		}
	}

	// Overall summary
	fmt.Println()
	fmt.Println(dim("  ── Overall ─────────────────────────────────────────────────────────"))
	switch {
	case anyFail:
		fmt.Printf("  %s  %s\n\n", red("[FAIL]"), red("One or more checks failed — service may not work correctly."))
	case anyWarn:
		fmt.Printf("  %s  %s\n\n", yellow("[WARN]"), yellow("All critical checks passed — review warnings above."))
	default:
		fmt.Printf("  %s  %s\n\n", green("[ OK ]"), green("All checks passed."))
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// destAddress extracts a human-readable address string from destination opts.
func destAddress(dcfg config.DestinationConfig) string {
	get := func(key string) string {
		v, _ := dcfg.Opts[key]
		s, _ := v.(string)
		return s
	}
	getInt := func(key string, def int) int {
		v, ok := dcfg.Opts[key]
		if !ok {
			return def
		}
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		}
		return def
	}

	switch dcfg.Type {
	case "local":
		if p := get("path"); p != "" {
			return "local://" + p
		}
	case "s3":
		ep := get("endpoint")
		bucket := get("bucket")
		if ep != "" {
			return ep + "/" + bucket
		}
		region := get("region")
		return fmt.Sprintf("s3://%s (%s)", bucket, region)
	case "ftp":
		host := get("host")
		port := getInt("port", 21)
		base := get("base_path")
		return fmt.Sprintf("ftp://%s:%d%s", host, port, base)
	case "sftp":
		host := get("host")
		port := getInt("port", 22)
		base := get("base_path")
		return fmt.Sprintf("sftp://%s:%d%s", host, port, base)
	}
	return dcfg.Type
}

// execOutput runs a command and returns combined stdout output.
func execOutput(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).Output()
	return string(out), err
}

// formatBytes converts bytes to a human-readable string.
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
