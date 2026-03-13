package cli

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/htb/htb-replicator/internal/metadata"
)

// ANSI colour codes — only emitted when stdout is a TTY.
const (
	clReset   = "\033[0m"
	clBold    = "\033[1m"
	clDim     = "\033[2m"
	clRed     = "\033[31m"
	clGreen   = "\033[32m"
	clYellow  = "\033[33m"
	clBlue    = "\033[34m"
	clMagenta = "\033[35m"
	clCyan    = "\033[36m"
	clWhite   = "\033[37m"
)

var logsLast int
var logsDest string

var logsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Show colorful sync-run history with per-destination results",
	Long: `Reads the metadata store and prints a visual history of past sync runs.

Each run shows:
  • When it ran and how long it took
  • Source → every destination
  • Per-destination: ✓ synced count  ✗ failed count  bytes transferred

Examples:
  replicator logs                  # last 10 runs
  replicator logs --last 20        # last 20 runs
  replicator logs --dest sftp-bkp  # filter to one destination`,
	RunE: runLogs,
}

func init() {
	logsCmd.Flags().IntVarP(&logsLast, "last", "n", 10, "number of most-recent runs to show")
	logsCmd.Flags().StringVar(&logsDest, "dest", "", "filter output to this destination ID")
}

func runLogs(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	store, err := metadata.NewSQLiteStore(ctx, cfg.MetadataDB)
	if err != nil {
		return fmt.Errorf("open metadata store: %w", err)
	}
	defer store.Close()

	runs, err := store.ListSyncRuns(ctx, logsLast)
	if err != nil {
		return fmt.Errorf("list sync runs: %w", err)
	}

	tty := isTTY(os.Stdout)
	p := &logPrinter{tty: tty}

	p.header(len(runs), logsLast)

	if len(runs) == 0 {
		p.printf("%s  No sync runs recorded yet.%s\n", p.col(clDim), p.col(clReset))
		return nil
	}

	for i, run := range runs {
		// Fetch per-destination stats for the run's time window.
		windowEnd := time.Now()
		if run.FinishedAt != nil {
			windowEnd = *run.FinishedAt
		}
		destStats, err := store.GetDestinationStatsInWindow(ctx, run.StartedAt, windowEnd)
		if err != nil {
			destStats = nil // non-fatal — show the run without breakdown
		}

		// Apply --dest filter.
		if logsDest != "" {
			destStats = filterDest(destStats, logsDest)
		}

		p.run(i+1, run, destStats)
	}

	return nil
}

// ─── printer ──────────────────────────────────────────────────────────────────

type logPrinter struct{ tty bool }

func (p *logPrinter) col(code string) string {
	if p.tty {
		return code
	}
	return ""
}

func (p *logPrinter) printf(format string, a ...interface{}) {
	fmt.Printf(format, a...)
}

func (p *logPrinter) header(found, requested int) {
	width := 72
	title := fmt.Sprintf("  HTB-Replicator — Sync History  (showing %d of last %d)", found, requested)
	p.printf("%s%s┌%s┐%s\n", p.col(clCyan), p.col(clBold), strings.Repeat("─", width), p.col(clReset))
	p.printf("%s%s│%-*s│%s\n", p.col(clCyan), p.col(clBold), width, title, p.col(clReset))
	p.printf("%s%s└%s┘%s\n\n", p.col(clCyan), p.col(clBold), strings.Repeat("─", width), p.col(clReset))
}

func (p *logPrinter) run(n int, r metadata.SyncRun, dests []metadata.DestinationRunStat) {
	// ── Run header line ────────────────────────────────────────────────────────
	statusLabel, statusColor := runStatusLabel(r.Status)
	durStr := "running…"
	if r.FinishedAt != nil {
		durStr = r.FinishedAt.Sub(r.StartedAt).Round(time.Second).String()
	}

	p.printf(
		"%s%s  #%-3d%s  %s%s%s  %sduration: %s%s  %s%s%s\n",
		p.col(clBold), p.col(clWhite), n, p.col(clReset),
		p.col(clDim), r.StartedAt.Local().Format("2006-01-02 15:04:05"), p.col(clReset),
		p.col(clDim), durStr, p.col(clReset),
		p.col(clBold), p.col(statusColor)+statusLabel, p.col(clReset),
	)

	// ── Run meta line ──────────────────────────────────────────────────────────
	p.printf(
		"       %slisted: %s%-6d%s  submitted: %s%d%s\n",
		p.col(clDim),
		p.col(clReset)+p.col(clWhite), r.ObjectsListed, p.col(clReset),
		p.col(clWhite), r.TasksSubmitted, p.col(clReset),
	)

	// ── Source line ────────────────────────────────────────────────────────────
	srcType := cfg.Source.Type
	if srcType == "" {
		srcType = "s3"
	}
	p.printf(
		"       %ssource:%s %s%s%s\n",
		p.col(clDim), p.col(clReset),
		p.col(clMagenta)+p.col(clBold), srcType, p.col(clReset),
	)

	// ── Destination rows ───────────────────────────────────────────────────────
	if len(dests) == 0 {
		// Either no activity yet or all filtered out.
		for _, dc := range cfg.Destinations {
			if logsDest != "" && dc.ID != logsDest {
				continue
			}
			p.printf(
				"         %s→%s %-20s %s[%s]%s  %s—  no activity recorded%s\n",
				p.col(clCyan), p.col(clReset),
				dc.ID,
				p.col(clDim), dc.Type, p.col(clReset),
				p.col(clDim), p.col(clReset),
			)
		}
	} else {
		// Match configured destination types for display.
		destTypeMap := make(map[string]string, len(cfg.Destinations))
		for _, dc := range cfg.Destinations {
			destTypeMap[dc.ID] = dc.Type
		}

		for _, d := range dests {
			destType := destTypeMap[d.DestinationID]
			if destType == "" {
				destType = "?"
			}
			p.destRow(d, destType)
		}
	}

	if r.ErrorMessage != "" {
		p.printf(
			"       %s⚠ error: %s%s\n",
			p.col(clRed), r.ErrorMessage, p.col(clReset),
		)
	}

	p.printf("\n")
}

func (p *logPrinter) destRow(d metadata.DestinationRunStat, destType string) {
	// Synced count — green when > 0, dim when 0.
	syncedColor := p.col(clDim)
	if d.Synced > 0 {
		syncedColor = p.col(clGreen) + p.col(clBold)
	}

	// Failed count — red when > 0, dim when 0.
	failedColor := p.col(clDim)
	if d.Failed > 0 {
		failedColor = p.col(clRed) + p.col(clBold)
	}

	// Bytes — only show when something was transferred.
	bytesStr := ""
	if d.BytesWritten > 0 {
		bytesStr = fmt.Sprintf("  %s%s%s", p.col(clDim), formatBytes(d.BytesWritten), p.col(clReset))
	}

	p.printf(
		"         %s→%s %-20s %s[%-4s]%s  "+
			"%s✓ %4d synced%s   "+
			"%s✗ %4d failed%s"+
			"%s\n",
		p.col(clCyan), p.col(clReset),
		d.DestinationID,
		p.col(clBlue), destType, p.col(clReset),
		syncedColor, d.Synced, p.col(clReset),
		failedColor, d.Failed, p.col(clReset),
		bytesStr,
	)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func runStatusLabel(status string) (label, colorCode string) {
	switch strings.ToLower(status) {
	case "completed":
		return "[  OK  ]", clGreen
	case "failed":
		return "[ FAIL ]", clRed
	case "running":
		return "[  ...  ]", clYellow
	default:
		return "[" + status + "]", clDim
	}
}

func filterDest(stats []metadata.DestinationRunStat, id string) []metadata.DestinationRunStat {
	var out []metadata.DestinationRunStat
	for _, d := range stats {
		if d.DestinationID == id {
			out = append(out, d)
		}
	}
	return out
}

