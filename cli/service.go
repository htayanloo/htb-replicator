package cli

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"text/template"

	"github.com/spf13/cobra"
)

var serviceCmd = &cobra.Command{
	Use:   "service",
	Short: "Manage the system service (install, uninstall, start, stop, status)",
	Long: `Install and manage htb-replicator as a system service.

Supported platforms:
  Linux   — systemd  (requires root or sudo)
  macOS   — launchd  (installs to ~/Library/LaunchAgents or /Library/LaunchDaemons)
  Windows — NSSM     (requires NSSM installed and admin privileges)`,
}

var serviceInstallCmd = &cobra.Command{
	Use:   "install",
	Short: "Install the service into the system service manager",
	RunE:  runServiceInstall,
}

var serviceUninstallCmd = &cobra.Command{
	Use:   "uninstall",
	Short: "Remove the service from the system service manager",
	RunE:  runServiceUninstall,
}

var serviceStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the service via the system service manager",
	RunE:  runServiceStart,
}

var serviceStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the service via the system service manager",
	RunE:  runServiceStop,
}

var serviceStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show the service status via the system service manager",
	RunE:  runServiceStatus,
}

var (
	serviceUser       string
	serviceConfigPath string
	serviceSystem     bool
)

func init() {
	serviceInstallCmd.Flags().StringVar(&serviceUser, "user", "replicator",
		"User account the service will run as (Linux only)")
	serviceInstallCmd.Flags().StringVar(&serviceConfigPath, "config-path", "/etc/htb-replicator/config.yaml",
		"Absolute path to the config file used by the service")
	serviceInstallCmd.Flags().BoolVar(&serviceSystem, "system", false,
		"macOS: install as system-level daemon (/Library/LaunchDaemons). Default: user LaunchAgent")

	serviceCmd.AddCommand(
		serviceInstallCmd,
		serviceUninstallCmd,
		serviceStartCmd,
		serviceStopCmd,
		serviceStatusCmd,
	)
}

// ─── install ─────────────────────────────────────────────────────────────────

func runServiceInstall(cmd *cobra.Command, _ []string) error {
	binary, err := resolveExecutable()
	if err != nil {
		return err
	}
	switch runtime.GOOS {
	case "linux":
		return installSystemd(binary)
	case "darwin":
		return installLaunchd(binary)
	case "windows":
		return installWindows(binary)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

// ─── uninstall ───────────────────────────────────────────────────────────────

func runServiceUninstall(_ *cobra.Command, _ []string) error {
	switch runtime.GOOS {
	case "linux":
		return uninstallSystemd()
	case "darwin":
		return uninstallLaunchd()
	case "windows":
		return uninstallWindows()
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

// ─── start / stop / status ───────────────────────────────────────────────────

func runServiceStart(_ *cobra.Command, _ []string) error {
	switch runtime.GOOS {
	case "linux":
		return runCmd("systemctl", "start", systemdServiceName)
	case "darwin":
		return runCmd("launchctl", "start", launchdLabel)
	case "windows":
		return nssmCmd("start", windowsServiceName)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

func runServiceStop(_ *cobra.Command, _ []string) error {
	switch runtime.GOOS {
	case "linux":
		return runCmd("systemctl", "stop", systemdServiceName)
	case "darwin":
		return runCmd("launchctl", "stop", launchdLabel)
	case "windows":
		return nssmCmd("stop", windowsServiceName)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

func runServiceStatus(_ *cobra.Command, _ []string) error {
	switch runtime.GOOS {
	case "linux":
		return runCmd("systemctl", "status", systemdServiceName)
	case "darwin":
		return runCmd("launchctl", "list", launchdLabel)
	case "windows":
		return nssmCmd("status", windowsServiceName)
	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}

// ─── Linux / systemd ─────────────────────────────────────────────────────────

const (
	systemdServiceName = "htb-replicator"
	systemdUnitPath    = "/etc/systemd/system/htb-replicator.service"
)

const systemdUnitTmpl = `[Unit]
Description=htb-replicator S3 Replication Service
Documentation=https://github.com/htb/htb-replicator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User={{ .User }}
Group={{ .User }}
ExecStart={{ .Binary }} start --config {{ .Config }}
Restart=on-failure
RestartSec=10s
StandardOutput=journal
StandardError=journal
SyslogIdentifier=htb-replicator

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ReadWritePaths=/data /var/lib/htb-replicator

[Install]
WantedBy=multi-user.target
`

type systemdParams struct {
	Binary string
	User   string
	Config string
}

func installSystemd(binary string) error {
	content, err := renderTmpl(systemdUnitTmpl, systemdParams{
		Binary: binary,
		User:   serviceUser,
		Config: serviceConfigPath,
	})
	if err != nil {
		return fmt.Errorf("render systemd unit: %w", err)
	}
	if err := os.WriteFile(systemdUnitPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("write unit file %s (try with sudo): %w", systemdUnitPath, err)
	}
	fmt.Printf("wrote %s\n", systemdUnitPath)
	if err := runCmd("systemctl", "daemon-reload"); err != nil {
		return err
	}
	if err := runCmd("systemctl", "enable", systemdServiceName); err != nil {
		return err
	}
	fmt.Printf("\nService installed. Commands:\n  sudo systemctl start %s\n  journalctl -u %s -f\n",
		systemdServiceName, systemdServiceName)
	return nil
}

func uninstallSystemd() error {
	_ = runCmd("systemctl", "stop", systemdServiceName)
	_ = runCmd("systemctl", "disable", systemdServiceName)
	if err := os.Remove(systemdUnitPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove unit file: %w", err)
	}
	_ = runCmd("systemctl", "daemon-reload")
	fmt.Println("systemd service removed")
	return nil
}

// ─── macOS / launchd ─────────────────────────────────────────────────────────

const launchdLabel = "com.htb.htb-replicator"

const launchdPlistTmpl = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{{ .Label }}</string>

  <key>ProgramArguments</key>
  <array>
    <string>{{ .Binary }}</string>
    <string>start</string>
    <string>--config</string>
    <string>{{ .Config }}</string>
  </array>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>

  <key>StandardOutPath</key>
  <string>{{ .LogDir }}/htb-replicator.stdout.log</string>

  <key>StandardErrorPath</key>
  <string>{{ .LogDir }}/htb-replicator.stderr.log</string>

  <key>WorkingDirectory</key>
  <string>{{ .WorkDir }}</string>
</dict>
</plist>
`

type launchdParams struct {
	Label   string
	Binary  string
	Config  string
	LogDir  string
	WorkDir string
}

func installLaunchd(binary string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("get home dir: %w", err)
	}

	plistDir := filepath.Join(home, "Library", "LaunchAgents")
	logDir := filepath.Join(home, "Library", "Logs")
	if serviceSystem {
		plistDir = "/Library/LaunchDaemons"
		logDir = "/var/log"
	}

	for _, d := range []string{plistDir, logDir} {
		if err := os.MkdirAll(d, 0755); err != nil {
			return fmt.Errorf("create dir %s: %w", d, err)
		}
	}

	content, err := renderTmpl(launchdPlistTmpl, launchdParams{
		Label:   launchdLabel,
		Binary:  binary,
		Config:  serviceConfigPath,
		LogDir:  logDir,
		WorkDir: filepath.Dir(binary),
	})
	if err != nil {
		return fmt.Errorf("render plist: %w", err)
	}

	plistPath := filepath.Join(plistDir, launchdLabel+".plist")
	if err := os.WriteFile(plistPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("write plist %s: %w", plistPath, err)
	}
	fmt.Printf("wrote %s\n", plistPath)

	if err := runCmd("launchctl", "load", plistPath); err != nil {
		return err
	}
	fmt.Printf("\nService installed. Commands:\n  launchctl start %s\n  tail -f %s/htb-replicator.stdout.log\n",
		launchdLabel, logDir)
	return nil
}

func uninstallLaunchd() error {
	home, _ := os.UserHomeDir()
	plistPath := filepath.Join(home, "Library", "LaunchAgents", launchdLabel+".plist")
	if serviceSystem {
		plistPath = "/Library/LaunchDaemons/" + launchdLabel + ".plist"
	}
	_ = runCmd("launchctl", "unload", plistPath)
	if err := os.Remove(plistPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove plist: %w", err)
	}
	fmt.Println("launchd service removed")
	return nil
}

// ─── Windows / NSSM ──────────────────────────────────────────────────────────

const windowsServiceName = "htb-replicator"

func installWindows(binary string) error {
	nssmPath, err := exec.LookPath("nssm")
	if err != nil {
		return fmt.Errorf("nssm not found in PATH — download from https://nssm.cc/ and place in PATH")
	}

	appArgs := fmt.Sprintf("start --config %s", serviceConfigPath)
	commands := [][]string{
		{nssmPath, "install", windowsServiceName, binary},
		{nssmPath, "set", windowsServiceName, "AppParameters", appArgs},
		{nssmPath, "set", windowsServiceName, "DisplayName", "htb-replicator S3 Replication"},
		{nssmPath, "set", windowsServiceName, "Description", "Continuously replicates S3 objects to configured destinations"},
		{nssmPath, "set", windowsServiceName, "Start", "SERVICE_AUTO_START"},
	}
	for _, c := range commands {
		if err := runCmd(c[0], c[1:]...); err != nil {
			return err
		}
	}
	fmt.Printf("\nService installed. Commands:\n  nssm start %s\n  nssm stop %s\n",
		windowsServiceName, windowsServiceName)
	return nil
}

func uninstallWindows() error {
	nssmPath, err := exec.LookPath("nssm")
	if err != nil {
		return fmt.Errorf("nssm not found in PATH")
	}
	_ = runCmd(nssmPath, "stop", windowsServiceName)
	return runCmd(nssmPath, "remove", windowsServiceName, "confirm")
}

func nssmCmd(action, name string) error {
	nssmPath, err := exec.LookPath("nssm")
	if err != nil {
		return fmt.Errorf("nssm not found in PATH")
	}
	return runCmd(nssmPath, action, name)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func renderTmpl(tmplStr string, data any) (string, error) {
	t, err := template.New("").Parse(tmplStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// runCmd executes a command, inheriting stdout/stderr.
func runCmd(name string, args ...string) error {
	c := exec.Command(name, args...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s %v: %w", name, args, err)
	}
	return nil
}

func resolveExecutable() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	return filepath.EvalSymlinks(exe)
}
