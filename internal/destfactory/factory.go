// Package destfactory provides a factory function for constructing Destination
// implementations without creating import cycles between the destinations
// interface package and its concrete sub-packages.
package destfactory

import (
	"fmt"

	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/destinations"
	"github.com/htb/htb-replicator/internal/destinations/ftp"
	"github.com/htb/htb-replicator/internal/destinations/local"
	s3dest "github.com/htb/htb-replicator/internal/destinations/s3"
	"github.com/htb/htb-replicator/internal/destinations/sftp"
)

// New constructs the correct Destination implementation based on cfg.Type.
// Supported types: "local", "s3", "ftp", "sftp".
func New(cfg config.DestinationConfig) (destinations.Destination, error) {
	switch cfg.Type {
	case "local":
		return local.New(cfg)
	case "s3":
		return s3dest.New(cfg)
	case "ftp":
		return ftp.New(cfg)
	case "sftp":
		return sftp.New(cfg)
	default:
		return nil, fmt.Errorf("unknown destination type %q for destination %q", cfg.Type, cfg.ID)
	}
}
