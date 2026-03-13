// Package sourcefactory provides a factory function for constructing Source
// implementations without creating import cycles between the source interface
// package and its concrete sub-packages.
package sourcefactory

import (
	"fmt"

	"github.com/htb/htb-replicator/config"
	"github.com/htb/htb-replicator/internal/source"
	ftpsrc "github.com/htb/htb-replicator/internal/sources/ftp"
	localsrc "github.com/htb/htb-replicator/internal/sources/local"
	s3src "github.com/htb/htb-replicator/internal/sources/s3"
	sftpsrc "github.com/htb/htb-replicator/internal/sources/sftp"
)

// New constructs the correct Source implementation based on cfg.Type.
// Supported types: "s3", "sftp", "ftp", "local".
func New(cfg config.SourceConfig) (source.Source, error) {
	opts := cfg.Opts
	if opts == nil {
		opts = make(map[string]interface{})
	}

	switch cfg.Type {
	case "s3":
		return s3src.New(opts)
	case "sftp":
		return sftpsrc.New(opts)
	case "ftp":
		return ftpsrc.New(opts)
	case "local":
		return localsrc.New(opts)
	default:
		return nil, fmt.Errorf("unknown source type %q (valid: s3, sftp, ftp, local)", cfg.Type)
	}
}
