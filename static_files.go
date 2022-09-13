package jobgo

import (
	"embed"
)

//go:embed wwwroot
var staticFiles embed.FS
