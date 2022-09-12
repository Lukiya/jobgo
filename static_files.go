package jobgo

import (
	"embed"
)

//go:embed wwwroot
var StaticFiles embed.FS
