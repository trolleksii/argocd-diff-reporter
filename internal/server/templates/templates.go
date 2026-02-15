package templates

import (
	"embed"
	"fmt"
	"html/template"
)

//go:embed all:html
var templateFS embed.FS

var templates map[string]string = map[string]string{
	"index":       "html/index.html",
	"latestprs":   "html/latestprs.html",
	"summary":     "html/summary.html",
	"table":       "html/partials/table.html",
	"report":      "html/partials/report.html",
	"diff-header": "html/partials/diff-header.html",
	"diff-footer": "html/partials/diff-footer.html",
	"diff-detail": "html/partials/diff-detail.html",
}

// Manager handles template loading and execution
type Manager map[string]*template.Template

// NewManager creates a new template manager
func NewManager() Manager {
	m := Manager{}
	for name, file := range templates {
		content, err := templateFS.ReadFile(file)
		if err != nil {
			panic(fmt.Sprintf("failed to load template %s: %v", name, err))
		}

		template, err := template.New(name).Parse(string(content))
		if err != nil {
			panic(fmt.Sprintf("failed to parse template %s: %v", name, err))
		}
		m[name] = template
	}

	return m
}
