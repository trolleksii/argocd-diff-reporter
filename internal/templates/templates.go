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
	"summary":     "html/summary.html",
	"report":      "html/report.html",
	"table":       "html/partials/table.html",
	"latestprs":   "html/partials/latest-prs.html",
	"diff-header": "html/partials/diff-header.html",
	"diff-footer": "html/partials/diff-footer.html",
	"diff-detail": "html/partials/diff-detail.html",
}

// Catalog is a map of available templates keyed by name.
type Catalog map[string]*template.Template

func NewCatalog() Catalog {
	m := Catalog{}
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
