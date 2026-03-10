package reports

import (
	"fmt"
	"html/template"
	"slices"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"

	"github.com/gonvenience/neat"
	"github.com/gonvenience/ytbx"

	"github.com/trolleksii/argocd-diff-reporter/internal/dyff"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
)

func LoadManifest(name string, manifest []byte) (ytbx.InputFile, error) {
	var file ytbx.InputFile = ytbx.InputFile{Location: name}
	documents, err := ytbx.LoadDocuments(manifest)
	if err != nil {
		return file, fmt.Errorf("unable to parse data: %w", err)
	}
	file.Documents = documents
	return file, nil
}

func WriteDiffReport(ct templates.Catalog, from, to ytbx.InputFile, excludedPaths []string, report *models.Report) {
	out := make(chan dyff.Diff)
	go dyff.CompareInputFiles(from, to, out)
	for d := range out {
		if slices.Contains(excludedPaths, d.Path.String()) {
			continue
		}
		path := pathToString(d.Path, len(from.Documents) > 1)
		fillReportDiffSection(report, ct, path, d.Details)
	}
}

// pathToString converts a path to string
func pathToString(path *ytbx.Path, showPathRoot bool) string {
	var result string

	if path == nil {
		return "(file level)"
	}

	if path.PathElements == nil {
		result = "/"
	} else {
		sections := []string{""}
		for _, element := range path.PathElements {
			switch {
			case element.Name != "" && element.Key == "":
				sections = append(sections, element.Name)
			case element.Name != "" && element.Key != "":
				sections = append(sections, element.Key+"="+element.Name)
			default:
				sections = append(sections, fmt.Sprintf("%d", element.Idx))
			}
		}
		result = strings.Join(sections, "/")
	}

	if showPathRoot {
		result += " (" + path.RootDescription() + ")"
	}

	return result
}

// convertDiffDetail converts a dyff.Detail to DiffDetailData
func convertDiffDetail(detail dyff.Detail) (md models.DiffDetail) {
	switch detail.Kind {
	case dyff.ADDITION:
		md.ChangeType = "addition"
		md.Symbol = "+"
		md.Text = "Added"
		md.Content = yamlToString(detail.To)
	case dyff.REMOVAL:
		md.ChangeType = "removal"
		md.Symbol = "-"
		md.Text = "Removed"
		md.Content = yamlToString(detail.From)
	case dyff.MODIFICATION:
		md.ChangeType = "modification"
		md.Symbol = "±"
		md.Text = "Modified"
		md.FromContent = yamlToString(detail.From)
		md.ToContent = yamlToString(detail.To)
	case dyff.ORDERCHANGE:
		md.ChangeType = "orderchange"
		md.Symbol = "⇆"
		md.Text = "Order Changed"
		md.FromContent = yamlToString(detail.From)
		md.ToContent = yamlToString(detail.To)
	}
	return
}

// fillReportDiffSection renders a complete diff section using templates
func fillReportDiffSection(report *models.Report, ct templates.Catalog, path string, details []dyff.Detail) {
	var result strings.Builder
	ct["diff-header"].Execute(&result, struct{ Path string }{Path: path})
	for _, detail := range details {
		updateReportCounts(detail.Kind, report)
		templateData := convertDiffDetail(detail)
		ct["diff-detail"].Execute(&result, templateData)
	}
	ct["diff-footer"].Execute(&result, nil)
	report.Body = report.Body + template.HTML(result.String())
}

// updateReportCounts updates report counters based on diff kind
func updateReportCounts(kind rune, report *models.Report) {
	switch kind {
	case dyff.ADDITION:
		report.DiffStats.Additions++
	case dyff.REMOVAL:
		report.DiffStats.Removals++
	case dyff.MODIFICATION:
		report.DiffStats.Modifications++
	case dyff.ORDERCHANGE:
		report.DiffStats.OrderChanges++
	}
	report.DiffStats.DiffCount++
}

// yamlToString converts a YAML node to string
func yamlToString(input any) string {
	if input == nil {
		return "<nil>"
	}

	switch node := input.(type) {
	case *yamlv3.Node:
		if node.Tag == "!!null" {
			return "<nil>"
		}
	}

	out, _ := neat.NewOutputProcessor(false, false, nil).ToYAML(input)
	return out
}
