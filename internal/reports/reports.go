// Copyright (c) 2019 The Homeport Team
//
// This file contains code derived from https://github.com/homeport/dyff,
// licensed under the MIT License. See THIRD_PARTY_NOTICES for details.

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
	var md strings.Builder
	for d := range out {
		if slices.Contains(excludedPaths, d.Path.String()) {
			continue
		}
		path := pathToString(d.Path, len(from.Documents) > 1)
		fillReportDiffSection(report, ct, path, d.Details)
		writeMarkdownDiffSection(&md, path, d.Details)
	}
	report.BodyMarkdown = md.String()
}

// writeMarkdownDiffSection appends one markdown section per changed path:
// a heading followed by a ```diff fence per detail. A removal+addition pair
// (how dyff reports a replaced list item) and multi-line modifications are
// collapsed into a single unified line diff instead of full before/after
// dumps — large mostly-unchanged values would otherwise double in size.
func writeMarkdownDiffSection(md *strings.Builder, path string, details []dyff.Detail) {
	fmt.Fprintf(md, "### %s\n\n", path)
	if from, to, ok := removalAdditionPair(details); ok {
		writeFencedBlock(md, "Modified", diffLines(from, to, 3))
		return
	}
	for _, detail := range details {
		data := convertDiffDetail(detail)
		var content string
		switch detail.Kind {
		case dyff.ADDITION:
			content = prefixLines("+ ", data.Content)
		case dyff.REMOVAL:
			content = prefixLines("- ", data.Content)
		default:
			if isMultiline(data.FromContent) || isMultiline(data.ToContent) {
				content = diffLines(data.FromContent, data.ToContent, 3)
			} else {
				content = prefixLines("- ", data.FromContent) + prefixLines("+ ", data.ToContent)
			}
		}
		writeFencedBlock(md, data.Text, content)
	}
}

func writeFencedBlock(md *strings.Builder, label, content string) {
	// A longer fence keeps the block valid when the YAML itself contains ```
	fence := "```"
	if strings.Contains(content, "```") {
		fence = "````"
	}
	fmt.Fprintf(md, "**%s**\n%sdiff\n%s%s\n\n", label, fence, content, fence)
}

// removalAdditionPair matches a section holding exactly one removal and one
// addition, so both can render as a single unified diff.
func removalAdditionPair(details []dyff.Detail) (from, to string, ok bool) {
	if len(details) != 2 {
		return "", "", false
	}
	contents := map[rune]string{}
	for _, d := range details {
		contents[d.Kind] = convertDiffDetail(d).Content
	}
	from, hasFrom := contents[dyff.REMOVAL]
	to, hasTo := contents[dyff.ADDITION]
	return from, to, hasFrom && hasTo
}

type diffOp struct {
	kind byte // '-', '+', ' ', or '@' for a gap between hunks
	line string
}

// diffOps computes line-level diff ops for from/to, keeping ctx context
// lines around each change and marking skipped runs with a '@' op.
// ponytail: O(n*m) LCS table; fine for manifest-sized values.
func diffOps(from, to string, ctx int) []diffOp {
	a := strings.Split(strings.TrimRight(from, "\n"), "\n")
	b := strings.Split(strings.TrimRight(to, "\n"), "\n")
	n, m := len(a), len(b)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}

	var ops []diffOp
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			ops = append(ops, diffOp{' ', a[i]})
			i, j = i+1, j+1
		case lcs[i+1][j] >= lcs[i][j+1]:
			ops = append(ops, diffOp{'-', a[i]})
			i++
		default:
			ops = append(ops, diffOp{'+', b[j]})
			j++
		}
	}
	for ; i < n; i++ {
		ops = append(ops, diffOp{'-', a[i]})
	}
	for ; j < m; j++ {
		ops = append(ops, diffOp{'+', b[j]})
	}

	keep := make([]bool, len(ops))
	for k, o := range ops {
		if o.kind != ' ' {
			for l := max(0, k-ctx); l <= min(len(ops)-1, k+ctx); l++ {
				keep[l] = true
			}
		}
	}

	var out []diffOp
	skipping := false
	for k, o := range ops {
		if !keep[k] {
			skipping = true
			continue
		}
		if skipping {
			out = append(out, diffOp{'@', ""})
			skipping = false
		}
		out = append(out, o)
	}
	return out
}

// diffLines renders a unified line diff of from/to with ctx context lines,
// gaps between hunks marked with "@@".
func diffLines(from, to string, ctx int) string {
	var out strings.Builder
	for _, o := range diffOps(from, to, ctx) {
		if o.kind == '@' {
			out.WriteString("@@\n")
			continue
		}
		out.WriteString(strings.TrimRight(string(o.kind)+" "+o.line, " "))
		out.WriteByte('\n')
	}
	return out.String()
}

var diffLineTypes = map[byte]string{'+': "addition", '-': "removal", ' ': "context", '@': "gap"}

// diffLineList renders the same unified diff as typed lines for the HTML template.
func diffLineList(from, to string, ctx int) []models.DiffLine {
	ops := diffOps(from, to, ctx)
	lines := make([]models.DiffLine, len(ops))
	for i, o := range ops {
		text := "@@"
		if o.kind != '@' {
			text = strings.TrimRight(string(o.kind)+" "+o.line, " ")
		}
		lines[i] = models.DiffLine{Type: diffLineTypes[o.kind], Text: text}
	}
	return lines
}

func isMultiline(s string) bool {
	return strings.Contains(strings.TrimRight(s, "\n"), "\n")
}

// prefixLines prefixes every line of s and normalizes the trailing newline.
func prefixLines(prefix, s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString(strings.TrimRight(prefix+line, " "))
		b.WriteByte('\n')
	}
	return b.String()
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

// fillReportDiffSection renders a complete diff section using templates.
// Like the markdown renderer, a removal+addition pair and multi-line
// modifications collapse into a single unified line diff.
func fillReportDiffSection(report *models.Report, ct templates.Catalog, path string, details []dyff.Detail) {
	var result strings.Builder
	ct["diff-header"].Execute(&result, struct{ Path string }{Path: path})
	if from, to, ok := removalAdditionPair(details); ok {
		for _, detail := range details {
			updateReportCounts(detail.Kind, report)
		}
		ct["diff-detail"].Execute(&result, unifiedDetail(from, to))
	} else {
		for _, detail := range details {
			updateReportCounts(detail.Kind, report)
			templateData := convertDiffDetail(detail)
			if detail.Kind == dyff.MODIFICATION && (isMultiline(templateData.FromContent) || isMultiline(templateData.ToContent)) {
				templateData = unifiedDetail(templateData.FromContent, templateData.ToContent)
			}
			ct["diff-detail"].Execute(&result, templateData)
		}
	}
	ct["diff-footer"].Execute(&result, nil)
	report.Body = report.Body + template.HTML(result.String())
}

// unifiedDetail renders a before/after pair as a single unified line diff entry.
func unifiedDetail(from, to string) models.DiffDetail {
	return models.DiffDetail{
		ChangeType: "modification",
		Symbol:     "±",
		Text:       "Modified",
		Lines:      diffLineList(from, to, 3),
	}
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
