package reports

import (
	_ "embed"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dyffyaml "go.yaml.in/yaml/v3"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/trolleksii/argocd-diff-reporter/internal/dyff"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
	"github.com/trolleksii/argocd-diff-reporter/internal/templates"
)

//go:embed testdata/base.yaml
var baseYAML []byte

//go:embed testdata/head.yaml
var headYAML []byte

// ---------------------------------------------------------------------------
// LoadManifest
// ---------------------------------------------------------------------------

func TestLoadManifest_ValidYAML(t *testing.T) {
	input, err := LoadManifest("test", baseYAML)
	require.NoError(t, err)
	assert.Equal(t, "test", input.Location)
	assert.NotEmpty(t, input.Documents)
}

func TestLoadManifest_InvalidYAML(t *testing.T) {
	_, err := LoadManifest("bad", []byte("not: valid: yaml: ["))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to parse data")
}

func TestLoadManifest_EmptyBytes(t *testing.T) {
	// Empty input may result in either an empty document list or a parse error;
	// either way it must not panic.
	input, err := LoadManifest("empty", []byte{})
	if err != nil {
		assert.Contains(t, err.Error(), "unable to parse data")
	} else {
		// If it succeeds, the document slice should be empty or contain only nulls.
		_ = input
	}
}

// ---------------------------------------------------------------------------
// WriteDiffReport
// ---------------------------------------------------------------------------

func TestWriteDiffReport_DetectsModification(t *testing.T) {
	ct := templates.NewCatalog()

	from, err := LoadManifest("base", baseYAML)
	require.NoError(t, err)

	to, err := LoadManifest("head", headYAML)
	require.NoError(t, err)

	report := &models.Report{}
	WriteDiffReport(ct, from, to, nil, report)

	assert.Greater(t, report.DiffStats.DiffCount, 0, "expected at least one diff")
	assert.Greater(t, report.DiffStats.Modifications, 0, "expected at least one modification")
	assert.NotEmpty(t, string(report.Body), "expected non-empty HTML body")
}

func TestWriteDiffReport_BodyContainsHTML(t *testing.T) {
	ct := templates.NewCatalog()

	from, err := LoadManifest("base", baseYAML)
	require.NoError(t, err)

	to, err := LoadManifest("head", headYAML)
	require.NoError(t, err)

	report := &models.Report{}
	WriteDiffReport(ct, from, to, nil, report)

	body := string(report.Body)
	assert.Contains(t, body, "<", "expected HTML content in report body")
}

func TestWriteDiffReport_ExcludedPathSuppressesDiff(t *testing.T) {
	ct := templates.NewCatalog()

	from, err := LoadManifest("base", baseYAML)
	require.NoError(t, err)

	to, err := LoadManifest("head", headYAML)
	require.NoError(t, err)

	// Discover the actual path string produced for the changed key by running
	// once without exclusions, then re-running with it excluded.
	discovery := &models.Report{}
	WriteDiffReport(ct, from, to, nil, discovery)
	require.Greater(t, discovery.DiffStats.DiffCount, 0, "prerequisite: diff must be detected before exclusion")

	// The changed field lives under data/key; its GoPatch path is /data/key.
	excluded := &models.Report{}
	WriteDiffReport(ct, from, to, []string{"/data/key"}, excluded)
	assert.Equal(t, 0, excluded.DiffStats.DiffCount, "diff count should be 0 when the only changed path is excluded")
}

func TestWriteDiffReport_IdenticalManifests_NoDiff(t *testing.T) {
	ct := templates.NewCatalog()

	from, err := LoadManifest("base", baseYAML)
	require.NoError(t, err)

	to, err := LoadManifest("base-copy", baseYAML)
	require.NoError(t, err)

	report := &models.Report{}
	WriteDiffReport(ct, from, to, nil, report)

	assert.Equal(t, 0, report.DiffStats.DiffCount)
	assert.Empty(t, string(report.Body))
}

// ---------------------------------------------------------------------------
// convertDiffDetail
// ---------------------------------------------------------------------------

// dyffScalarNode unmarshals s as YAML and returns the scalar child of the
// document node, suitable for use in a dyff.Detail's From/To fields.
func dyffScalarNode(t *testing.T, s string) *dyffyaml.Node {
	t.Helper()
	var doc dyffyaml.Node
	require.NoError(t, dyffyaml.Unmarshal([]byte(s), &doc))
	return doc.Content[0]
}

func TestConvertDiffDetail(t *testing.T) {
	cases := []struct {
		name       string
		detail     dyff.Detail
		changeType string
		symbol     string
		text       string
		content    string
		from       string
		to         string
	}{
		{
			name:       "addition",
			detail:     dyff.Detail{Kind: dyff.ADDITION, To: dyffScalarNode(t, "added-value")},
			changeType: "addition", symbol: "+", text: "Added",
			content: "added-value",
		},
		{
			name:       "removal",
			detail:     dyff.Detail{Kind: dyff.REMOVAL, From: dyffScalarNode(t, "removed-value")},
			changeType: "removal", symbol: "-", text: "Removed",
			content: "removed-value",
		},
		{
			name:       "modification",
			detail:     dyff.Detail{Kind: dyff.MODIFICATION, From: dyffScalarNode(t, "before"), To: dyffScalarNode(t, "after")},
			changeType: "modification", symbol: "±", text: "Modified",
			from: "before", to: "after",
		},
		{
			name:       "orderchange",
			detail:     dyff.Detail{Kind: dyff.ORDERCHANGE, From: dyffScalarNode(t, "first"), To: dyffScalarNode(t, "second")},
			changeType: "orderchange", symbol: "⇆", text: "Order Changed",
			from: "first", to: "second",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			md := convertDiffDetail(tc.detail)

			assert.Equal(t, tc.changeType, md.ChangeType)
			assert.Equal(t, tc.symbol, md.Symbol)
			assert.Equal(t, tc.text, md.Text)

			if tc.content != "" {
				assert.NotEmpty(t, md.Content)
				assert.Contains(t, md.Content, tc.content)
				assert.Equal(t, "", md.FromContent)
				assert.Equal(t, "", md.ToContent)
			} else {
				assert.Equal(t, "", md.Content)
				assert.NotEmpty(t, md.FromContent)
				assert.Contains(t, md.FromContent, tc.from)
				assert.NotEmpty(t, md.ToContent)
				assert.Contains(t, md.ToContent, tc.to)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// updateReportCounts
// ---------------------------------------------------------------------------

func TestUpdateReportCounts_PerKind(t *testing.T) {
	cases := []struct {
		name          string
		kind          rune
		additions     int
		removals      int
		modifications int
		orderChanges  int
	}{
		{name: "addition", kind: dyff.ADDITION, additions: 1},
		{name: "removal", kind: dyff.REMOVAL, removals: 1},
		{name: "modification", kind: dyff.MODIFICATION, modifications: 1},
		{name: "orderchange", kind: dyff.ORDERCHANGE, orderChanges: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			report := &models.Report{}
			updateReportCounts(tc.kind, report)

			assert.Equal(t, tc.additions, report.DiffStats.Additions)
			assert.Equal(t, tc.removals, report.DiffStats.Removals)
			assert.Equal(t, tc.modifications, report.DiffStats.Modifications)
			assert.Equal(t, tc.orderChanges, report.DiffStats.OrderChanges)
			assert.Equal(t, 1, report.DiffStats.DiffCount)
		})
	}
}

func TestUpdateReportCounts_MixedAggregation(t *testing.T) {
	report := &models.Report{}

	kinds := []rune{
		dyff.ADDITION,
		dyff.ADDITION,
		dyff.REMOVAL,
		dyff.MODIFICATION,
		dyff.MODIFICATION,
		dyff.MODIFICATION,
		dyff.ORDERCHANGE,
	}
	for _, k := range kinds {
		updateReportCounts(k, report)
	}

	assert.Equal(t, 2, report.DiffStats.Additions)
	assert.Equal(t, 1, report.DiffStats.Removals)
	assert.Equal(t, 3, report.DiffStats.Modifications)
	assert.Equal(t, 1, report.DiffStats.OrderChanges)
	assert.Equal(t, 7, report.DiffStats.DiffCount)
}

// ---------------------------------------------------------------------------
// yamlToString
// ---------------------------------------------------------------------------

func TestYamlToString(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		assert.Equal(t, "<nil>", yamlToString(nil))
	})

	cases := []struct {
		name     string
		node     func(t *testing.T) *yamlv3.Node
		contains []string
		exact    string
	}{
		{
			name:  "null tag",
			node:  func(*testing.T) *yamlv3.Node { return &yamlv3.Node{Tag: "!!null"} },
			exact: "<nil>",
		},
		{
			name: "scalar",
			node: func(t *testing.T) *yamlv3.Node {
				var doc yamlv3.Node
				require.NoError(t, yamlv3.Unmarshal([]byte("hello"), &doc))
				return doc.Content[0]
			},
			contains: []string{"hello"},
		},
		{
			name: "sequence",
			node: func(t *testing.T) *yamlv3.Node {
				var doc yamlv3.Node
				require.NoError(t, yamlv3.Unmarshal([]byte("- a\n- b\n- c\n"), &doc))
				return doc.Content[0]
			},
			contains: []string{"a", "b", "c"},
		},
		{
			name: "mapping",
			node: func(t *testing.T) *yamlv3.Node {
				var doc yamlv3.Node
				require.NoError(t, yamlv3.Unmarshal([]byte("key: value\nother: 42\n"), &doc))
				return doc.Content[0]
			},
			contains: []string{"key", "value", "other", "42"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := yamlToString(tc.node(t))
			if tc.exact != "" {
				assert.Equal(t, tc.exact, got)
			} else {
				assert.NotEmpty(t, got)
				for _, s := range tc.contains {
					assert.Contains(t, got, s)
				}
			}
		})
	}
}
