package dyff

import (
	_ "embed"
	"testing"

	"github.com/gonvenience/ytbx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/modification-base.yaml
var modificationBaseYAML []byte

//go:embed testdata/modification-head.yaml
var modificationHeadYAML []byte

//go:embed testdata/addition-base.yaml
var additionBaseYAML []byte

//go:embed testdata/addition-head.yaml
var additionHeadYAML []byte

//go:embed testdata/removal-base.yaml
var removalBaseYAML []byte

//go:embed testdata/removal-head.yaml
var removalHeadYAML []byte

//go:embed testdata/identical.yaml
var identicalYAML []byte

//go:embed testdata/nonempty.yaml
var nonemptyYAML []byte

//go:embed testdata/reordered-keys.yaml
var reorderedKeysYAML []byte

func loadFixture(t *testing.T, data []byte) ytbx.InputFile {
	t.Helper()
	docs, err := ytbx.LoadDocuments(data)
	require.NoError(t, err)
	return ytbx.InputFile{Documents: docs}
}

func collectDiffs(t *testing.T, from, to ytbx.InputFile) []Diff {
	t.Helper()
	out := make(chan Diff)
	go CompareInputFiles(from, to, out)
	var diffs []Diff
	for d := range out {
		diffs = append(diffs, d)
	}
	return diffs
}

func TestCompare_Modification(t *testing.T) {
	from := loadFixture(t, modificationBaseYAML)
	to := loadFixture(t, modificationHeadYAML)

	diffs := collectDiffs(t, from, to)
	require.NotEmpty(t, diffs, "expected at least one diff")

	var foundModification bool
	for _, d := range diffs {
		for _, detail := range d.Details {
			if detail.Kind == MODIFICATION &&
				detail.From != nil && detail.From.Value == "nginx:1.24" &&
				detail.To != nil && detail.To.Value == "nginx:1.25" {
				foundModification = true
			}
		}
	}

	assert.True(t, foundModification, "expected a MODIFICATION diff with From=nginx:1.24 and To=nginx:1.25")
}

func TestCompare_Addition(t *testing.T) {
	from := loadFixture(t, additionBaseYAML)
	to := loadFixture(t, additionHeadYAML)

	diffs := collectDiffs(t, from, to)
	require.NotEmpty(t, diffs, "expected at least one diff")

	var foundAddition bool
	for _, d := range diffs {
		for _, detail := range d.Details {
			if detail.Kind == ADDITION {
				foundAddition = true
			}
		}
	}

	assert.True(t, foundAddition, "expected at least one diff detail with Kind == ADDITION for the new ConfigMap resource")
}

func TestCompare_Removal(t *testing.T) {
	from := loadFixture(t, removalBaseYAML)
	to := loadFixture(t, removalHeadYAML)

	diffs := collectDiffs(t, from, to)
	require.NotEmpty(t, diffs, "expected at least one diff")

	var foundRemoval bool
	for _, d := range diffs {
		for _, detail := range d.Details {
			if detail.Kind == REMOVAL {
				foundRemoval = true
			}
		}
	}

	assert.True(t, foundRemoval, "expected at least one diff detail with Kind == REMOVAL for the deleted ConfigMap resource")
}

func TestCompare_Identical(t *testing.T) {
	from := loadFixture(t, identicalYAML)
	to := loadFixture(t, identicalYAML)

	diffs := collectDiffs(t, from, to)
	assert.Empty(t, diffs, "expected zero diffs when base and head are identical")
}

func TestCompare_EmptyBase(t *testing.T) {
	from := ytbx.InputFile{Documents: nil}
	to := loadFixture(t, nonemptyYAML)

	diffs := collectDiffs(t, from, to)
	assert.NotEmpty(t, diffs, "expected diffs when base is empty and head is non-empty")
}

func TestCompare_EmptyHead(t *testing.T) {
	from := loadFixture(t, nonemptyYAML)
	to := ytbx.InputFile{Documents: nil}

	diffs := collectDiffs(t, from, to)
	assert.NotEmpty(t, diffs, "expected diffs when base is non-empty and head is empty")
}

func TestCompare_BothEmpty(t *testing.T) {
	from := ytbx.InputFile{Documents: nil}
	to := ytbx.InputFile{Documents: nil}

	diffs := collectDiffs(t, from, to)
	assert.Empty(t, diffs, "expected zero diffs when both base and head are empty")
}

func TestCompare_ReorderedKeys(t *testing.T) {
	from := loadFixture(t, identicalYAML)
	to := loadFixture(t, reorderedKeysYAML)

	diffs := collectDiffs(t, from, to)
	assert.Empty(t, diffs, "expected zero diffs when only YAML key order differs (IgnoreOrderChanges is true)")
}
