package nats

import (
	"html/template"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// ---------------------------------------------------------------------------
// Marshal / Unmarshal round-trips
// ---------------------------------------------------------------------------

func TestMarshalUnmarshal_DiffStats(t *testing.T) {
	original := models.DiffStats{
		DiffCount:     5,
		Additions:     10,
		Removals:      3,
		Modifications: 2,
		OrderChanges:  1,
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.DiffStats](data)
	require.NoError(t, err)
	assert.Equal(t, original, got)
}

func TestMarshalUnmarshal_App(t *testing.T) {
	original := models.App{
		Errors: []string{"error one", "error two"},
		DiffStats: models.DiffStats{
			DiffCount:     7,
			Additions:     4,
			Removals:      2,
			Modifications: 1,
			OrderChanges:  0,
		},
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.App](data)
	require.NoError(t, err)
	assert.Equal(t, original, got)
}

func TestMarshalUnmarshal_App_EmptyErrors(t *testing.T) {
	original := models.App{
		Errors:    nil,
		DiffStats: models.DiffStats{},
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.App](data)
	require.NoError(t, err)
	assert.Equal(t, original.DiffStats, got.DiffStats)
}

func TestMarshalUnmarshal_FileResult(t *testing.T) {
	original := models.FileResult{
		Status: "modified",
		Errors: []string{"file-level error"},
		Apps: map[string]models.App{
			"app-a": {
				Errors: []string{"app error"},
				DiffStats: models.DiffStats{
					DiffCount: 3,
					Additions: 1,
					Removals:  2,
				},
			},
			"app-b": {
				Errors:    nil,
				DiffStats: models.DiffStats{DiffCount: 0},
			},
		},
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.FileResult](data)
	require.NoError(t, err)
	assert.Equal(t, original.Status, got.Status)
	assert.Equal(t, original.Errors, got.Errors)
	require.Len(t, got.Apps, len(original.Apps))
	for k, v := range original.Apps {
		assert.Equal(t, v.DiffStats, got.Apps[k].DiffStats)
	}
}

func TestMarshalUnmarshal_PullRequest(t *testing.T) {
	original := models.PullRequest{
		Number:  "42",
		Author:  "alice",
		Owner:   "org",
		Repo:    "repo",
		Title:   "Fix the thing",
		BaseSHA: "aaaa",
		HeadSHA: "bbbb",
		Status:  models.PipelineSucceeded,
		Files: map[string]models.FileResult{
			"apps/app1.yaml": {
				Status: "modified",
				Errors: nil,
				Apps: map[string]models.App{
					"my-app": {
						Errors:    []string{},
						DiffStats: models.DiffStats{DiffCount: 1, Additions: 1},
					},
				},
			},
		},
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.PullRequest](data)
	require.NoError(t, err)
	assert.Equal(t, original.Number, got.Number)
	assert.Equal(t, original.Author, got.Author)
	assert.Equal(t, original.Owner, got.Owner)
	assert.Equal(t, original.Repo, got.Repo)
	assert.Equal(t, original.Title, got.Title)
	assert.Equal(t, original.BaseSHA, got.BaseSHA)
	assert.Equal(t, original.HeadSHA, got.HeadSHA)
	assert.Equal(t, original.Status, got.Status)
	require.Contains(t, got.Files, "apps/app1.yaml")
	assert.Equal(t, original.Files["apps/app1.yaml"].Status, got.Files["apps/app1.yaml"].Status)
}

func TestMarshalUnmarshal_PullRequest_AllStatuses(t *testing.T) {
	for _, status := range []models.PipelineStatus{
		models.PipelineFailed,
		models.PipelineInProgress,
		models.PipelineSucceeded,
	} {
		pr := models.PullRequest{Number: "1", Status: status}
		data, err := Marshal(pr)
		require.NoError(t, err)

		got, err := Unmarshal[models.PullRequest](data)
		require.NoError(t, err)
		assert.Equal(t, status, got.Status)
	}
}

func TestMarshalUnmarshal_AppSpec(t *testing.T) {
	original := models.AppSpec{
		AppName:   "my-application",
		Namespace: "default",
		Source: models.AppSource{
			RepoURL:   "https://github.com/org/repo",
			Revision:  "HEAD",
			Path:      "charts/my-app",
			ChartName: "my-chart",
		},
		Helm: models.HelmSpec{
			ReleaseName: "my-release",
			Values: map[string]any{
				"replicaCount": 2,
				"image":        "nginx:latest",
			},
		},
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.AppSpec](data)
	require.NoError(t, err)
	assert.Equal(t, original.AppName, got.AppName)
	assert.Equal(t, original.Namespace, got.Namespace)
	assert.Equal(t, original.Source.RepoURL, got.Source.RepoURL)
	assert.Equal(t, original.Source.Revision, got.Source.Revision)
	assert.Equal(t, original.Source.Path, got.Source.Path)
	assert.Equal(t, original.Source.ChartName, got.Source.ChartName)
	assert.Equal(t, original.Helm.ReleaseName, got.Helm.ReleaseName)
}

func TestMarshalUnmarshal_Report(t *testing.T) {
	original := models.Report{
		Owner:    "org",
		Repo:     "repo",
		PRNumber: "7",
		BaseSHA:  "base123",
		HeadSHA:  "head456",
		File:     "manifests/deployment.yaml",
		AppName:  "web-app",
		Body:     template.HTML("<div>diff content</div>"),
		DiffStats: models.DiffStats{
			DiffCount:     2,
			Additions:     1,
			Removals:      1,
			Modifications: 0,
			OrderChanges:  0,
		},
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.Report](data)
	require.NoError(t, err)
	assert.Equal(t, original.Owner, got.Owner)
	assert.Equal(t, original.Repo, got.Repo)
	assert.Equal(t, original.PRNumber, got.PRNumber)
	assert.Equal(t, original.BaseSHA, got.BaseSHA)
	assert.Equal(t, original.HeadSHA, got.HeadSHA)
	assert.Equal(t, original.File, got.File)
	assert.Equal(t, original.AppName, got.AppName)
	assert.Equal(t, original.Body, got.Body)
	assert.Equal(t, original.DiffStats, got.DiffStats)
}

func TestMarshalUnmarshal_Progress(t *testing.T) {
	original := models.Progress{
		TotalApps:     20,
		ProcessedApps: 15,
	}
	data, err := Marshal(original)
	require.NoError(t, err)

	got, err := Unmarshal[models.Progress](data)
	require.NoError(t, err)
	assert.Equal(t, original, got)
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

func TestHeaders_GetExistingKey(t *testing.T) {
	h := Headers{"foo": "bar"}
	assert.Equal(t, "bar", h.Get("foo"))
}

func TestHeaders_GetMissingKeyOnNilMap(t *testing.T) {
	h := Headers(nil)
	// Go map reads on nil map return zero value — should not panic
	assert.Equal(t, "", h.Get("anything"))
}

func TestHeaders_Keys(t *testing.T) {
	h := Headers{
		"alpha": "1",
		"beta":  "2",
		"gamma": "3",
	}
	keys := h.Keys()
	sort.Strings(keys)
	assert.Equal(t, []string{"alpha", "beta", "gamma"}, keys)
}
