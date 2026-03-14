package models

import "html/template"

// ProcessedPR represents a pull request that has been through the diff rendering pipeline.
type ProcessedPR struct {
	Number string
	Title  string
	Status PipelineStatus
}

// PipelineStatus represents the outcome of the diff rendering pipeline for a pull request.
type PipelineStatus int

const (
	PipelineFailed     PipelineStatus = -1
	PipelineInProgress PipelineStatus = 0
	PipelineSucceeded  PipelineStatus = 1
)

type AppSpec struct {
	AppName   string    `json:"appName"`
	Namespace string    `json:"namespace"`
	Source    AppSource `json:"source"`
	Helm      HelmSpec  `json:"helm"`
	Kustomize string    `json:"kustomize,omitempty"`
}

type AppSource struct {
	RepoURL   string `json:"repoUrl"`
	Revision  string `json:"revision"`
	Path      string `json:"path,omitempty"`
	ChartName string `json:"chart,omitempty"`
}

type HelmSpec struct {
	ReleaseName string         `json:"release"`
	Values      map[string]any `json:"values,omitempty"`
}

// FileProcessingSpec contains details of how to treat a particular file.
// FileName - is the name of the file in the snapshot
// ArtifactName - is the name to use for artifact(manifest) storage
// EmptyArtifactSHA - an empty manifest must be created with the same ArtifactName but provided SHA
type FileProcessingSpec struct {
	FileName         string
	ArtifactName     string
	EmptyCounterpart bool
}

// PullRequest holds GitHub pull request metadata and the aggregated results
// of rendering all changed files through the diff pipeline.
type PullRequest struct {
	Number  string
	Author  string
	Owner   string
	Repo    string
	Title   string
	BaseSHA string
	HeadSHA string
	Files   map[string]FileResult
	Success bool
}

// FileResult holds the rendering outcome for a single changed file in a pull request.
// A file may contain one or more Application or ApplicationSet manifests.
// Apps is the aggregate map of all Applications generated from this file,
// including those expanded from ApplicationSets.
// Errors contains any file-level or ApplicationSet processing errors.
type FileResult struct {
	Status string
	Errors []string
	Apps   map[string]App
}

// App holds the diff rendering result for a single ArgoCD Application.
// Errors contains any processing errors encountered during rendering.
type App struct {
	Errors    []string
	DiffStats DiffStats
}

// DiffStats holds a summary of changes detected in an Application's rendered output
// between the base and head commits of a pull request.
type DiffStats struct {
	DiffCount     int
	Additions     int
	Removals      int
	Modifications int
	OrderChanges  int
}

// Report holds the data for rendering a diff report for a single Application.
// It combines PR metadata, file context, diff statistics, and the pre-rendered
// HTML diff output.
type Report struct {
	PRNumber  string
	BaseSHA   string
	HeadSHA   string
	File      string
	AppName   string
	Body      template.HTML
	DiffStats DiffStats
}

// DiffDetail holds the data for rendering a single diff entry within a report.
type DiffDetail struct {
	ChangeType  string
	Symbol      string
	Text        string
	Content     string
	FromContent string
	ToContent   string
}
