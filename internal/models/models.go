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

// Index holds latest processed PRs
type Index []ProcessedPR

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

type FileChange struct {
	FileName    string
	Counterpart string
}
