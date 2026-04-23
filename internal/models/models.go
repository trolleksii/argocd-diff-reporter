package models

import "html/template"

// PipelineStatus represents the outcome of the diff rendering pipeline for a pull request.
type PipelineStatus int

const (
	PipelineFailed     PipelineStatus = -1
	PipelineInProgress PipelineStatus = 0
	PipelineSucceeded  PipelineStatus = 1
)

type SourceType int

const (
	SourceTypeUndefined = iota
	SourceTypeHelm
	SourceTypeKustomize
	SourceTypeDirectory
)

type KustomizePatchTarget struct {
	Group              string `json:"group,omitempty"`
	Version            string `json:"version,omitempty"`
	Kind               string `json:"kind,omitempty"`
	Name               string `json:"name,omitempty"`
	Namespace          string `json:"namespace,omitempty"`
	LabelSelector      string `json:"labelSelector,omitempty"`
	AnnotationSelector string `json:"annotationSelector,omitempty"`
}

type KustomizePatch struct {
	Path   string                `json:"path,omitempty"`
	Patch  string                `json:"patch,omitempty"`
	Target *KustomizePatchTarget `json:"target,omitempty"`
}

type KustomizeReplica struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

type KustomizeSpec struct {
	NamePrefix             string             `json:"namePrefix,omitempty"`
	NameSuffix             string             `json:"nameSuffix,omitempty"`
	Namespace              string             `json:"namespace,omitempty"`
	CommonLabels           map[string]string  `json:"commonLabels,omitempty"`
	CommonAnnotations      map[string]string  `json:"commonAnnotations,omitempty"`
	ForceCommonLabels      bool               `json:"forceCommonLabels,omitempty"`
	ForceCommonAnnotations bool               `json:"forceCommonAnnotations,omitempty"`
	Images                 []string           `json:"images,omitempty"`
	Replicas               []KustomizeReplica `json:"replicas,omitempty"`
	Patches                []KustomizePatch   `json:"patches,omitempty"`
	Components             []string           `json:"components,omitempty"`
}

type AppSpec struct {
	AppName    string        `json:"appName"`
	SourceType SourceType    `json:"sourceType,omitempty"`
	Namespace  string        `json:"namespace"`
	Source     AppSource     `json:"source"`
	Helm       HelmSpec      `json:"helm"`
	Directory  DirectorySpec `json:"directory"`
	Kustomize  KustomizeSpec `json:"kustomize"`
}

// DirectorySpec holds directory source parameters for an application.
// Recurse controls whether subdirectories are traversed during rendering.
type DirectorySpec struct {
	Recurse bool `json:"recurse,omitempty"`
}

type AppSource struct {
	RepoURL   string `json:"repoUrl"`
	Revision  string `json:"revision"`
	Path      string `json:"path,omitempty"`
	ChartName string `json:"chart,omitempty"`
}

type HelmParameter struct {
	Name        string `json:"name"`
	Value       string `json:"value"`
	ForceString bool   `json:"forceString,omitempty"`
}

type HelmSpec struct {
	ReleaseName string          `json:"release"`
	Values      map[string]any  `json:"values,omitempty"`
	ValueFiles  []string        `json:"valueFiles,omitempty"`
	Parameters  []HelmParameter `json:"parameters,omitempty"`
}

type Progress struct {
	TotalApps     int
	ProcessedApps int
}

// FileProcessingSpec contains details of how to treat a particular file.
// FileName - is the name of the file in the snapshot
// ArtifactName - is the name to use for artifact(manifest) storage
// HasNoCounterpart - an empty manifest must be created with the same ArtifactName but provided SHA
type FileProcessingSpec struct {
	FileName         string
	ArtifactName     string
	HasNoCounterpart bool
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
	Status  PipelineStatus
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
	Owner     string
	Repo      string
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
