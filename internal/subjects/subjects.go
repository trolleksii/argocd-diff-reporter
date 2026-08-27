package subjects

const (
	// Webhook subjects
	WebhookPRChanged = "webhook.pr.changed" // webhook signals pr open/sync/reopen
	WebhookPRClosed  = "webhook.pr.closed"  // webhook signals pr closed

	// Git subjects
	GitFilesResolved = "git.files.resolved" // git worker got the file diff and signals events for base / head shas
	// this smells, maybe I need to combine them
	GitFilesSnapshotted = "git.files.snapshotted" // git took a snapshot of files at a base / head sha
	GitFilesMatched     = "git.files.matched"     // git signal to the coordinator that pr had changed files

	GitChartFetched = "git.chart.fetched" // git signals to helm worker that chart is ready to be rendered

	// Argo subjects
	ArgoHelmOCIParsed  = "argo.helm.oci.parsed"  // argo signals to fetch oci helm chart
	ArgoHelmHTTPParsed = "argo.helm.http.parsed" // argo signals to fetch http helm chart
	ArgoHelmGitParsed  = "argo.helm.git.parsed"  // argo signals to fetch git helm chart

	ArgoSideParsed = "argo.side.parsed" // argo signals coordinator with a complete list of apps rendered from base/head

	// Helm subjects
	HelmChartFetched = "helm.chart.fetched" // helm signals to helm worker that chart is ready to be rendered

	// Directory subjects
	ArgoDirectoryGitParsed = "argo.directory.git.parsed"
	GitDirectoryFetched    = "git.directory.fetched"

	// Coordinator subjects
	CoordinatorAppReady    = "coordinator.app.ready"
	ManifestRenderFinished = "coordinator.manifest.render.complete"
	PRProcessingCompleted  = "coordinator.pr.ready"

	// Diff subjects
	DiffReportGenerated = "diff.report.generated"
)
