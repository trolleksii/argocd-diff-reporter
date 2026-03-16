package subjects

const (
	// Webhook subjects
	WebhookPRChanged = "webhook.pr.changed"
	WebhookPRClosed  = "webhook.pr.closed"

	// Git subjects
	GitFilesResolved    = "git.files.resolved"
	GitFilesSnapshotted = "git.files.snapshotted"
	GitChartFetched     = "git.chart.fetched"
	GitChartFetchFailed = "git.chart.fetch.failed"

	// Argo subjects
	ArgoHelmOCIParsed       = "argo.helm.oci.parsed"
	ArgoHelmHTTPParsed      = "argo.helm.http.parsed"
	ArgoHelmGitParsed       = "argo.helm.git.parsed"
	ArgoHelmEmptyParsed     = "argo.helm.empty.parsed"
	ArgoAppGenerationFailed = "argo.app.generation.failed"
	ArgoFileParseFailed     = "argo.file.parsing.failed"
	ArgoTotalUpdated        = "argo.total.updated"

	// Helm subjects
	HelmChartFetched         = "helm.chart.fetched"
	HelmChartFetchFailed     = "helm.chart.fetch.failed"
	HelmManifestRendered     = "helm.manifest.rendered"
	HelmManifestRenderFailed = "helm.manifest.render.failed"

	// Coordinator subjects
	CoordinatorAppReady = "coordinator.app.ready"

	// Diff subjects
	DiffReportGenerated = "diff.report.generated"
)
