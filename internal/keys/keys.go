// Package keys builds the KV and object-store keys shared by workers and
// server handlers, so every producer and consumer agrees on the layout.
package keys

// Index is the KV key holding the UI's pull request index.
const Index = "index"

// PR is the KV key of a pull request's summary record.
func PR(owner, repo, number string) string {
	return owner + "." + repo + "." + number
}

// WorkOrder is the KV key of one run's per-app progress record.
func WorkOrder(owner, repo, number, runId string) string {
	return owner + "." + repo + "." + number + "." + runId
}

// CheckRun is the KV key of the GitHub check-run id created for a head commit.
func CheckRun(owner, repo, number, headSha string) string {
	return "checks" + "." + owner + "." + repo + "." + number + "." + headSha
}

// Manifest is the object-store key of one app's rendered manifest on one side.
func Manifest(owner, repo, number, sha, origin, app string) string {
	return owner + "." + repo + "." + number + "." + sha + "." + origin + "." + app
}

// Report is the object-store key of one app's diff report.
func Report(owner, repo, number, baseSha, headSha, origin, app string) string {
	return owner + "." + repo + "." + number + "." + baseSha + "." + headSha + "." + origin + "." + app
}

// Nats-Msg-Id values. Workers forward received headers verbatim, so every
// publish must stamp its own id (or delete the inherited one): JetStream
// dedups on the id alone, stream-wide, and a reused id is silently dropped.

// MsgIDHeader is the NATS header JetStream dedups on.
const MsgIDHeader = "Nats-Msg-Id"

// MsgIDSides identifies the git.files.matched event of one run.
func MsgIDSides(runId string) string {
	return "sides." + runId
}

// MsgIDAppReady identifies the coordinator.app.ready dispatch of one app in one run.
func MsgIDAppReady(owner, repo, number, runId, app string) string {
	return "ready." + WorkOrder(owner, repo, number, runId) + "." + app
}

// MsgIDReport identifies the diff.report.generated event of one app in one run.
func MsgIDReport(owner, repo, number, runId, origin, app string) string {
	return "report." + WorkOrder(owner, repo, number, runId) + "." + origin + "." + app
}

// MsgIDDone identifies the coordinator.pr.ready event of one run; a run completes once.
func MsgIDDone(owner, repo, number, runId string) string {
	return "done." + WorkOrder(owner, repo, number, runId)
}

// MsgIDFiles identifies the git.files.resolved event of one side of a run.
func MsgIDFiles(owner, repo, number, runId, sha string) string {
	return "files." + WorkOrder(owner, repo, number, runId) + "." + sha
}

// MsgIDSnapshot identifies the git.files.snapshotted event of one side of a run.
func MsgIDSnapshot(owner, repo, number, runId, sha string) string {
	return "snapshot." + WorkOrder(owner, repo, number, runId) + "." + sha
}

// MsgIDSide identifies the argo.side.parsed event of one side of a run.
func MsgIDSide(owner, repo, number, runId, sha string) string {
	return "side." + WorkOrder(owner, repo, number, runId) + "." + sha
}

// MsgIDApp identifies the argo.*.parsed dispatch of one app on one side of a run.
func MsgIDApp(owner, repo, number, runId, sha, origin, app string) string {
	return "app." + WorkOrder(owner, repo, number, runId) + "." + sha + "." + origin + "." + app
}

// MsgIDFetched identifies the *.fetched event of one app on one side of a run.
func MsgIDFetched(owner, repo, number, runId, sha, origin, app string) string {
	return "fetched." + WorkOrder(owner, repo, number, runId) + "." + sha + "." + origin + "." + app
}

// MsgIDRender identifies the manifest.render.complete event of one app on one side of a run.
func MsgIDRender(owner, repo, number, runId, sha, origin, app string) string {
	return "render." + WorkOrder(owner, repo, number, runId) + "." + sha + "." + origin + "." + app
}
