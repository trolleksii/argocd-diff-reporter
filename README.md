# ArgoCD Diff Reporter

A tool that generates Kubernetes manifest diffs for pull requests that modify ArgoCD Applications and/or ApplicationSets. When a PR is opened or updated, it renders the affected applications at both the base and head commits, computes the diff, and posts the results as a GitHub Check Run with a link to an interactive web UI.

## Why

You bought into the GitOps idea and run ArgoCD in auto-sync mode, but you want full transparency into what will change in your cluster before hitting merge on your Pull Requests. A single line change in the Application YAML file may cascade into a massive diff across multiple raw manifests. This tool fills the gap in change transparenty.

## Features

- **ApplicationSet expansion** — automatically generates concrete Applications from ApplicationSets so every generated app gets its own diff
- **Multi-file, multi-document PRs** — accommodates various possible GitOps layouts, whether you use ApplicationSets, Applications, or a mix of both, single-document or multi-document yaml files
- **Helm, Kustomize, and directory sources** — supports three major sources for ArgoCD Applications.
- **Private Helm/OCI registries** — uses repositories configured in ArgoCD to pull sources from private repositories
- **Interactive web UI** — dashboard of recent PRs, per-PR summary of affected apps, and detailed side-by-side diffs with live progress updates over WebSocket
- **GitHub Check Runs** — creates a check on each PR with a summary and a link to the full report; supports re-runs via CheckSuite events

## How It Works

Built on top of embedded NATS server for coordination and artifact storage.

1. A GitHub webhook fires on PR open/sync/reopen
2. The git worker clones the repo and snapshots the changed files at both commits
3. The argo worker parses Application and ApplicationSet manifests from the changed files
4. Helm/Kustomize/directory workers render manifests for each app at both commits
5. The diff worker compares the rendered manifests and generates an HTML report
6. The GitHub checks worker posts the result back to the PR as a Check Run
7. Any open UI views are instantly updated with latest content

## Prerequisites

- A **GitHub App** with permissions for Check Runs and webhook events (PullRequest, CheckSuite)
- RBAC rules to allow access to secrets and configmaps. This is necessary for app to discover secrets with Cluster and Repo credentials necessary to render ApplicationSets and pull charts from private Repositories.
- **Go 1.25+** (to build from source)

## Configuration

The app is configured via a YAML file (default: `config.yml`, override with `CONFIG_PATH` env var).

```yaml
global:
  storageDir: ./data

server:
  addr: 0.0.0.0:8000

github:
  appId: <your-github-app-id>
  installationId: <your-installation-id>
  privateKey: <your-rsa-private-key>   # or set GITHUB_APP_PRIVATE_KEY env var

webhook:
  secret: <your-webhook-secret>        # or set GITHUB_WEBHOOK_SECRET env var
  # the app will ignore webhook events from repos that are not on this list
  repositories:
    - owner: my-org
      repo: k8s-apps

log:
  level: info
  format: json

# optional
# provides a glance into where time is spent during pr processing
tracing:
  endpoint: 127.0.0.1:4318
  service: argocd-diff-reporter
  version: v1.0

# optional
# by default assumes it is deployed in the same namespace as argocd
argocd:
  namespace: argocd

workers:
  gitWorker:
    # the app will ignore changes in files that are not in this list
    fileGlobs: ["system/manifests/argoapps/*.yaml"]
  githubChecks:
    # base url used to generate refs to UI pages in GitHub Checks
    uiBaseUrl: https://diff-reporter.example.com
```

Sensitive values (`appId`, `installationId`, `privateKey`, `webhook.secret`) can be set via environment variables: `GITHUB_APP_ID`, `GITHUB_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_WEBHOOK_SECRET`.

## Running

### Locally

You will need kubeconfig context set to a cluster with ArgoCD.

```bash
make local
```

### Docker

```bash
docker build -t argocd-diff-reporter .
docker run -v ./config.yml:/app/config.yml -p 8000:8000 argocd-diff-reporter
```

### Kubernetes

See the [deployment manifests](./deploy) example.

## License

See [LICENSE](LICENSE).
