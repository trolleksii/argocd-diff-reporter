# Architecture

Work in progress.

## Overview

A distributed system for rendering ArgoCD application diffs on GitHub Pull Requests. Translates PR changes (ApplicationSets/Applications) into final Kubernetes manifests and presents diffs to developers before merge.

**Problem:** ArgoCD's multi-stage rendering (ApplicationSet → Application → Helm/Kustomize → Manifests) makes it hard to understand PR impact.

**Solution:** Automated diff pipeline that checks out code at base/head SHA, renders to final manifests, calculates diffs, and displays in UI and/or GitHub Checks.

## Design Goals

- **Simple by default:** Single binary for small teams, distributed mode for scale
- **Scalable:** Independent scaling of webhook ingestion vs compute-heavy rendering
- **Reliable:** Queue-based processing with retries, no lost events
- **Fast feedback:** Async processing with real-time UI updates

## Deployment modes

The application can be run in a single binary mode which is perfect for small teams or local development.

Distributed mode allows independent scaling of components and provides higher throughput, reliability and availability.

## System components

At a high level ArgoCD Diff Reporter consists of the following components:

1. Application Server
2. Message bus
3. Workers(Git, ArgoCD, Helm, Kustomize, Diff)


## Application Server

The application server serves both as a receiver of Github Webhook events and as a UI for viewing Diff Reports.
User has access to three types of views:
- Repo preview (latest updated pull request and their processing status)
- PR overview (map of the files that were changed in a Pull Request to individual applications with their processing status)
- Application Diff Report

UI is built using HTMX with Websockets support for real time updates.

## Message Bus: NATS JetStream

Core infrastructure providing:
- **Queue:** Durable event streams with replay capability
- **Storage:** KV store for diff reports (embedded or standalone)
- **Flexibility:** Embedded mode (single binary) or distributed (scale independently)

Chosen over Kafka/RabbitMQ for:
- Simpler ops (no Zookeeper, lower resources)
- Built-in KV eliminates need for separate database
- Sufficient throughput for webhook workload (<10k events/day)

## Workers

Git workers are responsible for taking snapshots of Git repositories. This may be repository of a triggering Pull Request, or repository where helm chart is located. It is the biggest bottleneck in the entire application and scaling it out is the way to increase the number of pull requests you can process in parallel.

Orchestrator glues up other workers, performing fan-in/fan-out

Argo workers are responsible for rendering ApplicationSets into individual Applications.

Helm workers are responsible for fetching and rendering Helm charts.

Kustomize workers used for rendering customized plain manifests.

Diff workers generate and store Diff Reports.

## Processing Flow

1. **GitHub webhook** → Application Server publishes `PREvent` to NATS stream
2. **Orchestrator Worker** consumes event:
   - Requests snapshots from Git Worker (base + head SHA)
   - Sends ApplicationSets to Argo Worker → Applications
   - Routes to Helm/Kustomize Workers based on app type
   - Sends rendered manifests to Diff Worker
   - Notifies users about updates
3. **Diff Worker** generates report → stores in NATS KV (compressed)
4. **UI** reads from KV, displays real-time via WebSocket

## Orchestration

 Webhook             Fetcher/Snapshotter                    Argo                                HELM                               Coordinator                     Differ                Coordinator
pr.changed -> pr.enriched -> repo.snapshot.complete -> appset.render.complete -> helm.fetch.complete -> helm.render.complete--> report.preparation.complete -> report.generate.complete---> pr.processed
                        \                                                    \-> helm.fetch.complete -> helm.render.complete-/                                                           /
                         \-> repo.snapshot.complete -> appset.render.complete -> helm.fetch.complete -> helm.render.complete--> report.preparation.complete -> report.generate.complete-/
                                                                             \-> helm.fetch.complete -> helm.render.complete-/

Stages.
1. Raw event data; filtration according to config. 
   Payload: initial PR info
2. Fetching commits, finding from_files and to_files. Appending PR info with it.
   Payload: SHA, files list, PR Info.
3. Taking a snapshot of files at a sha.
   Payload: PR info, sha, repo location on disk
4. Loading files one by one; rendering applicationsets(or kustomize in the future).
   Payload: PR info, helm release info.
5. Fetching helm chart over git or https.
   Payload: PR info, chart location, helm release info
6. Rendering helm chart.
   Payload: PR info, manifests location in object store
7. Waiting until both base and sha helm renders are done
   Payload: PR info, location of manifests for base and head
8. Rendering of the diff report.
   Payload: PR info, location of the repord in object store
9. Notify stakeholdersi; consolidate PR info in the kv store 


## Data Storage

TBD

## Webhook events

The Pull Request event(opened, synchronized) that reaches the webhook server already contains some of the information necessary for processing and reporting the result:
  - Repository Owner and Name
  - PR Number
  - PR Title
  - PR Author
  - Branches(base and head)
  - Commit SHAs(base and head)

# Workers

## Git worker

Git worker has two responsibilities:

1. Fetch commits from the remote. Find the list of files that were changed.
   Lsten at `pr.changed`
   Fan-out to `pr.enriched`.
   Inputs:
     - Base SHA
     - Head SHA
     - PR Metadata
   Outputs:
     - PR metadata
     - Commit SHA
     - Changed files

2. Create snapshot of changed files.
   Listen at `pr.enriched`
   Publish to `repo.snapshot.complete`.
   Inputs:
     - PR metadata
     - Commit SHA
     - Changed files
   Outputs:
     - PR metadata
     - Commit SHA
     - Changed files
     - Snapshot Location
3. Create snapshot of a directory.
   Listen for events at `appset.render.complete`
   Publish to `helm.fetch.complete` (TBD: what about kustomize?)
   Inputs:
     - ...
   Outputs:
     - PR metadata
     - File name
     - Application name
     - Helm values location
     - Snapshot location

### Snapshot of files

In this context worker is always aware of commit shas it needs to fetch and a precise list of files to snapshot.

### Snapshot of a directory(ArgoCD Application source)

In this context source is a directory in a git repo. The main challenge is that `targetRevision` in ArgoCD manifests can be either of branch, tag, or commit sha. The worker has to detect which type is it, and then fetch the associated commit and make a snapshot of that directory.
