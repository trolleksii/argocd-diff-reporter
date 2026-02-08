# Architecture

This is a rough view on how system will look like.

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

## Data Storage

TBD
