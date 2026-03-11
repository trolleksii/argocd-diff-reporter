# Instructions

You have access to the gopls-lsp plugin. Use it to navigate effectively through the codebase.

# Architecture Overview

## Overview

**argocd-diff-reporter** is a Go-based web service that provides visual diffs for ArgoCD ApplicationSets in Kubernetes GitOps workflows. It reacts to GitHub PRs, renders Application into their filnal representation, and generates structured diff reports showing what changes will be deployed, helping teams preview and validate infrastructure changes before they're merged and deployed via ArgoCD.

Reports can be viewed with web-based UI.

## Core Concept

The system is a **data enrichment pipeline** that forks into parallel workstreams, each producing artifacts with deterministic IDs. A separate **reactive event loop** observes artifact completion and performs higher-order operations when a complete subset is available.

An example:
1. A single PR forks into rendering the base and the head
2. Each of those may have one or more files with Applications/ApplicationSets
3. ApplicationSets will render one or more Application
4. Each application will be rendered into the manifest
5. Diffing module need base and head version of the same application
6. Reporting PR success need results of diffing all Applications grouped by filename that owns them.


The system is comprised of multiple workers specialising in a particular domain (git operations, helm operations, kustomize operations, argocd operations, etc). Each worker get's the event with processing details, performs the work and passes thei enriched data until eventually either manifest is produced and stored, or the error is reported.

The Coordinator is an event loop that looks for completion of logical units of work (eg base+head manifest of an app, all diff reports for a file, all files for the PR) and then updates the PR status in the database.
