package coordinator

import (
	"slices"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// Index holds the N most recently updated PRs, most recent first.
type Index struct {
	items  []models.PullRequest
	maxCap int
}

// NewIndex creates a new index of processed PRs of certain maximum capacity, and an optional initial state.
func NewIndex(maxCap int, state []models.PullRequest) *Index {
	items := make([]models.PullRequest, 0, maxCap)
	if state != nil {
		//append at most maxCap items from the state into the index
		items = append(items, state[:min(len(state), maxCap)]...)
	}
	return &Index{
		items:  items,
		maxCap: maxCap,
	}
}

// Update refreshes the index considering a new PR processing event.
func (idx *Index) Update(pr models.PullRequest) {
	for i, item := range idx.items {
		if item.Owner == pr.Owner && item.Repo == pr.Repo && item.Number == pr.Number {
			idx.items = append(idx.items[:i], idx.items[i+1:]...)
			break
		}
	}
	if len(idx.items) >= idx.maxCap {
		idx.items = idx.items[:len(idx.items)-1] // drop oldest
	}
	idx.items = append([]models.PullRequest{pr}, idx.items...)
}

// UpdateStatus sets the status of a tracked PR. No-op if the PR is not found.
func (idx *Index) UpdateStatus(pr models.PullRequest) {
	for i, item := range idx.items {
		if item.Owner == pr.Owner && item.Repo == pr.Repo && item.Number == pr.Number {
			idx.items[i].Status = pr.Status
			return
		}
	}
}

// GetElements returns all tracked PRs, most recent first.
func (idx *Index) GetElements() []models.PullRequest {
	return slices.Clone(idx.items)
}
