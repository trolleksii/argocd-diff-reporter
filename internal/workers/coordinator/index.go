package coordinator

import (
	"slices"

	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// Index holds the N most recently updated PRs, most recent first.
type Index struct {
	items  []models.ProcessedPR
	maxCap int
}

// NewIndex creates a new index of processed PRs of certain maximum capacity, and an optional initial state.
func NewIndex(maxCap int, state []models.ProcessedPR) *Index {
	items := make([]models.ProcessedPR, 0, maxCap)
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
func (idx *Index) Update(pr models.ProcessedPR) {
	for i, item := range idx.items {
		if item.Number == pr.Number {
			idx.items = append(idx.items[:i], idx.items[i+1:]...)
			break
		}
	}
	if len(idx.items) >= idx.maxCap {
		idx.items = idx.items[:len(idx.items)-1] // drop oldest
	}
	idx.items = append([]models.ProcessedPR{pr}, idx.items...)
}

// UpdateStatus sets the status of a tracked PR. No-op if the PR is not found.
func (idx *Index) UpdateStatus(number string, status models.PipelineStatus) {
	for i, item := range idx.items {
		if item.Number == number {
			idx.items[i].Status = status
			return
		}
	}
}

// GetElements returns all tracked PRs, most recent first.
func (idx *Index) GetElements() []models.ProcessedPR {
	return slices.Clone(idx.items)
}
