package coordinator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trolleksii/argocd-diff-reporter/internal/models"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func pr(number, owner, repo string, status models.PipelineStatus) models.PullRequest {
	return models.PullRequest{
		Number: number,
		Owner:  owner,
		Repo:   repo,
		Status: status,
	}
}

func numbers(prs []models.PullRequest) []string {
	out := make([]string, len(prs))
	for i, p := range prs {
		out[i] = p.Number
	}
	return out
}

// ---------------------------------------------------------------------------
// NewIndex
// ---------------------------------------------------------------------------

func TestNewIndex_EmptyState(t *testing.T) {
	idx := NewIndex(5)
	require.NotNil(t, idx)
	assert.Empty(t, idx.GetElements())
}

func TestLoad_WithState(t *testing.T) {
	state := []models.PullRequest{
		pr("1", "org", "repo", models.PipelineSucceeded),
		pr("2", "org", "repo", models.PipelineInProgress),
	}
	idx := NewIndex(5)
	idx.Load(state)
	elems := idx.GetElements()
	require.Len(t, elems, 2)
	assert.Equal(t, "1", elems[0].Number)
	assert.Equal(t, "2", elems[1].Number)
}

func TestLoad_StateTruncatedToMaxCap(t *testing.T) {
	state := []models.PullRequest{
		pr("1", "org", "repo", models.PipelineSucceeded),
		pr("2", "org", "repo", models.PipelineSucceeded),
		pr("3", "org", "repo", models.PipelineSucceeded),
	}
	idx := NewIndex(2)
	idx.Load(state)
	elems := idx.GetElements()
	require.Len(t, elems, 2)
	assert.Equal(t, []string{"1", "2"}, numbers(elems))
}

func TestNewIndex_ZeroCapacity(t *testing.T) {
	idx := NewIndex(0)
	require.NotNil(t, idx)
	assert.Empty(t, idx.GetElements())
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

func TestUpdate_NewPRAddedToFront(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("2", "org", "repo", models.PipelineInProgress))

	elems := idx.GetElements()
	require.Len(t, elems, 2)
	assert.Equal(t, "2", elems[0].Number, "most recent PR should be first")
	assert.Equal(t, "1", elems[1].Number)
}

func TestUpdate_ExistingPRMovedToFront(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("2", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("3", "org", "repo", models.PipelineInProgress))

	// Re-update PR "1" with new data — it should move to front
	updated := pr("1", "org", "repo", models.PipelineSucceeded)
	updated.Title = "updated title"
	idx.Update(updated)

	elems := idx.GetElements()
	require.Len(t, elems, 3)
	assert.Equal(t, "1", elems[0].Number, "updated PR should be at the front")
	assert.Equal(t, "updated title", elems[0].Title, "PR data should be updated")
	assert.Equal(t, models.PipelineSucceeded, elems[0].Status)
}

func TestUpdate_LRUEviction(t *testing.T) {
	maxCap := 3
	idx := NewIndex(maxCap)

	idx.Update(pr("1", "org", "repo", models.PipelineSucceeded))
	idx.Update(pr("2", "org", "repo", models.PipelineSucceeded))
	idx.Update(pr("3", "org", "repo", models.PipelineSucceeded))

	// Adding a 4th PR should evict the oldest ("1")
	idx.Update(pr("4", "org", "repo", models.PipelineSucceeded))

	elems := idx.GetElements()
	require.Len(t, elems, maxCap, "index should not exceed maxCap")
	assert.Equal(t, []string{"4", "3", "2"}, numbers(elems))
	for _, e := range elems {
		assert.NotEqual(t, "1", e.Number, "oldest PR should have been evicted")
	}
}

func TestUpdate_LRUEviction_ReinsertedPRNotDoubleEvicted(t *testing.T) {
	// Filling to cap then re-inserting an existing PR should not shrink the list
	idx := NewIndex(3)
	idx.Update(pr("1", "org", "repo", models.PipelineSucceeded))
	idx.Update(pr("2", "org", "repo", models.PipelineSucceeded))
	idx.Update(pr("3", "org", "repo", models.PipelineSucceeded))

	// Re-update an existing PR — count should remain 3, nothing evicted
	idx.Update(pr("2", "org", "repo", models.PipelineSucceeded))

	elems := idx.GetElements()
	assert.Len(t, elems, 3)
	assert.Equal(t, "2", elems[0].Number)
}

// ---------------------------------------------------------------------------
// UpdateStatus
// ---------------------------------------------------------------------------

func TestUpdateStatus_ExistingPR(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("2", "org", "repo", models.PipelineInProgress))

	idx.UpdateStatus(models.PullRequest{
		Number: "1",
		Owner:  "org",
		Repo:   "repo",
		Status: models.PipelineSucceeded,
	})

	elems := idx.GetElements()
	var pr1 *models.PullRequest
	for i := range elems {
		if elems[i].Number == "1" {
			pr1 = &elems[i]
			break
		}
	}
	require.NotNil(t, pr1)
	assert.Equal(t, models.PipelineSucceeded, pr1.Status)
}

func TestUpdateStatus_NonexistentPR_NoOp(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineInProgress))

	// Should not panic and should not change anything
	idx.UpdateStatus(models.PullRequest{
		Number: "999",
		Owner:  "org",
		Repo:   "repo",
		Status: models.PipelineFailed,
	})

	elems := idx.GetElements()
	require.Len(t, elems, 1)
	assert.Equal(t, models.PipelineInProgress, elems[0].Status)
}

func TestUpdateStatus_MatchesOwnerRepoAndNumber(t *testing.T) {
	// UpdateStatus matches on Owner+Repo+Number; use distinct numbers to keep both in the index
	// (Update deduplicates by Number alone, so use different numbers for different repos).
	idx := NewIndex(5)
	idx.Update(pr("10", "org-a", "repo", models.PipelineInProgress))
	idx.Update(pr("11", "org-b", "repo", models.PipelineInProgress))

	// UpdateStatus with Owner=org-a, Number=10 should only touch the first entry
	idx.UpdateStatus(models.PullRequest{
		Number: "10",
		Owner:  "org-a",
		Repo:   "repo",
		Status: models.PipelineSucceeded,
	})

	elems := idx.GetElements()
	require.Len(t, elems, 2)
	for _, e := range elems {
		if e.Number == "10" {
			assert.Equal(t, models.PipelineSucceeded, e.Status, "PR 10/org-a should be updated")
		} else {
			assert.Equal(t, models.PipelineInProgress, e.Status, "PR 11/org-b should be unchanged")
		}
	}
}

func TestUpdateStatus_DoesNotChangeOrder(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("2", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("3", "org", "repo", models.PipelineInProgress))

	idx.UpdateStatus(models.PullRequest{
		Number: "1",
		Owner:  "org",
		Repo:   "repo",
		Status: models.PipelineFailed,
	})

	assert.Equal(t, []string{"3", "2", "1"}, numbers(idx.GetElements()))
}

// ---------------------------------------------------------------------------
// GetElements
// ---------------------------------------------------------------------------

func TestGetElements_ReturnsClone(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineInProgress))
	idx.Update(pr("2", "org", "repo", models.PipelineInProgress))

	elems := idx.GetElements()
	// Mutate the returned slice
	elems[0].Title = "mutated"
	elems = append(elems, pr("99", "org", "repo", models.PipelineSucceeded))

	// The index should be unaffected
	fresh := idx.GetElements()
	require.Len(t, fresh, 2, "index length should not change after mutating the returned slice")
	assert.NotEqual(t, "mutated", fresh[0].Title, "index items should not be affected by external mutation")
}

func TestGetElements_MostRecentFirst(t *testing.T) {
	idx := NewIndex(5)
	idx.Update(pr("1", "org", "repo", models.PipelineSucceeded))
	idx.Update(pr("2", "org", "repo", models.PipelineSucceeded))
	idx.Update(pr("3", "org", "repo", models.PipelineSucceeded))

	assert.Equal(t, []string{"3", "2", "1"}, numbers(idx.GetElements()))
}
