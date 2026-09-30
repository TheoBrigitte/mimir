// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const testJobID = "11111111-1111-1111-1111-111111111111"

type backfillTestServer struct {
	t         *testing.T
	mtx       sync.Mutex
	requests  []backfillTestRequest
	responses map[string][]int
}

type backfillTestRequest struct {
	path string
	file string
	body string
}

func newBackfillTestServer(t *testing.T, responses map[string][]int) (*backfillTestServer, *MimirClient) {
	s := &backfillTestServer{t: t, responses: responses}
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)

	c, err := New(Config{Address: srv.URL, ID: "test"}, log.NewNopLogger())
	require.NoError(t, err)
	c.backfillRetries = backoff.Config{MinBackoff: time.Millisecond, MaxBackoff: time.Millisecond, MaxRetries: 3}
	return s, c
}

func (s *backfillTestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Errorf("reading request body: %v", err)
	}

	s.mtx.Lock()
	s.requests = append(s.requests, backfillTestRequest{path: r.URL.Path, file: r.URL.Query().Get("path"), body: string(body)})
	status := http.StatusOK
	if queued := s.responses[r.URL.Path]; len(queued) > 0 {
		status = queued[0]
		s.responses[r.URL.Path] = queued[1:]
	}
	s.mtx.Unlock()

	if status != http.StatusOK {
		http.Error(w, http.StatusText(status), status)
		return
	}
	if r.URL.Path == "/api/v1/backfill/start" {
		_, _ = fmt.Fprintf(w, `{"job":%q}`, testJobID)
	}
}

func (s *backfillTestServer) paths() []string {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	paths := make([]string, 0, len(s.requests))
	for _, r := range s.requests {
		paths = append(paths, r.path)
	}
	return paths
}

func writeTestBlock(t *testing.T, dir string, blockID ulid.ULID, chunk string) string {
	blockDir := filepath.Join(dir, blockID.String())
	require.NoError(t, os.MkdirAll(filepath.Join(blockDir, block.ChunksDirname), 0o755))

	meta, err := json.Marshal(block.Meta{BlockMeta: tsdb.BlockMeta{ULID: blockID, Version: 1, MinTime: 100, MaxTime: 200}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, block.MetaFilename), meta, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, block.IndexFilename), []byte("index"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, block.ChunksDirname, "000001"), []byte(chunk), 0o644))
	return blockDir
}

func blockRequestPaths(blockID ulid.ULID, steps ...string) []string {
	paths := make([]string, 0, len(steps))
	for _, step := range steps {
		paths = append(paths, "/api/v1/backfill/"+testJobID+"/block/"+blockID.String()+"/"+step)
	}
	return paths
}

func TestStartBackfillJob(t *testing.T) {
	_, c := newBackfillTestServer(t, nil)

	jobID, err := c.StartBackfillJob(t.Context())
	require.NoError(t, err)
	assert.Equal(t, testJobID, jobID)
}

func TestFinishBackfillJob(t *testing.T) {
	s, c := newBackfillTestServer(t, nil)

	require.NoError(t, c.FinishBackfillJob(t.Context(), testJobID))
	assert.Equal(t, []string{"/api/v1/backfill/" + testJobID + "/finish"}, s.paths())
}

func TestUploadBackfillBlocks(t *testing.T) {
	dir := t.TempDir()
	uploaded := ulid.MustNew(1000, nil)
	rejected := ulid.MustNew(2000, nil)
	existing := ulid.MustNew(3000, nil)
	uploadedDir := writeTestBlock(t, dir, uploaded, "chunk")
	rejectedDir := writeTestBlock(t, dir, rejected, "chunk")
	existingDir := writeTestBlock(t, dir, existing, "chunk")

	s, c := newBackfillTestServer(t, map[string][]int{
		blockRequestPaths(rejected, "start")[0]: {http.StatusUnprocessableEntity},
		blockRequestPaths(existing, "start")[0]: {http.StatusConflict},
	})

	err := c.UploadBackfillBlocks(t.Context(), testJobID, []string{uploadedDir, rejectedDir, existingDir})
	require.EqualError(t, err, fmt.Sprintf("failed to upload 1 block(s) to backfill job %s: %s", testJobID, rejectedDir))

	var expected []string
	expected = append(expected, blockRequestPaths(uploaded, "start", "files", "files", "finish")...)
	expected = append(expected, blockRequestPaths(rejected, "start")...)
	expected = append(expected, blockRequestPaths(existing, "start")...)
	assert.Equal(t, expected, s.paths())
}

func TestUploadBackfillBlocks_Retries(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	filesPath := blockRequestPaths(blockID, "files")[0]
	finishPath := blockRequestPaths(blockID, "finish")[0]

	for name, tc := range map[string]struct {
		responses     map[string][]int
		expectedPaths []string
		expectFailure bool
	}{
		"retries a transient file upload failure and sends the whole file again": {
			responses:     map[string][]int{filesPath: {http.StatusServiceUnavailable}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "files", "finish"),
		},
		"retries 429": {
			responses:     map[string][]int{finishPath: {http.StatusTooManyRequests}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "finish", "finish"),
		},
		"treats a conflict on a retried finish as success": {
			responses:     map[string][]int{finishPath: {http.StatusInternalServerError, http.StatusConflict}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "finish", "finish"),
		},
		"treats a conflict on the first finish as an existing block": {
			responses:     map[string][]int{finishPath: {http.StatusConflict}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "finish"),
		},
		"does not retry other 4xx": {
			responses:     map[string][]int{filesPath: {http.StatusBadRequest}},
			expectedPaths: blockRequestPaths(blockID, "start", "files"),
			expectFailure: true,
		},
		"gives up after the maximum number of attempts": {
			responses:     map[string][]int{finishPath: {http.StatusBadGateway, http.StatusBadGateway, http.StatusBadGateway}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "finish", "finish", "finish"),
			expectFailure: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			blockDir := writeTestBlock(t, t.TempDir(), blockID, "chunk-content")
			s, c := newBackfillTestServer(t, tc.responses)

			err := c.UploadBackfillBlocks(t.Context(), testJobID, []string{blockDir})
			if tc.expectFailure {
				require.EqualError(t, err, fmt.Sprintf("failed to upload 1 block(s) to backfill job %s: %s", testJobID, blockDir))
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expectedPaths, s.paths())

			s.mtx.Lock()
			defer s.mtx.Unlock()
			expectedBodies := map[string]string{block.IndexFilename: "index", "chunks/000001": "chunk-content"}
			for _, r := range s.requests {
				if r.path == filesPath {
					assert.Equal(t, expectedBodies[r.file], r.body, "body of %s", r.file)
				}
			}
		})
	}
}
