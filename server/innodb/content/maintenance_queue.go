package content

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maintenanceQueueVersion = 1
	maxMaintenanceJobBytes  = 1 << 20
)

var (
	ErrMaintenanceConflict    = errors.New("maintenance job generation conflict")
	ErrMaintenanceLeaseFenced = errors.New("maintenance worker lease is fenced")
)

type MaintenanceStatus string

const (
	MaintenanceQueued MaintenanceStatus = "QUEUED"
	MaintenanceLeased MaintenanceStatus = "LEASED"
	MaintenanceReady  MaintenanceStatus = "READY"
	MaintenanceFailed MaintenanceStatus = "FAILED"
)

// MaintenanceRequest identifies deterministic work over one immutable source
// and processor version. Re-enqueueing the same request is idempotent.
type MaintenanceRequest struct {
	Source              SourceRef
	ProcessorID         string
	ProcessorVersion    string
	ConfigurationDigest string
}

// MaintenanceJob is a durable state snapshot. Each transition publishes an
// immutable generation so readers never observe a partial update.
type MaintenanceJob struct {
	ID               string            `json:"id"`
	Generation       uint64            `json:"generation"`
	Source           SourceRef         `json:"source"`
	ProcessorID      string            `json:"processor_id"`
	ProcessorVersion string            `json:"processor_version"`
	ConfigurationSHA string            `json:"configuration_sha256"`
	Status           MaintenanceStatus `json:"status"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	NextAttemptAt    time.Time         `json:"next_attempt_at,omitempty"`
	Attempts         uint32            `json:"attempts"`
	FencingToken     uint64            `json:"fencing_token"`
	LeaseOwner       string            `json:"lease_owner,omitempty"`
	LeaseExpiresAt   time.Time         `json:"lease_expires_at,omitempty"`
	ResultDigest     string            `json:"result_digest,omitempty"`
	LastError        string            `json:"last_error,omitempty"`
}

type MaintenanceLease struct {
	Job          MaintenanceJob
	Owner        string
	FencingToken uint64
	ExpiresAt    time.Time
}

type maintenanceEnvelope struct {
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

type MaintenanceQueue struct {
	dir string
	mu  sync.Mutex
}

func (s *SegmentStore) OpenMaintenanceQueue() (*MaintenanceQueue, error) {
	if s == nil {
		return nil, fmt.Errorf("CONTENT segment store is nil")
	}
	dir := filepath.Join(s.root, "maintenance")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("create CONTENT maintenance directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("CONTENT maintenance path must be a non-symlink directory")
	}
	return &MaintenanceQueue{dir: dir}, nil
}

func (q *MaintenanceQueue) Enqueue(request MaintenanceRequest, now time.Time) (MaintenanceJob, bool, error) {
	if q == nil {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance queue is nil")
	}
	if err := validateMaintenanceRequest(request); err != nil {
		return MaintenanceJob{}, false, err
	}
	if now.IsZero() {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance enqueue time must be set")
	}
	request.Source.PrimaryKey = append([]byte(nil), request.Source.PrimaryKey...)
	request.ProcessorID = strings.TrimSpace(request.ProcessorID)
	request.ProcessorVersion = strings.TrimSpace(request.ProcessorVersion)
	id, err := maintenanceJobID(request)
	if err != nil {
		return MaintenanceJob{}, false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, found, err := q.readLatest(id); err != nil {
		return MaintenanceJob{}, false, err
	} else if found {
		return existing, false, nil
	}
	job := MaintenanceJob{
		ID: id, Generation: 1, Source: request.Source, ProcessorID: request.ProcessorID,
		ProcessorVersion: request.ProcessorVersion, ConfigurationSHA: strings.ToLower(request.ConfigurationDigest), Status: MaintenanceQueued,
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
	if err := q.publish(0, job); err != nil {
		if errors.Is(err, ErrMaintenanceConflict) {
			existing, found, readErr := q.readLatest(id)
			if readErr == nil && found {
				return existing, false, nil
			}
		}
		return MaintenanceJob{}, false, err
	}
	return cloneMaintenanceJob(job), true, nil
}

func (q *MaintenanceQueue) Get(id string) (MaintenanceJob, bool, error) {
	if q == nil || !validMaintenanceID(id) {
		return MaintenanceJob{}, false, fmt.Errorf("invalid maintenance queue or job ID")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, found, err := q.readLatest(id)
	return cloneMaintenanceJob(job), found, err
}

// Claim chooses the oldest ready request and increments its fencing token.
// Expired leases may be reclaimed; an old worker can no longer publish after
// the newer generation becomes visible.
func (q *MaintenanceQueue) Claim(owner string, now time.Time, duration time.Duration) (MaintenanceLease, bool, error) {
	if q == nil || strings.TrimSpace(owner) == "" || len(owner) > 256 || now.IsZero() || duration <= 0 || now.Add(duration).Before(now) {
		return MaintenanceLease{}, false, fmt.Errorf("invalid maintenance lease request")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	jobs, err := q.listLatest()
	if err != nil {
		return MaintenanceLease{}, false, err
	}
	sort.Slice(jobs, func(i, j int) bool {
		if !jobs[i].CreatedAt.Equal(jobs[j].CreatedAt) {
			return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
		}
		return jobs[i].ID < jobs[j].ID
	})
	for _, job := range jobs {
		claimable := job.Status == MaintenanceQueued && !job.NextAttemptAt.After(now) ||
			job.Status == MaintenanceLeased && !job.LeaseExpiresAt.After(now)
		if !claimable {
			continue
		}
		if job.Generation == ^uint64(0) || job.FencingToken == ^uint64(0) || job.Attempts == ^uint32(0) {
			return MaintenanceLease{}, false, fmt.Errorf("maintenance job counters exhausted")
		}
		job.Generation++
		job.FencingToken++
		job.Attempts++
		job.Status = MaintenanceLeased
		job.LeaseOwner = strings.TrimSpace(owner)
		job.LeaseExpiresAt = now.Add(duration).UTC()
		job.NextAttemptAt = time.Time{}
		job.UpdatedAt = now.UTC()
		if err := q.publish(job.Generation-1, job); err != nil {
			if errors.Is(err, ErrMaintenanceConflict) {
				continue
			}
			return MaintenanceLease{}, false, err
		}
		return MaintenanceLease{Job: cloneMaintenanceJob(job), Owner: job.LeaseOwner, FencingToken: job.FencingToken, ExpiresAt: job.LeaseExpiresAt}, true, nil
	}
	return MaintenanceLease{}, false, nil
}

func (q *MaintenanceQueue) Heartbeat(lease MaintenanceLease, now time.Time, duration time.Duration) (MaintenanceLease, error) {
	if q == nil || now.IsZero() || duration <= 0 || now.Add(duration).Before(now) {
		return MaintenanceLease{}, fmt.Errorf("invalid maintenance heartbeat")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.validateLease(lease, now)
	if err != nil {
		return MaintenanceLease{}, err
	}
	if job.Generation == ^uint64(0) {
		return MaintenanceLease{}, fmt.Errorf("maintenance job generation exhausted")
	}
	job.Generation++
	job.LeaseExpiresAt = now.Add(duration).UTC()
	job.UpdatedAt = now.UTC()
	if err := q.publish(job.Generation-1, job); err != nil {
		return MaintenanceLease{}, err
	}
	return MaintenanceLease{Job: cloneMaintenanceJob(job), Owner: job.LeaseOwner, FencingToken: job.FencingToken, ExpiresAt: job.LeaseExpiresAt}, nil
}

func (q *MaintenanceQueue) Complete(lease MaintenanceLease, now time.Time, resultDigest string) (MaintenanceJob, error) {
	if q == nil || !validSHA256Hex(resultDigest) || now.IsZero() {
		return MaintenanceJob{}, fmt.Errorf("invalid maintenance completion")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.validateLease(lease, now)
	if err != nil {
		return MaintenanceJob{}, err
	}
	if job.Generation == ^uint64(0) {
		return MaintenanceJob{}, fmt.Errorf("maintenance job generation exhausted")
	}
	job.Generation++
	job.Status = MaintenanceReady
	job.ResultDigest = strings.ToLower(resultDigest)
	job.LeaseOwner = ""
	job.LeaseExpiresAt = time.Time{}
	job.LastError = ""
	job.UpdatedAt = now.UTC()
	if err := q.publish(job.Generation-1, job); err != nil {
		return MaintenanceJob{}, err
	}
	return cloneMaintenanceJob(job), nil
}

func (q *MaintenanceQueue) Retry(lease MaintenanceLease, now time.Time, delay time.Duration, cause error) (MaintenanceJob, error) {
	if q == nil || now.IsZero() || delay < 0 || cause == nil || now.Add(delay).Before(now) {
		return MaintenanceJob{}, fmt.Errorf("invalid maintenance retry")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.validateLease(lease, now)
	if err != nil {
		return MaintenanceJob{}, err
	}
	if job.Generation == ^uint64(0) {
		return MaintenanceJob{}, fmt.Errorf("maintenance job generation exhausted")
	}
	message := maintenanceErrorMessage(cause)
	job.Generation++
	job.Status = MaintenanceQueued
	job.NextAttemptAt = now.Add(delay).UTC()
	job.LeaseOwner = ""
	job.LeaseExpiresAt = time.Time{}
	job.LastError = message
	job.UpdatedAt = now.UTC()
	if err := q.publish(job.Generation-1, job); err != nil {
		return MaintenanceJob{}, err
	}
	return cloneMaintenanceJob(job), nil
}

func (q *MaintenanceQueue) Fail(lease MaintenanceLease, now time.Time, cause error) (MaintenanceJob, error) {
	if q == nil || now.IsZero() || cause == nil {
		return MaintenanceJob{}, fmt.Errorf("invalid maintenance failure")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	job, err := q.validateLease(lease, now)
	if err != nil {
		return MaintenanceJob{}, err
	}
	if job.Generation == ^uint64(0) {
		return MaintenanceJob{}, fmt.Errorf("maintenance job generation exhausted")
	}
	message := maintenanceErrorMessage(cause)
	job.Generation++
	job.Status = MaintenanceFailed
	job.LeaseOwner = ""
	job.LeaseExpiresAt = time.Time{}
	job.LastError = message
	job.UpdatedAt = now.UTC()
	if err := q.publish(job.Generation-1, job); err != nil {
		return MaintenanceJob{}, err
	}
	return cloneMaintenanceJob(job), nil
}

func (q *MaintenanceQueue) validateLease(lease MaintenanceLease, now time.Time) (MaintenanceJob, error) {
	if !validMaintenanceID(lease.Job.ID) || strings.TrimSpace(lease.Owner) == "" || lease.FencingToken == 0 {
		return MaintenanceJob{}, ErrMaintenanceLeaseFenced
	}
	job, found, err := q.readLatest(lease.Job.ID)
	if err != nil {
		return MaintenanceJob{}, err
	}
	if !found || job.Status != MaintenanceLeased || job.Generation != lease.Job.Generation ||
		job.FencingToken != lease.FencingToken || job.LeaseOwner != lease.Owner || !job.LeaseExpiresAt.After(now) {
		return MaintenanceJob{}, ErrMaintenanceLeaseFenced
	}
	return job, nil
}

func (q *MaintenanceQueue) listLatest() ([]MaintenanceJob, error) {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return nil, fmt.Errorf("list maintenance jobs: %w", err)
	}
	ids := make(map[string]struct{})
	for _, entry := range entries {
		if !entry.IsDir() || !validMaintenanceID(entry.Name()) {
			continue
		}
		ids[entry.Name()] = struct{}{}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	jobs := make([]MaintenanceJob, 0, len(ordered))
	for _, id := range ordered {
		job, found, err := q.readLatest(id)
		if err != nil {
			return nil, err
		}
		if found {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (q *MaintenanceQueue) readLatest(id string) (MaintenanceJob, bool, error) {
	jobDir := filepath.Join(q.dir, id)
	directoryInfo, err := os.Lstat(jobDir)
	if errors.Is(err, os.ErrNotExist) {
		return MaintenanceJob{}, false, nil
	}
	if err != nil || !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance job path must be a non-symlink directory")
	}
	entries, err := os.ReadDir(jobDir)
	if err != nil {
		return MaintenanceJob{}, false, fmt.Errorf("list maintenance job generations: %w", err)
	}
	var latestName string
	var latestGeneration uint64
	found := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".job") {
			continue
		}
		generationText := strings.TrimSuffix(entry.Name(), ".job")
		generation, parseErr := strconv.ParseUint(generationText, 10, 64)
		if parseErr != nil || generation == 0 {
			continue
		}
		if !found || generation > latestGeneration {
			latestName, latestGeneration, found = entry.Name(), generation, true
		}
	}
	if !found {
		return MaintenanceJob{}, false, nil
	}
	path := filepath.Join(jobDir, latestName)
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxMaintenanceJobBytes {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance job generation is not a bounded regular file")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return MaintenanceJob{}, false, fmt.Errorf("read maintenance job generation: %w", err)
	}
	var envelope maintenanceEnvelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return MaintenanceJob{}, false, fmt.Errorf("decode maintenance job envelope: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF || envelope.Version != maintenanceQueueVersion || len(envelope.Payload) == 0 {
		return MaintenanceJob{}, false, fmt.Errorf("invalid maintenance job envelope")
	}
	digest := sha256.Sum256(envelope.Payload)
	if envelope.SHA256 != hex.EncodeToString(digest[:]) {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance job checksum mismatch")
	}
	var job MaintenanceJob
	payloadDecoder := json.NewDecoder(bytes.NewReader(envelope.Payload))
	payloadDecoder.DisallowUnknownFields()
	if err := payloadDecoder.Decode(&job); err != nil {
		return MaintenanceJob{}, false, fmt.Errorf("decode maintenance job payload: %w", err)
	}
	if err := payloadDecoder.Decode(new(any)); err != io.EOF {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance job payload contains trailing data")
	}
	if job.ID != id || job.Generation != latestGeneration {
		return MaintenanceJob{}, false, fmt.Errorf("maintenance job filename does not match payload")
	}
	if err := validateMaintenanceJob(job); err != nil {
		return MaintenanceJob{}, false, err
	}
	return job, true, nil
}

func (q *MaintenanceQueue) publish(expectedGeneration uint64, job MaintenanceJob) error {
	jobDir := filepath.Join(q.dir, job.ID)
	if expectedGeneration == 0 {
		if err := os.Mkdir(jobDir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("create maintenance job directory: %w", err)
		}
		if err := syncDirectory(q.dir); err != nil {
			return fmt.Errorf("sync maintenance queue directory: %w", err)
		}
	}
	current, found, err := q.readLatest(job.ID)
	if err != nil {
		return err
	}
	if (!found && expectedGeneration != 0) || (found && current.Generation != expectedGeneration) {
		return ErrMaintenanceConflict
	}
	if (expectedGeneration == 0 && job.Generation != 1) || (expectedGeneration > 0 && job.Generation != expectedGeneration+1) {
		return fmt.Errorf("maintenance job generation must advance exactly once")
	}
	encoded, err := marshalMaintenanceJob(job)
	if err != nil {
		return err
	}
	if len(encoded) > maxMaintenanceJobBytes {
		return fmt.Errorf("maintenance job exceeds %d bytes", maxMaintenanceJobBytes)
	}
	temporary, err := os.CreateTemp(jobDir, ".stage-")
	if err != nil {
		return fmt.Errorf("create maintenance job staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	finalPath := filepath.Join(jobDir, maintenanceJobFilename(job.Generation))
	if err := os.Link(temporaryPath, finalPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrMaintenanceConflict
		}
		return fmt.Errorf("publish maintenance job generation: %w", err)
	}
	if err := syncDirectory(jobDir); err != nil {
		return fmt.Errorf("sync maintenance job directory: %w", err)
	}
	return nil
}

func maintenanceJobID(request MaintenanceRequest) (string, error) {
	identity, err := json.Marshal(struct {
		Source              SourceRef `json:"source"`
		ProcessorID         string    `json:"processor_id"`
		ProcessorVersion    string    `json:"processor_version"`
		ConfigurationDigest string    `json:"configuration_sha256"`
	}{request.Source, request.ProcessorID, request.ProcessorVersion, strings.ToLower(request.ConfigurationDigest)})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(identity)
	return hex.EncodeToString(digest[:]), nil
}

func validateMaintenanceRequest(request MaintenanceRequest) error {
	if err := request.Source.Validate(); err != nil {
		return fmt.Errorf("maintenance source is invalid: %w", err)
	}
	if !validMaintenanceName(request.ProcessorID) || !validMaintenanceName(request.ProcessorVersion) || !validSHA256Hex(request.ConfigurationDigest) {
		return fmt.Errorf("maintenance processor identity is invalid")
	}
	return nil
}

func validateMaintenanceJob(job MaintenanceJob) error {
	if !validMaintenanceID(job.ID) || job.Generation == 0 || job.CreatedAt.IsZero() || job.UpdatedAt.IsZero() ||
		job.CreatedAt.After(job.UpdatedAt) || validateMaintenanceRequest(MaintenanceRequest{
		Source: job.Source, ProcessorID: job.ProcessorID, ProcessorVersion: job.ProcessorVersion,
		ConfigurationDigest: job.ConfigurationSHA,
	}) != nil {
		return fmt.Errorf("invalid maintenance job identity or timestamps")
	}
	switch job.Status {
	case MaintenanceQueued:
		if job.LeaseOwner != "" || !job.LeaseExpiresAt.IsZero() || job.ResultDigest != "" {
			return fmt.Errorf("queued maintenance job contains lease or result state")
		}
	case MaintenanceLeased:
		if job.LeaseOwner == "" || job.LeaseExpiresAt.IsZero() || job.FencingToken == 0 || job.ResultDigest != "" {
			return fmt.Errorf("leased maintenance job has incomplete lease state")
		}
	case MaintenanceReady:
		if job.FencingToken == 0 || job.LeaseOwner != "" || !job.LeaseExpiresAt.IsZero() || !validSHA256Hex(job.ResultDigest) {
			return fmt.Errorf("ready maintenance job has invalid result state")
		}
	case MaintenanceFailed:
		if job.FencingToken == 0 || job.LeaseOwner != "" || !job.LeaseExpiresAt.IsZero() || job.LastError == "" {
			return fmt.Errorf("failed maintenance job has invalid error state")
		}
	default:
		return fmt.Errorf("unknown maintenance job status %q", job.Status)
	}
	if uint64(job.Attempts) < job.FencingToken {
		return fmt.Errorf("maintenance attempts cannot be lower than its fencing token")
	}
	if job.ResultDigest != "" && !validSHA256Hex(job.ResultDigest) {
		return fmt.Errorf("invalid maintenance result digest")
	}
	return nil
}

func marshalMaintenanceJob(job MaintenanceJob) ([]byte, error) {
	if err := validateMaintenanceJob(job); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("encode maintenance job: %w", err)
	}
	digest := sha256.Sum256(payload)
	return json.Marshal(maintenanceEnvelope{Version: maintenanceQueueVersion, Payload: payload, SHA256: hex.EncodeToString(digest[:])})
}

func cloneMaintenanceJob(job MaintenanceJob) MaintenanceJob {
	job.Source.PrimaryKey = append([]byte(nil), job.Source.PrimaryKey...)
	return job
}

func maintenanceErrorMessage(cause error) string {
	message := strings.TrimSpace(cause.Error())
	if message == "" {
		return "unspecified maintenance failure"
	}
	if len(message) > 4096 {
		message = message[:4096]
	}
	return message
}

func maintenanceJobFilename(generation uint64) string {
	return fmt.Sprintf("%020d.job", generation)
}

func validMaintenanceID(id string) bool {
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == id
}

func validMaintenanceName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("._:-", char) {
			continue
		}
		return false
	}
	return true
}

func validSHA256Hex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == strings.ToLower(value)
}
