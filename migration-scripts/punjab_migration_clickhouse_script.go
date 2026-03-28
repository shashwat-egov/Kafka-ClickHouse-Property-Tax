package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

var istLocation *time.Location

func init() {
	var err error
	istLocation, err = time.LoadLocation("Asia/Kolkata")
	if err != nil {
		panic("failed to load IST timezone")
	}
}

const (
	// 20k rows per batch (5x smaller than migration script's 100k).
	// Smaller batches = more frequent checkpoint saves = less work lost on VPN drop.
	defaultBatchSize = 20_000
	defaultWorkers   = 8
)

// ─── Helpers ────────────────────────────────────────────────────────────────

func msToIST(ms *int64) time.Time {
	if ms == nil {
		return time.Time{}
	}
	return time.UnixMilli(*ms).UTC()
}

func msToISTVal(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

func nullableString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func computeFinancialYear(epochMs int64) string {
	if epochMs == 0 {
		return ""
	}
	t := time.UnixMilli(epochMs).In(istLocation)
	startYear := t.Year()
	if t.Month() < time.April {
		startYear--
	}
	return fmt.Sprintf("%d-%02d", startYear, (startYear+1)%100)
}

func d(f float64) decimal.Decimal {
	return decimal.NewFromFloat(f)
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// ─── Checkpoint store (local JSON file) ─────────────────────────────────────
//
// After every successful batch send, the last keyset key is saved to a local
// JSON file.  If the VPN drops mid-sync, the next run reads this file and
// resumes each table+tenant from the exact row where it stopped.

// TenantProgress tracks per-tenant resume state.
type TenantProgress struct {
	LastKey    string `json:"last_key"`
	RowsSynced int64  `json:"rows_synced"`
	Completed  bool   `json:"completed"`
}

// TableCheckpoint stores per-table in-flight state.
type TableCheckpoint struct {
	Watermark    int64                      `json:"watermark"`
	NewWatermark int64                      `json:"new_watermark"`
	Phase        string                     `json:"phase,omitempty"` // for multi-phase tables (demand)
	Tenants      map[string]*TenantProgress `json:"tenants"`
	Completed    bool                       `json:"completed,omitempty"`
}

// CheckpointData is the root structure persisted to disk.
type CheckpointData struct {
	Tables map[string]*TableCheckpoint `json:"tables"`
}

// CheckpointStore manages atomic reads/writes of the checkpoint file.
// Disk writes are throttled to at most once per saveInterval (default 5s)
// to avoid I/O pressure from 64+ concurrent workers.  In-memory state is
// always current; on crash you lose at most ~5 seconds of progress.
type CheckpointStore struct {
	mu           sync.Mutex
	filePath     string
	data         CheckpointData
	dirty        bool
	lastSaveTime time.Time
	saveInterval time.Duration
}

const defaultSaveInterval = 5 * time.Second

func NewCheckpointStore(filePath string) *CheckpointStore {
	return &CheckpointStore{
		filePath:     filePath,
		data:         CheckpointData{Tables: make(map[string]*TableCheckpoint)},
		saveInterval: defaultSaveInterval,
	}
}

// Load reads an existing checkpoint file. Returns nil if file does not exist.
func (cs *CheckpointStore) Load() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	raw, err := os.ReadFile(cs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // first run
		}
		return fmt.Errorf("read checkpoint: %w", err)
	}
	if err := json.Unmarshal(raw, &cs.data); err != nil {
		return fmt.Errorf("parse checkpoint: %w", err)
	}
	if cs.data.Tables == nil {
		cs.data.Tables = make(map[string]*TableCheckpoint)
	}
	return nil
}

// save writes checkpoint atomically (write tmp → rename). Caller must hold mu.
func (cs *CheckpointStore) save() error {
	raw, err := json.MarshalIndent(cs.data, "", "  ")
	if err != nil {
		return err
	}
	tmp := cs.filePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, cs.filePath); err != nil {
		return err
	}
	cs.dirty = false
	cs.lastSaveTime = time.Now()
	return nil
}

// saveIfDue writes to disk only if saveInterval has elapsed since last write.
// Caller must hold mu. Returns nil if throttled (no-op).
func (cs *CheckpointStore) saveIfDue() error {
	cs.dirty = true
	if time.Since(cs.lastSaveTime) >= cs.saveInterval {
		return cs.save()
	}
	return nil
}

// Flush forces an immediate write to disk if there are pending changes.
// Safe to call from any goroutine.
func (cs *CheckpointStore) Flush() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if cs.dirty {
		return cs.save()
	}
	return nil
}

// InitTable creates or resets a table checkpoint entry.
func (cs *CheckpointStore) InitTable(table string, watermark, newWatermark int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	existing := cs.data.Tables[table]
	if existing != nil && !existing.Completed {
		// Already initialized from a previous interrupted run — keep it
		return
	}
	cs.data.Tables[table] = &TableCheckpoint{
		Watermark:    watermark,
		NewWatermark: newWatermark,
		Tenants:      make(map[string]*TenantProgress),
	}
	_ = cs.save()
}

// GetTableCheckpoint returns a snapshot of the table checkpoint (nil if not found).
func (cs *CheckpointStore) GetTableCheckpoint(table string) *TableCheckpoint {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.data.Tables[table]
}

// GetTenantProgress returns a tenant's checkpoint (nil if not found).
func (cs *CheckpointStore) GetTenantProgress(table, tenant string) *TenantProgress {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	tc := cs.data.Tables[table]
	if tc == nil {
		return nil
	}
	return tc.Tenants[tenant]
}

// UpdateTenant saves the current keyset position for a tenant.
// Disk write is throttled — in-memory state is always up-to-date.
func (cs *CheckpointStore) UpdateTenant(table, tenant, lastKey string, rowsSynced int64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	tc := cs.data.Tables[table]
	if tc == nil {
		tc = &TableCheckpoint{Tenants: make(map[string]*TenantProgress)}
		cs.data.Tables[table] = tc
	}
	tc.Tenants[tenant] = &TenantProgress{
		LastKey:    lastKey,
		RowsSynced: rowsSynced,
	}
	_ = cs.saveIfDue() // throttled: writes at most every 5s
}

// MarkTenantDone marks a tenant as fully synced. Always flushes to disk
// (called once per tenant, not on the hot path).
func (cs *CheckpointStore) MarkTenantDone(table, tenant string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	tc := cs.data.Tables[table]
	if tc == nil {
		return
	}
	if tp := tc.Tenants[tenant]; tp != nil {
		tp.Completed = true
	} else {
		tc.Tenants[tenant] = &TenantProgress{Completed: true}
	}
	_ = cs.save() // always flush: one-time per tenant
}

// SetPhase sets the current phase for multi-phase tables (demand).
func (cs *CheckpointStore) SetPhase(table, phase string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	tc := cs.data.Tables[table]
	if tc == nil {
		return
	}
	tc.Phase = phase
	_ = cs.save()
}

// GetPhase returns the current phase for a table.
func (cs *CheckpointStore) GetPhase(table string) string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	tc := cs.data.Tables[table]
	if tc == nil {
		return ""
	}
	return tc.Phase
}

// MarkTableDone marks a table as fully synced and removes its tenant entries.
func (cs *CheckpointStore) MarkTableDone(table string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	if tc := cs.data.Tables[table]; tc != nil {
		tc.Completed = true
		tc.Tenants = nil
		tc.Phase = ""
	}
	_ = cs.save()
}

// Remove deletes the checkpoint file (called after a fully successful run).
func (cs *CheckpointStore) Remove() error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	err := os.Remove(cs.filePath)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// ─── Watermark management (ClickHouse) ──────────────────────────────────────

func ensureWatermarkTable(ctx context.Context, chConn clickhouse.Conn) error {
	return chConn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _incremental_sync_watermark (
			table_name         String,
			last_sync_epoch_ms Int64,
			synced_at          DateTime DEFAULT now()
		) ENGINE = ReplacingMergeTree(synced_at)
		ORDER BY table_name
	`)
}

func getWatermark(ctx context.Context, chConn clickhouse.Conn, tableName string) (int64, error) {
	var wm int64
	err := chConn.QueryRow(ctx,
		`SELECT last_sync_epoch_ms FROM _incremental_sync_watermark FINAL WHERE table_name = $1`,
		tableName,
	).Scan(&wm)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return 0, nil
		}
		return 0, err
	}
	return wm, nil
}

func updateWatermark(ctx context.Context, chConn clickhouse.Conn, tableName string, epochMs int64) error {
	return chConn.Exec(ctx,
		`INSERT INTO _incremental_sync_watermark (table_name, last_sync_epoch_ms) VALUES ($1, $2)`,
		tableName, epochMs,
	)
}

func getMaxModifiedTime(ctx context.Context, pgPool *pgxpool.Pool, query string) (int64, error) {
	var maxMs int64
	err := pgPool.QueryRow(ctx, query).Scan(&maxMs)
	if err != nil {
		return 0, fmt.Errorf("max modified time: %w", err)
	}
	return maxMs, nil
}

func watermarkFilter(column string, ms int64) string {
	if ms <= 0 {
		return ""
	}
	return fmt.Sprintf(" AND %s > %d", column, ms)
}

// ─── Core migration engine ─────────────────────────────────────────────────

type tenantWithCount struct {
	id    string
	count int64
}

func getDistinctTenants(ctx context.Context, pgPool *pgxpool.Pool, query string) ([]string, error) {
	rows, err := pgPool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query tenants: %w", err)
	}
	defer rows.Close()
	var tenants []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tenants = append(tenants, t)
	}
	return tenants, rows.Err()
}

func getTenantsWithCounts(ctx context.Context, pgPool *pgxpool.Pool, tenantQuery string) ([]tenantWithCount, error) {
	countQuery := strings.Replace(tenantQuery, "SELECT DISTINCT tenantid", "SELECT tenantid, COUNT(*)", 1)
	if idx := strings.LastIndex(strings.ToUpper(countQuery), "ORDER BY"); idx != -1 {
		countQuery = countQuery[:idx]
	}
	countQuery += " GROUP BY tenantid ORDER BY COUNT(*) DESC"

	rows, err := pgPool.Query(ctx, countQuery)
	if err != nil {
		log.Printf("WARN: count query failed (%v), falling back to unsorted tenants", err)
		tenants, err2 := getDistinctTenants(ctx, pgPool, tenantQuery)
		if err2 != nil {
			return nil, err2
		}
		result := make([]tenantWithCount, len(tenants))
		for i, t := range tenants {
			result[i] = tenantWithCount{id: t, count: 0}
		}
		return result, nil
	}
	defer rows.Close()

	var result []tenantWithCount
	for rows.Next() {
		var t string
		var c int64
		if err := rows.Scan(&t, &c); err != nil {
			return nil, err
		}
		result = append(result, tenantWithCount{id: t, count: c})
	}
	return result, rows.Err()
}

type pageResult struct {
	lastKey string
	count   int
	err     error
}

func fetchPage(
	ctx context.Context,
	pgPool *pgxpool.Pool,
	chInsert string,
	chConn clickhouse.Conn,
	paginatedQuery string,
	processRow func(scan func(dest ...any) error, appendFn func(v ...any) error) error,
) (batch interface {
	Send() error
	Abort() error
}, result pageResult) {
	rows, err := pgPool.Query(ctx, paginatedQuery)
	if err != nil {
		return nil, pageResult{err: fmt.Errorf("query: %w", err)}
	}

	b, err := chConn.PrepareBatch(ctx, chInsert)
	if err != nil {
		rows.Close()
		return nil, pageResult{err: fmt.Errorf("prepare batch: %w", err)}
	}

	var keysetVal string
	var allDest []any
	count := 0
	for rows.Next() {
		wrappedScan := func(dest ...any) error {
			needed := 1 + len(dest)
			if cap(allDest) < needed {
				allDest = make([]any, 0, needed)
			}
			allDest = allDest[:0]
			allDest = append(allDest, &keysetVal)
			allDest = append(allDest, dest...)
			return rows.Scan(allDest...)
		}
		if err := processRow(wrappedScan, b.Append); err != nil {
			rows.Close()
			return nil, pageResult{err: fmt.Errorf("row %d: %w", count, err)}
		}
		count++
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, pageResult{err: fmt.Errorf("rows err: %w", err)}
	}
	return b, pageResult{lastKey: keysetVal, count: count}
}

// migrateForTenant runs keyset-paginated sync for a single tenant.
//
// startKey: if non-empty, resume from this keyset position (skip rows <= startKey).
// onBatchDone: called after each batch is successfully sent to CH, with the
// last keyset value and cumulative rows so far.  Used for checkpointing.
func migrateForTenant(
	ctx context.Context,
	pgPool *pgxpool.Pool,
	chConn clickhouse.Conn,
	tableName string,
	tenantID string,
	tenantIdx, totalTenants int,
	pgQueryTemplate string,
	chInsert string,
	batchSize int,
	keysetColumn string,
	processRow func(scan func(dest ...any) error, appendFn func(v ...any) error) error,
	globalCounter *int64,
	startKey string,
	onBatchDone func(lastKey string, rowsSoFar int64),
) (int64, error) {
	pgQuery := strings.ReplaceAll(pgQueryTemplate, "$1", quoteLiteral(tenantID))
	pgQueryWithKeyset := strings.Replace(pgQuery, "SELECT", "SELECT "+keysetColumn+",", 1)

	var tenantTotal int64
	var lastKey string
	page := 0

	type prefetchResult struct {
		batch interface {
			Send() error
			Abort() error
		}
		result pageResult
		page   int
	}

	// buildQuery constructs a keyset-paginated query.
	// If lk is empty, fetches from the beginning; otherwise fetches rows > lk.
	buildQuery := func(lk string) string {
		if lk == "" {
			return fmt.Sprintf("%s ORDER BY %s LIMIT %d",
				pgQueryWithKeyset, keysetColumn, batchSize)
		}
		return fmt.Sprintf("%s AND %s > %s ORDER BY %s LIMIT %d",
			pgQueryWithKeyset, keysetColumn, quoteLiteral(lk), keysetColumn, batchSize)
	}

	logPage := func(p int, lk string) {
		if lk == "" {
			log.Printf("[%s] tenant %s (%d/%d) — page %d: from start",
				tableName, tenantID, tenantIdx, totalTenants, p)
		} else {
			log.Printf("[%s] tenant %s (%d/%d) — page %d: %s > '%s'",
				tableName, tenantID, tenantIdx, totalTenants, p, keysetColumn, lk)
		}
	}

	// If resuming, start from the checkpoint key
	lastKey = startKey
	page = 1
	logPage(page, lastKey)
	curBatch, curResult := fetchPage(ctx, pgPool, chInsert, chConn, buildQuery(lastKey), processRow)
	if curResult.err != nil {
		return 0, curResult.err
	}
	if curResult.count == 0 {
		if curBatch != nil {
			_ = curBatch.Abort()
		}
		return 0, nil
	}
	lastKey = curResult.lastKey

	for {
		nextPage := page + 1
		nextKey := lastKey
		prefetchCh := make(chan prefetchResult, 1)
		go func(p int, lk string) {
			logPage(p, lk)
			b, r := fetchPage(ctx, pgPool, chInsert, chConn, buildQuery(lk), processRow)
			prefetchCh <- prefetchResult{batch: b, result: r, page: p}
		}(nextPage, nextKey)

		if err := curBatch.Send(); err != nil {
			pf := <-prefetchCh
			if pf.batch != nil {
				_ = pf.batch.Abort()
			}
			return tenantTotal, fmt.Errorf("send: %w", err)
		}

		tenantTotal += int64(curResult.count)
		atomic.AddInt64(globalCounter, int64(curResult.count))
		log.Printf("[%s] tenant %s (%d/%d) — page %d: sent %d rows (tenant total: %d)",
			tableName, tenantID, tenantIdx, totalTenants, page, curResult.count, tenantTotal)

		// Checkpoint: save progress after each successful batch
		if onBatchDone != nil {
			onBatchDone(lastKey, tenantTotal)
		}

		pf := <-prefetchCh
		if pf.result.err != nil {
			return tenantTotal, pf.result.err
		}
		if pf.result.count == 0 {
			if pf.batch != nil {
				_ = pf.batch.Abort()
			}
			break
		}

		page = pf.page
		curBatch = pf.batch
		curResult = pf.result
		lastKey = curResult.lastKey
	}
	return tenantTotal, nil
}

// parallelByTenant discovers tenants and distributes them across a worker pool.
// Uses cpStore to skip completed tenants and resume in-progress ones.
func parallelByTenant(
	ctx context.Context,
	pgPool *pgxpool.Pool,
	chConn clickhouse.Conn,
	tableName string,
	tenantQuery string,
	pgQueryTemplate string,
	chInsert string,
	batchSize int,
	workers int,
	keysetColumn string,
	processRow func(scan func(dest ...any) error, appendFn func(v ...any) error) error,
	globalCounter *int64,
	tenantFilter string,
	cpStore *CheckpointStore,
) (int64, error) {
	var tenantsWithCounts []tenantWithCount
	if tenantFilter != "" {
		tenantsWithCounts = []tenantWithCount{{id: tenantFilter, count: 0}}
	} else {
		var err error
		tenantsWithCounts, err = getTenantsWithCounts(ctx, pgPool, tenantQuery)
		if err != nil {
			return 0, fmt.Errorf("get tenants: %w", err)
		}
	}

	log.Printf("[%s] Starting — %d tenants, %d workers", tableName, len(tenantsWithCounts), workers)

	if len(tenantsWithCounts) == 0 {
		log.Printf("[%s] No tenants with new data, skipping", tableName)
		return 0, nil
	}

	sort.Slice(tenantsWithCounts, func(i, j int) bool {
		return tenantsWithCounts[i].count > tenantsWithCounts[j].count
	})

	type tenantWork struct {
		id  string
		idx int
	}
	tenantCh := make(chan tenantWork, len(tenantsWithCounts))
	for i, t := range tenantsWithCounts {
		tenantCh <- tenantWork{id: t.id, idx: i + 1}
	}
	close(tenantCh)
	totalTenants := len(tenantsWithCounts)

	activeWorkers := workers
	if activeWorkers > totalTenants {
		activeWorkers = totalTenants
	}

	var tableTotal int64
	var completedTenants int64
	var mu sync.Mutex
	var migrationErrors []string
	var wg sync.WaitGroup

	for i := 0; i < activeWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tw := range tenantCh {
				// ── Check checkpoint: skip completed, resume in-progress ──
				startKey := ""
				var previousRows int64
				if cpStore != nil {
					tp := cpStore.GetTenantProgress(tableName, tw.id)
					if tp != nil && tp.Completed {
						atomic.AddInt64(&completedTenants, 1)
						log.Printf("[%s] SKIP tenant %s (completed in checkpoint)",
							tableName, tw.id)
						continue
					}
					if tp != nil && tp.LastKey != "" {
						startKey = tp.LastKey
						previousRows = tp.RowsSynced
						log.Printf("[%s] RESUME tenant %s from key '%s' (%d rows already synced)",
							tableName, tw.id, startKey, previousRows)
					}
				}

				count, err := migrateForTenant(
					ctx, pgPool, chConn, tableName, tw.id,
					tw.idx, totalTenants,
					pgQueryTemplate, chInsert, batchSize,
					keysetColumn, processRow, globalCounter,
					startKey,
					func(lastKey string, rowsSoFar int64) {
						if cpStore != nil {
							cpStore.UpdateTenant(tableName, tw.id, lastKey, previousRows+rowsSoFar)
						}
					},
				)
				atomic.AddInt64(&tableTotal, count)
				done := atomic.AddInt64(&completedTenants, 1)

				if err != nil {
					mu.Lock()
					migrationErrors = append(migrationErrors, fmt.Sprintf("%s: %v", tw.id, err))
					mu.Unlock()
					log.Printf("[%s] FAILED tenant %s (%d/%d) after %d rows: %v",
						tableName, tw.id, done, totalTenants, count, err)
				} else {
					if cpStore != nil {
						cpStore.MarkTenantDone(tableName, tw.id)
					}
					log.Printf("[%s] tenant %s (%d/%d) done — %d rows",
						tableName, tw.id, done, totalTenants, count)
				}
			}
		}()
	}

	wg.Wait()

	total := atomic.LoadInt64(&tableTotal)
	log.Printf("[%s] Completed — %d total rows synced", tableName, total)

	if len(migrationErrors) > 0 {
		return total, fmt.Errorf("%d tenant(s) failed: %s",
			len(migrationErrors), strings.Join(migrationErrors, "; "))
	}
	return total, nil
}

// ─── Incremental sync: property_address_entity ──────────────────────────────

func syncPropertyAddress(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "property_address_entity"

	// Check for an existing checkpoint (interrupted previous run)
	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM eg_pt_property`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: lastmodifiedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("p.lastmodifiedtime", watermark)

	tenantQuery := fmt.Sprintf(
		`SELECT DISTINCT tenantid FROM eg_pt_property WHERE 1=1%s ORDER BY tenantid`, wf)

	pgQuery := fmt.Sprintf(`
		SELECT
			COALESCE(p.id, ''), COALESCE(p.tenantid, ''), COALESCE(p.propertyid, ''),
			COALESCE(p.surveyid, ''), COALESCE(p.accountid, ''),
			COALESCE(p.oldpropertyid, ''), COALESCE(p.propertytype, ''),
			COALESCE(p.usagecategory, ''), COALESCE(p.ownershipcategory, ''),
			COALESCE(p.status, ''), COALESCE(p.acknowldgementnumber, ''),
			COALESCE(p.creationreason, ''), COALESCE(p.nooffloors, 0),
			COALESCE(p.source, ''), COALESCE(p.channel, ''),
			COALESCE(p.landarea, 0)::float8, COALESCE(p.superbuiltuparea, 0)::float8,
			COALESCE(p.createdby, ''), COALESCE(p.createdtime, 0),
			COALESCE(p.lastmodifiedby, ''), COALESCE(p.lastmodifiedtime, 0),
			COALESCE(p.additionaldetails::text, ''),
			COALESCE(a.doorno, ''), COALESCE(a.plotno, ''),
			COALESCE(a.buildingname, ''), COALESCE(a.street, ''),
			COALESCE(a.landmark, ''), COALESCE(a.locality, ''),
			COALESCE(a.city, ''), COALESCE(a.district, ''),
			COALESCE(a.region, ''), COALESCE(a.state, ''),
			COALESCE(a.country, 'IN'), COALESCE(a.pincode, ''),
			COALESCE(a.latitude, 0)::float8, COALESCE(a.longitude, 0)::float8
		FROM eg_pt_property p
		LEFT JOIN eg_pt_address a ON p.id = a.propertyid
		WHERE p.tenantid = $1%s`, wf)

	const chInsert = `INSERT INTO property_address_entity (
		id, tenant_id, property_id, survey_id, account_id,
		old_property_id, property_type, usage_category, ownership_category,
		status, acknowledgement_number, creation_reason, no_of_floors,
		source, channel, land_area, super_built_up_area,
		created_by, created_time, last_modified_by, last_modified_time,
		financial_year, additionaldetails,
		door_no, plot_no, building_name, street, landmark,
		locality, city, district, region, state, country,
		pin_code, latitude, longitude)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "p.id",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				id, tenantID, propertyID, surveyID, accountID        string
				oldPropertyID, propertyType, usageCat                string
				ownershipCat, status, ackNum, creationReason         string
				noOfFloors                                           int64
				source, channel                                      string
				landArea, superBuiltUpArea                           float64
				createdBy                                            string
				createdTimeMs                                        int64
				lastModifiedBy                                       string
				lastModifiedTimeMs                                   int64
				additionalDetails                                    string
				doorNo, plotNo, buildingName, street, landmark       string
				locality, city, district, region, stateVal, country  string
				pinCode                                              string
				lat, lon                                             float64
			)
			if err := scan(
				&id, &tenantID, &propertyID, &surveyID, &accountID,
				&oldPropertyID, &propertyType, &usageCat, &ownershipCat,
				&status, &ackNum, &creationReason, &noOfFloors,
				&source, &channel, &landArea, &superBuiltUpArea,
				&createdBy, &createdTimeMs, &lastModifiedBy, &lastModifiedTimeMs,
				&additionalDetails,
				&doorNo, &plotNo, &buildingName, &street, &landmark,
				&locality, &city, &district, &region, &stateVal, &country,
				&pinCode, &lat, &lon,
			); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			return appendFn(
				id, tenantID, propertyID, surveyID, accountID,
				oldPropertyID, propertyType, usageCat, ownershipCat,
				status, ackNum, creationReason, int8(noOfFloors),
				source, channel, d(landArea), d(superBuiltUpArea),
				createdBy, msToISTVal(createdTimeMs),
				lastModifiedBy, msToISTVal(lastModifiedTimeMs),
				computeFinancialYear(createdTimeMs), additionalDetails,
				doorNo, plotNo, buildingName, street, landmark,
				locality, city, district, region, stateVal, country,
				pinCode, d(lat), d(lon),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: property_unit_entity ─────────────────────────────────

func syncPropertyUnit(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "property_unit_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM eg_pt_unit`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: lastmodifiedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("u.lastmodifiedtime", watermark)

	tenantQuery := fmt.Sprintf(
		`SELECT DISTINCT tenantid FROM eg_pt_unit WHERE 1=1%s ORDER BY tenantid`, wf)

	pgQuery := fmt.Sprintf(`
		SELECT
			COALESCE(u.tenantid, ''), COALESCE(u.propertyid, ''),
			COALESCE(u.id, ''), COALESCE(u.floorno, 0),
			COALESCE(u.unittype, ''), COALESCE(u.usagecategory, ''),
			COALESCE(u.occupancytype, ''), COALESCE(u.occupancydate, 0),
			COALESCE(u.carpetarea, 0)::float8, COALESCE(u.builtuparea, 0)::float8,
			COALESCE(u.plintharea, 0)::float8, COALESCE(u.superbuiltuparea, 0)::float8,
			COALESCE(u.arv, 0)::float8, COALESCE(u.constructiontype, ''),
			COALESCE(u.constructiondate, 0),
			CASE WHEN COALESCE(u.active, true) THEN 1 ELSE 0 END,
			COALESCE(u.createdby, ''), COALESCE(u.createdtime, 0),
			COALESCE(u.lastmodifiedby, ''), COALESCE(u.lastmodifiedtime, 0),
			COALESCE(p.propertyid, ''), COALESCE(p.propertytype, ''),
			COALESCE(p.ownershipcategory, ''), COALESCE(p.status, ''),
			COALESCE(p.nooffloors, 0)
		FROM eg_pt_unit u
		JOIN eg_pt_property p ON u.propertyid = p.id
		WHERE u.tenantid = $1%s`, wf)

	const chInsert = `INSERT INTO property_unit_entity (
		tenant_id, property_uuid, unit_id, floor_no, unit_type,
		usage_category, occupancy_type, occupancy_date,
		carpet_area, built_up_area, plinth_area, super_built_up_area,
		arv, construction_type, construction_date, active,
		created_by, created_time, last_modified_by, last_modified_time,
		property_id, property_type, ownership_category,
		property_status, no_of_floors)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "u.id",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				tenantID, propertyUUID, unitID string
				floorNo                        int64
				unitType, usageCat             string
				occupancyType                  string
				occupancyDateMs                int64
				carpetArea, builtUpArea        float64
				plinthArea, superBuiltUpArea   float64
				arv                            float64
				constructionType               string
				constructionDate               int64
				active                         int32
				createdBy                      string
				createdTimeMs                  int64
				lastModifiedBy                 string
				lastModifiedTimeMs             int64
				propertyID, propertyType       string
				ownershipCat, propertyStatus   string
				noOfFloors                     int64
			)
			if err := scan(
				&tenantID, &propertyUUID, &unitID, &floorNo,
				&unitType, &usageCat, &occupancyType, &occupancyDateMs,
				&carpetArea, &builtUpArea, &plinthArea, &superBuiltUpArea,
				&arv, &constructionType, &constructionDate, &active,
				&createdBy, &createdTimeMs, &lastModifiedBy, &lastModifiedTimeMs,
				&propertyID, &propertyType, &ownershipCat, &propertyStatus,
				&noOfFloors,
			); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			return appendFn(
				tenantID, propertyUUID, unitID, int8(floorNo),
				unitType, usageCat, occupancyType,
				msToISTVal(occupancyDateMs),
				d(carpetArea), d(builtUpArea), d(plinthArea), d(superBuiltUpArea),
				d(arv), constructionType, constructionDate, uint8(active),
				createdBy, msToISTVal(createdTimeMs),
				lastModifiedBy, msToISTVal(lastModifiedTimeMs),
				propertyID, propertyType, ownershipCat, propertyStatus,
				int8(noOfFloors),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: property_owner_entity ────────────────────────────────

func syncPropertyOwner(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "property_owner_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM eg_pt_owner`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: lastmodifiedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("o.lastmodifiedtime", watermark)

	tenantQuery := fmt.Sprintf(
		`SELECT DISTINCT tenantid FROM eg_pt_owner WHERE 1=1%s ORDER BY tenantid`, wf)

	pgQuery := fmt.Sprintf(`
		SELECT
			COALESCE(o.tenantid, ''), COALESCE(o.propertyid, ''),
			COALESCE(o.ownerinfouuid, ''), COALESCE(o.userid, ''),
			COALESCE(o.status, ''),
			CASE WHEN COALESCE(o.isprimaryowner, false) THEN 1 ELSE 0 END,
			COALESCE(o.ownertype, ''), COALESCE(o.ownershippercentage, ''),
			COALESCE(o.institutionid, ''), COALESCE(o.relationship, ''),
			COALESCE(o.createdby, ''), COALESCE(o.createdtime, 0),
			COALESCE(o.lastmodifiedby, ''), COALESCE(o.lastmodifiedtime, 0),
			COALESCE(p.propertyid, ''), COALESCE(p.propertytype, ''),
			COALESCE(p.ownershipcategory, ''), COALESCE(p.status, ''),
			COALESCE(p.nooffloors, 0)
		FROM eg_pt_owner o
		JOIN eg_pt_property p ON o.propertyid = p.id
		WHERE o.tenantid = $1%s`, wf)

	const chInsert = `INSERT INTO property_owner_entity (
		tenant_id, property_uuid, owner_info_uuid, user_id, status,
		is_primary_owner, owner_type, ownership_percentage,
		institution_id, relationship,
		created_by, created_time, last_modified_by, last_modified_time,
		property_id, property_type, ownership_category,
		property_status, no_of_floors)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "o.ownerinfouuid",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				tenantID, propertyUUID         string
				ownerInfoUUID, userID, status  string
				isPrimary                      int32
				ownerType, ownershipPct        string
				institutionID, relationship    string
				createdBy                      string
				createdTimeMs                  int64
				lastModifiedBy                 string
				lastModifiedTimeMs             int64
				propertyID, propertyType       string
				ownershipCat, propertyStatus   string
				noOfFloors                     int64
			)
			if err := scan(
				&tenantID, &propertyUUID, &ownerInfoUUID, &userID, &status,
				&isPrimary, &ownerType, &ownershipPct,
				&institutionID, &relationship,
				&createdBy, &createdTimeMs, &lastModifiedBy, &lastModifiedTimeMs,
				&propertyID, &propertyType, &ownershipCat, &propertyStatus,
				&noOfFloors,
			); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			return appendFn(
				tenantID, propertyUUID, ownerInfoUUID, userID, status,
				uint8(isPrimary), ownerType, ownershipPct,
				institutionID, relationship,
				createdBy, msToISTVal(createdTimeMs),
				lastModifiedBy, msToISTVal(lastModifiedTimeMs),
				propertyID, propertyType, ownershipCat, propertyStatus,
				int8(noOfFloors),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: property_assessment_entity ───────────────────────────

func syncAssessment(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "property_assessment_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM eg_pt_asmt_assessment`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: lastmodifiedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("lastmodifiedtime", watermark)

	tenantQuery := fmt.Sprintf(
		`SELECT DISTINCT tenantid FROM eg_pt_asmt_assessment WHERE 1=1%s ORDER BY tenantid`, wf)

	pgQuery := fmt.Sprintf(`
		SELECT
			COALESCE(id,''),
			COALESCE(tenantid,''),
			COALESCE(assessmentnumber,''),
			COALESCE(propertyid,''),
			COALESCE(financialyear,''),
			COALESCE(status,''),
			COALESCE(source,''),
			COALESCE(channel,''),
			COALESCE(createdby,''),
			COALESCE(createdtime,0),
			COALESCE(lastmodifiedby,''),
			COALESCE(lastmodifiedtime,0)
		FROM eg_pt_asmt_assessment
		WHERE tenantid = $1%s`, wf)

	const chInsert = `INSERT INTO property_assessment_entity (
		assessmentnumber, tenant_id, propertyid,
		financialyear, status, source, channel,
		created_by, created_time,
		last_modified_by, last_modified_time)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "id",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				id, tenantID, assessmentNumber    string
				propertyID                        string
				financialYear, status             string
				source, channel                   string
				createdBy, lastModifiedBy         string
				createdTimeMs, lastModifiedTimeMs int64
			)
			if err := scan(
				&id, &tenantID, &assessmentNumber,
				&propertyID,
				&financialYear, &status,
				&source, &channel,
				&createdBy, &createdTimeMs,
				&lastModifiedBy, &lastModifiedTimeMs,
			); err != nil {
				return err
			}
			return appendFn(
				assessmentNumber, tenantID, propertyID,
				financialYear, status, source, channel,
				createdBy, msToISTVal(createdTimeMs),
				lastModifiedBy, msToISTVal(lastModifiedTimeMs),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: payment_with_details_entity ──────────────────────────

func syncPaymentWithDetails(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "payment_with_details_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM egcl_payment`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: lastmodifiedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("p.lastmodifiedtime", watermark)

	tenantQuery := fmt.Sprintf(
		`SELECT DISTINCT tenantid FROM egcl_payment WHERE 1=1%s ORDER BY tenantid`,
		watermarkFilter("lastmodifiedtime", watermark))

	pgQuery := fmt.Sprintf(`
		SELECT
			p.id,
			p.tenantid,
			p.totaldue,
			p.totalamountpaid,
			COALESCE(p.transactionnumber, ''),
			p.transactiondate,
			COALESCE(p.paymentmode, ''),
			p.instrumentdate,
			COALESCE(p.instrumentnumber, ''),
			COALESCE(p.instrumentstatus, ''),
			COALESCE(p.ifsccode, ''),
			COALESCE(p.additionaldetails::text, '{}'),
			COALESCE(p.payerid, ''),
			COALESCE(p.paymentstatus, ''),
			COALESCE(p.createdby, ''),
			p.createdtime,
			COALESCE(p.lastmodifiedby, ''),
			p.lastmodifiedtime,
			p.filestoreid,
			COALESCE(d.receiptnumber, ''),
			d.receiptdate,
			COALESCE(d.receipttype, ''),
			COALESCE(d.businessservice, ''),
			COALESCE(d.billid, ''),
			d.manualreceiptnumber,
			d.manualreceiptdate
		FROM egcl_payment p
		JOIN egcl_paymentdetail d ON p.id = d.paymentid
		WHERE p.tenantid = $1%s`, wf)

	const chInsert = `INSERT INTO payment_with_details_entity (
		tenant_id, payment_id, total_due, total_amount_paid,
		transaction_number, transaction_date, payment_mode,
		instrument_date, instrument_number, instrument_status,
		ifsc_code, additional_details, payer_id, payment_status,
		created_by, created_time, last_modified_by, last_modified_time,
		filestore_id, receiptnumber, receiptdate, receipttype,
		businessservice, billid, manualreceiptnumber, manualreceiptdate)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "d.id",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				paymentID, tenantID                      string
				totalDue, totalPaid                      float64
				txnNumber                                string
				txnDateMs                                int64
				paymentMode                              string
				instrumentDateMs                         *int64
				instrumentNumber, instrumentStatus       string
				ifscCode                                 string
				additionalDetails                        string
				payerID, paymentStatus                   string
				createdBy                                string
				createdTimeMs                            int64
				lastModifiedBy                           string
				lastModifiedTimeMs                       int64
				filestoreID                              *string
				receiptNumber                            string
				receiptDateMs                            int64
				receiptType                              string
				businessService                          string
				billID                                   string
				manualReceiptNumber                      *string
				manualReceiptDateMs                      *int64
			)
			if err := scan(
				&paymentID, &tenantID,
				&totalDue, &totalPaid,
				&txnNumber, &txnDateMs,
				&paymentMode, &instrumentDateMs,
				&instrumentNumber, &instrumentStatus,
				&ifscCode, &additionalDetails,
				&payerID, &paymentStatus,
				&createdBy, &createdTimeMs,
				&lastModifiedBy, &lastModifiedTimeMs,
				&filestoreID,
				&receiptNumber, &receiptDateMs,
				&receiptType, &businessService,
				&billID,
				&manualReceiptNumber, &manualReceiptDateMs,
			); err != nil {
				return err
			}
			return appendFn(
				tenantID, paymentID,
				d(totalDue), d(totalPaid),
				txnNumber, msToISTVal(txnDateMs),
				paymentMode, msToIST(instrumentDateMs),
				instrumentNumber, instrumentStatus,
				ifscCode, additionalDetails,
				payerID, paymentStatus,
				createdBy, msToISTVal(createdTimeMs),
				lastModifiedBy, msToISTVal(lastModifiedTimeMs),
				nullableString(filestoreID),
				receiptNumber, msToISTVal(receiptDateMs),
				receiptType, businessService,
				billID,
				nullableString(manualReceiptNumber),
				msToIST(manualReceiptDateMs),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: bill_entity ──────────────────────────────────────────

func syncBill(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "bill_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM egcl_bill`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: lastmodifiedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("lastmodifiedtime", watermark)

	tenantQuery := fmt.Sprintf(
		`SELECT DISTINCT tenantid FROM egcl_bill WHERE 1=1%s ORDER BY tenantid`, wf)

	pgQuery := fmt.Sprintf(`
		SELECT
			COALESCE(id,''),
			COALESCE(tenantid,''),
			COALESCE(consumercode,''),
			COALESCE(businessservice,''),
			COALESCE(totalamount,0)::float8,
			COALESCE(status,''),
			COALESCE(createdby,''),
			COALESCE(createdtime,0),
			COALESCE(lastmodifiedby,''),
			COALESCE(lastmodifiedtime,0)
		FROM egcl_bill
		WHERE tenantid = $1%s`, wf)

	const chInsert = `INSERT INTO bill_entity (
		bill_id, tenant_id, consumercode, businessservice,
		totalamount, status,
		created_by, created_time,
		last_modified_by, last_modified_time)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "id",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				id, tenantID, consumerCode, businessService string
				totalAmount                                 float64
				status                                      string
				createdBy, lastModifiedBy                   string
				createdTimeMs, lastModifiedTimeMs           int64
			)
			if err := scan(
				&id, &tenantID, &consumerCode,
				&businessService, &totalAmount,
				&status,
				&createdBy, &createdTimeMs,
				&lastModifiedBy, &lastModifiedTimeMs,
			); err != nil {
				return err
			}
			return appendFn(
				id, tenantID, consumerCode,
				businessService, d(totalAmount),
				status,
				createdBy, msToISTVal(createdTimeMs),
				lastModifiedBy, msToISTVal(lastModifiedTimeMs),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: property_audit_entity ─────────────────────────────

func syncPropertyAudit(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "property_audit_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (watermark: %d → %d)", table, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(auditcreatedtime), 0) FROM eg_pt_property_audit`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
	}
	log.Printf("[%s] Syncing delta: auditcreatedtime > %d (up to %d)", table, watermark, newWatermark)

	wf := watermarkFilter("auditcreatedtime", watermark)

	tenantQuery := fmt.Sprintf(`
		SELECT DISTINCT property->>'tenantId'
		FROM eg_pt_property_audit
		WHERE property->>'tenantId' IS NOT NULL%s
		ORDER BY 1`, wf)

	pgQuery := fmt.Sprintf(`
		SELECT
			COALESCE(property->>'tenantId', ''),
			COALESCE(propertyid, ''),
			COALESCE(property->>'propertyType', ''),
			COALESCE(property->>'ownershipCategory', ''),
			COALESCE(property->>'usageCategory', ''),
			COALESCE(property->>'status', ''),
			COALESCE(property->'workflow'->'state'->>'state', ''),
			COALESCE((property->>'superBuiltUpArea')::float8, 0),
			COALESCE((property->>'landArea')::float8, 0),
			CASE WHEN property->'owners' IS NOT NULL AND jsonb_typeof(property->'owners') = 'array'
				THEN jsonb_array_length(property->'owners')
				ELSE 0
			END,
			auditcreatedtime,
			COALESCE((property->'auditDetails'->>'createdTime')::bigint, 0),
			COALESCE((property->'auditDetails'->>'lastModifiedTime')::bigint, 0)
		FROM eg_pt_property_audit
		WHERE property->>'tenantId' = $1%s`, wf)

	const chInsert = `INSERT INTO property_audit_entity (
		tenant_id, property_id, property_type,
		ownership_category, usage_category, property_status, workflow_state,
		super_built_up_area, land_area, owner_count,
		audit_created_time, created_time, last_modified_time)`

	count, err := parallelByTenant(ctx, pgPool, chConn,
		table, tenantQuery, pgQuery, chInsert,
		batchSize, workers, "audituuid",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				tenantID, propertyID       string
				propertyType               string
				ownershipCat, usageCat     string
				propertyStatus             string
				workflowState              string
				superBuiltUpArea, landArea float64
				ownerCount                 int32
				auditCreatedTimeMs         int64
				createdTimeMs              int64
				lastModifiedTimeMs         int64
			)
			if err := scan(
				&tenantID, &propertyID,
				&propertyType,
				&ownershipCat, &usageCat,
				&propertyStatus,
				&workflowState,
				&superBuiltUpArea, &landArea,
				&ownerCount,
				&auditCreatedTimeMs,
				&createdTimeMs, &lastModifiedTimeMs,
			); err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			return appendFn(
				tenantID, propertyID,
				propertyType,
				ownershipCat, usageCat,
				propertyStatus,
				workflowState,
				d(superBuiltUpArea), d(landArea),
				uint8(ownerCount),
				msToISTVal(auditCreatedTimeMs),
				msToISTVal(createdTimeMs),
				msToISTVal(lastModifiedTimeMs),
			)
		},
		globalCounter, tenant, cpStore,
	)
	if err != nil {
		return count, err
	}

	if count > 0 {
		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return count, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}
	cpStore.MarkTableDone(table)
	return count, nil
}

// ─── Incremental sync: demand_with_details_entity (two-phase) ───────────────
//
// Demand uses staging tables: if interrupted, the current phase restarts
// from scratch (staging tables are dropped and recreated for the active phase).
// Per-tenant checkpointing is used within each phase.

func syncDemandWithDetails(
	ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn,
	batchSize, workers int, globalCounter *int64, tenant string,
	cpStore *CheckpointStore,
) (int64, error) {
	const table = "demand_with_details_entity"

	cp := cpStore.GetTableCheckpoint(table)
	var watermark, newWatermark int64

	if cp != nil && !cp.Completed {
		watermark = cp.Watermark
		newWatermark = cp.NewWatermark
		log.Printf("[%s] Resuming interrupted sync (phase: %s, watermark: %d → %d)",
			table, cp.Phase, watermark, newWatermark)
	} else {
		var err error
		watermark, err = getWatermark(ctx, chConn, table)
		if err != nil {
			return 0, fmt.Errorf("get watermark: %w", err)
		}
		newWatermark, err = getMaxModifiedTime(ctx, pgPool,
			`SELECT COALESCE(MAX(lastmodifiedtime), 0) FROM egbs_demand_v1 WHERE businessservice = 'PT'`)
		if err != nil {
			return 0, err
		}
		if newWatermark <= watermark {
			log.Printf("[%s] No new data (watermark: %d, max in PG: %d)", table, watermark, newWatermark)
			return 0, nil
		}
		cpStore.InitTable(table, watermark, newWatermark)
		cpStore.SetPhase(table, "staging_demand")
	}

	wf := watermarkFilter("lastmodifiedtime", watermark)
	phase := cpStore.GetPhase(table)
	var totalSynced int64

	// ── Phase 1: Stream demands to staging ──
	if phase == "" || phase == "staging_demand" {
		log.Printf("[%s] Phase 1: Creating staging tables & streaming demands...", table)
		if err := createDemandStagingTables(ctx, chConn); err != nil {
			return 0, fmt.Errorf("create staging: %w", err)
		}

		demandTenantQuery := fmt.Sprintf(
			`SELECT DISTINCT tenantid FROM egbs_demand_v1 WHERE businessservice = 'PT'%s ORDER BY tenantid`, wf)

		demandPgQuery := fmt.Sprintf(`
			SELECT
				COALESCE(tenantid,''), COALESCE(id,''), COALESCE(consumercode,''),
				COALESCE(consumertype,''), COALESCE(businessservice,''), COALESCE(payer,''),
				COALESCE(taxperiodfrom,0), COALESCE(taxperiodto,0), COALESCE(status,''),
				CASE WHEN COALESCE(ispaymentcompleted,false) THEN 1 ELSE 0 END,
				COALESCE(minimumamountpayable,0)::float8,
				COALESCE(billexpirytime,0), COALESCE(fixedbillexpirydate,0),
				COALESCE(createdby,''), COALESCE(createdtime,0),
				COALESCE(lastmodifiedby,''), COALESCE(lastmodifiedtime,0)
			FROM egbs_demand_v1
			WHERE businessservice = 'PT' AND tenantid = $1%s`, wf)

		// Use a separate checkpoint key for demand staging tenants
		cpStore.InitTable("_stg_demand", 0, 0)

		demandCount, err := parallelByTenant(ctx, pgPool, chConn,
			"_stg_demand", demandTenantQuery, demandPgQuery,
			`INSERT INTO _stg_demand`,
			batchSize, workers, "id",
			func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
				var (
					tenantid, id, consumercode       string
					consumertype, businesssvc, payer string
					taxperiodfrom, taxperiodto       int64
					status                           string
					ispaymentcompleted               int32
					minAmtPayable                    float64
					billexpiry, fixedbillexpiry      int64
					createdby                        string
					createdtime                      int64
					lastmodifiedby                   string
					lastmodifiedtime                 int64
				)
				if err := scan(
					&tenantid, &id, &consumercode,
					&consumertype, &businesssvc, &payer,
					&taxperiodfrom, &taxperiodto, &status,
					&ispaymentcompleted, &minAmtPayable,
					&billexpiry, &fixedbillexpiry,
					&createdby, &createdtime,
					&lastmodifiedby, &lastmodifiedtime,
				); err != nil {
					return fmt.Errorf("scan: %w", err)
				}
				return appendFn(
					tenantid, id, consumercode,
					consumertype, businesssvc, payer,
					taxperiodfrom, taxperiodto, status,
					uint8(ispaymentcompleted), minAmtPayable,
					billexpiry, fixedbillexpiry,
					createdby, createdtime,
					lastmodifiedby, lastmodifiedtime,
				)
			},
			globalCounter, tenant, cpStore,
		)
		if err != nil {
			return 0, fmt.Errorf("stream demands: %w", err)
		}
		totalSynced += demandCount
		log.Printf("[%s] Staged %d demand rows", table, demandCount)
		cpStore.SetPhase(table, "staging_detail")
	}

	// ── Phase 2: Stream demand details to staging ──
	phase = cpStore.GetPhase(table)
	if phase == "staging_detail" {
		log.Printf("[%s] Phase 2: Streaming demand details...", table)
		detailTenantQuery := fmt.Sprintf(
			`SELECT DISTINCT tenantid FROM egbs_demand_v1 WHERE businessservice = 'PT'%s ORDER BY tenantid`, wf)

		detailPgQuery := fmt.Sprintf(`
			SELECT
				COALESCE(dd.tenantid,''), COALESCE(dd.demandid,''),
				COALESCE(dd.taxheadcode,''),
				COALESCE(dd.taxamount,0)::float8, COALESCE(dd.collectionamount,0)::float8
			FROM egbs_demanddetail_v1 dd
			WHERE dd.tenantid = $1
			  AND dd.demandid IN (
				SELECT id FROM egbs_demand_v1
				WHERE businessservice = 'PT' AND tenantid = $1%s
			  )`, wf)

		cpStore.InitTable("_stg_demanddetail", 0, 0)

		detailCount, err := parallelByTenant(ctx, pgPool, chConn,
			"_stg_demanddetail", detailTenantQuery, detailPgQuery,
			`INSERT INTO _stg_demanddetail`,
			batchSize, workers, "dd.id",
			func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
				var (
					tenantid, demandid, taxheadcode string
					taxamount, collectionamount     float64
				)
				if err := scan(&tenantid, &demandid, &taxheadcode, &taxamount, &collectionamount); err != nil {
					return fmt.Errorf("scan: %w", err)
				}
				return appendFn(tenantid, demandid, taxheadcode, taxamount, collectionamount)
			},
			globalCounter, tenant, cpStore,
		)
		if err != nil {
			return totalSynced, fmt.Errorf("stream details: %w", err)
		}
		totalSynced += detailCount
		log.Printf("[%s] Staged %d detail rows", table, detailCount)
		cpStore.SetPhase(table, "pivot")
	}

	// ── Phase 3: Pivot in ClickHouse ──
	phase = cpStore.GetPhase(table)
	if phase == "pivot" {
		log.Printf("[%s] Phase 3: Pivoting in ClickHouse...", table)
		if err := pivotDemandInClickHouse(ctx, chConn); err != nil {
			return totalSynced, fmt.Errorf("pivot: %w", err)
		}
		log.Printf("[%s] Pivot complete", table)

		dropDemandStagingTables(ctx, chConn)

		if err := updateWatermark(ctx, chConn, table, newWatermark); err != nil {
			return totalSynced, fmt.Errorf("update watermark: %w", err)
		}
		log.Printf("[%s] Watermark advanced to %d", table, newWatermark)
	}

	cpStore.MarkTableDone(table)
	cpStore.MarkTableDone("_stg_demand")
	cpStore.MarkTableDone("_stg_demanddetail")
	return totalSynced, nil
}

// Demand staging helpers

func createDemandStagingTables(ctx context.Context, chConn clickhouse.Conn) error {
	queries := []string{
		`DROP TABLE IF EXISTS _stg_demand`,
		`DROP TABLE IF EXISTS _stg_demanddetail`,
		`CREATE TABLE _stg_demand (
			tenantid String, id String, consumercode String, consumertype String,
			businessservice String, payer String, taxperiodfrom Int64, taxperiodto Int64,
			status String, ispaymentcompleted UInt8, minimumamountpayable Float64,
			billexpirytime Int64, fixedbillexpirydate Int64,
			createdby String, createdtime Int64, lastmodifiedby String, lastmodifiedtime Int64
		) ENGINE = MergeTree() ORDER BY (tenantid, id)`,
		`CREATE TABLE _stg_demanddetail (
			tenantid String, demandid String, taxheadcode String,
			taxamount Float64, collectionamount Float64
		) ENGINE = MergeTree() ORDER BY (tenantid, demandid)`,
	}
	for _, q := range queries {
		if err := chConn.Exec(ctx, q); err != nil {
			return fmt.Errorf("staging table: %w", err)
		}
	}
	return nil
}

func dropDemandStagingTables(ctx context.Context, chConn clickhouse.Conn) {
	_ = chConn.Exec(ctx, "DROP TABLE IF EXISTS _stg_demand")
	_ = chConn.Exec(ctx, "DROP TABLE IF EXISTS _stg_demanddetail")
}

func pivotDemandInClickHouse(ctx context.Context, chConn clickhouse.Conn) error {
	pivotCtx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	const pivotSQL = `
INSERT INTO demand_with_details_entity (
	tenant_id, demand_id, consumer_code, consumer_type, business_service, payer,
	tax_period_from, tax_period_to, demand_status,
	financial_year, minimum_amount_payable, bill_expiry_time, fixed_bill_expiry_date,
	total_tax_amount, total_collection_amount,
	pt_tax, pt_cancer_cess, pt_fire_cess, pt_roundoff,
	pt_owner_exemption, pt_unit_usage_exemption,
	pt_advance_carryforward,
	pt_decimal_ceiling_debit,
	pt_time_rebate,
	pt_decimal_ceiling_credit,
	pt_time_penalty,
	pt_adhoc_penalty,
	pt_adhoc_rebate,
	pt_time_interest,
	outstanding_amount, is_paid,
	created_by, created_time, last_modified_by, last_modified_time
)
SELECT
	d.tenantid, d.id,
	d.consumercode, d.consumertype, d.businessservice, d.payer,
	fromUnixTimestamp64Milli(d.taxperiodfrom, 'Asia/Kolkata'),
	fromUnixTimestamp64Milli(d.taxperiodto, 'Asia/Kolkata'),
	d.status,
	if(d.taxperiodfrom = 0, '',
		concat(
			toString(if(toMonth(toDateTime(intDiv(d.taxperiodfrom, 1000))) < 4,
				toYear(toDateTime(intDiv(d.taxperiodfrom, 1000))) - 1,
				toYear(toDateTime(intDiv(d.taxperiodfrom, 1000))))),
			'-',
			leftPad(toString((if(toMonth(toDateTime(intDiv(d.taxperiodfrom, 1000))) < 4,
				toYear(toDateTime(intDiv(d.taxperiodfrom, 1000))),
				toYear(toDateTime(intDiv(d.taxperiodfrom, 1000))) + 1) % 100)), 2, '0')
		)
	),
	d.minimumamountpayable, d.billexpirytime, d.fixedbillexpirydate,
	sum(dd.taxamount), sum(dd.collectionamount),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_TAX'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_CANCER_CESS'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_FIRE_CESS'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_ROUNDOFF'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_OWNER_EXEMPTION'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_UNIT_USAGE_EXEMPTION'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_ADVANCE_CARRYFORWARD'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_DECIMAL_CEILING_DEBIT'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_TIME_REBATE'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_DECIMAL_CEILING_CREDIT'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_TIME_PENALTY'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_ADHOC_PENALTY'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_ADHOC_REBATE'),
	sumIf(dd.taxamount, dd.taxheadcode = 'PT_TIME_INTEREST'),
	sum(dd.taxamount) - sum(dd.collectionamount),
	toUInt8(if(sum(dd.taxamount) - sum(dd.collectionamount) <= 0, 1, 0)),
	d.createdby, fromUnixTimestamp64Milli(d.createdtime, 'Asia/Kolkata'),
	d.lastmodifiedby, fromUnixTimestamp64Milli(d.lastmodifiedtime, 'Asia/Kolkata')
FROM _stg_demanddetail dd
INNER JOIN _stg_demand d ON dd.tenantid = d.tenantid AND dd.demandid = d.id
GROUP BY
	d.tenantid, d.id, d.consumercode, d.consumertype, d.businessservice, d.payer,
	d.taxperiodfrom, d.taxperiodto, d.status, d.ispaymentcompleted,
	d.minimumamountpayable, d.billexpirytime, d.fixedbillexpirydate,
	d.createdby, d.createdtime, d.lastmodifiedby, d.lastmodifiedtime
SETTINGS join_algorithm = 'full_sorting_merge', max_execution_time = 3600, max_bytes_before_external_group_by = 10000000000`

	return chConn.Exec(pivotCtx, pivotSQL)
}

// ─── Main ───────────────────────────────────────────────────────────────────

type syncResult struct {
	table   string
	rows    int64
	elapsed time.Duration
	err     error
}

var allTables = []string{
	"property_address", "property_unit", "property_owner",
	"demand_details", "assessment", "payment_with_details",
	"bill", "property_audit",
}

func main() {
	var (
		pgHost         string
		pgPort         int
		pgDB           string
		pgUser         string
		pgPassword     string
		chHost         string
		chPort         int
		chDB           string
		chUser         string
		chPassword     string
		chProtocol     string
		chSecure       bool
		batchSize      int
		workers        int
		tables         string
		tenant         string
		resetWM        bool
		checkpointFile string
	)

	flag.StringVar(&pgHost, "pg-host", "localhost", "PostgreSQL host")
	flag.IntVar(&pgPort, "pg-port", 5432, "PostgreSQL port")
	flag.StringVar(&pgDB, "pg-db", "", "PostgreSQL database (required)")
	flag.StringVar(&pgUser, "pg-user", "postgres", "PostgreSQL user")
	flag.StringVar(&pgPassword, "pg-password", "", "PostgreSQL password")
	flag.StringVar(&chHost, "ch-host", "z0tz5tcsm5.ap-south-1.aws.clickhouse.cloud", "ClickHouse host")
	flag.IntVar(&chPort, "ch-port", 9440, "ClickHouse port")
	flag.StringVar(&chDB, "ch-db", "punjab_property_tax", "ClickHouse database")
	flag.StringVar(&chUser, "ch-user", "default", "ClickHouse user")
	flag.StringVar(&chPassword, "ch-password", "XJRDo8_ZPh_qs", "ClickHouse password")
	flag.StringVar(&chProtocol, "ch-protocol", "auto", "ClickHouse protocol: auto, http, native")
	flag.BoolVar(&chSecure, "ch-secure", true, "Enable TLS for ClickHouse")
	flag.IntVar(&batchSize, "batch-size", defaultBatchSize, "Rows per batch (default 20k for safer checkpointing)")
	flag.IntVar(&workers, "workers", defaultWorkers, "Workers per table")
	flag.StringVar(&tables, "tables", "all", "Comma-separated table names (or 'all')")
	flag.StringVar(&tenant, "tenant", "", "Sync only this tenant ID (e.g. 'pb.amritsar')")
	flag.BoolVar(&resetWM, "reset-watermark", false, "Reset watermarks for specified tables (forces full re-sync)")
	flag.StringVar(&checkpointFile, "checkpoint-file", "sync_checkpoint.json", "Path to checkpoint file for resume on crash")
	flag.Parse()

	if pgDB == "" {
		fmt.Fprintln(os.Stderr, "Error: -pg-db is required")
		flag.Usage()
		os.Exit(1)
	}

	// Resolve checkpoint file to absolute path
	if !filepath.IsAbs(checkpointFile) {
		if wd, err := os.Getwd(); err == nil {
			checkpointFile = filepath.Join(wd, checkpointFile)
		}
	}

	tablesToSync := allTables
	if tables != "all" {
		tablesToSync = strings.Split(tables, ",")
		for i := range tablesToSync {
			tablesToSync[i] = strings.TrimSpace(tablesToSync[i])
		}
	}

	var protocol clickhouse.Protocol
	switch chProtocol {
	case "http":
		protocol = clickhouse.HTTP
	case "native":
		protocol = clickhouse.Native
	default:
		protocol = clickhouse.HTTP
		if chPort == 9000 || chPort == 9440 {
			protocol = clickhouse.Native
		}
	}

	log.Println("PostgreSQL -> ClickHouse Incremental Sync")
	log.Println("==================================================")
	log.Printf("PostgreSQL:      %s:%d/%s", pgHost, pgPort, pgDB)
	log.Printf("ClickHouse:      %s:%d/%s (protocol=%v)", chHost, chPort, chDB, protocol)
	log.Printf("Batch size:      %d", batchSize)
	log.Printf("Workers/table:   %d", workers)
	log.Printf("Tables:          %s", strings.Join(tablesToSync, ", "))
	log.Printf("Checkpoint file: %s", checkpointFile)
	if tenant != "" {
		log.Printf("Tenant filter:   %s", tenant)
	}
	if resetWM {
		log.Printf("Reset watermark: YES (will force full re-sync)")
	}
	log.Println("==================================================")

	ctx := context.Background()

	// ── Initialize checkpoint store ──
	cpStore := NewCheckpointStore(checkpointFile)
	if err := cpStore.Load(); err != nil {
		log.Fatalf("Load checkpoint: %v", err)
	}
	// Check if resuming from a previous interrupted run
	if _, err := os.Stat(checkpointFile); err == nil {
		log.Printf("Loaded checkpoint from %s (resuming interrupted sync)", checkpointFile)
	}

	// Connect to PostgreSQL
	log.Println("Connecting to PostgreSQL...")
	pgConnStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		pgHost, pgPort, pgDB, pgUser, pgPassword)
	poolConfig, err := pgxpool.ParseConfig(pgConnStr)
	if err != nil {
		log.Fatalf("Parse PG config: %v", err)
	}
	poolConfig.MaxConns = int32(len(allTables)*workers + 4)

	pgPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Create PG pool: %v", err)
	}
	defer pgPool.Close()

	if err := pgPool.Ping(ctx); err != nil {
		log.Fatalf("PG ping: %v", err)
	}
	log.Printf("Connected to PostgreSQL (pool max=%d)", poolConfig.MaxConns)

	// Connect to ClickHouse
	log.Println("Connecting to ClickHouse...")
	chOpts := &clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", chHost, chPort)},
		Protocol: protocol,
		Auth: clickhouse.Auth{
			Database: chDB,
			Username: chUser,
			Password: chPassword,
		},
		Settings: clickhouse.Settings{
			"max_insert_block_size": uint64(batchSize),
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns: len(allTables)*workers + 4,
	}
	if chSecure {
		chOpts.TLS = &tls.Config{}
	}
	chConn, err := clickhouse.Open(chOpts)
	if err != nil {
		log.Fatalf("Open ClickHouse: %v", err)
	}
	if err := chConn.Ping(ctx); err != nil {
		log.Fatalf("CH ping: %v", err)
	}
	log.Println("Connected to ClickHouse")

	if err := ensureWatermarkTable(ctx, chConn); err != nil {
		log.Fatalf("Create watermark table: %v", err)
	}

	// Reset watermarks if requested
	tableNameMap := map[string]string{
		"property_address":     "property_address_entity",
		"property_unit":        "property_unit_entity",
		"property_owner":       "property_owner_entity",
		"demand_details":       "demand_with_details_entity",
		"assessment":           "property_assessment_entity",
		"payment_with_details": "payment_with_details_entity",
		"bill":                 "bill_entity",
		"property_audit":       "property_audit_entity",
	}
	if resetWM {
		for _, t := range tablesToSync {
			chTable := tableNameMap[t]
			if chTable == "" {
				continue
			}
			if err := updateWatermark(ctx, chConn, chTable, 0); err != nil {
				log.Fatalf("Reset watermark for %s: %v", chTable, err)
			}
			log.Printf("Watermark reset to 0 for %s", chTable)
		}
		// Also remove checkpoint file since we're starting fresh
		_ = cpStore.Remove()
		cpStore = NewCheckpointStore(checkpointFile)
	}

	// Global progress counter + reporter
	var globalCounter int64
	overallStart := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				current := atomic.LoadInt64(&globalCounter)
				elapsed := time.Since(overallStart).Seconds()
				rate := float64(current) / elapsed
				log.Printf("PROGRESS: %d total rows | %.0f rows/sec | %.0fs elapsed",
					current, rate, elapsed)
			}
		}
	}()

	// Build sync function map
	type syncFn func(context.Context, *pgxpool.Pool, clickhouse.Conn, int, int, *int64, string, *CheckpointStore) (int64, error)
	syncFns := map[string]syncFn{
		"property_address":     syncPropertyAddress,
		"property_unit":        syncPropertyUnit,
		"property_owner":       syncPropertyOwner,
		"demand_details":       syncDemandWithDetails,
		"assessment":           syncAssessment,
		"payment_with_details": syncPaymentWithDetails,
		"bill":                 syncBill,
		"property_audit":       syncPropertyAudit,
	}

	// Run all table syncs concurrently
	results := make([]syncResult, len(tablesToSync))
	var wg sync.WaitGroup

	for i, table := range tablesToSync {
		fn, ok := syncFns[table]
		if !ok {
			log.Printf("Unknown table: %s (skipping)", table)
			results[i] = syncResult{table: table, err: fmt.Errorf("unknown table")}
			continue
		}

		wg.Add(1)
		go func(idx int, tbl string, sFn syncFn) {
			defer wg.Done()
			start := time.Now()
			count, err := sFn(ctx, pgPool, chConn, batchSize, workers, &globalCounter, tenant, cpStore)
			results[idx] = syncResult{
				table:   tbl,
				rows:    count,
				elapsed: time.Since(start),
				err:     err,
			}
			if err != nil {
				log.Printf("FAILED: %s — %v", tbl, err)
			}
		}(i, table, fn)
	}

	wg.Wait()
	close(done)

	overallElapsed := time.Since(overallStart)

	// Summary
	log.Println()
	log.Println("==================================================")
	log.Println("INCREMENTAL SYNC SUMMARY")
	log.Println("==================================================")

	var totalRows int64
	hasError := false
	for _, r := range results {
		status := "OK"
		if r.err != nil {
			status = fmt.Sprintf("FAILED: %v", r.err)
			hasError = true
		} else if r.rows == 0 {
			status = "UP-TO-DATE"
		}
		totalRows += r.rows
		log.Printf("  %-25s  %12d rows  %8.1fs  [%s]", r.table, r.rows, r.elapsed.Seconds(), status)
	}
	log.Println("--------------------------------------------------")
	log.Printf("  %-25s  %12d rows  %8.1fs", "TOTAL", totalRows, overallElapsed.Seconds())
	if overallElapsed.Seconds() > 0 {
		log.Printf("  Throughput: %.0f rows/sec", float64(totalRows)/overallElapsed.Seconds())
	}
	log.Println("==================================================")

	// Flush any throttled checkpoint data to disk before exit
	if err := cpStore.Flush(); err != nil {
		log.Printf("WARN: final checkpoint flush failed: %v", err)
	}

	if hasError {
		log.Printf("Some tables failed — checkpoint preserved at %s", checkpointFile)
		log.Println("Re-run the script to resume from where it stopped.")
		os.Exit(1)
	}

	// All tables succeeded — clean up checkpoint file
	if err := cpStore.Remove(); err != nil {
		log.Printf("WARN: could not remove checkpoint file: %v", err)
	} else {
		log.Println("All tables synced successfully — checkpoint file removed.")
	}
}