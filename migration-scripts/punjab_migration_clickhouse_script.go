package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
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
	defaultBatchSize = 100_000
	defaultWorkers   = 8
)

// ─── Helpers ────────────────────────────────────────────────────────────────

// func msToTime(ms int64) time.Time {
// 	if ms == 0 {
// 		return time.Unix(0, 0).UTC()
// 	}
// 	return time.UnixMilli(ms).UTC()
// }

// msToIST converts a PG epoch-millis timestamp to a Go time in UTC.
// PG stores genuine UTC epochs (e.g. 07:53 UTC). ClickHouse displays
// DateTime64 in its server timezone (UTC), so storing the epoch as-is
// preserves the same wall-clock digits (07:53) in query output.
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

// quoteLiteral safely quotes a string for SQL embedding.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// getCompletedTenants compares per-tenant row counts between PG and CH.
// Returns a set of tenant IDs where CH count >= PG count (i.e., fully migrated).
func getCompletedTenants(
	ctx context.Context,
	pgPool *pgxpool.Pool,
	chConn clickhouse.Conn,
	tableName string,
	pgCountQuery string,
	chCountQuery string,
) (map[string]bool, error) {
	// Get PG counts
	pgCounts := make(map[string]int64)
	pgRows, err := pgPool.Query(ctx, pgCountQuery)
	if err != nil {
		return nil, fmt.Errorf("pg count query: %w", err)
	}
	defer pgRows.Close()
	for pgRows.Next() {
		var tenant string
		var count int64
		if err := pgRows.Scan(&tenant, &count); err != nil {
			return nil, fmt.Errorf("pg count scan: %w", err)
		}
		pgCounts[tenant] = count
	}
	if err := pgRows.Err(); err != nil {
		return nil, fmt.Errorf("pg count rows: %w", err)
	}

	// Get CH counts
	chCounts := make(map[string]int64)
	chRows, err := chConn.Query(ctx, chCountQuery)
	if err != nil {
		return nil, fmt.Errorf("ch count query: %w", err)
	}
	defer chRows.Close()
	for chRows.Next() {
		var tenant string
		var count uint64
		if err := chRows.Scan(&tenant, &count); err != nil {
			return nil, fmt.Errorf("ch count scan: %w", err)
		}
		chCounts[tenant] = int64(count)
	}

	// Compare: tenant is complete if CH >= PG
	completed := make(map[string]bool)
	for tenant, pgCount := range pgCounts {
		chCount := chCounts[tenant]
		if chCount >= pgCount {
			completed[tenant] = true
		}
	}

	if len(completed) > 0 {
		log.Printf("[%s] Resume: %d/%d tenants already complete, will skip them",
			tableName, len(completed), len(pgCounts))
	}
	return completed, nil
}

// ─── Core migration engine ─────────────────────────────────────────────────

// tenantWithCount holds a tenant ID and its row count for sorting.
type tenantWithCount struct {
	id    string
	count int64
}

// getDistinctTenants returns the list of distinct tenant IDs from a table.
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

// getTenantsWithCounts returns tenants sorted by row count descending (largest first).
// It derives a COUNT query from the tenant discovery query.
func getTenantsWithCounts(ctx context.Context, pgPool *pgxpool.Pool, tenantQuery string) ([]tenantWithCount, error) {
	// Transform "SELECT DISTINCT tenantid FROM ... ORDER BY tenantid"
	// into    "SELECT tenantid, COUNT(*) FROM ... GROUP BY tenantid ORDER BY COUNT(*) DESC"
	countQuery := strings.Replace(tenantQuery, "SELECT DISTINCT tenantid", "SELECT tenantid, COUNT(*)", 1)
	// Remove existing ORDER BY and add GROUP BY + ORDER BY count DESC
	if idx := strings.LastIndex(strings.ToUpper(countQuery), "ORDER BY"); idx != -1 {
		countQuery = countQuery[:idx]
	}
	countQuery += " GROUP BY tenantid ORDER BY COUNT(*) DESC"

	rows, err := pgPool.Query(ctx, countQuery)
	if err != nil {
		// Fallback: if count query fails, return unsorted
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

// pageResult holds the result of scanning one page from PG.
type pageResult struct {
	lastKey string
	count   int
	err     error
}

// fetchPage queries PG for one page and scans rows directly into the CH batch.
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

// migrateForTenant runs a keyset-paginated migration for a single tenant's data.
// Uses a pipeline: while CH is sending batch N, PG is already fetching batch N+1.
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
) (int64, error) {
	pgQuery := strings.ReplaceAll(pgQueryTemplate, "$1", quoteLiteral(tenantID))
	pgQueryWithKeyset := strings.Replace(pgQuery, "SELECT", "SELECT "+keysetColumn+",", 1)

	var tenantTotal int64
	var lastKey string
	page := 0

	// Pipeline: prefetch next page from PG while sending current batch to CH
	type prefetchResult struct {
		batch interface {
			Send() error
			Abort() error
		}
		result pageResult
		page   int
	}

	buildQuery := func(p int, lk string) string {
		if p == 1 {
			return fmt.Sprintf("%s ORDER BY %s LIMIT %d",
				pgQueryWithKeyset, keysetColumn, batchSize)
		}
		return fmt.Sprintf("%s AND %s > %s ORDER BY %s LIMIT %d",
			pgQueryWithKeyset, keysetColumn, quoteLiteral(lk), keysetColumn, batchSize)
	}

	logPage := func(p int, lk string) {
		if p == 1 {
			log.Printf("[%s] tenant %s (%d/%d) — page %d: ORDER BY %s LIMIT %d",
				tableName, tenantID, tenantIdx, totalTenants, p, keysetColumn, batchSize)
		} else {
			log.Printf("[%s] tenant %s (%d/%d) — page %d: %s > '%s' LIMIT %d",
				tableName, tenantID, tenantIdx, totalTenants, p, keysetColumn, lk, batchSize)
		}
	}

	// Fetch first page synchronously
	page = 1
	logPage(page, "")
	curBatch, curResult := fetchPage(ctx, pgPool, chInsert, chConn, buildQuery(page, ""), processRow)
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
		// Start prefetching next page in background
		nextPage := page + 1
		nextKey := lastKey
		prefetchCh := make(chan prefetchResult, 1)
		go func(p int, lk string) {
			logPage(p, lk)
			b, r := fetchPage(ctx, pgPool, chInsert, chConn, buildQuery(p, lk), processRow)
			prefetchCh <- prefetchResult{batch: b, result: r, page: p}
		}(nextPage, nextKey)

		// Send current batch to CH (overlaps with PG prefetch)
		if err := curBatch.Send(); err != nil {
			// Drain and abort prefetch
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

		// Wait for prefetch to complete
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

		// Advance to the prefetched page
		page = pf.page
		curBatch = pf.batch
		curResult = pf.result
		lastKey = curResult.lastKey
	}

	return tenantTotal, nil
}

// parallelByTenant discovers tenants and distributes them across a worker pool.
// Tenants are sorted largest-first to minimize tail latency.
// Each worker processes one tenant at a time using keyset pagination.
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
	skipTenants map[string]bool,
	tenantFilter string,
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
		log.Printf("[%s] No tenants found, skipping", tableName)
		return 0, nil
	}

	// Filter out already-completed tenants
	var pending []tenantWithCount
	for _, t := range tenantsWithCounts {
		if skipTenants[t.id] {
			log.Printf("[%s] SKIP tenant %s (already complete)", tableName, t.id)
			continue
		}
		pending = append(pending, t)
	}
	if len(pending) == 0 {
		log.Printf("[%s] All %d tenants already complete, nothing to do", tableName, len(tenantsWithCounts))
		return 0, nil
	}
	if len(skipTenants) > 0 {
		log.Printf("[%s] %d tenants to migrate (%d skipped)", tableName, len(pending), len(tenantsWithCounts)-len(pending))
	}

	// Sort pending tenants largest-first to minimize tail latency
	sort.Slice(pending, func(i, j int) bool {
		return pending[i].count > pending[j].count
	})
	if len(pending) > 0 && pending[0].count > 0 {
		log.Printf("[%s] Largest tenant: %s (%d rows), smallest: %s (%d rows)",
			tableName, pending[0].id, pending[0].count,
			pending[len(pending)-1].id, pending[len(pending)-1].count)
	}

	// Distribute tenants across workers
	type tenantWork struct {
		id  string
		idx int
	}
	tenantCh := make(chan tenantWork, len(pending))
	for i, t := range pending {
		tenantCh <- tenantWork{id: t.id, idx: i + 1}
	}
	close(tenantCh)
	totalTenants := len(pending)

	activeWorkers := workers
	if activeWorkers > len(pending) {
		activeWorkers = len(pending)
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
				count, err := migrateForTenant(
					ctx, pgPool, chConn, tableName, tw.id,
					tw.idx, totalTenants,
					pgQueryTemplate, chInsert, batchSize,
					keysetColumn, processRow, globalCounter,
				)
				atomic.AddInt64(&tableTotal, count)
				done := atomic.AddInt64(&completedTenants, 1)

				if err != nil {
					mu.Lock()
					migrationErrors = append(migrationErrors, fmt.Sprintf("%s: %v", tw.id, err))
					mu.Unlock()
					log.Printf("[%s] FAILED tenant %s (%d/%d) after %d rows: %v", tableName, tw.id, done, totalTenants, count, err)
				} else {
					log.Printf("[%s] tenant %s (%d/%d) done — %d rows", tableName, tw.id, done, totalTenants, count)
				}
			}
		}()
	}

	wg.Wait()

	total := atomic.LoadInt64(&tableTotal)
	log.Printf("[%s] Completed — %d total rows", tableName, total)

	if len(migrationErrors) > 0 {
		return total, fmt.Errorf("%d tenant(s) failed: %s", len(migrationErrors), strings.Join(migrationErrors, "; "))
	}
	return total, nil
}

// ─── Table 1: property_address_fact ─────────────────────────────────────────

func migratePropertyAddress(ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn, batchSize, workers int, globalCounter *int64, resume bool, tenant string) (int64, error) {
	const tenantQuery = `SELECT DISTINCT tenantid FROM eg_pt_property ORDER BY tenantid`

	// $1 is replaced with quoted tenant_id at runtime
	const pgQuery = `
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
		WHERE p.tenantid = $1`

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

	var skipTenants map[string]bool
	if resume {
		var err error
		skipTenants, err = getCompletedTenants(ctx, pgPool, chConn,
			"property_address_entity",
			`SELECT tenantid, count(*) FROM eg_pt_property GROUP BY tenantid`,
			`SELECT tenant_id, count() FROM property_address_entity FINAL GROUP BY tenant_id`,
		)
		if err != nil {
			return 0, fmt.Errorf("resume check: %w", err)
		}
	}

	return parallelByTenant(ctx, pgPool, chConn,
		"property_address_entity", tenantQuery, pgQuery, chInsert,
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
		globalCounter,
		skipTenants,
		tenant,
	)
}

func migrateAssessment(
    ctx context.Context,
    pgPool *pgxpool.Pool,
    chConn clickhouse.Conn,
    batchSize, workers int,
    globalCounter *int64,
    resume bool,
    tenant string,
) (int64, error) {

    const tenantQuery = `
        SELECT DISTINCT tenantid
        FROM eg_pt_asmt_assessment
        ORDER BY tenantid`

    const pgQuery = `
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
        WHERE tenantid = $1`

    const chInsert = `
        INSERT INTO property_assessment_entity (
            assessmentnumber, tenant_id, propertyid,
            financialyear, status, source, channel,
            created_by, created_time,
            last_modified_by, last_modified_time
        )`

    var skipTenants map[string]bool
    if resume {
        var err error


		
        skipTenants, err = getCompletedTenants(
            ctx, pgPool, chConn,
            "property_assessment_entity",
            `SELECT tenantid, count(*) FROM eg_pt_asmt_assessment GROUP BY tenantid`,
            `SELECT tenant_id, count() FROM property_assessment_entity FINAL GROUP BY tenant_id`,
        )
        if err != nil {
            return 0, fmt.Errorf("resume check: %w", err)
        }
    }

    return parallelByTenant(
        ctx, pgPool, chConn,
        "property_assessment_entity",
        tenantQuery,
        pgQuery,
        chInsert,
        batchSize,
        workers,
        "id",
        func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
            var (
                id, tenantID, assessmentNumber string
                propertyID                     string
                financialYear, status          string
                source, channel                string
                createdBy, lastModifiedBy      string
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
        globalCounter,
        skipTenants,
        tenant,
    )
}

func migratePaymentWithDetails(
    ctx context.Context,
    pgPool *pgxpool.Pool,
    chConn clickhouse.Conn,
    batchSize, workers int,
    globalCounter *int64,
    resume bool,
    tenant string,
) (int64, error) {

    const tenantQuery = `
        SELECT DISTINCT tenantid
        FROM egcl_payment
        ORDER BY tenantid`

    const pgQuery = `
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
        JOIN egcl_paymentdetail d
          ON p.id = d.paymentid
        WHERE p.tenantid = $1`

    const chInsert = `
        INSERT INTO payment_with_details_entity (
            tenant_id,
            payment_id,
            total_due,
            total_amount_paid,
            transaction_number,
            transaction_date,
            payment_mode,
            instrument_date,
            instrument_number,
            instrument_status,
            ifsc_code,
            additional_details,
            payer_id,
            payment_status,
            created_by,
            created_time,
            last_modified_by,
            last_modified_time,
            filestore_id,
            receiptnumber,
            receiptdate,
            receipttype,
            businessservice,
            billid,
            manualreceiptnumber,
            manualreceiptdate
        )`

    var skipTenants map[string]bool
    if resume {
        var err error
        skipTenants, err = getCompletedTenants(
            ctx, pgPool, chConn,
            "payment_with_details_entity",
            `SELECT tenantid, count(*) FROM egcl_payment GROUP BY tenantid`,
            `SELECT tenant_id, count() FROM payment_with_details_entity FINAL GROUP BY tenant_id`,
        )
        if err != nil {
            return 0, fmt.Errorf("resume check: %w", err)
        }
    }

    return parallelByTenant(
        ctx, pgPool, chConn,
        "payment_with_details_entity",
        tenantQuery,
        pgQuery,
        chInsert,
        batchSize,
        workers,
        "d.id",
        func(scan func(dest ...any) error, appendFn func(v ...any) error) error {

            var (
                paymentID, tenantID string
                totalDue, totalPaid float64
                txnNumber string
                txnDateMs int64
                paymentMode string
                instrumentDateMs *int64
                instrumentNumber, instrumentStatus string
                ifscCode string
                additionalDetails string
                payerID, paymentStatus string
                createdBy string
                createdTimeMs int64
                lastModifiedBy string
                lastModifiedTimeMs int64
                filestoreID *string

                receiptNumber string
                receiptDateMs int64
                receiptType string
                businessService string
                billID string
                manualReceiptNumber *string
                manualReceiptDateMs *int64
            )

            if err := scan(
                &paymentID, &tenantID,
                &totalDue, &totalPaid,
                &txnNumber, &txnDateMs,
                &paymentMode,
                &instrumentDateMs,
                &instrumentNumber,
                &instrumentStatus,
                &ifscCode,
                &additionalDetails,
                &payerID,
                &paymentStatus,
                &createdBy,
                &createdTimeMs,
                &lastModifiedBy,
                &lastModifiedTimeMs,
                &filestoreID,

                &receiptNumber,
                &receiptDateMs,
                &receiptType,
                &businessService,
                &billID,
                &manualReceiptNumber,
                &manualReceiptDateMs,
            ); err != nil {
                return err
            }

            return appendFn(
                tenantID,
                paymentID,
                d(totalDue),
                d(totalPaid),
                txnNumber,
                msToISTVal(txnDateMs),
                paymentMode,
                msToIST(instrumentDateMs),
                instrumentNumber,
                instrumentStatus,
                ifscCode,
                additionalDetails,
                payerID,
                paymentStatus,
                createdBy,
                msToISTVal(createdTimeMs),
                lastModifiedBy,
                msToISTVal(lastModifiedTimeMs),
                nullableString(filestoreID),
                receiptNumber,
                msToISTVal(receiptDateMs),
                receiptType,
                businessService,
                billID,
                nullableString(manualReceiptNumber),
                msToIST(manualReceiptDateMs),
            )
        },
        globalCounter,
        skipTenants,
        tenant,
    )
}


func migrateBill(
    ctx context.Context,
    pgPool *pgxpool.Pool,
    chConn clickhouse.Conn,
    batchSize, workers int,
    globalCounter *int64,
    resume bool,
    tenant string,
) (int64, error) {

    const tenantQuery = `
        SELECT DISTINCT tenantid
        FROM egcl_bill
        ORDER BY tenantid`

    const pgQuery = `
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
        WHERE tenantid = $1`

    const chInsert = `
        INSERT INTO bill_entity (
            bill_id, tenant_id, consumercode,
            businessservice,
            totalamount,
            status,
            created_by, created_time,
            last_modified_by, last_modified_time
        )`

    var skipTenants map[string]bool
    if resume {
        var err error
        skipTenants, err = getCompletedTenants(
            ctx, pgPool, chConn,
            "bill_entity",
            `SELECT tenantid, count(*) FROM egcl_bill GROUP BY tenantid`,
            `SELECT tenant_id, count() FROM bill_entity FINAL GROUP BY tenant_id`,
        )
        if err != nil {
            return 0, err
        }
    }

    return parallelByTenant(
        ctx, pgPool, chConn,
        "bill_entity",
        tenantQuery,
        pgQuery,
        chInsert,
        batchSize,
        workers,
        "id",
        func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
            var (
                id, tenantID, consumerCode, businessService string
                totalAmount float64
                status string
                createdBy, lastModifiedBy string
                createdTimeMs, lastModifiedTimeMs int64
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
                businessService,
                d(totalAmount),
                status,
                createdBy, msToISTVal(createdTimeMs),
                lastModifiedBy, msToISTVal(lastModifiedTimeMs),
            )
        },
        globalCounter,
        skipTenants,
        tenant,
    )
}


// ─── Table 2: property_unit_fact ────────────────────────────────────────────

func migratePropertyUnit(ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn, batchSize, workers int, globalCounter *int64, resume bool, tenant string) (int64, error) {
	const tenantQuery = `SELECT DISTINCT tenantid FROM eg_pt_unit ORDER BY tenantid`

	const pgQuery = `
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
		WHERE u.tenantid = $1`

	const chInsert = `INSERT INTO property_unit_entity (
		tenant_id, property_uuid, unit_id, floor_no, unit_type,
		usage_category, occupancy_type, occupancy_date,
		carpet_area, built_up_area, plinth_area, super_built_up_area,
		arv, construction_type, construction_date, active,
		created_by, created_time, last_modified_by, last_modified_time,
		property_id, property_type, ownership_category,
		property_status, no_of_floors)`

	var skipTenants map[string]bool
	if resume {
		var err error
		skipTenants, err = getCompletedTenants(ctx, pgPool, chConn,
			"property_unit_entity",
			`SELECT u.tenantid, count(*) FROM eg_pt_unit u JOIN eg_pt_property p ON u.propertyid = p.id GROUP BY u.tenantid`,
			`SELECT tenant_id, count() FROM property_unit_entity FINAL GROUP BY tenant_id`,
		)
		if err != nil {
			return 0, fmt.Errorf("resume check: %w", err)
		}
	}

	return parallelByTenant(ctx, pgPool, chConn,
		"property_unit_entity", tenantQuery, pgQuery, chInsert,
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
		globalCounter,
		skipTenants,
		tenant,
	)
}

// ─── Table 3: property_owner_fact ───────────────────────────────────────────

func migratePropertyOwner(ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn, batchSize, workers int, globalCounter *int64, resume bool, tenant string) (int64, error) {
	const tenantQuery = `SELECT DISTINCT tenantid FROM eg_pt_owner ORDER BY tenantid`

	const pgQuery = `
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
		WHERE o.tenantid = $1`

	const chInsert = `INSERT INTO property_owner_entity (
		tenant_id, property_uuid, owner_info_uuid, user_id, status,
		is_primary_owner, owner_type, ownership_percentage,
		institution_id, relationship,
		created_by, created_time, last_modified_by, last_modified_time,
		property_id, property_type, ownership_category,
		property_status, no_of_floors)`

	var skipTenants map[string]bool
	if resume {
		var err error
		skipTenants, err = getCompletedTenants(ctx, pgPool, chConn,
			"property_owner_entity",
			`SELECT o.tenantid, count(DISTINCT o.ownerinfouuid) FROM eg_pt_owner o JOIN eg_pt_property p ON o.propertyid = p.id GROUP BY o.tenantid`,
			`SELECT tenant_id, count() FROM property_owner_entity FINAL GROUP BY tenant_id`,
		)
		if err != nil {
			return 0, fmt.Errorf("resume check: %w", err)
		}
	}

	return parallelByTenant(ctx, pgPool, chConn,
		"property_owner_entity", tenantQuery, pgQuery, chInsert,
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
		globalCounter,
		skipTenants,
		tenant,
	)
}

// ─── Table 4: demand_with_details_fact (two-phase: PG stream → CH staging → CH pivot) ──

func createDemandStagingTables(ctx context.Context, chConn clickhouse.Conn) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS _stg_demand (
			tenantid String, id String, consumercode String, consumertype String,
			businessservice String, payer String, taxperiodfrom Int64, taxperiodto Int64,
			status String, ispaymentcompleted UInt8, minimumamountpayable Float64,
			billexpirytime Int64, fixedbillexpirydate Int64,
			createdby String, createdtime Int64, lastmodifiedby String, lastmodifiedtime Int64
		) ENGINE = MergeTree() ORDER BY (tenantid, id)`,
		`CREATE TABLE IF NOT EXISTS _stg_demanddetail (
			tenantid String, demandid String, taxheadcode String,
			taxamount Float64, collectionamount Float64
		) ENGINE = MergeTree() ORDER BY (tenantid, demandid)`,
	}
	for _, q := range queries {
		if err := chConn.Exec(ctx, q); err != nil {
			return fmt.Errorf("create staging table: %w", err)
		}
	}
	return nil
}

func dropDemandStagingTables(ctx context.Context, chConn clickhouse.Conn) {
	_ = chConn.Exec(ctx, "DROP TABLE IF EXISTS _stg_demand")
	_ = chConn.Exec(ctx, "DROP TABLE IF EXISTS _stg_demanddetail")
}

func migrateDemandRaw(ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn, batchSize, workers int, globalCounter *int64, skipTenants map[string]bool, tenantFilter string) (int64, error) {
	const tenantQuery = `SELECT DISTINCT tenantid FROM egbs_demand_v1 WHERE businessservice = 'PT' ORDER BY tenantid`
	const pgQuery = `
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
		WHERE businessservice = 'PT' AND tenantid = $1`
	const chInsert = `INSERT INTO _stg_demand`

	return parallelByTenant(ctx, pgPool, chConn,
		"_stg_demand", tenantQuery, pgQuery, chInsert,
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
		globalCounter,
		skipTenants,
		tenantFilter,
	)
}



func migrateDemandDetailRaw(ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn, batchSize, workers int, globalCounter *int64, skipTenants map[string]bool, tenantFilter string) (int64, error) {
	const tenantQuery = `SELECT DISTINCT tenantid FROM egbs_demand_v1 WHERE businessservice = 'PT' ORDER BY tenantid`
	const pgQuery = `
		SELECT
			COALESCE(dd.tenantid,''), COALESCE(dd.demandid,''),
			COALESCE(dd.taxheadcode,''),
			COALESCE(dd.taxamount,0)::float8, COALESCE(dd.collectionamount,0)::float8
		FROM egbs_demanddetail_v1 dd
		WHERE dd.tenantid = $1
		  AND dd.demandid IN (
			SELECT id FROM egbs_demand_v1 WHERE businessservice = 'PT' AND tenantid = $1
		  )`
	const chInsert = `INSERT INTO _stg_demanddetail`

	return parallelByTenant(ctx, pgPool, chConn,
		"_stg_demanddetail", tenantQuery, pgQuery, chInsert,
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
		globalCounter,
		skipTenants,
		tenantFilter,
	)
}

func pivotDemandInClickHouse(ctx context.Context, chConn clickhouse.Conn) error {
	log.Println("[demand] Phase 3: Pivoting in ClickHouse (JOIN + GROUP BY)...")
	pivotCtx, cancel := context.WithTimeout(ctx, 1*time.Hour)
	defer cancel()

	const pivotSQL = `
INSERT INTO demand_with_details_entity (
	tenant_id, demand_id, consumer_code, consumer_type, business_service, payer,
	tax_period_from, tax_period_to, demand_status, is_payment_completed,
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
	d.status, d.ispaymentcompleted,
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

func migrateDemandWithDetails(ctx context.Context, pgPool *pgxpool.Pool, chConn clickhouse.Conn, batchSize, workers int, globalCounter *int64, resume bool, tenant string) (int64, error) {
	// Two-phase demand migration for 1.8B+ detail rows:
	// Phase 1-2: Stream raw demands + details from PG → CH staging (no JOIN/GROUP BY in PG)
	// Phase 3:   Pivot in ClickHouse (JOIN + GROUP BY — CH is 10-100x faster at aggregation)

	var skipTenants map[string]bool
	if resume {
		var err error
		skipTenants, err = getCompletedTenants(ctx, pgPool, chConn,
			"demand_with_details_entity",
			`SELECT tenantid, count(*) FROM egbs_demand_v1 WHERE businessservice = 'PT' GROUP BY tenantid`,
			`SELECT tenant_id, count() FROM demand_with_details_entity FINAL GROUP BY tenant_id`,
		)
		if err != nil {
			return 0, fmt.Errorf("resume check: %w", err)
		}
	}

	// Staging tables already exist with demand data loaded — skip creation and drop
	// log.Println("[demand] Phase 1: Creating ClickHouse staging tables...")
	// if err := createDemandStagingTables(ctx, chConn); err != nil {
	// 	return 0, fmt.Errorf("create staging: %w", err)
	// }
	// defer dropDemandStagingTables(ctx, chConn)

	log.Println("[demand] Phase 2: Streaming demand details from PostgreSQL (demand staging skipped)...")
	var detailCount int64
	var detailErr error

	// Demand already in staging — only stream demand details
	detailCount, detailErr = migrateDemandDetailRaw(ctx, pgPool, chConn, batchSize, workers, globalCounter, skipTenants, tenant)

	if detailErr != nil {
		return 0, fmt.Errorf("stream details: %w", detailErr)
	}
	log.Printf("[demand] Staged: demand (already loaded) + %d details", detailCount)

	if err := pivotDemandInClickHouse(ctx, chConn); err != nil {
		return detailCount, fmt.Errorf("pivot: %w", err)
	}
	log.Printf("[demand] Pivot complete → demand_with_details_entity")

	return detailCount, nil
}

// ─── Table: property_audit_entity_v1 (from eg_pt_property_audit) ─────────

func migratePropertyAudit(
	ctx context.Context,
	pgPool *pgxpool.Pool,
	chConn clickhouse.Conn,
	batchSize, workers int,
	globalCounter *int64,
	resume bool,
	tenant string,
) (int64, error) {

	// tenantId lives inside the JSONB, not as a top-level column
	const tenantQuery = `
		SELECT DISTINCT property->>'tenantId'
		FROM eg_pt_property_audit
		WHERE property->>'tenantId' IS NOT NULL
		ORDER BY 1`

	const pgQuery = `
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
				THEN ARRAY(SELECT elem->>'uuid' FROM jsonb_array_elements(property->'owners') AS elem WHERE elem->>'uuid' IS NOT NULL)
				ELSE ARRAY[]::text[]
			END,
			auditcreatedtime,
			COALESCE((property->'auditDetails'->>'createdTime')::bigint, 0),
			COALESCE((property->'auditDetails'->>'lastModifiedTime')::bigint, 0)
		FROM eg_pt_property_audit
		WHERE property->>'tenantId' = $1`

	const chInsert = `
		INSERT INTO property_audit_entity_v1 (
			tenant_id, property_id, property_type,
			ownership_category, usage_category, property_status, workflow_state,
			super_built_up_area, land_area, owner_user_ids,
			audit_created_time, created_time, last_modified_time
		)`

	var skipTenants map[string]bool
	if resume {
		var err error
		skipTenants, err = getCompletedTenants(
			ctx, pgPool, chConn,
			"property_audit_entity_v1",
			`SELECT property->>'tenantId', count(*) FROM eg_pt_property_audit WHERE property->>'tenantId' IS NOT NULL GROUP BY property->>'tenantId'`,
			`SELECT tenant_id, count() FROM property_audit_entity_v1 GROUP BY tenant_id`,
		)
		if err != nil {
			return 0, fmt.Errorf("resume check: %w", err)
		}
	}

	return parallelByTenant(
		ctx, pgPool, chConn,
		"property_audit_entity_v1",
		tenantQuery,
		pgQuery,
		chInsert,
		batchSize,
		workers,
		"audituuid",
		func(scan func(dest ...any) error, appendFn func(v ...any) error) error {
			var (
				tenantID, propertyID       string
				propertyType               string
				ownershipCat, usageCat     string
				propertyStatus             string
				workflowState              string
				superBuiltUpArea, landArea float64
				ownerUserIDs               []string
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
				&ownerUserIDs,
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
				ownerUserIDs,
				msToISTVal(auditCreatedTimeMs),
				msToISTVal(createdTimeMs),
				msToISTVal(lastModifiedTimeMs),
			)
		},
		globalCounter,
		skipTenants,
		tenant,
	)
}

// ─── Main ───────────────────────────────────────────────────────────────────

type migrationResult struct {
	table   string
	rows    int64
	elapsed time.Duration
	err     error
}

var allTables = []string{"property_address", "property_unit", "property_owner", "demand_details", "assessment", "payment_with_details", "bill", "property_audit"}

func main() {
	var (
		pgHost     string
		pgPort     int
		pgDB       string
		pgUser     string
		pgPassword string
		chHost     string
		chPort     int
		chDB       string
		chUser     string
		chPassword string
		chProtocol string
		chSecure   bool
		batchSize  int
		workers    int
		tables     string
		resume     bool
		tenant     string
	)

	flag.StringVar(&pgHost, "pg-host", "localhost", "PostgreSQL host")
	flag.IntVar(&pgPort, "pg-port", 5432, "PostgreSQL port")
	flag.StringVar(&pgDB, "pg-db", "", "PostgreSQL database (required)")
	flag.StringVar(&pgUser, "pg-user", "postgres", "PostgreSQL user")
	flag.StringVar(&pgPassword, "pg-password", "", "PostgreSQL password")
	flag.StringVar(&chHost, "ch-host", "z0tz5tcsm5.ap-south-1.aws.clickhouse.cloud", "ClickHouse host")
	flag.IntVar(&chPort, "ch-port", 9440, "ClickHouse port (9440=secure native, 8443=secure HTTP)")
	flag.StringVar(&chDB, "ch-db", "punjab_property_tax", "ClickHouse database")
	flag.StringVar(&chUser, "ch-user", "default", "ClickHouse user")
	flag.StringVar(&chPassword, "ch-password", "", "ClickHouse password")
	flag.StringVar(&chProtocol, "ch-protocol", "auto", "ClickHouse protocol: auto, http, native (auto=native for port 9440/9000, http otherwise)")
	flag.BoolVar(&chSecure, "ch-secure", true, "Enable TLS for ClickHouse connection")
	flag.IntVar(&batchSize, "batch-size", defaultBatchSize, "Rows per batch (fetch + insert)")
	flag.IntVar(&workers, "workers", defaultWorkers, "Workers per table (parallel tenant processing)")
	flag.StringVar(&tables, "tables", "all", "Comma-separated: property_address,property_unit,property_owner,demand_details (or 'all')")
	flag.BoolVar(&resume, "resume", false, "Resume mode: skip tenants already fully migrated (compares PG vs CH row counts)")
	flag.StringVar(&tenant, "tenant", "", "Migrate only this tenant ID (e.g. 'pb.amritsar'). If empty, all tenants are migrated.")
	flag.Parse()

	if pgDB == "" {
		fmt.Fprintln(os.Stderr, "Error: -pg-db is required")
		flag.Usage()
		os.Exit(1)
	}

	// Determine tables to migrate
	tablesToMigrate := allTables
	if tables != "all" {
		tablesToMigrate = strings.Split(tables, ",")
		for i := range tablesToMigrate {
			tablesToMigrate[i] = strings.TrimSpace(tablesToMigrate[i])
		}
	}

	// Determine ClickHouse protocol
	var protocol clickhouse.Protocol
	switch chProtocol {
	case "http":
		protocol = clickhouse.HTTP
	case "native":
		protocol = clickhouse.Native
	default: // "auto"
		protocol = clickhouse.HTTP
		if chPort == 9000 || chPort == 9440 {
			protocol = clickhouse.Native
		}
	}

	log.Println("PostgreSQL -> ClickHouse Migration (Go)")
	log.Println("==================================================")
	log.Printf("PostgreSQL:      %s:%d/%s", pgHost, pgPort, pgDB)
	log.Printf("ClickHouse:      %s:%d/%s (protocol=%v)", chHost, chPort, chDB, protocol)
	log.Printf("Batch size:      %d", batchSize)
	log.Printf("Workers/table:   %d", workers)
	log.Printf("Tables:          %s", strings.Join(tablesToMigrate, ", "))
	if tenant != "" {
		log.Printf("Tenant filter:   %s", tenant)
	}
	log.Printf("Resume mode:     %v", resume)
	log.Println("==================================================")

	ctx := context.Background()

	// Connect to PostgreSQL
	// Pool size: 4 tables * N workers/table + buffer
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

	// Build migration function map
	type migrateFn func(context.Context, *pgxpool.Pool, clickhouse.Conn, int, int, *int64, bool, string) (int64, error)
	migrationFns := map[string]migrateFn{
		"property_address":     migratePropertyAddress,
		"property_unit":        migratePropertyUnit,
		"property_owner":       migratePropertyOwner,
		"demand_details":       migrateDemandWithDetails,
		"assessment":           migrateAssessment,
		"payment_with_details": migratePaymentWithDetails,
		"bill":                 migrateBill,
		"property_audit":       migratePropertyAudit,
	}

	// Run all table migrations concurrently
	results := make([]migrationResult, len(tablesToMigrate))
	var wg sync.WaitGroup

	for i, table := range tablesToMigrate {
		fn, ok := migrationFns[table]
		if !ok {
			log.Printf("Unknown table: %s (skipping)", table)
			results[i] = migrationResult{table: table, err: fmt.Errorf("unknown table")}
			continue
		}

		wg.Add(1)
		go func(idx int, tbl string, mFn migrateFn) {
			defer wg.Done()
			start := time.Now()
			count, err := mFn(ctx, pgPool, chConn, batchSize, workers, &globalCounter, resume, tenant)
			results[idx] = migrationResult{
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
	close(done) // stop progress reporter

	overallElapsed := time.Since(overallStart)

	// Summary
	log.Println()
	log.Println("==================================================")
	log.Println("MIGRATION SUMMARY")
	log.Println("==================================================")

	var totalRows int64
	hasError := false
	for _, r := range results {
		status := "OK"
		if r.err != nil {
			status = fmt.Sprintf("FAILED: %v", r.err)
			hasError = true
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

	if hasError {
		os.Exit(1)
	}
}