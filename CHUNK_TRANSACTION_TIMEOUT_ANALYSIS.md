# Chunk Transaction Timeout Analysis

## Problem Summary
Chunked transactions with `SET TRANSACTION CHUNK 100` fail silently after ~211 chunks (21,100 rows out of 81,753 total). Data is lost without error messages.

## Root Cause
**Client timeout during INSERT execution, not during commit wait.**

### Evidence
1. Logs show transaction stops mid-chunk at `tran_ops=72` (72 out of 100 rows in chunk 211)
2. 2-minute gap in logs between last operation and `reset_clnt`
3. No "transaction too big" errors
4. Heartbeats ARE being sent during commit wait, but NOT during INSERT execution

### Timeline of Events
```
14:57:34 - Processing chunk 211, tran_ops=72 (last logged operation)
[2-minute gap - client timeout]
14:59:25 - reset_clnt called (client disconnected)
Result: Chunk 211 rolled back, all subsequent chunks never processed
```

## Current Heartbeat Coverage
✅ Heartbeats sent during OSQL wait (osql_read_buffer)
✅ Heartbeats sent during block processor wait (wait_till_max_wait_or_timeout)
❌ NO heartbeats sent during SQL operation execution (INSERT/UPDATE/DELETE)

## Performance Analysis
- **Chunk size**: 100 rows
- **Table indices**: 5 indices + 1 primary key = 6 total
- **Operations per chunk**: ~201 bplog entries (not 600 as expected - likely optimized)
- **Successful chunks**: 211 chunks
- **Total committed**: 21,100 rows
- **Lost data**: 60,653 rows (74% data loss)

## Why Heartbeats Alone Won't Fix This
The issue is NOT just the 5-second client socket timeout. Even with our heartbeat mechanism:
- Heartbeats keep connection alive during WAIT operations
- But individual INSERT statements can take >5 seconds due to:
  - Synchronous disk writes
  - Index updates (6 total)
  - Large row data (blob columns, long strings)

The client times out during the INSERTION phase, before reaching the commit wait where heartbeats are sent.

## The Real Issue: Missing Heartbeats During SQL Execution
Current code sends heartbeats in `wait_for_sql_query()` which runs WHILE WAITING for SQL to complete.
But for chunked transactions, the SQL thread is actively INSERTING data, not waiting.

## Proposed Solutions

### Option 1: Send Heartbeats During SQL Execution (Recommended)
Modify the SQL execution loop to periodically send heartbeats during long-running operations.

**Implementation:**
- Add heartbeat timer in SQL execution engine
- Send heartbeat every 250ms during INSERT/UPDATE/DELETE operations
- Minimal performance impact (just a heartbeat message every 250ms)

**Files to modify:**
- `db/sqlglue.c` - Add heartbeat calls in chunk_transaction() loop
- Or better: `db/sqlinterfaces.c` - Add heartbeat in write_response() called periodically

### Option 2: Increase Client Timeout Dramatically
Set `COMDB2_CONFIG_SOCKET_TIMEOUT=600000` (10 minutes) and `COMDB2_CONFIG_API_CALL_TIMEOUT=3600000` (1 hour).

**Pros:** Simple workaround
**Cons:**
- Doesn't fix root cause
- Will fail on even larger datasets
- Delays detection of real connection failures

### Option 3: Reduce Chunk Size
Use smaller chunks (e.g., `SET TRANSACTION CHUNK 10` instead of 100).

**Pros:** Each chunk completes faster, less likely to timeout
**Cons:**
- More chunk commits = more overhead
- Still doesn't fix timeout during individual slow INSERTs
- May hit chunk count limits

## Recommendation
**Implement Option 1** - Add heartbeats during SQL execution. This is the proper fix that:
1. Keeps client connection alive during long operations
2. Works for any data size
3. Minimal performance impact
4. Fixes the root cause, not just symptoms

The heartbeat mechanism we already implemented works correctly - we just need to extend it to cover the SQL execution phase, not just the commit wait phase.

## Additional Findings

### osql_max_trans Limit
- Default: 50,000 operations per transaction
- Counter `tran_ops` IS being reset properly between chunks
- Counter `replicant_numops` IS being reset properly between chunks
- This limit is NOT the cause of the failure

### Table Structure
```sql
storm_audit_hist:
- PRIMARY KEY
- KEY_RULE
- KEY_TIMESTAMP
- KEY_UUID_TIMESTAMP
- KEY_GROUP_REQ_ID
= 6 total indices
```

Each INSERT generates ~201 bplog operations (not 600), suggesting some optimization is happening.

