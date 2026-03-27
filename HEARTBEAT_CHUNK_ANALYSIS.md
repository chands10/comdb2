# Heartbeat and Chunk Transaction Analysis

## Overview
This document maps the heartbeat mechanism for chunk transactions in comdb2, showing how heartbeats keep connections alive while large transactions are being processed in chunks.

---

## 1. Heartbeat Sending During Chunk Transactions

### Primary Location: `db/sqlinterfaces.c` - `send_heartbeat()` function (Line 4934)

```c
static int send_heartbeat(struct sqlclntstate *clnt)
{
    /* if client didnt ask for heartbeats, dont send them */
    if (!clnt->heartbeat)
        return 0;

    if (!clnt->ready_for_heartbeats) {
        return 0;
    }

    write_response(clnt, RESPONSE_HEARTBEAT, 0, 0);
    return 0;
}
```

**Key Points:**
- Checks two conditions before sending: `clnt->heartbeat` and `clnt->ready_for_heartbeats`
- Writes a `RESPONSE_HEARTBEAT` to the client with 0 length
- Called at multiple points to keep connection alive

### Where Heartbeats are Sent After Chunk Commits

File: [`db/sqlinterfaces.c`](db/sqlinterfaces.c#L2456)

In function `handle_sql_commitrollback()` (Line 2383-2461):

```c
if (rc == SQLITE_OK) {
    /* send return code */
    Pthread_mutex_lock(&clnt->wait_mutex);
    write_response(clnt, RESPONSE_EFFECTS, 0, 0);

    /* Send heartbeat for chunked transactions to keep connection alive */
    if (sideeffects == TRANS_CLNTCOMM_CHUNK) {
        send_heartbeat(clnt);
    }

    /* do not turn heartbeats if this is a chunked transaction */
    if (sideeffects != TRANS_CLNTCOMM_CHUNK)
        clnt->ready_for_heartbeats = 0;
    ...
}
```

**Key Points:**
- Called after each chunk commit via `TRANS_CLNTCOMM_CHUNK` flag
- Explicitly sends heartbeat after chunk is committed
- Keeps `ready_for_heartbeats` enabled for chunk transactions (`!= TRANS_CLNTCOMM_CHUNK`)

---

## 2. Chunk Transaction Processing

### Chunk Transaction Handler: `db/sqlglue.c` - `chunk_transaction()` (Line 8751)

```c
static int chunk_transaction(BtCursor *pCur, struct sqlclntstate *clnt,
                             struct sql_thread *thd)
{
    // ... initialization ...

    if (clnt->dbtran.crtchunksize >= clnt->dbtran.maxchunksize) {
        // Current chunk size reached, commit it

        ++clnt->dbtran.nchunks;  // Increment chunk counter

        /* commit current transaction */
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_FNSH_STATE);
        rc = handle_sql_commitrollback(clnt->thd, clnt, TRANS_CLNTCOMM_CHUNK);

        // ... handle verify errors, throttling ...

        /* restart a new transaction */
        sql_set_sqlengine_state(clnt, __FILE__, __LINE__, SQLENG_PRE_STRT_STATE);
        rc = handle_sql_begin(clnt->thd, clnt, TRANS_CLNTCOMM_CHUNK);
    }
}
```

**Key Points:**
- Triggered when `clnt->dbtran.crtchunksize >= clnt->dbtran.maxchunksize`
- Calls `handle_sql_commitrollback()` with `TRANS_CLNTCOMM_CHUNK` flag
- This triggers the heartbeat sending mechanism
- Restarts a new transaction for the next chunk
- Located in file: [`db/sqlglue.c`](db/sqlglue.c#L8751-L8850)

---

## 3. "SET TRANSACTION CHUNK" Command Handling

### Location: `plugins/newsql/newsql.c` (Line 1740-1770)

```c
if (!sqlstr || ((tmp = atoi(sqlstr)) <= 0)) {
    snprintf(err, sizeof(err),
             "set transaction chunk N: missing chunk size N \"%s\"",
             sqlstr);
    rc = ii + 1;
} else if (clnt->dbtran.mode != TRANLEVEL_SOSQL) {
    // Check mode
    int snapshot_by_default =
        (clnt->dbtran.mode == TRANLEVEL_MODSNAP &&
         gbl_sql_tranlevel_default == TRANLEVEL_MODSNAP);
    if (snapshot_by_default) {
        logmsg(LOGMSG_DEBUG, "snapshot is on by default, use socksql instead\n");
        clnt->dbtran.mode = TRANLEVEL_SOSQL;
        clnt->dbtran.maxchunksize = tmp;  // ← SET CHUNK SIZE HERE
        clnt->verifyretry_off = 1;
    } else {
        snprintf(err, sizeof(err), "transaction chunks require SOCKSQL transaction mode");
        rc = ii + 1;
    }
} else {
    clnt->dbtran.maxchunksize = tmp;  // ← SET CHUNK SIZE HERE
    clnt->verifyretry_off = 1;
}
```

**Key Points:**
- Sets `clnt->dbtran.maxchunksize` to the chunk size parameter
- Requires `TRANLEVEL_SOSQL` transaction mode
- Disables verify retries in chunked mode (`clnt->verifyretry_off = 1`)
- File: [`plugins/newsql/newsql.c`](plugins/newsql/newsql.c#L1740-L1770)

---

## 4. `reset_clnt()` and Chunk Transaction Relationship

### Location: `db/sqlinterfaces.c` - `reset_clnt()` (Line 5394)

```c
void reset_clnt(struct sqlclntstate *clnt, int initial)
{
    int saved_nchunks = clnt->dbtran.nchunks;
    int saved_maxchunksize = clnt->dbtran.maxchunksize;
    logmsg(LOGMSG_USER, "reset_clnt called: initial=%d nchunks=%d maxchunksize=%d\n",
           initial, saved_nchunks, saved_maxchunksize);

    if (saved_nchunks > 0 || saved_maxchunksize > 0) {
        cheap_stack_trace();  // Log stack trace for debugging
    }

    // ... initialization or reset ...

    clnt->dbtran.nchunks = 0;
    clnt->heartbeat = 0;

    // ... clear chunk state ...

    if (clnt->dbtran.nchunks > 0 || clnt->dbtran.maxchunksize > 0) {
        logmsg(LOGMSG_USER,
               "reset_clnt: bzero dbtran clearing chunks - nchunks=%d maxchunksize=%d\n",
               clnt->dbtran.nchunks, clnt->dbtran.maxchunksize);
        cheap_stack_trace();
    }
    bzero(&clnt->dbtran, sizeof(dbtran_type));

    clnt->dbtran.crtchunksize = clnt->dbtran.maxchunksize = 0;
    clnt->dbtran.throttle_txn_chunks_msec = 0;
}
```

**Key Points:**
- Saves and logs chunk state before clearing
- Clears `clnt->dbtran.nchunks` and `clnt->dbtran.maxchunksize`
- Disables heartbeats (`clnt->heartbeat = 0`)
- Uses `cheap_stack_trace()` for debugging when chunks are present
- File: [`db/sqlinterfaces.c`](db/sqlinterfaces.c#L5394-L5550)

### Chunk State Structure: `db/sql.h` (Line 260-262)

```c
struct dbtran_type {
    // ... other fields ...
    int maxchunksize;     /* multi-transaction bulk mode */
    int nchunks;          /* number of chunks. 0 for a non-chunked transaction. */
    int crtchunksize;     /* how many rows are processed already */
    int throttle_txn_chunks_msec;  /* throttle between chunks */
    // ...
}
```

---

## 5. Heartbeat Management Flow

### Heartbeat Enablement: `wait_for_sql_query()` (Line 5073-5080)

File: [`db/sqlinterfaces.c`](db/sqlinterfaces.c#L5073)

```c
static int wait_for_sql_query(struct sqlclntstate *clnt)
{
    /* successful dispatch or queueing, enable heartbeats */
    Pthread_mutex_lock(&clnt->wait_mutex);
    if (clnt->exec_lua_thread)
        clnt->ready_for_heartbeats = 0;
    else
        clnt->ready_for_heartbeats = 1;  // ← ENABLED HERE
    Pthread_mutex_unlock(&clnt->wait_mutex);

    // ... heartbeat sending loop ...

    if (clnt->heartbeat) {
        if (clnt->osql.replay != OSQL_RETRY_NONE || in_client_trans(clnt)) {
            send_heartbeat(clnt);
        }
        // Heartbeat timeout loop sends heartbeats every gbl_client_heartbeat_ms
        struct timespec mshb;
        mshb.tv_sec = (gbl_client_heartbeat_ms / 1000);
        mshb.tv_nsec = (gbl_client_heartbeat_ms % 1000) * 1000000;

        while (1) {
            if (rc == ETIMEDOUT) {
                TIMESPEC_SUB(st, last, diff);
                if (diff.tv_sec >= clnt->heartbeat) {
                    last = st;
                    send_heartbeat(clnt);  // ← PERIODIC HEARTBEAT
                }
            }
        }
    }
}
```

**Key Points:**
- Sets `clnt->ready_for_heartbeats = 1` when query is dispatched
- Periodically sends heartbeats every `gbl_client_heartbeat_ms` (default 100ms)
- Waits for SQL thread to complete via condition variables
- File: [`db/sqlinterfaces.c`](db/sqlinterfaces.c#L5073-L5150)

### Heartbeat Configuration

File: `db/sqlinterfaces.c` (Line 297)
```c
int gbl_client_heartbeat_ms = 100;
```

---

## 6. Client-Side Heartbeat Handling

### Location: `cdb2api/cdb2api.c` - Response handler (Line 4187)

```c
case RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT:
    // Handle heartbeat response

    if (hdr.length == 0) {
        if (hdr.type != RESPONSE_HEADER__SQL_RESPONSE_HEARTBEAT) {
            fprintf(stderr, "%s: invalid response type for 0-length %d\n",
                    __func__, hdr.type);
            rc = -1;
            goto after_callback;
        }
        debugprint("hdr length (0) from mach %s - going to retry\n",
                   hndl->hosts[hndl->connected_host]);

        /* If we have an AT_RECEIVE_HEARTBEAT event, invoke it now. */
        cdb2_event *e = NULL;
        void *callbackrc;
        while ((e = cdb2_next_callback(hndl, CDB2_AT_RECEIVE_HEARTBEAT, e)) !=
               NULL) {
            int unused;
            (void)unused;
            callbackrc = cdb2_invoke_callback(hndl, e, 1, CDB2_QUERY_STATE,
                                             hdr.state);
            PROCESS_EVENT_CTRL_AFTER(hndl, e, unused, callbackrc);
        }
    }
```

**Key Points:**
- Identifies heartbeat by header length == 0 and type == `SQL_RESPONSE_HEARTBEAT`
- Invokes client callbacks with `CDB2_AT_RECEIVE_HEARTBEAT` event
- File: [`cdb2api/cdb2api.c`](cdb2api/cdb2api.c#L4187)

### Heartbeat Protocol Definition

File: `protobuf/sqlresponse.proto` (Line 85)
```protobuf
SQL_RESPONSE_HEARTBEAT = 205;  // This is same as FSQL_HEARTBEAT
```

---

## 7. Related Data Structures

### Client State: `db/sql.h` (Line 782)

```c
struct sqlclntstate {
    // ... many fields ...
    uint8_t heartbeat;              /* heartbeat interval in seconds */
    uint8_t ready_for_heartbeats;   /* flag to enable/disable heartbeat sending */
    // ...
    struct dbtran_type dbtran;      /* chunk transaction state */
    // ...
}
```

---

## 8. Key Files Summary

| File | Purpose | Key Functions |
|------|---------|----------------|
| [`db/sqlinterfaces.c`](db/sqlinterfaces.c) | Main heartbeat logic | `send_heartbeat()`, `wait_for_sql_query()`, `reset_clnt()`, `handle_sql_commitrollback()` |
| [`db/sqlglue.c`](db/sqlglue.c) | Chunk transaction execution | `chunk_transaction()` |
| [`plugins/newsql/newsql.c`](plugins/newsql/newsql.c) | SQL command parsing | SET TRANSACTION CHUNK handling |
| [`cdb2api/cdb2api.c`](cdb2api/cdb2api.c) | Client API for receiving heartbeats | Response handling, callback invocation |
| [`db/sql.h`](db/sql.h) | Data structures | `dbtran_type`, `sqlclntstate` definitions |
| [`protobuf/sqlresponse.proto`](protobuf/sqlresponse.proto) | Protocol definitions | `SQL_RESPONSE_HEARTBEAT` |

---

## 9. Flow Diagram: Chunk Transaction with Heartbeats

```
Client: "SET TRANSACTION CHUNK 100"
  ↓
plugins/newsql/newsql.c (line 1754)
  Sets clnt->dbtran.maxchunksize = 100

Client: "BEGIN"
  ↓
db/sqlinterfaces.c: wait_for_sql_query()
  Sets clnt->ready_for_heartbeats = 1
  Starts heartbeat send loop (every 100ms)

Client: Sends INSERT/UPDATE/DELETE statements
  ↓
db/sqlglue.c: chunk_transaction()
  When crtchunksize >= maxchunksize:
    - Calls handle_sql_commitrollback(TRANS_CLNTCOMM_CHUNK)
    - Increments nchunks

db/sqlinterfaces.c: handle_sql_commitrollback()
  Sends heartbeat via send_heartbeat()
  Keeps ready_for_heartbeats = 1

  ↓
Restarts new transaction for next chunk

Client receives heartbeats every 100ms to keep connection alive
  ↓
cdb2api/cdb2api.c: Response handler
  Identifies heartbeat (length == 0)
  Invokes CDB2_AT_RECEIVE_HEARTBEAT callbacks
  ↓
Client recognizes it's still in transaction

Client: "COMMIT"
  ↓
db/sqlinterfaces.c: handle_sql_commitrollback()
  Final commit sends heartbeat
  Sets ready_for_heartbeats = 0 (when sideeffects != CHUNK)

db/sqlinterfaces.c: reset_clnt()
  Clears nchunks, maxchunksize, heartbeat flags
```

---

## 10. Recent Changes and Debugging

### Debug Logging

The code contains extensive logging for debugging chunk transactions:

**File: `db/sqlinterfaces.c`**
- Line 5398: `"reset_clnt called: initial=%d nchunks=%d maxchunksize=%d\n"`
- Line 5455: `"reset_clnt: clearing nchunks (was %d) initial=%d from %s:%d\n"`
- Line 2456: `"handle_sql_commitrollback: do_commitrollback returned rc=%d sideeffects=%d\n"`

**File: `db/sqlglue.c`**
- Line 5126: `"sqlite3BtreeCommit: sql='%s' maxchunksize=%d nchunks=%d mode=%d from %s:%d\n"`
- Line 5162: `"RECOM commit: nchunks=%d sideeffects=%d maxchunksize=%d from %s:%d\n"`

### Current Issue Symptoms

Based on the log output in the workspace, heartbeats are not being sent in time after a while of running chunk transactions. Potential issues:

1. **`ready_for_heartbeats` not being set for chunk transactions** - May need to ensure it stays enabled between chunks
2. **Heartbeat send loop timing issues** - The `wait_for_sql_query()` timeout calculation may not be working correctly
3. **Client reconnection** - Client receives no heartbeat and reconnects, breaking the chunk transaction

---

## 11. Relevant Configuration

File: `db/sqlinterfaces.c` (Line 297)
```c
int gbl_client_heartbeat_ms = 100;  // Default 100ms between heartbeats
```

File: `docs/pages/config/config_files.md`
```
|heartbeat_send_time | 5 (seconds) | Send heartbeats this often.
|osql_heartbeat_send_time | 5 (sec) | Like heartbeat_send_time for the offload network
```

---

## Summary Table

| Component | File | Line | Function/Var |
|-----------|------|------|--------------|
| Heartbeat send | `db/sqlinterfaces.c` | 4934 | `send_heartbeat()` |
| Heartbeat after chunk | `db/sqlinterfaces.c` | 2456 | `handle_sql_commitrollback()` |
| Chunk detection | `db/sqlglue.c` | 8751 | `chunk_transaction()` |
| SET TRANSACTION CHUNK | `plugins/newsql/newsql.c` | 1754 | `maxchunksize` assignment |
| reset_clnt chunk clear | `db/sqlinterfaces.c` | 5455 | `reset_clnt()` |
| Heartbeat enable | `db/sqlinterfaces.c` | 5080 | `wait_for_sql_query()` |
| Heartbeat interval | `db/sqlinterfaces.c` | 297 | `gbl_client_heartbeat_ms` |
| Client heartbeat recv | `cdb2api/cdb2api.c` | 4214 | Response handler |
