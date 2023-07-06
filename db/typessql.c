#include <typessql.h>

int gbl_typessql = 1;
int gbl_typessql_records_max = 1000;

struct row {
    char *packed;
    Mem *unpacked;
    long long row_size;
    int *types;
};
typedef struct row row_t;

struct typessql {
    int *col_types;
    int ncols;
    queue_type *rows_queue;
    row_t *current_row;
    int other_rc; // initialized to -1. Set if rc for grabbing next row is not SQLITE_ROW
};
typedef struct typessql typessql_t;

static void free_row(row_t *row, int ncols) {
    if (!row)
        return;

    if (row->unpacked)
        sqlite3UnpackedResultFree(&row->unpacked, ncols);
    sqlite3_free(row->packed);
    free(row->types);
    free(row);
}

void free_typessql_state(struct sqlclntstate *clnt) {
    typessql_t *typessql_state = clnt->typessql_state;
    free(typessql_state->col_types);
    queue_free(typessql_state->rows_queue);
    free_row(typessql_state->current_row, typessql_state->ncols);
    free(typessql_state);
}

static int typessql_column_count(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    return clnt->adapter.column_count ? clnt->adapter.column_count(clnt, stmt) : sqlite3_column_count(stmt);
}

// if iCol >= ncols try to return non-null type
// else return type of column for specific row (use case: check if field in row is NULL)
static int typessql_column_type(struct sqlclntstate *clnt, sqlite3_stmt *stmt, int iCol)
{
    typessql_t *typessql_state = clnt->typessql_state;
    int nonNullType = 0;
    if (iCol >= typessql_state->ncols) {
        iCol -= typessql_state->ncols;
        nonNullType = 1;
    }
    if (nonNullType && typessql_state->col_types)
        return typessql_state->col_types[iCol];
    else if (typessql_state->current_row)
        return typessql_state->current_row->types[iCol];

    return clnt->adapter.column_type ? clnt->adapter.column_type(clnt, stmt, iCol) : sqlite3_column_type(stmt, iCol);
}

// return if null column still exists
static int update_column_types(struct sqlclntstate *clnt, sqlite3_stmt *stmt, row_t *row)
{
    int null_col_exists = 0;
    typessql_t *typessql_state = clnt->typessql_state;
    for (int i = 0; i < typessql_state->ncols; i++) {
        // not calling types column type here currently cuz this will read from queue
        int r = clnt->adapter.column_type ? clnt->adapter.column_type(clnt, stmt, i) : sqlite3_column_type(stmt, i);
        row->types[i] = r;
        if (typessql_state->col_types[i] == SQLITE_NULL)
            typessql_state->col_types[i] = r;
        if (typessql_state->col_types[i] == SQLITE_NULL)
            null_col_exists = 1;
    }

    return null_col_exists;
}

int typessql_next_row(struct sqlclntstate *clnt, sqlite3_stmt *stmt)
{
    int r = SQLITE_ROW;
    typessql_t *typessql_state;
    int initialize = 0;
    if (!clnt->typessql_state) {
        clnt->typessql_state = calloc(1, sizeof(typessql_t));
        typessql_state = clnt->typessql_state;
        typessql_state->other_rc = -1; // initialize
        typessql_state->ncols = typessql_column_count(clnt, stmt);
        typessql_state->rows_queue = queue_new();
        initialize = 1;
    } else
        typessql_state = clnt->typessql_state;

    if (initialize && gbl_typessql_records_max > 0) {
        assert(!typessql_state->col_types);
        typessql_state->col_types = malloc(typessql_state->ncols * sizeof(int));
        for (int i = 0; i < typessql_state->ncols; i++) {
            typessql_state->col_types[i] = SQLITE_NULL;
        }

        int null_col_exists = 1;
        while (null_col_exists && queue_count(typessql_state->rows_queue) < gbl_typessql_records_max) {
            r = clnt->adapter.next_row ? clnt->adapter.next_row(clnt, stmt) : sqlite3_maybe_step(clnt, stmt);
            if (r != SQLITE_ROW) {
                typessql_state->other_rc = r;
                if (0 && initialize) { // TODO: Test if this is needed (don't think so)
                    free(typessql_state->col_types);
                    typessql_state->col_types = NULL;
                }
                break;
            }

            // add row to queue
            row_t *row = calloc(1, sizeof(row_t));
            row->types = malloc(typessql_state->ncols * sizeof(int));
            null_col_exists = update_column_types(clnt, stmt, row);
            row->packed = sqlite3PackedResult(stmt, &row->row_size);
            if (queue_add(typessql_state->rows_queue, row))
                abort();
        }
        if (null_col_exists && queue_count(typessql_state->rows_queue) > 0) // only print if results set is non-empty
            logmsg(LOGMSG_WARN, "%s: Unable to find all non-null types for query %s.\n", __func__, clnt->sql); // TODO: Add fingerprint/sql
    }

    if (typessql_state->current_row) {
        free_row(typessql_state->current_row, typessql_state->ncols);
        typessql_state->current_row = NULL;
    }
    // if queue return from queue
    if (queue_count(typessql_state->rows_queue) > 0) {
        typessql_state->current_row = (row_t *)queue_next(typessql_state->rows_queue);
        return SQLITE_ROW;
    }

    if (typessql_state->other_rc != -1) {
        r = typessql_state->other_rc;
        typessql_state->other_rc = -1; // reset
    } else
        r = clnt->adapter.next_row ? clnt->adapter.next_row(clnt, stmt) : sqlite3_maybe_step(clnt, stmt);

    return r;
}

#define FUNC_COLUMN_TYPE(ret, type)                                                                                             \
    static ret typessql_column_##type(struct sqlclntstate *clnt,                                                                \
                                      sqlite3_stmt *stmt, int iCol)                                                             \
    {                                                                                                                           \
        typessql_t *typessql_state = clnt->typessql_state;                                                                 \
        if (typessql_state->current_row) {                                                                                      \
            row_t *row = typessql_state->current_row;                                                                          \
            if (!row->unpacked)                                                                                                 \
                row->unpacked = sqlite3UnpackedResult(                                                                          \
                    stmt, typessql_state->ncols, row->packed, row->row_size);                                                   \
            Vdbe *pVm = (Vdbe *)stmt;                                                                                           \
            row->unpacked[iCol].tz = pVm->tzname;                                                                               \
            return sqlite3_value_##type(&row->unpacked[iCol]);                                                                  \
        }                                                                                                                       \
        return clnt->adapter.column_##type ? clnt->adapter.column_##type(clnt, stmt, iCol) : sqlite3_column_##type(stmt, iCol); \
    }

FUNC_COLUMN_TYPE(sqlite_int64, int64)
FUNC_COLUMN_TYPE(double, double)
FUNC_COLUMN_TYPE(int, bytes)
FUNC_COLUMN_TYPE(const unsigned char *, text)
FUNC_COLUMN_TYPE(const void *, blob)
FUNC_COLUMN_TYPE(const dttz_t *, datetime)

static const intv_t *typessql_column_interval(struct sqlclntstate *clnt,
                                              sqlite3_stmt *stmt, int iCol,
                                              int type)
{
    typessql_t *typessql_state = clnt->typessql_state;
    if (typessql_state->current_row) {
        row_t *row = typessql_state->current_row;
        if (!row->unpacked)
            row->unpacked = sqlite3UnpackedResult(
                stmt, typessql_state->ncols, row->packed, row->row_size);
        Vdbe *pVm = (Vdbe *)stmt;
        row->unpacked[iCol].tz = pVm->tzname;
        return sqlite3_value_interval(&row->unpacked[iCol], type);
    }
    return clnt->adapter.column_interval ? clnt->adapter.column_interval(clnt, stmt, iCol, type) : sqlite3_column_interval(stmt, iCol, type);
}

/*
TODO:
Need to update that one spot that also calls sql functions. Check if any other spots (don't think any other spots)
handle_fdb_push/dohsql calls client reset. Might call reset on typessql. Change where set and reset
 - Inserting from generate_series causes adapter to set to self. Maybe this is also using dohsql?

If type is null check if sqlite3_column_decltype gets you a type (should)
 - Declared type won't be called until already gone through 1000 records for typessql
Add column_types non null function maybe (nah)
Maybe change validate to use cache? (shouldn't matter)
Check if freeing stuff correctly, like packed and unpacked memory. Look into dohsql (should be)
*/
void _master_clnt_set(struct sqlclntstate *clnt, struct plugin_callbacks *adapter)
{
    if (adapter->next_row == typessql_next_row) {
        printf("ATTEMPTING TO SET ADAPTER TO SELF\n");
        return;
        // abort();
    }

    clnt->backup = clnt->plugin;
    clnt->adapter_backup = clnt->adapter;

    clnt->adapter = *adapter;
    clnt->plugin = clnt->adapter;

    clnt->plugin.column_count = typessql_column_count;
    clnt->plugin.next_row = typessql_next_row;
    clnt->plugin.column_type = typessql_column_type;
    clnt->plugin.column_int64 = typessql_column_int64;
    clnt->plugin.column_double = typessql_column_double;
    clnt->plugin.column_text = typessql_column_text;
    clnt->plugin.column_bytes = typessql_column_bytes;
    clnt->plugin.column_blob = typessql_column_blob;
    clnt->plugin.column_datetime = typessql_column_datetime;
    clnt->plugin.column_interval = typessql_column_interval;
    // clnt->plugin.sqlite_error = clnt->adapter.sqlite_error;
    // clnt->plugin.param_count = clnt->adapter.param_count;
    // clnt->plugin.param_value = clnt->adapter.param_value;
    // clnt->plugin.param_index = clnt->adapter.param_index;
}
