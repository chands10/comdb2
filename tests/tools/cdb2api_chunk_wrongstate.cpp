/*
 * Repro for the blocked-write fallout during a chunk transaction.
 *
 * Same workload as cdb2api_chunk, but this one behaves like a real
 * application: when a statement inside the transaction fails, it issues a
 * rollback to clean up.
 *
 * With check-hb-on-blocked-write disabled the insert fails with
 * CDB2ERR_TRAN_IO_ERROR "Database disconnected while in transaction." after
 * the api has already reconnected.  retry_queries() returns without clearing
 * hndl->in_trans, so the handle still believes it is in a transaction while
 * the new connection has never seen a 'begin'.  The rollback below therefore
 * sails past the api's (is_commit && !in_trans) guard and lands on the server,
 * which logs:
 *
 *   sqlengine entering wrong state from state 0 ...
 *   handle_sql_wrongstate: api should have blocked this
 */
#include <cstdio>
#include <cstdlib>
#include <cdb2api.h>

static cdb2_hndl_tp *hndl;
static int num_heartbeats = 0;

static void *on_heartbeat(cdb2_hndl_tp *db, void *dummy0, int dummy1, void **dummy2)
{
    ++num_heartbeats;
    return NULL;
}

static int setup(const char *sql)
{
    int rc = cdb2_run_statement(hndl, sql);
    if (rc != 0)
        fprintf(stderr, "setup '%s' rc:%d %s\n", sql, rc, cdb2_errstr(hndl));
    return rc;
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        fprintf(stderr, "usage: %s <dbname> <tier> <total>\n", argv[0]);
        return EXIT_FAILURE;
    }
    char *dbname = argv[1];
    char *tier = argv[2];
    int total = atoi(argv[3]);
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    if (cdb2_open(&hndl, dbname, tier, 0) != 0) {
        fprintf(stderr, "cdb2_open failed\n");
        return EXIT_FAILURE;
    }
    cdb2_register_event(hndl, CDB2_AT_RECEIVE_HEARTBEAT, (cdb2_event_ctrl)0, on_heartbeat, NULL, 0);

    if (setup("drop table if exists chunk") != 0) return EXIT_FAILURE;
    if (setup("create table chunk(i longlong primary key, hello cstring(64) "
              "default('hello, world!') index, d datetime default(now()) index)") != 0) return EXIT_FAILURE;
    if (setup("set transaction chunk 50") != 0) return EXIT_FAILURE;
    if (setup("begin") != 0) return EXIT_FAILURE;

    int i;
    cdb2_bind_param(hndl, "i", CDB2_INTEGER, &i, sizeof(i));

    int failed_at = -1, insert_rc = 0;
    for (i = 0; i < total; ++i) {
        insert_rc = cdb2_run_statement(hndl, "insert into chunk(i) values(@i)");
        if (insert_rc != 0) {
            failed_at = i;
            break;
        }
    }

    if (failed_at < 0) {
        int rc = cdb2_run_statement(hndl, "commit");
        printf("NOREPRO: %d inserts ok, commit rc:%d heartbeats:%d\n", total, rc, num_heartbeats);
        cdb2_clearbindings(hndl);
        cdb2_close(hndl);
        return EXIT_SUCCESS;
    }

    printf("INSERT-FAILED row:%d rc:%d errstr:'%s' heartbeats:%d\n", failed_at, insert_rc,
           cdb2_errstr(hndl), num_heartbeats);

    /* What a real application does next. */
    int rb = cdb2_run_statement(hndl, "rollback");
    printf("ROLLBACK rc:%d errstr:'%s'\n", rb, cdb2_errstr(hndl));

    if (rb == CDB2ERR_BADSTATE)
        printf("VERDICT: api blocked the rollback locally (no server round-trip)\n");
    else
        printf("VERDICT: rollback was sent to the server\n");

    cdb2_clearbindings(hndl);
    cdb2_close(hndl);
    return EXIT_SUCCESS;
}
