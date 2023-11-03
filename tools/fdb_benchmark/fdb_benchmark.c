#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>

#include <cdb2api.h>

#define DBDO(db, expr, what, shouldberc, fatal) do {int _rc = expr; if (_rc != shouldberc) {     fprintf(stderr, "%s: expected %d got %d %s\n", what, shouldberc, _rc, db ? cdb2_errstr(db) : "???");     if (fatal)         exit(1); } } while (0)

void open_db(cdb2_hndl_tp **db_ref) {
    cdb2_hndl_tp *db = *db_ref;
    DBDO(db, cdb2_open(db_ref, "testdb", "dev-10-34-22-66", CDB2_DIRECT_CPU), "open", CDB2_OK, 1);
}

void close_db(cdb2_hndl_tp *db) {
    DBDO(db, cdb2_close(db), "run", CDB2_OK, 1);
}

void run_query(cdb2_hndl_tp **db_ref, int numRecords, int reuse_connection) {
    char query[100];
    sprintf(query, "select * from local_testdb2.t limit %d;", numRecords);

    if (!reuse_connection) open_db(db_ref);
    cdb2_hndl_tp *db = *db_ref;

    DBDO(db, cdb2_run_statement(db, query), "run", CDB2_OK, 1);
    for (int i = 0; i < numRecords; ++i) {
        DBDO(db, cdb2_next_record(db), "next", CDB2_OK, 1);
    }
    DBDO(db, cdb2_next_record(db), "query done", CDB2_OK_DONE, 1);
    
    if (!reuse_connection) close_db(db);
}

int main(int argc, char *argv[]) {
    struct timeval t1, t2;
    double ms;
    int records[] = {1,10,100,1000,10000,100000};
    int size = sizeof(records) / sizeof(records[0]);
    printf("Testing with %d records\n", size);

    cdb2_hndl_tp *db;

    // populate table
    // cdb2_hndl_tp *fdb;
    // DBDO(fdb, cdb2_open(&fdb, "testdb2", "local", 0), "open", CDB2_OK, 1);
    // DBDO(fdb, cdb2_run_statement(fdb, "drop table if exists t;"), "run", CDB2_OK, 1);
    // DBDO(fdb, cdb2_run_statement(fdb, "create table t(a int);"), "run", CDB2_OK, 1);
    // DBDO(fdb, cdb2_run_statement(fdb, "insert into t select * from generate_series(1, 50000);"), "run", CDB2_OK, 1);
    // DBDO(fdb, cdb2_run_statement(fdb, "insert into t select * from generate_series(50001, 100000);"), "run", CDB2_OK, 1);
    // close_db(fdb);

    for (int new_fdb = 0; new_fdb < 2; new_fdb++) {
        open_db(&db);
        if (new_fdb == 0) {
            DBDO(db, cdb2_run_statement(db, "put tunable foreign_db_push_redirect 0"), "run", CDB2_OK, 1);
            DBDO(db, cdb2_next_record(db), "put tunable off done", CDB2_OK_DONE, 1);
        } else {
            DBDO(db, cdb2_run_statement(db, "put tunable foreign_db_push_redirect 1"), "run", CDB2_OK, 1);
            DBDO(db, cdb2_next_record(db), "put tunable on done", CDB2_OK_DONE, 1);
            DBDO(db, cdb2_run_statement(db, "put tunable foreign_db_push_remote 1"), "run", CDB2_OK, 1);
            DBDO(db, cdb2_next_record(db), "put tunable on done", CDB2_OK_DONE, 1);
        }
        close_db(db);
        for (int reuse_connection = 0; reuse_connection < 2; ++reuse_connection) {
            if (reuse_connection) open_db(&db);
            for (int r = 0; r < size; ++r) { // num records in query
                int numRecords = records[r];
                gettimeofday(&t1, 0);
                run_query(&db, numRecords, reuse_connection);
                gettimeofday(&t2, 0);
                ms = (t2.tv_sec - t1.tv_sec + (t2.tv_usec - t1.tv_usec) * 1e-6) * 1000;
                printf("new fdb = %d, reuse connection = %d, num records = %d, time = %f\n",
                        new_fdb, reuse_connection, numRecords, ms);
            }
            if (reuse_connection) close_db(db);
        }
    }
    
}
