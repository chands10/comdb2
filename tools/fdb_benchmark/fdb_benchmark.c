#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <libgen.h>
#include <unistd.h>

#include <cdb2api.h>

#define DBNAME_LEN 64
#define TYPE_LEN 64
#define POLICY_LEN 24
#define MAX_NODES 128
#define CDB2HOSTNAME_LEN 128
#define MAX_STACK 512 /* Size of call-stack which opened the handle */


static void *check_hostname_and_port_cb(cdb2_hndl_tp *hndl, void *user_arg, int argc, void **argv)
{
    if (argv[0] == NULL) {
        fprintf(stderr, "host name is null???\n");
        abort();
    }
    if ((int)(intptr_t)argv[1] == -1) {
        fprintf(stderr, "port number is invalid???\n");
        abort();
    }
    return NULL;
}

static int set_ssl_mode_require(cdb2_hndl_tp *hndl)
{
    int rc = cdb2_run_statement(hndl, "SET SSL_MODE REQUIRE");
    if (rc) {
        fprintf(stderr, "Error running set stmt %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    rc = cdb2_run_statement(hndl, "SELECT 1");
    if (rc) {
        fprintf(stderr, "Error running select 1 %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: Expected record %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Expected done %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    return 0;
}

static int enable_fdbpr(cdb2_hndl_tp *hndl)
{
    int rc = cdb2_run_statement(hndl, "put tunable foreign_db_push_remote 1");
    if (rc) {
        fprintf(stderr, "Error setting tunable foreign_db_push_remote %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_run_statement(hndl, "put tunable foreign_db_push_redirect 1");
    if (rc) {
        fprintf(stderr, "Error setting tunable foreign_db_push_redirect %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    sleep(1);

    // make sure tunable is on
    rc = cdb2_run_statement(hndl, "select value from comdb2_tunables where name in ('foreign_db_push_remote', 'foreign_db_push_redirect')");
    if (rc) {
        fprintf(stderr, "Error running query %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: Expected record %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    char *value = (char *)cdb2_column_value(hndl, 0);
    if (strcmp(value, "ON") != 0) {
        fprintf(stderr, "Expected tunable ON, got %s\n", value);
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        fprintf(stderr, "%s: Expected record %d %s\n", __func__, rc, cdb2_errstr(hndl));
        return -1;
    }
    value = (char *)cdb2_column_value(hndl, 0);
    if (strcmp(value, "ON") != 0) {
        fprintf(stderr, "Expected tunable ON, got %s\n", value);
        return -1;
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK_DONE) {
        fprintf(stderr, "Expected done %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    return 0;
}

static int disable_fdbpr(cdb2_hndl_tp *hndl)
{
    int rc = cdb2_run_statement(hndl, "put tunable foreign_db_push_remote 0");
    if (rc) {
        fprintf(stderr, "Error resetting tunable foreign_db_push_remote %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_run_statement(hndl, "put tunable foreign_db_push_redirect 0");
    if (rc) {
        fprintf(stderr, "Error resetting tunable foreign_db_push_redirect %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    return 0;
}

static int create_table(cdb2_hndl_tp *hndl)
{
    int rc = cdb2_run_statement(hndl, "drop table if exists fdbpr");
    if (rc) {
        fprintf(stderr, "Error dropping table %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    rc = cdb2_run_statement(hndl, "create table fdbpr { schema { int i null=yes datetime d null=yes }}");
    if (rc) {
        fprintf(stderr, "Error creating table %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_run_statement(hndl, "insert into fdbpr(i) values (10), (20)");
    if (rc) {
        fprintf(stderr, "Error inserting %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }
    rc = cdb2_run_statement(hndl, "insert into fdbpr(d) values ('20230913T'), ('20230914T')");
    if (rc) {
        fprintf(stderr, "Error inserting %d %s\n", rc, cdb2_errstr(hndl));
        return -1;
    }

    return 0;
}

/* Verify that fdb hndl inherits parent settings. Also test cdb2_errstr */
int ssl_test()
{
    int rc;
    // create fdb table
    cdb2_hndl_tp *cdb2h_fdb = NULL;
    rc = cdb2_open(&cdb2h_fdb, "basic45538", "local", 0);
    if (rc != 0) {
        fprintf(stderr, "error connecting to %s\n", "basic45538");
        return rc;
    }
    if (create_table(cdb2h_fdb))
        return -1;

    cdb2_close(cdb2h_fdb);

    cdb2_hndl_tp *hndl = NULL;
    rc = cdb2_open(&hndl, "basic30284", "local", 0); // ssl
    
    if (rc != 0) {
        fprintf(stderr, "error connecting to %s\n", "basic30284");
        return rc;
    }

    if (set_ssl_mode_require(hndl))
        return -1;
    
    if (enable_fdbpr(hndl))
        return -1;

    rc = cdb2_run_statement(hndl, "select d from local_basic45538.fdbpr");
    if (0) { // !hndl->fdb_hndl) {
        fprintf(stderr, "%s: error didn't get fdb hndl, got rc %d\n", __func__, rc);
        return -1;
    }
    if (rc == 0 || strcmp(cdb2_errstr(hndl), "The database does not support SSL.") != 0) {
        fprintf(stderr, "%s %d isn't supposed to succeed. %d %s\n", __func__, __LINE__, rc, cdb2_errstr(hndl));
        return -1;
    }

    if (disable_fdbpr(hndl))
        return -1;

    cdb2_close(hndl);
    unsetenv("SSL_MODE");

    return 0;
}


int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);

    if (ssl_test() != 0)
        return -1;

    printf("%s - pass\n", basename(argv[0]));
    return 0;
}
