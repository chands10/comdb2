#undef NDEBUG
#include <assert.h>
#include <cstdio>
#include <libgen.h>
#include <signal.h>

#include <cdb2api.h>
#include <cdb2api_test.h>

#define tier "default"

static void run(char *db, bool no_fail)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    rc = cdb2_open(&hndl, db, tier, 0);
    assert(rc == 0);

    if (!no_fail) {
        set_fail_sockpool(-1); /* always fail */
        set_fail_sb(1);
    }

    rc = cdb2_run_statement(hndl, "select * from sqlite_master where null");
    assert(rc == 0);
    
    if (!no_fail) {
        set_fail_sockpool(0);
    }
    
    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK_DONE);

    cdb2_effects_tp effects;
    rc = cdb2_get_effects(hndl, &effects);
    assert(rc == 0);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}

static void rtcpu_comdb2db_and_no_sockpool(char *db)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;

    set_fail_sockpool(-1); /* always fail */
    
    int fail = 3;
    set_fail_tcp(fail);
    set_fail_dbhosts_invalid_response(fail);
    set_fail_dbhosts_bad_response(fail);
    set_fail_dbhosts_cant_read_response(fail);
    set_fail_dbinfo_invalid_header(fail);
    set_fail_dbinfo_invalid_response(fail);
    set_fail_dbinfo_no_response(fail);

    rc = cdb2_open(&hndl, db, tier, 0);
    assert(rc == 0);

    rc = cdb2_run_statement(hndl, "select * from sqlite_master where null");
    assert(rc == 0);

    rc = cdb2_next_record(hndl);
    assert(rc == CDB2_OK_DONE);

    rc = cdb2_close(hndl);
    assert(rc == 0);
}

int main(int argc, char **argv)
{
    signal(SIGPIPE, SIG_IGN);
    int get_dbinfo = 0; /* enabled by default */
    int num_get_dbhosts = 0;
    int num_skip_dbinfo = 0;
    int num_sockpool_fds = 0;
    int num_sql_connects = 0;
    int num_tcp_connects = 0;

    char *db = argv[1];
    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    test_process_env_vars();

    assert(get_dbinfo_state() == get_dbinfo);
    assert(get_num_get_dbhosts() == num_get_dbhosts);
    assert(get_num_skip_dbinfo() == num_skip_dbinfo);
    assert(get_num_sockpool_fd() == num_sockpool_fds);
    assert(get_num_sql_connects() == num_sql_connects);
    assert(get_num_tcp_connects() == num_tcp_connects);


    run(db, true); /* first cdb2_open -- optimization enabled by default */
    assert(get_dbinfo_state() == get_dbinfo);
    assert(get_num_get_dbhosts() == num_get_dbhosts); /* optimization should skip get_dbhosts */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed once */
    assert(get_num_sockpool_fd() == ++num_sockpool_fds); /* get fd once */
    assert(get_num_sql_connects() == num_sql_connects); /* no need to make connection */
    assert(get_num_tcp_connects() == num_tcp_connects); /* sockpool made all the connections */


    run(db, true); /* second cdb2_open -- optimization enabled by default */
    assert(get_dbinfo_state() == get_dbinfo); /* should still use optimization */
    assert(get_num_get_dbhosts() == num_get_dbhosts); /* optimization should skip get_dbhosts */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed once */
    assert(get_num_sockpool_fd() == ++num_sockpool_fds); /* get fd once */
    assert(get_num_sql_connects() == num_sql_connects); /* no need to make connection */
    assert(get_num_tcp_connects() == num_tcp_connects); /* sockpool made all the connections */


    set_fail_read(3); /* test reconnect after repeated bad reads */
    run(db, true);
    assert(get_dbinfo_state() == get_dbinfo); /* should still use optimization */
    assert(get_num_get_dbhosts() == ++num_get_dbhosts); /* reconnect should force us to obtain dbinfo */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed once */
    ++num_sockpool_fds; /* fd from sockpool in cdb2_open from optimization */
    ++num_sockpool_fds; /* fd from sockpool for dbinfo after first failure */
    num_sockpool_fds += 3; /* 3 connections */
    assert(get_num_sockpool_fd() == num_sockpool_fds);
    assert(get_num_sql_connects() == (num_sql_connects += 3));  /* reconnect 3 times */
    assert(get_num_tcp_connects() == num_tcp_connects); /* sockpool made all the connections */


    set_fail_send(1); /* test reconnect after bad write */
    run(db, true);
    assert(get_dbinfo_state() == get_dbinfo); /* should still use optimization */
    assert(get_num_get_dbhosts() == ++num_get_dbhosts); /* reconnect should force us to obtain dbinfo */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed once */
    ++num_sockpool_fds; /* fd from sockpool in cdb2_open from optimization */
    ++num_sockpool_fds; /* fd from sockpool for dbinfo after first failure */
    ++num_sockpool_fds; /* fd from sockpool for query */
    assert(get_num_sockpool_fd() >= num_sockpool_fds);
    num_sockpool_fds = get_num_sockpool_fd();
    assert(get_num_sql_connects() >= ++num_sql_connects); /* reconnect at least once */
    num_sql_connects = get_num_sql_connects();
    assert(get_num_tcp_connects() == num_tcp_connects); /* sockpool made all the connections */


    run(db, true); /* things running normal again */
    assert(get_dbinfo_state() == get_dbinfo); /* should still use optimization */
    assert(get_num_get_dbhosts() == num_get_dbhosts); /* optimization should skip call to get_dbhosts */
    assert(get_num_sockpool_fd() == ++num_sockpool_fds); /* get fd from sockpool */
    assert(get_num_sql_connects() == num_sql_connects); /* no need to make connection */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed */
    assert(get_num_tcp_connects() == num_tcp_connects); /* sockpool made all the connections */


    run(db, false); /* get bad fds from sockpool - api makes tcp connection */
    assert(get_dbinfo_state() == get_dbinfo); /* should still use optimization */
    assert(get_num_get_dbhosts() == ++num_get_dbhosts); /* make dbinfo call */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed */
    assert(get_num_sockpool_fd() == ++num_sockpool_fds); /* get fd from sockpool */
    assert(get_num_sql_connects() >= ++num_sql_connects); /* reconnect at least once */
    num_sql_connects = get_num_sql_connects();
    assert(get_num_tcp_connects() >= ++num_tcp_connects); /* at least one tcp connect */
    num_tcp_connects = get_num_tcp_connects();


    run(db, true); /* things running normal again */
    assert(get_dbinfo_state() == get_dbinfo); /* should still use optimization */
    assert(get_num_get_dbhosts() == num_get_dbhosts); /* optimization should skip call to get_dbhosts */
    assert(get_num_sockpool_fd() == ++num_sockpool_fds); /* get fd from sockpool */
    assert(get_num_sql_connects() == num_sql_connects); /* no need to make connection */
    assert(get_num_skip_dbinfo() == ++num_skip_dbinfo); /* optimization should have executed */
    assert(get_num_tcp_connects() == num_tcp_connects); /* sockpool made all the connections */


    rtcpu_comdb2db_and_no_sockpool(db);

    return 0;
}
