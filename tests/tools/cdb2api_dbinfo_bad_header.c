/*
 * cdb2_dbinfo_query() must reject a reply that isn't a sane dbinfo response
 * before allocating anything based on its length. Stray bytes on a pooled
 * socket read as a header can otherwise ask for an enormous allocation, which
 * aborts the client under allocators that abort on failure.
 *
 * A fake comdb2db runs in a background thread. With allow_pmux_route on, every
 * connection goes through it: it accepts "rte", answers dbinfo with a bad
 * header, and closes the connection on anything else.
 */
#include <arpa/inet.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cdb2api.h>
#include <cdb2api_test.h>
#include <sqlquery.pb-c.h>
#include <sqlresponse.pb-c.h>

#define API_CALL_TIMEOUT_MS 2000
#define SLACK_MS 1000
#define GIVE_UP_SECS 60

struct newsqlheader {
    int type;
    int compression;
    int state;
    int length;
};

enum bad_header {
    GARBAGE,    /* random type, negative length */
    TOO_LONG,   /* right type, implausibly large length */
    WRONG_TYPE, /* well-formed dbinfo payload under the wrong header type */
};

static const char *bad_header_str[] = {"garbage", "too long", "wrong type"};

static enum bad_header mode;

static int64_t epochms(void)
{
    struct timeval now;
    gettimeofday(&now, NULL);
    return (int64_t)now.tv_sec * 1000 + now.tv_usec / 1000;
}

static int read_full(int fd, void *buf, size_t len)
{
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0)
            return -1;
        p += n;
        len -= n;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0)
            return -1;
        p += n;
        len -= n;
    }
    return 0;
}

static int read_line(int fd, char *buf, size_t len)
{
    size_t i = 0;
    while (i < len - 1) {
        if (read(fd, &buf[i], 1) != 1)
            return -1;
        if (buf[i] == '\n')
            break;
        ++i;
    }
    buf[i] = '\0';
    return 0;
}

static int send_bad_dbinfo_response(int fd, int port)
{
    CDB2DBINFORESPONSE__Nodeinfo node = CDB2__DBINFORESPONSE__NODEINFO__INIT;
    node.name = "localhost";
    node.number = 1;
    node.has_port = 1;
    node.port = port;
    CDB2DBINFORESPONSE__Nodeinfo *nodes[] = {&node};

    CDB2DBINFORESPONSE response = CDB2__DBINFORESPONSE__INIT;
    response.master = &node;
    response.n_nodes = 1;
    response.nodes = nodes;

    size_t len = cdb2__dbinforesponse__get_packed_size(&response);
    unsigned char *buf = malloc(len);
    cdb2__dbinforesponse__pack(&response, buf);

    struct newsqlheader hdr = {0};
    switch (mode) {
    case GARBAGE:
        hdr.type = htonl(0x17030300);
        hdr.length = htonl(0xdeadbeef);
        break;
    case TOO_LONG:
        hdr.type = htonl(RESPONSE_HEADER__DBINFO_RESPONSE);
        hdr.length = htonl(0x7fffffff);
        break;
    case WRONG_TYPE:
        hdr.type = htonl(RESPONSE_HEADER__SQL_RESPONSE);
        hdr.length = htonl(len);
        break;
    }
    int rc = write_full(fd, &hdr, sizeof(hdr)) || write_full(fd, buf, len);
    free(buf);
    return rc;
}

struct conn {
    int fd;
    int port;
};

static void *serve_connection(void *arg)
{
    struct conn *c = arg;
    char line[256];

    /* cdb2portmux_route(): "rte comdb2/replication/<dbname>" */
    if (read_line(c->fd, line, sizeof(line)) != 0 || strncmp(line, "rte ", 4) != 0)
        goto out;
    if (write_full(c->fd, "0\n", 2) != 0)
        goto out;
    /* "newsql" or "@newsql" */
    if (read_line(c->fd, line, sizeof(line)) != 0)
        goto out;

    struct newsqlheader hdr;
    if (read_full(c->fd, &hdr, sizeof(hdr)) != 0)
        goto out;
    int type = ntohl(hdr.type);
    int len = ntohl(hdr.length);
    unsigned char *buf = NULL;
    if (len > 0) {
        buf = malloc(len);
        if (read_full(c->fd, buf, len) != 0) {
            free(buf);
            goto out;
        }
    }
    CDB2QUERY *query = type == CDB2_REQUEST_TYPE__CDB2QUERY ? cdb2__query__unpack(NULL, len, buf) : NULL;
    free(buf);
    int is_dbinfo = query && query->dbinfo;
    if (query)
        cdb2__query__free_unpacked(query, NULL);
    if (is_dbinfo)
        send_bad_dbinfo_response(c->fd, c->port);

out:
    close(c->fd);
    free(c);
    return NULL;
}

static void *fake_comdb2db(void *arg)
{
    int lfd = *(int *)arg;
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    getsockname(lfd, (struct sockaddr *)&addr, &addrlen);
    while (1) {
        int fd = accept(lfd, NULL, NULL);
        if (fd < 0)
            continue;
        struct conn *c = malloc(sizeof(*c));
        c->fd = fd;
        c->port = ntohs(addr.sin_port);
        pthread_t t;
        pthread_create(&t, NULL, serve_connection, c);
        pthread_detach(t);
    }
    return NULL;
}

static void give_up(int sig)
{
    static const char msg[] = "cdb2_open() did not return - fail\n";
    (void)sig;
    ssize_t n = write(2, msg, sizeof(msg) - 1);
    (void)n;
    _exit(1);
}

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    signal(SIGPIPE, SIG_IGN);
    signal(SIGALRM, give_up);

    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int on = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    struct sockaddr_in addr = {.sin_family = AF_INET, .sin_addr.s_addr = htonl(INADDR_LOOPBACK)};
    socklen_t addrlen = sizeof(addr);
    if (bind(lfd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(lfd, 16) != 0 ||
        getsockname(lfd, (struct sockaddr *)&addr, &addrlen) != 0) {
        perror("fake comdb2db");
        return 1;
    }
    int port = ntohs(addr.sin_port);
    pthread_t t;
    pthread_create(&t, NULL, fake_comdb2db, &lfd);

    char cfg[] = "/tmp/cdb2api_dbinfo_bad_header.XXXXXX";
    int cfd = mkstemp(cfg);
    FILE *f = fdopen(cfd, "w");
    fprintf(f,
            "comdb2db localhost\n"
            "comdb2_config:default_type=dev\n"
            "comdb2_config:portmuxport=%d\n"
            "comdb2_config:allow_pmux_route=on\n"
            "comdb2_config:api_call_timeout=%d\n"
            "comdb2_config:enforce_api_call_timeout=on\n"
            "comdb2_feature:use_bmsd=0\n",
            port, API_CALL_TIMEOUT_MS);
    fclose(f);

    cdb2_disable_sockpool();
    cdb2_set_comdb2db_config(cfg);

    int failed = 0;
    for (mode = GARBAGE; mode <= WRONG_TYPE; ++mode) {
        int rejects_before = get_num_dbinfo_bad_header();
        alarm(GIVE_UP_SECS);
        cdb2_hndl_tp *hndl = NULL;
        int64_t start = epochms();
        int rc = cdb2_open(&hndl, "fakedb", "dev", 0);
        int64_t elapsed = epochms() - start;
        alarm(0);
        int rejects = get_num_dbinfo_bad_header() - rejects_before;
        printf("%s: cdb2_open rc %d in %" PRId64 "ms, %d bad headers rejected: %s\n", bad_header_str[mode], rc,
               elapsed, rejects, cdb2_errstr(hndl));
        cdb2_close(hndl);
        if (rc == 0) {
            fprintf(stderr, "%s: cdb2_open succeeded on a bad dbinfo header - fail\n", bad_header_str[mode]);
            failed = 1;
        }
        if (rejects == 0) {
            fprintf(stderr, "%s: bad dbinfo header was not rejected - fail\n", bad_header_str[mode]);
            failed = 1;
        }
        if (elapsed > API_CALL_TIMEOUT_MS + SLACK_MS) {
            fprintf(stderr, "%s: cdb2_open took %" PRId64 "ms, expected at most %dms - fail\n", bad_header_str[mode],
                    elapsed, API_CALL_TIMEOUT_MS + SLACK_MS);
            failed = 1;
        }
    }
    unlink(cfg);
    return failed;
}
