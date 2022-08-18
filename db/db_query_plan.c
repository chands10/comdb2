/*
   Copyright 2022 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <cson.h>
#include "sql.h"
#include "bdb_api.h"

hash_t *gbl_query_plan_hash = NULL;
hash_t *gbl_query_plan_hash_to_add = NULL; // items that need to be added to llmeta
pthread_mutex_t gbl_query_plan_hash_mu = PTHREAD_MUTEX_INITIALIZER;
int gbl_query_plan_max_queries = 1000;
extern int gbl_debug_print_query_plans;
extern double gbl_query_plan_percentage;

static char *form_query_plan(const struct client_query_stats *query_stats) {
    struct strbuf *query_plan_buf;
    const struct client_query_path_component *c;
    char *query_plan;

    if (query_stats->n_components == 0) {
        return NULL;
    }

    query_plan_buf = strbuf_new();
    for (int i = 0; i < query_stats->n_components; i++) {
        if (i > 0) {
            strbuf_append(query_plan_buf, ", ");
        }
        c = &query_stats->path_stats[i];
        strbuf_appendf(query_plan_buf, "table %s index %d", c->table, c->ix);
    }

    query_plan = strdup((char *)strbuf_buf(query_plan_buf));
    strbuf_free(query_plan_buf);
    return query_plan;
}

/* if nexecutions is present then we are updating from llmeta, else nexecutions is just incremented by 1
 * Assume already have lock on hash
 */
static void add_query_plan_int_int(const char *query, const char *query_plan, hash_t *query_plan_hash, int *alert_once_query, double current_cost_per_row, const int *nexecutions, int compare) {
    struct query_plan_item *q = hash_find(query_plan_hash, &query_plan);
    if (q == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(query_plan_hash);
        if (nents >= gbl_query_plan_max_queries) {
            if (*alert_once_query) {
                logmsg(LOGMSG_WARN,
                       "Stopped tracking query plans for query %s, hit max #plans %d.\n",
                       query, gbl_query_plan_max_queries);
                *alert_once_query = 0;
            }
            return;
        } else {
            q = calloc(1, sizeof(struct query_plan_item));
            q->plan = strdup(query_plan);
            q->total_cost_per_row = current_cost_per_row;
            q->nexecutions = nexecutions ? *nexecutions : 1;
            hash_add(query_plan_hash, q);
        }
    } else {
        assert(!nexecutions); // if updating from llmeta then should only be added once
        q->total_cost_per_row += current_cost_per_row;
        q->nexecutions++;
    }

    if (compare) { // compare query plans
        double average_cost_per_row = q->total_cost_per_row / q->nexecutions;
        void *ent;
        unsigned int bkt;
        double alt_avg;
        double significance = 1 + gbl_query_plan_percentage / 100;
        for (q = (struct query_plan_item*)hash_first(query_plan_hash, &ent, &bkt); q; q = (struct query_plan_item*)hash_next(query_plan_hash, &ent, &bkt)) {
            alt_avg = q->total_cost_per_row / q->nexecutions;
            if (alt_avg * significance < average_cost_per_row) { // should be at least equal if same query plan
                logmsg(LOGMSG_WARN, "For query %s:\n"
                "Currently using query plan %s, which has an average cost per row of %f.\n"
                "But query plan %s has a lower average cost per row of %f.\n", query, query_plan, average_cost_per_row, q->plan, alt_avg);
            }
        }
    }
}

/* if nexecutions is present then we are updating from llmeta, else nexecutions is just incremented by 1 in add_query_plan_inner_inner
 * Assume already have lock on hash
 * to_add is 1 when using gbl_query_plan_hash_to_add
*/
static void add_query_plan_int(const char *query, const char *query_plan, double current_cost_per_row, const int *nexecutions, int to_add) {
    if (gbl_query_plan_hash == NULL) {
        bdb_del_query_plan_cson(); // for testing
        gbl_query_plan_hash = hash_init_strptr(0);
    }
    if (gbl_query_plan_hash_to_add == NULL) {
        gbl_query_plan_hash_to_add = hash_init_strptr(0);
    }

    hash_t *query_hash = to_add ? gbl_query_plan_hash_to_add : gbl_query_plan_hash;
    int compare = !(to_add || nexecutions);
    struct query_item *q = hash_find(query_hash, &query);
    if (q == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(query_hash);
        if (nents >= gbl_query_plan_max_queries) {
            static int alert_once = 1;
            if (alert_once) {
                logmsg(LOGMSG_WARN,
                       "Stopped tracking new queries in query plan, hit max #queries %d.\n",
                       gbl_query_plan_max_queries);
                alert_once = 0;
            }
        } else {
            q = calloc(1, sizeof(struct query_item));
            q->query = strdup(query);
            q->query_plan_hash = hash_init_strptr(0);
            q->alert_once_query = 1;
            add_query_plan_int_int(query, query_plan, q->query_plan_hash, &q->alert_once_query, current_cost_per_row, nexecutions, compare);
            hash_add(query_hash, q);
        }
    } else {
        add_query_plan_int_int(query, query_plan, q->query_plan_hash, &q->alert_once_query, current_cost_per_row, nexecutions, compare);
    }

    if (gbl_debug_print_query_plans) {
        void *ent, *ent2;
        unsigned int bkt, bkt2;
        struct query_plan_item *p;
        logmsg(LOGMSG_WARN, "START\n");
        for (q = (struct query_item*)hash_first(query_hash, &ent, &bkt); q; q = (struct query_item*)hash_next(query_hash, &ent, &bkt)) {
            logmsg(LOGMSG_WARN, "QUERY: %s\n", q->query);
            for (p = (struct query_plan_item*)hash_first(q->query_plan_hash, &ent2, &bkt2); p; p = (struct query_plan_item*)hash_next(q->query_plan_hash, &ent2, &bkt2)) {
                logmsg(LOGMSG_WARN, "plan: %s, total cost per row: %f, num executions: %d, average: %f\n", p->plan, p->total_cost_per_row, p->nexecutions, p->total_cost_per_row / p->nexecutions);
            }
        }
        logmsg(LOGMSG_WARN, "END\n\n");
    }
}

void add_query_plan(const struct client_query_stats *query_stats, int rows, const char *query) {
    char *query_plan = form_query_plan(query_stats);
    if (!query_plan || rows <= 0) { // can't calculate cost per row if 0 rows
        return;
    }

    double current_cost_per_row = query_stats->cost / rows;
    Pthread_mutex_lock(&gbl_query_plan_hash_mu);
    add_query_plan_int(query, query_plan, current_cost_per_row, NULL, 0);
    add_query_plan_int(query, query_plan, current_cost_per_row, NULL, 1);
    Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
    free(query_plan);
}

void clear_query_plans(int *queries_count, int *plans_count, int to_add) {
    hash_t *query_hash = to_add ? gbl_query_plan_hash_to_add : gbl_query_plan_hash;
    Pthread_mutex_lock(&gbl_query_plan_hash_mu);
    if (query_hash == NULL) {
        Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
        if (queries_count)
            *queries_count = 0;
        if (plans_count)
            *plans_count = 0;
        return;
    }

    // update queries count if not null
    hash_info(query_hash, NULL, NULL, NULL, NULL, queries_count, NULL, NULL);

    void *ent, *ent2;
    unsigned int bkt, bkt2;
    struct query_item *q;
    struct query_plan_item *p;
    int plans_count_tmp = 0;
    int current_query_num_plans;
    for (q = (struct query_item*)hash_first(query_hash, &ent, &bkt); q; q = (struct query_item*)hash_next(query_hash, &ent, &bkt)) {
        // update plans count
        hash_info(q->query_plan_hash, NULL, NULL, NULL, NULL, &current_query_num_plans, NULL, NULL);
        plans_count_tmp += current_query_num_plans;

        // free query plan hash
        for (p = (struct query_plan_item*)hash_first(q->query_plan_hash, &ent2, &bkt2); p; p = (struct query_plan_item*)hash_next(q->query_plan_hash, &ent2, &bkt2)) {
            free(p->plan);
            free(p);
        }
        hash_clear(q->query_plan_hash);
        hash_free(q->query_plan_hash);

        free(q->query);
        free(q);
    }
    hash_clear(query_hash);
    hash_free(query_hash);
    if (to_add)
        gbl_query_plan_hash_to_add = NULL;
    else
        gbl_query_plan_hash = NULL;

    Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
    if (plans_count)
        *plans_count = plans_count_tmp;
    return;
}

// reset hash and place latest query stats from llmeta into hash
static int update_hash_from_llmeta() {
    clear_query_plans(NULL, NULL, 0);

    char *blob = NULL;
    int len;
    int rc = bdb_get_query_plan_cson(&blob, &len);
    assert(rc == 0 || (rc == 1 && blob == NULL));

    if (blob == NULL) // could be fine?
        return 1;

    cson_value *rootV = NULL;
    cson_object *rootObj = NULL;

    rc = cson_parse_string(&rootV, blob, len);

    if (0 != rc) {
       logmsg(LOGMSG_ERROR, "update_hash_from_llmeta:Error code %d (%s)!\n", rc,
               cson_rc_string(rc));
        free(blob);
        return 1;
    }

    assert(cson_value_is_object(rootV));
    rootObj = cson_value_get_object(rootV);
    assert(rootObj != NULL);

    cson_object_iterator query_iter, query_plan_iter;
    rc = cson_object_iter_init(rootObj, &query_iter);
    if (0 != rc) {
        printf("Error code %d (%s)!\n", rc, cson_rc_string(rc));
        rc = 1;
        goto out;
    }

    cson_kvp *kvp, *kvp2; // key/value pair
    while ((kvp = cson_object_iter_next(&query_iter))) {
        cson_string const *ckey = cson_kvp_key(kvp);
        const char *query = cson_string_cstr(ckey);
        cson_value *query_planV = cson_kvp_value(kvp);
        assert(cson_value_is_object(query_planV));
        cson_object *query_planObj = cson_value_get_object(query_planV);
        assert(query_planObj != NULL);
        rc = cson_object_iter_init(query_planObj, &query_plan_iter);
        if (0 != rc) {
            printf("Error code %d (%s)!\n", rc, cson_rc_string(rc));
            rc = 1;
            goto out;
        }

        while ((kvp2 = cson_object_iter_next(&query_plan_iter))) {
            cson_string const *ckey2 = cson_kvp_key(kvp2);
            const char *query_plan = cson_string_cstr(ckey2);
            cson_value *statsV = cson_kvp_value(kvp2);
            assert(cson_value_is_object(statsV));
            cson_object *statsObj = cson_value_get_object(statsV);
            assert(statsObj != NULL);
            cson_value *total_cost_per_rowV = cson_object_get(statsObj, "total_cost_per_row");
            cson_value *nexecutionsV = cson_object_get(statsObj, "nexecutions");
            if (total_cost_per_rowV == NULL || nexecutionsV == NULL) {
                rc = 1;
                goto out;
            }
            double total_cost_per_row = cson_value_get_double(total_cost_per_rowV);
            int nexecutions = cson_value_get_integer(nexecutionsV);
            Pthread_mutex_lock(&gbl_query_plan_hash_mu);
            add_query_plan_int(query, query_plan, total_cost_per_row, &nexecutions, 0);
            Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
        }
    }

out:
    cson_value_free(rootV);
    free(blob);
    return rc;
}

// caller is responsible for memory in *ret
static int convert_global_hash_to_cson(char **ret) {
    Pthread_mutex_lock(&gbl_query_plan_hash_mu);
    if (gbl_query_plan_hash == NULL) {
        Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
        *ret = strdup("{}");
        return 0;
    }

    // Create a rootV object:
    cson_value *rootV = cson_value_new_object();
    cson_object *rootObj = cson_value_get_object(rootV);

    void *ent, *ent2;
    unsigned int bkt, bkt2;
    struct query_item *q;
    struct query_plan_item *p;
    cson_value *query_planV = NULL;
    cson_object *query_planObj = NULL;
    cson_value *statsV = NULL;
    cson_object *statsObj = NULL;

    for (q = (struct query_item*)hash_first(gbl_query_plan_hash, &ent, &bkt); q; q = (struct query_item*)hash_next(gbl_query_plan_hash, &ent, &bkt)) {
        query_planV = cson_value_new_object();
        query_planObj = cson_value_get_object(query_planV);
        for (p = (struct query_plan_item*)hash_first(q->query_plan_hash, &ent2, &bkt2); p; p = (struct query_plan_item*)hash_next(q->query_plan_hash, &ent2, &bkt2)) {
            statsV = cson_value_new_object();
            statsObj = cson_value_get_object(statsV);
            cson_object_set(statsObj, "total_cost_per_row", cson_value_new_double(p->total_cost_per_row));
            cson_object_set(statsObj, "nexecutions", cson_value_new_integer(p->nexecutions));
            cson_object_set(query_planObj, p->plan, statsV);
        }
        cson_object_set(rootObj, q->query, query_planV);
    }

    Pthread_mutex_unlock(&gbl_query_plan_hash_mu);

    cson_buffer buf;
    int rc = cson_output_buffer(rootV, &buf); // write obj to buffer
    if (0 != rc) {
        logmsg(LOGMSG_ERROR, "cson_output_buffer returned rc %d", rc);
    } else {
        // JSON data is the first (buf.used) bytes of (buf.mem).
        *ret = strndup((const char *)buf.mem, buf.used);
    }

    // Clean up
    cson_value_free(rootV);
    return rc;
}

static int update_llmeta() {
    // TODO: Should be in one lock to prevent changes
    int rc = update_hash_from_llmeta(); // grab the latest llmeta
    if (rc != 0) {
        return rc;
    }

    // TODO: add changes

    char *query_plan_cson = NULL;
    rc = convert_global_hash_to_cson(&query_plan_cson);
    if (rc != 0) {
        return rc;
    }

    assert(query_plan_cson);
    int len = strlen(query_plan_cson);
    if (len > 2) { // "{}" if len == 2
        bdb_set_query_plan_cson(query_plan_cson, len);
    } else {
        bdb_del_query_plan_cson();
    }

    free(query_plan_cson);
    clear_query_plans(NULL, NULL, 1);

    return rc;
}
