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

#include "sql.h"

hash_t *gbl_query_plan_hash = NULL;
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

static void add_query_plan_int(const char *query, const char *query_plan, hash_t *query_plan_hash, int *alert_once_query, double current_cost_per_row) {
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
            q->nexecutions = 1;
            hash_add(query_plan_hash, q);
        }
    } else {
        q->total_cost_per_row += current_cost_per_row;
        q->nexecutions++;
    }

    // compare query plans
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

void add_query_plan(const struct client_query_stats *query_stats, int rows, const char *query) {
    char *query_plan = form_query_plan(query_stats);
    if (!query_plan || rows <= 0) { // can't calculate cost per row if 0 rows
        return;
    }

    double current_cost_per_row = query_stats->cost / rows;
    Pthread_mutex_lock(&gbl_query_plan_hash_mu);
    if (gbl_query_plan_hash == NULL) {
        gbl_query_plan_hash = hash_init_strptr(0);
    }
    struct query_item *q = hash_find(gbl_query_plan_hash, &query);
    if (q == NULL) {
        /* make sure we haven't generated an unreasonable number of these */
        int nents = hash_get_num_entries(gbl_query_plan_hash);
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
            add_query_plan_int(query, query_plan, q->query_plan_hash, &q->alert_once_query, current_cost_per_row);
            hash_add(gbl_query_plan_hash, q);
        }
    } else {
        add_query_plan_int(query, query_plan, q->query_plan_hash, &q->alert_once_query, current_cost_per_row);
    }

    if (gbl_debug_print_query_plans) {
        void *ent, *ent2;
        unsigned int bkt, bkt2;
        struct query_plan_item *p;
        logmsg(LOGMSG_WARN, "START\n");
        for (q = (struct query_item*)hash_first(gbl_query_plan_hash, &ent, &bkt); q; q = (struct query_item*)hash_next(gbl_query_plan_hash, &ent, &bkt)) {
            logmsg(LOGMSG_WARN, "QUERY: %s\n", q->query);
            for (p = (struct query_plan_item*)hash_first(q->query_plan_hash, &ent2, &bkt2); p; p = (struct query_plan_item*)hash_next(q->query_plan_hash, &ent2, &bkt2)) {
                logmsg(LOGMSG_WARN, "plan: %s, total cost per row: %f, num executions: %d, average: %f\n", p->plan, p->total_cost_per_row, p->nexecutions, p->total_cost_per_row / p->nexecutions);
            }
        }
        logmsg(LOGMSG_WARN, "END\n\n");
    }

    Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
    free(query_plan);
}

void clear_query_plans(int *queries_count, int *plans_count) {
    Pthread_mutex_lock(&gbl_query_plan_hash_mu);
    if (gbl_query_plan_hash == NULL) {
        Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
        if (queries_count)
            *queries_count = 0;
        if (plans_count)
            *plans_count = 0;
        return;
    }

    // update queries count if not null
    hash_info(gbl_query_plan_hash, NULL, NULL, NULL, NULL, queries_count, NULL, NULL);

    void *ent, *ent2;
    unsigned int bkt, bkt2;
    struct query_item *q;
    struct query_plan_item *p;
    int plans_count_tmp = 0;
    int current_query_num_plans;
    for (q = (struct query_item*)hash_first(gbl_query_plan_hash, &ent, &bkt); q; q = (struct query_item*)hash_next(gbl_query_plan_hash, &ent, &bkt)) {
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
    hash_clear(gbl_query_plan_hash);
    hash_free(gbl_query_plan_hash);
    gbl_query_plan_hash = NULL;

    Pthread_mutex_unlock(&gbl_query_plan_hash_mu);
    if (plans_count)
        *plans_count = plans_count_tmp;
    return;
}
