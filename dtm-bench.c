#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "hiredis.h"

#define MAX_HOST_LEN 50
#define MAX_HOSTS 4
#define NUM_KEYS 9999
#define KEY_LENGTH 10
#define NUM_VALUES 9
#define VALUE_LENGTH 30

struct performance_data {
    unsigned int total_requests;
    struct timespec total_latency;
    struct timespec max_latency;
};

struct thread_params;
typedef void (*BenchMethod)(redisContext *, struct thread_params *);

struct host {
    char host[MAX_HOST_LEN];
    unsigned int port;
};

struct client_params {
    struct host hosts[MAX_HOSTS];
    unsigned int num_hosts;
    BenchMethod method;
    unsigned int started;
    unsigned int finished;
    char keys[NUM_KEYS][KEY_LENGTH];
    char values[NUM_VALUES][VALUE_LENGTH];
};

struct thread_params {
    const struct client_params * client;
    struct performance_data perf;
    unsigned int host_index;
};

struct thread_data {
    pthread_t id;
    struct thread_params params;
};

void parse_hosts(struct client_params * params, char * hosts);
void init_performance_data(struct performance_data * perf);
void aggregate_performance_data(struct performance_data * aggregate, const struct thread_data * threads, unsigned int clients);
void add_timespec(struct timespec * dest, const struct timespec * that);
void max_timespec(struct timespec * dest, const struct timespec * that);
void diff_timespec(struct timespec * dest, const struct timespec * that);

void * redis_client(void * params);
void bench_get_set(redisContext * context, struct thread_params * params);
void bench_transaction(redisContext * context, struct thread_params * params);
void example(redisContext * context, struct thread_params * params);

int main(int argc, char ** argv) {
    if (argc < 5) {
        printf("usage: dtm-bench <host:port[,host:port[,...]]> <clients> <time> <method>\n");
        exit(1);
    }

    struct client_params params;
    params.num_hosts = 0;
    params.method = NULL;
    params.started = 0;
    params.finished = 0;

    unsigned int clients = (unsigned int)atoi(argv[2]);
    unsigned int seconds = (unsigned int)atoi(argv[3]);

    parse_hosts(&params, argv[1]);

    if (!clients) {
        printf("invalid number of clients specified: %d\n", clients);
        exit(1);
    }

    if (!seconds) {
        printf("invalid number of seconds specified: %d\n", seconds);
        exit(1);
    }

    if (!strcmp(argv[4], "get_set")) {
        params.method = &bench_get_set;
    } else if (!strcmp(argv[4], "trans")) {
        params.method = &bench_transaction;
    } else {
        printf("invalid method type: %s\n", argv[4]);
        exit(1);
    }

    for (unsigned int i = 0; i < NUM_KEYS; ++i) {
        sprintf(params.keys[i], "key%u", i);
    }
    for (unsigned int i = 0; i < NUM_VALUES; ++i) {
        sprintf(params.values[i], "abcdefghijklmnopqrstuvwxyz%u", i);
    }

    printf("creating %d clients connecting to each of %d hosts\n", clients, params.num_hosts);
    struct thread_data * threads = (struct thread_data *)malloc(clients * sizeof(struct thread_data) * params.num_hosts);
    for (unsigned int i = 0; i < (clients * params.num_hosts); ++i) {
        threads[i].params.client = &params;
        threads[i].params.host_index = i % params.num_hosts;
        init_performance_data(&threads[i].params.perf);
        if (pthread_create(&threads[i].id, NULL, redis_client, &threads[i].params)) {
            printf("Error creating redis client thread\n");
            exit(1);
        }
    }

    printf("starting clients\n");
    struct timespec elapsed_time;
    clock_gettime(CLOCK_MONOTONIC, &elapsed_time);
    params.started = 1;

    printf("clients running for %d seconds\n", seconds);
    sleep(seconds);

    params.finished = 1;
    printf("stopping clients\n");
    for (unsigned int i = 0; i < (clients * params.num_hosts); ++i) {
        void * unused = NULL;
        pthread_join(threads[i].id, &unused);
    }
    struct timespec end_time;
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    diff_timespec(&elapsed_time, &end_time);

    struct performance_data aggregate;
    init_performance_data(&aggregate);
    aggregate_performance_data(&aggregate, threads, (clients * params.num_hosts));

    double average_latency = (((double)aggregate.total_latency.tv_sec + ((double)aggregate.total_latency.tv_nsec * 0.000000001)) / (double)aggregate.total_requests);
    unsigned int requests_per_sec = (unsigned int)((double)aggregate.total_requests / ((double)elapsed_time.tv_sec + ((double)elapsed_time.tv_nsec * 0.000000001)));

    printf("total requests:  %u\n", aggregate.total_requests);
    printf("total latency:   %u.%09lu\n", (unsigned int)aggregate.total_latency.tv_sec, aggregate.total_latency.tv_nsec);
    printf("average latency: %8.6f\n", average_latency);
    printf("max latency:     %u.%09lu\n", (unsigned int)aggregate.max_latency.tv_sec, aggregate.max_latency.tv_nsec);
    printf("elapsed time:    %u.%09lu\n", (unsigned int)elapsed_time.tv_sec, elapsed_time.tv_nsec);
    printf("requests/sec:    %u\n", requests_per_sec);

    return 0;
}

void parse_hosts(struct client_params * params, char * hosts) {
    char * token =  strtok(hosts, ":");
    while ((token != NULL) && (params->num_hosts < MAX_HOSTS)) {
        char * host = token;
        char * port = strtok(NULL, ",");
        strncpy(params->hosts[params->num_hosts].host, host, MAX_HOST_LEN);
        params->hosts[params->num_hosts].port = atoi(port);
        params->num_hosts++;
        token = strtok(NULL, ":");
    }
}

void init_performance_data(struct performance_data * perf) {
    memset(perf, 0, sizeof(*perf));
}

void aggregate_performance_data(struct performance_data * aggregate, const struct thread_data * threads, unsigned int clients) {
    for (unsigned int i = 0; i < clients; ++i) {
        aggregate->total_requests += threads[i].params.perf.total_requests;
        add_timespec(&aggregate->total_latency, &threads[i].params.perf.total_latency);
        max_timespec(&aggregate->max_latency, &threads[i].params.perf.max_latency);
    }
}

void add_timespec(struct timespec * dest, const struct timespec * that) {
    if ((dest->tv_nsec + that->tv_nsec) > 999999999) {
        dest->tv_sec = dest->tv_sec + that->tv_sec + 1;
        dest->tv_nsec = (dest->tv_nsec + that->tv_nsec) - 1000000000;
    } else {
        dest->tv_sec = dest->tv_sec + that->tv_sec;
        dest->tv_nsec = dest->tv_nsec + that->tv_nsec;
    }
}

void max_timespec(struct timespec * dest, const struct timespec * that) {
    if ((that->tv_sec > dest->tv_sec) ||
        ((that->tv_sec == dest->tv_sec) && (that->tv_nsec > dest->tv_nsec))) {
        *dest = *that;
    }
}

void diff_timespec(struct timespec * dest, const struct timespec * that) {
    if ((that->tv_nsec - dest->tv_nsec) < 0) {
        dest->tv_sec = that->tv_sec - dest->tv_sec - 1;
        dest->tv_nsec = (1000000000 + that->tv_nsec) - dest->tv_nsec;
    } else {
        dest->tv_sec = that->tv_sec - dest->tv_sec;
        dest->tv_nsec = that->tv_nsec - dest->tv_nsec;
    }
}

void * redis_client(void * p) {
    struct thread_params * params = (struct thread_params *)p;

    struct timeval timeout = {5, 0};
    redisContext * context = redisConnectWithTimeout(params->client->hosts[params->host_index].host, params->client->hosts[params->host_index].port, timeout);
    if (context->err) {
        printf("Unable to connect to %s:%d : %s\n", params->client->hosts[params->host_index].host, params->client->hosts[params->host_index].port, context->errstr);
        exit(1);
    }

    params->client->method(context, params);

    redisFree(context);

    return NULL;
}

void bench_get_set(redisContext * context, struct thread_params * params) {
    while (!params->client->started) {
        usleep(1000);
    }

    unsigned int get = 1;
    unsigned int key = 0;
    unsigned int value = 0;
    while (!params->client->finished) {
        struct timespec start;
        redisReply * reply = NULL;
        int keylen = strlen(params->client->keys[key]);
        if (get) {
            clock_gettime(CLOCK_MONOTONIC, &start);
            reply = redisCommand(context, "GET %b", params->client->keys[key], keylen);
            get = 0;
        } else {
            int valuelen = strlen(params->client->values[value]);
            clock_gettime(CLOCK_MONOTONIC, &start);
            reply = redisCommand(context, "SET %b %b", params->client->keys[key], keylen, params->client->values[value], valuelen);

            ++value;
            if (value == NUM_VALUES) {
                value = 0;
            }
            get = 1;
        }
        freeReplyObject(reply);

        struct timespec end;
        clock_gettime(CLOCK_MONOTONIC, &end);

        params->perf.total_requests++;
        diff_timespec(&start, &end);
        add_timespec(&params->perf.total_latency, &start);
        max_timespec(&params->perf.max_latency, &start);

        ++key;
        if (key == NUM_KEYS) {
            key = 0;
        }
    }
}

void bench_transaction(redisContext * context, struct thread_params * params) {
    while (!params->client->started) {
        usleep(1);
    }

    unsigned int key = 0;
    unsigned int value = 0;
    while (!params->client->finished) {
        struct timespec start;
        struct timespec end;

        int keylen = strlen(params->client->keys[key]);
        int valuelen = strlen(params->client->values[value]);

        clock_gettime(CLOCK_MONOTONIC, &start);
        redisReply * reply = redisCommand(context, "WATCH %b", params->client->keys[key], keylen);
        clock_gettime(CLOCK_MONOTONIC, &end);
        freeReplyObject(reply);

        diff_timespec(&start, &end);
        add_timespec(&params->perf.total_latency, &start);
        max_timespec(&params->perf.max_latency, &start);

        clock_gettime(CLOCK_MONOTONIC, &start);
        reply = redisCommand(context, "MULTI");
        clock_gettime(CLOCK_MONOTONIC, &end);
        freeReplyObject(reply);

        diff_timespec(&start, &end);
        add_timespec(&params->perf.total_latency, &start);
        max_timespec(&params->perf.max_latency, &start);

        clock_gettime(CLOCK_MONOTONIC, &start);
        reply = redisCommand(context, "SET %b %b", params->client->keys[key], keylen, params->client->values[value], valuelen);
        clock_gettime(CLOCK_MONOTONIC, &end);
        freeReplyObject(reply);

        diff_timespec(&start, &end);
        add_timespec(&params->perf.total_latency, &start);
        max_timespec(&params->perf.max_latency, &start);

        clock_gettime(CLOCK_MONOTONIC, &start);
        reply = redisCommand(context, "EXEC");
        clock_gettime(CLOCK_MONOTONIC, &end);
        freeReplyObject(reply);

        diff_timespec(&start, &end);
        add_timespec(&params->perf.total_latency, &start);
        max_timespec(&params->perf.max_latency, &start);

        params->perf.total_requests++;

        ++value;
        if (value == NUM_VALUES) {
            value = 0;
        }

        ++key;
        if (key == NUM_KEYS) {
            key = 0;
        }
    }
}

