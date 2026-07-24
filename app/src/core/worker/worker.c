/**
 * @file worker.c
 *
 * @brief Core worker logic and operator pool management.
 * The worker module manages a pool of operator threads, each responsible for handling client connections. It provides functions to
 * initialize the worker pool, dispatch client connections to operators, and manage operator lifecycles.
 *
 * @note This module contains global state for the worker instance, because
 * it will be used, by design, by just a single thread per time, server core process for initialization and listener thread at runtime.
 */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif /* _GNU_SOURCE */

#include <db_server/core/worker/worker.h>

#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <db_server/core/worker/operator/operator.h>
#include <emlog.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG                           "srv_worker"

/* Bound on worker_run()'s wait for each operator's post-pin init (client parser alloc — a handful of
 * mallocs, microseconds in practice). Generous on purpose: this only fires once, at startup, and
 * exists to catch a genuinely stuck/failed thread, not to shave milliseconds off the happy path. */
#define WORKER_OPERATOR_INIT_TIMEOUT_S    2U
#define WORKER_OPERATOR_INIT_POLL_NS      1000000L /* 1ms */

/*****************************************************************************************************************************************
 * PRIVATE ENUMERATED VARIABLES
 *****************************************************************************************************************************************
 */
/* None */

/*****************************************************************************************************************************************
 * PRIVATE STRUCTURED TYPES
 *****************************************************************************************************************************************
 */

typedef struct
{
    /**
     * @brief Current worker status
     */
    worker_status_t status;

    /**
     * @brief Array of operators managed by this worker
     */
    operator_t* operators;

    /**
     * @brief Threads for each operator
     */
    pthread_t* operators_threads;

    /**
     * @brief Number of operators in the pool
     */
    uint8_t operators_count;

    /**
     * @brief True once worker_run() has created the operator threads — tells
     *        worker_destroy() whether there is anything to signal + join before
     *        it destroys operators (the worker_init fail path has none).
     */
    bool threads_started;

    /**
     * @brief Per-operator client-slot cap resolved once at worker_init() (DB_SERVER_MAX_CLIENTS or the
     * WORKER_MAX_CLIENTS default) — every operator gets this exact value. Kept here too (not just on
     * each operator_t) so _least_loaded_operator()'s blind-assignment threshold can scale with the
     * ACTUAL cap instead of a compile-time constant that a runtime override would make stale.
     */
    uint32_t max_clients;
} worker_t;

/*****************************************************************************************************************************************
 * PRIVATE VARIABLES DEFINITIONS
 *****************************************************************************************************************************************
 */

worker_t _worker = {0};

/*****************************************************************************************************************************************
 * PRIVATE FUNCTION DECLARATIONS
 *****************************************************************************************************************************************
 */

/**
 * @section balancer Lock-free operator selection strategy
 *
 * The dispatcher keeps a @c operator_t array. Each operator exposes an @c atomic_uint @c active_count that is updated by the operator
 * thread whenever clients are added/removed. The dispatcher thread chooses the "least loaded" operator by scanning the array and reading
 * these counters with @c memory_order_relaxed.
 *
 * Rationale and properties:
 * - No locks: selection uses only atomic loads, avoiding mutex contention
 *   between the dispatcher and operator threads.
 * - Eventually correct: counters may be slightly stale while an operator is
 *   in the middle of accepting/removing a client, but the race only affects
 *   a single assignment and quickly converges as counts are updated.
 * - Fast path for idle operators: if any operator reports zero load, it is
 *   immediately selected. This reduces latency during ramp-ups and helps
 *   cold operators receive their first connection quickly.
 * - Fairness: a linear scan taking the minimum produces near‑uniform
 *   distribution under steady load for small operator counts (tens). For
 *   very large pools the scan cost is still tiny compared to I/O, but a
 *   heap or power-of-two-choices scheme could be substituted later.
 * - Memory ordering: @c memory_order_relaxed is sufficient here because
 *   the only contract is to read a quasi‑current load number; there is no
 *   dependence on happens‑before relationships with the operator’s internal
 *   data structures.
 *
 * Complexity:
 * - Selection is O(N) in the number of operators per connection dispatch.
 *
 * Failure handling:
 * - If the destination operator’s ring is full (should be rare), the
 *   dispatcher closes the client FD and logs an error rather than blocking.
 */

static uint8_t _compute_operator_count(uint8_t cpu_count);

/**
 * @brief Resolve each operator's SPSC ring (mailbox) capacity, once, at worker init.
 *
 * @return A validated power-of-two capacity: DB_SERVER_RING_CAPACITY if set and valid
 *         (power of two, 8..1024), otherwise a built-in default.
 */
static uint32_t _compute_ring_capacity(void);

/**
 * @brief Resolve each operator's client-slot cap, once, at worker init.
 *
 * @return A validated cap: DB_SERVER_MAX_CLIENTS if set and valid (8..255), otherwise
 *         WORKER_MAX_CLIENTS (the compile-time default).
 */
static uint32_t _compute_max_clients(void);

/**
 * @brief Select the least-loaded operator using relaxed atomics.
 *
 * Implements a fast path for lightly loaded operators below the blind assignment limit.
 *
 * @param dispatcher Dispatcher instance.
 * @return Index of the selected operator.
 */
static operator_t* _least_loaded_operator(void);

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */
uint8_t worker_get_operators_count(void)
{
    return _worker.operators_count;
}

unsigned worker_active_clients_total(void)
{
    unsigned total = 0u;
    for(uint8_t i = 0; i < _worker.operators_count; ++i)
    {
        total += atomic_load_explicit(&_worker.operators[i].active_clients, memory_order_relaxed);
    }
    return total;
}

int worker_init(uint8_t cpu_count)
{
    _worker.operators_count = _compute_operator_count(cpu_count);
    if(_worker.operators_count == 0)
    {
        EML_ERROR(LOG_TAG, "init: compute_operator_count failed");
        return STATUS_FAILURE;
    }

    /* Resolved once, here, and passed to every operator — not re-read per operator. */
    const uint32_t ring_capacity = _compute_ring_capacity();
    _worker.max_clients          = _compute_max_clients();

    /* Allocate an operator for each cpu available for operators */
    _worker.operators = calloc(_worker.operators_count, sizeof(operator_t));
    if(!_worker.operators)
    {
        EML_ERROR(LOG_TAG, "init: operators allocation failed");
        return STATUS_FAILURE;
    }

    /* Allocate a thread for each operator */
    _worker.operators_threads = calloc(_worker.operators_count, sizeof(pthread_t));
    if(!_worker.operators_threads)
    {
        EML_ERROR(LOG_TAG, "init: operators_threads allocation failed");
        goto fail;
    }

    /* Initialize each operator */
    for(uint8_t op_idx = 0; op_idx < _worker.operators_count; ++op_idx)
    {
        if(operator_init(&_worker.operators[op_idx], op_idx, ring_capacity, _worker.max_clients) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "init: operator %u init failed", (unsigned)op_idx);
            goto fail;
        }

        /* Paranoia check. Not yet ACTIVE here by design (§4.3): each operator finishes its own
         * post-pin init (client parser alloc, response-arena first-touch) on its own thread, once
         * worker_run() starts it — this just confirms operator_init()'s boot-thread half (ring,
         * reactor, timer, wakeup) actually landed in the state it claims to have. */
        if(_worker.operators[op_idx].status != OPERATOR_STATUS_INITIALIZING)
        {
            EML_ERROR(LOG_TAG, "init: operator %u not ready, status %d", (unsigned)op_idx, _worker.operators[op_idx].status);
            goto fail;
        }
    }

    /* Set worker status to active */
    _worker.status = WORKER_STATUS_ACTIVE;

    EML_INFO(LOG_TAG, "Worker ready with %u operator%s (cpus=%u)", _worker.operators_count, (_worker.operators_count == 1) ? "" : "s",
             (unsigned)cpu_count);

    return STATUS_SUCCESS;

fail:
    worker_destroy();
    return STATUS_FAILURE;
}

int worker_dispatch_to_operator(int client_fd)
{
    if(client_fd < 0)
    {
        EML_ERROR(LOG_TAG, "_to_operator: invalid client fd");
        return STATUS_FAILURE;
    }

    if(_worker.status != WORKER_STATUS_ACTIVE)
    {
        EML_ERROR(LOG_TAG, "_to_operator: worker not active");
        return STATUS_FAILURE;
    }

    /* Get least loaded operator */
    operator_t* op = _least_loaded_operator();
    if(!op)
    {
        EML_ERROR(LOG_TAG, "_to_operator: failed to get least loaded operator");
        return STATUS_FAILURE;
    }

    /* Check operator's health */
    if(!op->ring || op->wakeup_ctx.fd == -1)
    {
        EML_ERROR(LOG_TAG, "_to_operator: least loaded operator not healthy");
        return STATUS_FAILURE;
    }

    /* Push new client's fd to the operator's ring */
    if(spsc_ring_push(op->ring, client_fd) != 0)
    {
        EML_ERROR(LOG_TAG, "_to_operator: failed pushing to operator's ring");
        return STATUS_FAILURE;
    }

    /* Queued for dispatch-load purposes until this operator's own thread dequeues it (see
     * operator.c's wakeup handler) — this is the other half of effective_load in
     * _least_loaded_operator, so a ring that's full is never invisible to the balancer. */
    atomic_fetch_add_explicit(&op->queued_clients, 1U, memory_order_relaxed);

    /* Signal to operator a new client's fd presence on the ring */
    uint64_t wakeup_var = 1U;
    if(write(op->wakeup_ctx.fd, &wakeup_var, sizeof wakeup_var) != sizeof wakeup_var)
    {
        EML_PERR(LOG_TAG, "_to_operator: write to wakeup fd %d failed", op->wakeup_ctx.fd);
        /* continue anyway, operator will eventually notice the new fd */
    }

#ifdef DEBUG
    EML_DBG(LOG_TAG, "_to_operator: assigned client fd %d to operator %d", client_fd, op->id);
#endif
    return STATUS_SUCCESS;
}

int worker_run(void)
{
    /* run operators threads */
    for(uint8_t op_idx = 0; op_idx < _worker.operators_count; op_idx++)
    {
        if(pthread_create(&_worker.operators_threads[op_idx], NULL, operator_thread, &_worker.operators[op_idx]) != 0)
        {
            EML_PERR(LOG_TAG, "operator %u thread creation failed", (unsigned)op_idx);
            /* Some threads may already be running: tear them down cleanly rather
             * than leaking or freeing from under them. worker_destroy() joins the
             * ones that started (threads_started stays false, so it won't try to
             * join the pthread_t slots we never created). */
            _worker.threads_started = (op_idx > 0);
            for(uint8_t j = 0; j < op_idx; j++)
            {
                operator_request_shutdown(&_worker.operators[j]);
                pthread_join(_worker.operators_threads[j], NULL);
            }
            _worker.threads_started = false;
            return STATUS_FAILURE;
        }
    }

    _worker.threads_started = true;

    /* Every operator thread now runs its own post-pin init (client parser alloc + response-arena
     * first-touch, MEMORY_MODEL.md §4.3) before it's ready to serve. Wait here for each to report
     * ACTIVE or INIT_FAILED — this is a one-time startup gate, not a steady-state poll, so a bounded
     * wait costs nothing on the happy path and catches a genuinely stuck/failed thread on the bad one. */
    for(uint8_t op_idx = 0; op_idx < _worker.operators_count; op_idx++)
    {
        struct timespec deadline;
        clock_gettime(CLOCK_MONOTONIC, &deadline);
        deadline.tv_sec += WORKER_OPERATOR_INIT_TIMEOUT_S;

        operator_status_t st;
        while((st = atomic_load(&_worker.operators[op_idx].status)) == OPERATOR_STATUS_INITIALIZING)
        {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC, &now);
            if(now.tv_sec > deadline.tv_sec || (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec))
            {
                EML_ERROR(LOG_TAG, "operator %u: post-pin init timed out after %us", (unsigned)op_idx,
                          (unsigned)WORKER_OPERATOR_INIT_TIMEOUT_S);
                st = OPERATOR_STATUS_INIT_FAILED;
                break;
            }
            struct timespec poll_interval = {.tv_sec = 0, .tv_nsec = WORKER_OPERATOR_INIT_POLL_NS};
            nanosleep(&poll_interval, NULL);
        }
        if(st != OPERATOR_STATUS_ACTIVE)
        {
            EML_ERROR(LOG_TAG, "operator %u: post-pin init failed (status=%d) — tearing the worker pool down", (unsigned)op_idx,
                      (int)st);
            /* threads_started is already true: worker_destroy() will request-shutdown + join every
             * operator (including any still mid-init, or not yet even started) before freeing. */
            worker_destroy();
            return STATUS_FAILURE;
        }
    }

    return STATUS_SUCCESS;
}

void worker_destroy(void)
{
    /* Ordered teardown: stop dispatch, ask each operator thread to stop
     * (atomic SHUTDOWN + wakeup), JOIN every thread so none can still touch its
     * ring/reactor/state, and only THEN destroy resources and free memory.
     * Freeing before the join is a use-after-free. */
    _worker.status = WORKER_STATUS_INACTIVE;

    if(_worker.threads_started)
    {
        for(size_t i = 0; i < _worker.operators_count; ++i)
        {
            operator_request_shutdown(&_worker.operators[i]);
        }
        for(size_t i = 0; i < _worker.operators_count; ++i)
        {
            pthread_join(_worker.operators_threads[i], NULL);
        }
        _worker.threads_started = false;
    }

    /* No operator thread is alive now — safe to destroy each operator. */
    if(_worker.operators)
    {
        for(size_t i = 0; i < _worker.operators_count; ++i)
        {
            operator_shutdown(&_worker.operators[i]);
        }
    }

    free(_worker.operators);
    _worker.operators = NULL;
    free(_worker.operators_threads);
    _worker.operators_threads = NULL;
    _worker.operators_count   = 0;
    _worker.max_clients       = 0;
}

/*****************************************************************************************************************************************
 * * PRIVATE FUNCTION DEFINITIONS
 *****************************************************************************************************************************************
 */

static operator_t* _least_loaded_operator(void)
{
    uint8_t      best_idx  = 0;
    unsigned int best_load = UINT_MAX; /* "no candidate yet" — effective_load can exceed WORKER_MAX_CLIENTS
                                         * (active + queued), so WORKER_MAX_CLIENTS itself is no longer a
                                         * valid "nothing found" sentinel; see the two comparisons below. */
    for(uint8_t i = 0; i < _worker.operators_count; ++i)
    {
        /* Skip full operators */
        if(_worker.operators[i].status == OPERATOR_STATUS_FULL) continue;

        /* effective_load = currently-handled + queued-but-not-yet-dequeued. active_clients
         * alone can read 0 while this operator's ring is already full — invisible saturation
         * that let the dispatcher keep routing new connections into an operator that was
         * about to reject them (docs/DB_APP_MAINTENANCE.md's dispatch-accounting review). */
        unsigned int active = atomic_load_explicit(&_worker.operators[i].active_clients, memory_order_relaxed);
        unsigned int queued = atomic_load_explicit(&_worker.operators[i].queued_clients, memory_order_relaxed);
        unsigned int load   = active + queued;

        /* when load is below the blind assignment limit just give it to the operator — 10% of the
         * ACTUAL resolved per-operator cap (DB_SERVER_MAX_CLIENTS override or the WORKER_MAX_CLIENTS
         * default), not a compile-time constant that a runtime override would make stale. */
        if(load < _worker.max_clients / 10U)
        {
#ifdef DEBUG
            EML_DBG(LOG_TAG, "selecting operator %u with load=%u (active=%u queued=%u, blind)", (unsigned)i, load, active, queued);
#endif
            return &_worker.operators[i];
        }

        /* calculate min load over all operators */
        if(load < best_load)
        {
#ifdef DEBUG
            EML_DBG(LOG_TAG, "operator %u has new best load=%u (active=%u queued=%u)", (unsigned)i, load, active, queued);
#endif
            best_load = load;
            best_idx  = i;
        }
        /* skip overloaded operators, there are better around */

        /* Here can see if all the operators are FULL */
        if(i == _worker.operators_count - 1 && best_load == UINT_MAX)
        {
            /* All operators are full, set worker status to full! */
            _worker.status = WORKER_STATUS_FULL;
        }
    }

    if(best_load == UINT_MAX)
    {
        EML_ERROR(LOG_TAG, "all operators are at full capacity");
        return NULL;
    }

#ifdef DEBUG
    EML_DBG(LOG_TAG, "[dispatch] selecting operator %u with load=%u", (unsigned)best_idx, best_load);
#endif

    return &_worker.operators[best_idx];
}

static uint8_t _compute_operator_count(uint8_t cpu_count)
{
    uint8_t available_cpus = 0;
    /* Reserve one CPU for listener when possible */
    if(cpu_count <= 2)
    {
        available_cpus = 1;
    }

    /* give all cpus - 1 to the operators */
    else
    {
        available_cpus = cpu_count - 1;
    }

    /* Default (auto) is one operator per core minus the listener's core, so a
     * dedicated box already uses every CPU (listener on CPU0, operators on
     * 1..N-1). DB_SERVER_WORKERS overrides (trusted input — env set by the
     * unit/operator):
     *   all   one operator per core INCLUDING CPU0 — the last operator's pin
     *         wraps onto the listener's core. The listener is epoll-idle
     *         between accepts, so donating its core buys one more full worker.
     *   1-255 exact count: cap (reserve cores for OS/nginx) or oversubscribe
     *         (pins wrap; warned). */
    const char* env = getenv("DB_SERVER_WORKERS");
    if(env && env[0] != '\0')
    {
        if(strcmp(env, "all") == 0 || strcmp(env, "ALL") == 0)
        {
            EML_INFO(LOG_TAG, "DB_SERVER_WORKERS=all: %u operator(s), one per core — the last shares CPU0 with the listener",
                     (unsigned)cpu_count);
            return cpu_count;
        }
        char* end = NULL;
        long  req = strtol(env, &end, 10);
        if(end && *end == '\0' && req >= 1 && req <= 255)
        {
            if(req > (long)available_cpus)
            {
                EML_WARN(LOG_TAG,
                         "DB_SERVER_WORKERS=%ld exceeds cores-for-operators=%u — operators will "
                         "share cores (oversubscribed; pinning wraps)",
                         req, (unsigned)available_cpus);
            }
            EML_INFO(LOG_TAG, "DB_SERVER_WORKERS override: %ld operator(s) (auto would be %u)", req, (unsigned)available_cpus);
            return (uint8_t)req;
        }
        EML_WARN(LOG_TAG, "DB_SERVER_WORKERS='%s' invalid (want 'all' or integer 1..255) — using auto %u", env, (unsigned)available_cpus);
    }

    EML_INFO(LOG_TAG, "Dispatcher sizing: cpu_count=%u, operators=%u (+1 listener core = all %u cores in use)", (unsigned)cpu_count,
             (unsigned)available_cpus, (unsigned)cpu_count);
    return available_cpus;
}

static uint32_t _compute_ring_capacity(void)
{
    const uint32_t default_capacity = 32U;

    /* Each operator's SPSC ring is its connection mailbox — how many accepted-but-not-yet-
     * dequeued clients it can hold before a dispatcher push is rejected outright (see
     * worker_dispatch_to_operator). The prior hardcoded 8 was a teaching-project-era
     * literal, never measured: tests/bench/ found it saturates fast under a burst of
     * concurrent slow (argon2id) logins even with the effective_load fix above. 32 gives
     * real headroom while staying a small, statically-sized, power-of-two allocation, per
     * this project's established allocation style — resolved once here at boot, not
     * re-read or resized per operator or at runtime. docs/DB_APP_MAINTENANCE.md's own
     * caution was that a larger ring should follow correct load accounting, not substitute
     * for it; this change ships both together, not the ring bump alone. */
    const char* env = getenv("DB_SERVER_RING_CAPACITY");
    if(env && env[0] != '\0')
    {
        char* end = NULL;
        long  req = strtol(env, &end, 10);
        if(end && *end == '\0' && req >= 8 && req <= 1024 && (req & (req - 1)) == 0)
        {
            EML_INFO(LOG_TAG, "DB_SERVER_RING_CAPACITY override: %ld", req);
            return (uint32_t)req;
        }
        EML_WARN(LOG_TAG, "DB_SERVER_RING_CAPACITY='%s' invalid (want a power of two in 8..1024) — using default %u", env,
                 (unsigned)default_capacity);
    }
    return default_capacity;
}

static uint32_t _compute_max_clients(void)
{
    /* Bounds mirror config_validate.c's _validate_max_clients_env() (8..255) — keep both in sync if
     * either changes. WORKER_MAX_CLIENTS is a compile-time default sized for the box this project
     * targets; the memory cost of raising it is trivial (client_t is dominated by its 32 KiB read
     * buffer), so this is a plain explicit override, not an adaptive/heuristic formula — same shape
     * as DB_SERVER_WORKERS / DB_SERVER_RING_CAPACITY above. */
    const char* env = getenv("DB_SERVER_MAX_CLIENTS");
    if(env && env[0] != '\0')
    {
        char* end = NULL;
        long  req = strtol(env, &end, 10);
        if(end && *end == '\0' && req >= 8 && req <= 255)
        {
            EML_INFO(LOG_TAG, "DB_SERVER_MAX_CLIENTS override: %ld (default would be %u)", req, (unsigned)WORKER_MAX_CLIENTS);
            return (uint32_t)req;
        }
        EML_WARN(LOG_TAG, "DB_SERVER_MAX_CLIENTS='%s' invalid (want an integer 8..255) — using default %u", env,
                 (unsigned)WORKER_MAX_CLIENTS);
    }
    return (uint32_t)WORKER_MAX_CLIENTS;
}
