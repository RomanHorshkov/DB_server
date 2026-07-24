/**
 * @file upload_worker.c
 * @brief Dedicated upload worker pool (see upload_worker.h).
 *
 * A small fixed pool of threads sharing one bounded mutex+condvar queue —
 * uploads are low-frequency on a home server, so the lock-free operator design
 * is unnecessary here; a simple queue is easier to get right. Each worker owns a
 * client_t and, per connection, spins up a fresh DB_http parser (with the same
 * stream gate the operators use), runs the existing client_handle (which reaches
 * client_upload_pump), then closes. Blocking the worker in the pump is fine —
 * that is its whole job; API operators are other threads.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

/*****************************************************************************************************************************************
 * PUBLIC INCLUDES
 *****************************************************************************************************************************************
 */
#include <db_server/core/worker/upload_worker.h>

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include <DB_http/DB_http.h>
#include <emlog.h>

#include <db_app/response/response.h>
#include <db_server/core/config_core.h>
#include <db_server/core/worker/operator/client/client.h>
#include <db_server/core/worker/operator/client/upload_pump.h>
#include <db_server/utils/socket_helper.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */
#define LOG_TAG            "srv_upload_pool"
#define UPLOAD_MAX_WORKERS 16u
/* Queue depth beyond the busy workers. Saturation → 503 (backpressure). A stalled
 * cohort can never starve API threads — they are different threads entirely. */
#define UPLOAD_QUEUE_MAX   32

/* Idle patience while receiving the request line + headers (before the pump owns the body).
 * No bytes for this long on a header = a dead/malicious peer. The pump has its own body idle timeout. */
#define UPLOAD_HEADER_IDLE_MS 10000

/*****************************************************************************************************************************************
 * PRIVATE STRUCTURED TYPEDEFS
 *****************************************************************************************************************************************
 */
typedef struct
{
    pthread_t       threads[UPLOAD_MAX_WORKERS];
    uint8_t         n_workers;
    uint8_t         worker_no_base;
    int             queue[UPLOAD_QUEUE_MAX];
    int             q_head;
    int             q_tail;
    int             q_count;
    int             q_max;    /* active queue depth <= UPLOAD_QUEUE_MAX; see upload_workers_init() */
    int             running;
    pthread_mutex_t lock;
    pthread_cond_t  cv;
} upload_pool_t;

/*****************************************************************************************************************************************
 * PRIVATE VARIABLES
 *****************************************************************************************************************************************
 */
static upload_pool_t _pool = {0};

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

/** @brief Milliseconds on CLOCK_MONOTONIC (never wall-clock — the deadline must be immune to clock steps). */
static uint64_t _mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

/**
 * @brief Drive one adopted upload connection to completion, then release it.
 *
 * client_handle() returns STATUS_SUCCESS on EAGAIN expecting to be called again (request headers can span several
 * reads), so a worker cannot call it once and close. This is the explicit driver loop: poll (bounded by both the
 * header idle timeout AND the absolute monotonic deadline) → client_handle → SUCCESS means "more input / still
 * alive", FAILURE means "finished (the pump ran) or the peer went away". Once headers complete, client_handle runs
 * the pump internally (its own body poll loop) and returns FAILURE, so the loop below only governs the header phase.
 */
static void _drive_upload(client_t* cli, int fd, uint8_t worker_no)
{
    client_adopt_fd(cli, fd); /* fd is non-blocking; the parser was installed once in _worker_main */

    const uint64_t deadline = _mono_ms() + (uint64_t)WORKER_UPLOAD_MAX_WALL_S * 1000u;

    for(;;)
    {
        const uint64_t now = _mono_ms();
        if(now >= deadline)
        {
            EML_WARN(LOG_TAG, "fd %d: upload exceeded %us wall deadline — aborting", fd, WORKER_UPLOAD_MAX_WALL_S);
            break;
        }

        const uint64_t left    = deadline - now;
        const int      timeout = left < (uint64_t)UPLOAD_HEADER_IDLE_MS ? (int)left : UPLOAD_HEADER_IDLE_MS;

        struct pollfd pfd = {.fd = cli->ctx.fd, .events = POLLIN};
        int           prc = poll(&pfd, 1, timeout);
        if(prc == 0)
        {
            /* Timed out with no data: either the header idle window elapsed or we reached the absolute deadline. */
            EML_WARN(LOG_TAG, "fd %d: no upload header data within %d ms — aborting", fd, timeout);
            break;
        }
        if(prc < 0)
        {
            if(errno == EINTR)
            {
                continue;
            }
            EML_PERR(LOG_TAG, "fd %d: upload poll failed", fd);
            break;
        }

        /* Readable. client_handle drains and parses; on headers-complete it runs the pump and returns FAILURE. */
        if(client_handle(cli, worker_no) == STATUS_FAILURE)
        {
            break; /* finished (pump ran + responded), peer closed, or a transport/parse error */
        }
        /* STATUS_SUCCESS: EAGAIN or more headers still to arrive — poll again. */
    }

    client_release_fd(cli);
}

/** @brief One upload worker: own a reusable client+parser, pull an fd off the shared queue, drive it, repeat. */
static void* _worker_main(void* arg)
{
    const uint8_t idx       = (uint8_t)(uintptr_t)arg;
    const uint8_t worker_no = (uint8_t)(_pool.worker_no_base + idx);

    /* One reusable client (32 KiB buffer) AND one reusable parser per worker — embedded style, no per-connection
     * allocation. client_adopt_fd() clears the parser between connections, so reuse carries no stale header state. */
    static _Thread_local client_t cli;
    memset(&cli, 0, sizeof cli);
    cli.ctx.fd = -1;

    /* This worker's own DB_app response arena (MEMORY_MODEL.md step 2) — client_upload_pump() builds
     * response bodies on this thread just like an operator does, so it needs its own bound arena too.
     * Bound once per worker thread, not per connection, mirroring the operator pool's arena_bind(). */
    static _Thread_local uint8_t resp_arena[DB_APP_RESPONSE_ARENA_BYTES];
    db_app_response_arena_bind(resp_arena, sizeof(resp_arena));

    DB_http_parser_t* parser = NULL;
    if(db_http_parser_init(&parser) != DB_http_status_OK ||
       db_http_parser_set_stream_gate(parser, upload_stream_gate, NULL) != DB_http_status_OK)
    {
        EML_ERROR(LOG_TAG, "upload worker %u: parser init/gate failed — worker exiting", (unsigned)idx);
        if(parser)
        {
            db_http_parser_kill(parser);
        }
        return NULL;
    }
    cli.http_parser = parser;

    EML_INFO(LOG_TAG, "upload worker %u up (db slot %u)", (unsigned)idx, (unsigned)worker_no);

    for(;;)
    {
        pthread_mutex_lock(&_pool.lock);
        while(_pool.running && _pool.q_count == 0)
        {
            pthread_cond_wait(&_pool.cv, &_pool.lock);
        }
        if(!_pool.running && _pool.q_count == 0)
        {
            pthread_mutex_unlock(&_pool.lock);
            break;
        }
        int fd = _pool.queue[_pool.q_head];
        _pool.q_head = (_pool.q_head + 1) % UPLOAD_QUEUE_MAX;
        _pool.q_count--;
        pthread_mutex_unlock(&_pool.lock);

        if(fd >= 0)
        {
            _drive_upload(&cli, fd, worker_no);
        }
    }

    db_http_parser_kill(parser);
    cli.http_parser = NULL;
    return NULL;
}

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int upload_workers_init(uint8_t n_workers, uint8_t worker_no_base, uint16_t queue_depth)
{
    if(n_workers == 0u || n_workers > UPLOAD_MAX_WORKERS)
    {
        EML_ERROR(LOG_TAG, "init: n_workers %u out of range (1..%u)", (unsigned)n_workers, UPLOAD_MAX_WORKERS);
        return -1;
    }
    if(queue_depth == 0u || queue_depth > UPLOAD_QUEUE_MAX)
    {
        EML_ERROR(LOG_TAG, "init: queue_depth %u out of range (1..%u)", (unsigned)queue_depth, UPLOAD_QUEUE_MAX);
        return -1;
    }
    if(_pool.running)
    {
        EML_ERROR(LOG_TAG, "init: already running");
        return -1;
    }

    _pool.n_workers      = n_workers;
    _pool.worker_no_base = worker_no_base;
    _pool.q_head = _pool.q_tail = _pool.q_count = 0;
    _pool.q_max                                 = (int)queue_depth;
    _pool.running                               = 1;
    if(pthread_mutex_init(&_pool.lock, NULL) != 0 || pthread_cond_init(&_pool.cv, NULL) != 0)
    {
        EML_ERROR(LOG_TAG, "init: mutex/cond init failed");
        _pool.running = 0;
        return -1;
    }

    for(uint8_t i = 0u; i < n_workers; i++)
    {
        if(pthread_create(&_pool.threads[i], NULL, _worker_main, (void*)(uintptr_t)i) != 0)
        {
            EML_ERROR(LOG_TAG, "init: pthread_create %u failed", (unsigned)i);
            _pool.n_workers = i; /* only i threads exist */
            upload_workers_shutdown();
            return -1;
        }
    }
    EML_INFO(LOG_TAG, "upload pool ready (%u workers, db slots %u..%u, queue %d)", (unsigned)n_workers, (unsigned)worker_no_base,
             (unsigned)(worker_no_base + n_workers - 1u), _pool.q_max);
    return 0;
}

int upload_worker_dispatch(int fd)
{
    if(fd < 0 || !_pool.running)
    {
        return -1;
    }
    pthread_mutex_lock(&_pool.lock);
    if(_pool.q_count >= _pool.q_max)
    {
        pthread_mutex_unlock(&_pool.lock);
        EML_WARN(LOG_TAG, "upload queue saturated (%d) — rejecting fd %d", _pool.q_max, fd);
        return -1; /* caller answers 503 + closes */
    }
    _pool.queue[_pool.q_tail] = fd;
    _pool.q_tail              = (_pool.q_tail + 1) % UPLOAD_QUEUE_MAX;
    _pool.q_count++;
    pthread_cond_signal(&_pool.cv);
    pthread_mutex_unlock(&_pool.lock);
    return 0;
}

void upload_workers_shutdown(void)
{
    if(!_pool.running && _pool.n_workers == 0u)
    {
        return;
    }
    pthread_mutex_lock(&_pool.lock);
    _pool.running = 0;
    /* Close any queued-but-unstarted fds so their peers don't hang. */
    while(_pool.q_count > 0)
    {
        int fd = _pool.queue[_pool.q_head];
        _pool.q_head = (_pool.q_head + 1) % UPLOAD_QUEUE_MAX;
        _pool.q_count--;
        if(fd >= 0)
        {
            socket_shutdown_and_close(fd);
        }
    }
    pthread_cond_broadcast(&_pool.cv);
    pthread_mutex_unlock(&_pool.lock);

    for(uint8_t i = 0u; i < _pool.n_workers; i++)
    {
        pthread_join(_pool.threads[i], NULL);
    }
    pthread_mutex_destroy(&_pool.lock);
    pthread_cond_destroy(&_pool.cv);
    _pool.n_workers = 0u;
    EML_INFO(LOG_TAG, "upload pool stopped");
}

uint8_t upload_workers_count(void)
{
    return _pool.running ? _pool.n_workers : 0u;
}
