/**
 * @file config_core.h
 * @brief Core listener, worker, timer, and fan-out configuration constants.
 */

#ifndef SERVER_CONFIG_CORE_H
#define SERVER_CONFIG_CORE_H

#include <db_server/core/config.h>

/*****************************************************************************************************************************************
 * PUBLIC ENUMERATED DECLARATIONS
 *****************************************************************************************************************************************
 */

typedef enum
{
    /**
     * @brief Uninitialized: not yet started.
     */
    SERVER_STATUS_UNINITIALIZED = 0,
    /**
     * @brief Active: ready to receive new clients.
     */
    SERVER_STATUS_ACTIVE        = 1,
    /**
     * @brief Shutdown: shutting down and cleaning up.
     */
    SERVER_STATUS_SHUTDOWN      = 2,
    /**
     * @brief Invalid: max value for server status.
     */
    SERVER_STATUS_INVALID       = 3,
} server_status;

typedef enum
{
    LISTENER_STATUS_INACTIVE = 0, /* listener is inactive */
    LISTENER_STATUS_ACTIVE   = 1, /* listener is active */
    LISTENER_STATUS_PAUSED   = 2, /* listener is paused */
    LISTENER_STATUS_SHUTDOWN = 3, /* listener to shutdown */
    LISTENER_STATUS_INVALID  = 4, /* max value for listener status */
} listener_status_t;

/**
 * @brief Max Epoll Batch Size: The maximum number of events to retrieve
 * and process in a single epoll_wait() call.
 *
 * This is NOT a limit on the number of connected clients. A larger batch size reduces syscall overhead under high load.
 */
#define MAX_FAN_OUT_SOCKETS                          64U

/*****************************************************************************************************************************************
 * LISTENER PROPERTIES
 *****************************************************************************************************************************************
 */
/* Max listening sockets: API (ipv4 + ipv6) + the dedicated upload listener (ipv4 + ipv6) */
#define SERVER_CORE_MAX_LISTENING_SOCKETS            4U

/* Kernel accept-queue depth (listen backlog) for listeners THIS process binds itself — i.e. the dev/direct
 * run (`./server <port|path>`). In PRODUCTION the sockets are systemd socket-activated: systemd calls
 * listen() before passing the fd, so THIS value never applies — the real backlog is `Backlog=` on
 * install/systemd/{api,upload}.socket (set to 1024, well above the upload pool). Kept generous here too so a
 * keepalive-less upload burst queues instead of being refused (→ nginx `503 backend_unavailable` instead of
 * the pool's graceful `upload_busy`). Trusted transport (only nginx connects), so no SYN-flood concern. */
#define SERVER_CORE_MAX_PENDING_SOCKETS_PER_LISTENER 1024U

/*****************************************************************************************************************************************
 * WORKER PROPERTIES
 *****************************************************************************************************************************************
 */

/* Max clients amount per operator (compile-time default). Explicit runtime override:
 * DB_SERVER_MAX_CLIENTS env var (8..255, validated in config_validate.c, resolved once in
 * worker.c's _compute_max_clients() and passed to every operator_init() call) — same shape as
 * DB_SERVER_WORKERS/DB_SERVER_RING_CAPACITY. client_t is dominated by a 32 KiB read buffer, so the
 * memory cost of a higher cap is trivial; this constant is only ever the FALLBACK when the env var
 * is unset or invalid. */
#define WORKER_MAX_CLIENTS                           64U

/* Client short timeout [s]: applied before first activity (initial request) */
#define WORKER_CLIENT_TIMEOUT_SHORT                  Seconds(15U)

/* Client long timeout [s]: applied after the first successful activity */
#define WORKER_CLIENT_TIMEOUT_LONG                   Minutes(1U)

/* Operator timer tick while clients are present [s] */
#define OPERATOR_TIMER_PERIOD_SHORT                  Seconds(5U)

/* Operator timer tick when idle [s] */
#define OPERATOR_TIMER_PERIOD_LONG                   Minutes(5U)

/*****************************************************************************************************************************************
 * GRACEFUL SHUTDOWN
 *****************************************************************************************************************************************
 */

/* Bound on server_run()'s post-listener-stop wait for operators' active_clients to drain to zero on
 * their own (SIGTERM/SIGINT — listener.c's _install_shutdown_signal_handler()). Ordinary API requests
 * finish in milliseconds; this exists to give a request that's mid-flight when the signal lands a real
 * chance to complete instead of being force-closed the instant the signal is handled, while still
 * guaranteeing termination (worker_destroy()'s hard operator_request_shutdown() runs unconditionally
 * once this elapses, exactly as it always has). Uploads are NOT covered by this — the isolated upload
 * pool (upload_worker.c) already drains gracefully on its own via pthread_join() on each worker's
 * current connection, unconditionally, with no bound here needed. */
#define SERVER_SHUTDOWN_GRACE_S                      10U

/*****************************************************************************************************************************************
 * UPLOAD WORKER POOL PROPERTIES  (DB_server/README.md — upload isolation)
 *****************************************************************************************************************************************
 */

/* Upload worker threads. Uploads run on their OWN pool (never inside an API operator), so a slow upload can never
 * head-of-line-block API traffic. Embedded style: a fixed, build-time count sized to the box. This is also the
 * per-server upload concurrency: a (WORKER_UPLOAD_COUNT + WORKER_UPLOAD_QUEUE_DEPTH)-th concurrent upload gets 503.
 * Each worker consumes one DB_app transaction slot ABOVE the operators' [0, ops) range. */
#define WORKER_UPLOAD_COUNT                          4U

/* Queue depth beyond the busy workers — the "+ WORKER_UPLOAD_QUEUE_DEPTH" half of the concurrency formula
 * above. Past (WORKER_UPLOAD_COUNT + WORKER_UPLOAD_QUEUE_DEPTH) concurrent uploads, upload_worker_dispatch()
 * rejects and the caller answers 503 upload_busy. Mirrors upload_worker.c's physical UPLOAD_QUEUE_MAX (the
 * hard array bound) — keep both in sync if either changes. */
#define WORKER_UPLOAD_QUEUE_DEPTH                    32U

/* Absolute wall-clock ceiling for one upload, CLOCK_MONOTONIC (defeats a forever-slow-but-never-idle client). */
#define WORKER_UPLOAD_MAX_WALL_S                     7200U /* 2 h — a 4 GiB upload at ~600 KiB/s still fits */

/* DB_SERVER_UPLOAD_WORKERS / DB_SERVER_UPLOAD_QUEUE_DEPTH: optional env overrides for the two constants
 * above, following the same explicit-override pattern as DB_SERVER_WORKERS / DB_SERVER_RING_CAPACITY
 * (worker.c). Validated fail-closed at startup by config_validate.c; applied in core.c. Their purpose is a
 * deterministically small pool for test/CI (e.g. DB_SERVER_UPLOAD_WORKERS=1 DB_SERVER_UPLOAD_QUEUE_DEPTH=1
 * makes the 503 upload_busy cap fire at the 3rd concurrent upload instead of needing dozens to reliably
 * outrace production-sized headroom) — production leaves both unset and gets the constants above. */

#endif /* SERVER_CONFIG_CORE_H */
