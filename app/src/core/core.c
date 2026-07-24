/**
 * @file core.c
 * @brief Core server logic and concurrency management.
 *
 * This module manages the primary components of the micro-HTTP server, including the listener, worker, and control threads. It provides a
 * unified interface for initialization and runtime operations.
 *
 * Usage:
 *   server_init(port);
 *   server_run();
 *
 * Exit Codes:
 *   STATUS_SUCCESS  (0)
 *   STATUS_FAILURE  (1)
 *
 * @author  Roman Horshkov <124358264+RomanHorshkov@users.noreply.github.com>
 * @date    2025‑05‑11
 * (c) 2025
 */

#define _GNU_SOURCE

#include <db_server/core/core.h>

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif /* _GNU_SOURCE */

#include <errno.h>
#include <limits.h>
#include <netdb.h>      /* socklen_t */
#include <pthread.h>    /* pthread_create(), pthread_join() */
#include <pwd.h>        /* pwd */
#include <stdbool.h>
#include <stdio.h>      /* printf(), fprintf(), etc. */
#include <stdlib.h>     /* malloc(), calloc(), NULL etc */
#include <sys/socket.h> /* socklen_t, socket(), bind(), setsockopt(), etc. */
#include <sys/types.h>
#include <unistd.h>     /* fork(), close(), pipe(), read(), write(), getlogin(), getcwd(), system() etc. */

#include <emlog.h>
#include <db_server/core/listener/listener.h>
#include <db_server/core/listener/sd_activation.h>
#include <db_server/core/worker/worker.h>
#include <db_server/core/worker/upload_worker.h>

#include <db_app.h>
#include <db_server/core/config_core.h>
#include <db_server/core/config_validate.h>
#include <db_server/utils/affinity.h>
#include <db_server/utils/socket_helper.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG "srv_core"

/*****************************************************************************************************************************************
 * PRIVATE STRUCTURED TYPES
 *****************************************************************************************************************************************
 */

/**
 *
 * idea: core possesses listener, worker, and pipeline. then, listener and worker gets pointer to pipeline from core during init. core also
 * manages the threads for listener, worker.
 */
typedef struct
{
    /* listener thread */
    pthread_t listener_thread;

    /* Detected CPU count for sizing worker/operator threads */
    uint8_t cpu_count;

    // future: tls_t *tls, etc. (startup config is validated, not stored — see config_validate_startup())
} server_t;

/*****************************************************************************************************************************************
 * PRIVATE VARIABLES DEFINITIONS
 *****************************************************************************************************************************************
 */

/**
 * @brief Singleton instance of the main server structure.
 *
 * Holds all core components (listener, worker, control threads, and pipe) for the lifetime of the server process. This instance is private
 * to the core module and should not be accessed directly from outside.
 */
static server_t server;

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS PROTOTYPES
 *****************************************************************************************************************************************
 */

static void _core_logger_bootstrap(void);

static uint8_t _core_detect_cpu_count(void);

#ifdef DEBUG
static void _p_dbg_info_init(void);
#endif

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int server_init(const char* api_spec, const char* upload_spec)
{
#ifdef DEBUG
    _p_dbg_info_init();
#endif /* DEBUG */

    _core_logger_bootstrap();

    /* Fail fast on a bad listen spec or env override before touching a single socket, thread, or
     * transaction slot — config_validate_startup() already logged the specific reason. */
    if(config_validate_startup(api_spec, upload_spec) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "startup config validation failed.");
        goto fail;
    }

    /* Detect available CPUs and keep the value for thread sizing */
    server.cpu_count = _core_detect_cpu_count();

    /* Transport selection (DB_server/README.md):
     *   - PRODUCTION: systemd socket activation. systemd created /run/home_server/{api,upload}.sock and
     *     handed us the listening fds; we adopt them (no bind/chmod/unlink in the backend).
     *   - DEV / direct run: no activation env → bind the specs passed on argv (a TCP port or unix path).
     * upload_enabled is then read back from the listener, so slot sizing is identical either way. */
    sd_listen_set_t sd;
    const int       activated = sd_take_listen_fds(&sd);
    if(activated < 0)
    {
        EML_ERROR(LOG_TAG, "socket activation environment is malformed.");
        goto fail;
    }

    if(activated)
    {
        if(listener_init_activated(&sd) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "listener failed to init from activation fds.");
            goto fail;
        }
    }
    else
    {
        if(!api_spec || api_spec[0] == '\0')
        {
            EML_ERROR(LOG_TAG, "no socket activation and no listen spec given (usage: server <api_spec> [upload_spec]).");
            goto fail;
        }
        const int want_upload = (upload_spec && upload_spec[0] != '\0');
        if(listener_init(api_spec, want_upload ? upload_spec : NULL) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "listener failed to init.");
            goto fail;
        }
    }

    const int upload_enabled = (int)listener_upload_active();

    /* Initialize the worker (operators) */
    if(worker_init(server.cpu_count) != 0)
    {
        EML_ERROR(LOG_TAG, "worker failed to init.");
        goto fail;
    }

    /* DB_app transaction slots: one per operator (operator id == worker_no, §9.3), PLUS one per upload worker.
     * Upload workers use slots ABOVE the operators' [0, ops) range and must never overlap them. Range-check the
     * SUM before narrowing to uint8_t (db_app_init's parameter type). */
    const unsigned operators     = (unsigned)worker_get_operators_count();
    const unsigned upload_workers = upload_enabled ? (unsigned)WORKER_UPLOAD_COUNT : 0u;
    const unsigned total_slots    = operators + upload_workers;
    if(operators == 0u || total_slots > 255u)
    {
        EML_ERROR(LOG_TAG, "invalid slot sizing: operators=%u + upload_workers=%u = %u (must be 1..255)", operators, upload_workers,
                  total_slots);
        goto fail;
    }

    if(db_app_init((uint8_t)total_slots) != 0)
    {
        EML_ERROR(LOG_TAG, "db_app failed to init.");
        goto fail;
    }

    /* Start the upload worker pool AFTER db_app_init (its slots must already exist). Workers claim
     * [operators, operators + upload_workers). */
    if(upload_enabled)
    {
        if(upload_workers_init((uint8_t)upload_workers, (uint8_t)operators) != 0)
        {
            EML_ERROR(LOG_TAG, "upload worker pool failed to init.");
            goto fail;
        }
        EML_INFO(LOG_TAG, "upload isolation ON: %u workers (db slots %u..%u)", upload_workers, operators, total_slots - 1u);
    }
    else
    {
        EML_INFO(LOG_TAG, "upload isolation OFF: uploads run in operators (no upload spec configured)");
    }

    /* Successful initialization */
    EML_INFO(LOG_TAG, "🚀 C Server initialized (%s)", activated ? "socket-activated" : (api_spec ? api_spec : "?"));
    return STATUS_SUCCESS;

fail:
    return STATUS_FAILURE;
}

void server_run(void)
{
    /* run listener thread */
    pthread_create(&server.listener_thread, NULL, listener_run, NULL);

    /* run operators threads */
    worker_run();

    /* wait listener thread (it stops accepting new API + upload connections first) */
    pthread_join(server.listener_thread, NULL);

    /* Shutdown order (DB_server/README.md): stop the upload pool BEFORE tearing down operators and DB_app.
     * upload_workers_shutdown() drains the queue, shuts active upload sockets (waking each worker's poll so its live
     * ticket aborts and rolls back), and JOINS the workers — so no upload commit can touch a torn LMDB env. Then the
     * operators, then DB_app/LMDB last. Safe/no-op when the pool was never started. */
    upload_workers_shutdown();
    worker_destroy();
    db_app_shutdown();
}

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static void _core_logger_bootstrap(void)
{
    static bool initialized = false;
    if(initialized) return;

#ifdef DEBUG
    emlog_init(EML_LEVEL_DBG, true);
    emlog_set_writev_flush(true);
    EML_INFO(LOG_TAG, "Debug logger active; stdout/stderr sink");
#else
    /* Production will go to journald */
    emlog_init(EML_LEVEL_INFO, false);
    emlog_set_writev_flush(false);
    EML_INFO(LOG_TAG, "Production logger active; journald sink");
#endif

    initialized = true;
}

static uint8_t _core_detect_cpu_count(void)
{
    /* srv_affinity_online_cpus() reads sched_getaffinity() (falling back to sysconf only if
     * that fails) so this reflects what the process is actually ALLOWED to use — a cgroup,
     * container, or systemd AllowedCPUs= restriction narrows this below the host's raw core
     * count. Sizing the operator pool from raw sysconf() instead (the previous behavior)
     * could spin up far more operators than legal CPUs, wrapping many threads onto the same
     * real core (see docs/DB_APP_MAINTENANCE.md's CPU/NUMA review). Affinity pinning already
     * used this effective set; sizing now agrees with it. */
    long cpus = srv_affinity_online_cpus();

    EML_INFO(LOG_TAG, "Detected %ld CPU%s available", cpus, (cpus == 1) ? "" : "s");
    if(cpus < 1)
    {
        EML_PERR(LOG_TAG, "srv_affinity_online_cpus() failed, defaulting to 1 CPU");
        return 1;
    }
    if(cpus > 255)
    {
        EML_WARN(LOG_TAG, "Detected CPU count %ld exceeds max supported 255, capping to 255", cpus);
        return 255;
    }

    return (uint8_t)cpus;
}

#ifdef DEBUG
static void _p_dbg_info_init(void)
{
    // all this is a partial representation of the server's core folder and
    // what is visible in it, from which user it is running, and the current working directory.
    /* Print current user */
    uid_t                uid = geteuid();
    const struct passwd* pw  = getpwuid(uid);
    if(pw)
    {
        printf(LOG_TAG "running as user: %s\n", pw->pw_name);
    }
    else
    {
        printf(LOG_TAG "running as user: UNKNOWN (uid=%d)\n", (int)uid);
    }

    /* Print current working directory */
    char cwd[PATH_MAX];
    if(getcwd(cwd, sizeof(cwd)) != NULL) printf(LOG_TAG "cwd: %s\n", cwd);

    /* List directory contents */
    printf(LOG_TAG "ls -la:\n");
    system("ls -la");
}
#endif
