/**
 * @file operator.c
 * @brief Worker operator thread: per‑thread reactor, mailbox, and clients.
 *
 * An operator owns:
 *  - a reactor (epoll) watching its client sockets, a timerfd, and a wakeup eventfd,
 *  - a single‑producer/single‑consumer ring used as a mailbox for new client FDs,
 *  - a lock‑free @c active_count updated on client add/remove for dispatcher balancing.
 */

#define _GNU_SOURCE

#include <db_server/core/worker/operator/operator.h>

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <emlog.h>

#include <db_server/core/reactor.h>
#include <db_server/core/worker/operator/client/client.h>
#include <db_server/core/worker/operator/client/response_writer.h>
#include <db_server/core/worker/operator/client/upload_pump.h>
#include <db_server/utils/affinity.h>

#include <DB_http/DB_http.h>

#include <db_server/core/worker/worker.h>
#include <db_server/utils/socket_helper.h>
#include <db_server/utils/time_helper.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG                 "srv_wrkr_operator"

#define REACTOR_DEL_MAX_RETRIES 3

/*****************************************************************************************************************************************
 * PRIVATE STUCTURED VARIABLES
 *****************************************************************************************************************************************
 */

/*****************************************************************************************************************************************
 * PRIVATE FUNCTION DECLARATIONS
 *****************************************************************************************************************************************
 */

/**
 * @brief Start and register the wakeup eventfd to reactor.
 */
static int _operator_wakeup_init(operator_t* op);

/**
 * @brief Initialize timerfd and add it to reactor.
 */
static int _operator_timer_init(operator_t* op);

/**
 * @brief Reactor callback for the wakeup eventfd. Drains the eventfd and
 *        dequeues all pending client FDs from the SPSC ring, registering
 *        them with the operator reactor.
 */
static int _operator_handle_wakeup_event(int fd, fd_ctx_t* ctx);

/**
 * @brief Reactor callback for client sockets. Delegates to client handler.
 */
static int _operator_handle_client_event(int fd, fd_ctx_t* ctx);

/**
 * @brief Reactor callback for timer ticks. Adjusts timer cadence and
 *        closes idle clients that exceeded their timeout.
 */
static int _operator_handle_timer_event(int fd, fd_ctx_t* ctx);

/**
 * @brief Find an available client slot or NULL if full.
 */
static client_t* _get_available_client_slot(operator_t* op);

/**
 * @brief Register a new client socket with the operator reactor.
 *        Updates @c active_clients with relaxed atomics.
 */
static int _operator_add_client(operator_t* op, int client_fd);

/**
 * @brief Unregister and close a client socket. Compacts the slot array and
 *        decrements @c active_clients with relaxed atomics.
 */
static int _operator_remove_client_by_fd(operator_t* op, int client_fd);

static void _operator_state_update(operator_t* op);
static void _operator_status_update(operator_t* op);

/**
 * @brief Switch timer cadence between idle and active based on load.
 */
static void _operator_timer_update(operator_t* op);

/**
 * @brief Close clients that exceeded per‑client inactivity timeouts.
 */
static void _clean_clients(operator_t* op);

/**
 * @brief Arm timerfd with a new frequency if it changed.
 */
static void _operator_set_timer(operator_t* op, uint32_t freq);

/*****************************************************************************************************************************************
 * PUBLIC FUNCTION DEFINITIONS
 *****************************************************************************************************************************************
 */

int operator_init(operator_t* op, uint8_t id, uint32_t ring_capacity, uint32_t max_clients)
{
    if(op == NULL || max_clients == 0u)
    {
        EML_ERROR(LOG_TAG, "init: invalid input");
        goto fail;
    }

    /* Set operator ID */
    op->id            = id;
    /* Initialize wakeup context fd */
    op->wakeup_ctx.fd = -1;
    /* Initialize timer fd */
    op->timer_fd      = -1;
    /* Initialize timer frequency */
    op->timer_period  = OPERATOR_TIMER_PERIOD_LONG;
    /* Initialize active/queued clients counts */
    atomic_store(&op->active_clients, 0);
    atomic_store(&op->queued_clients, 0);

    /* Client slots — calloc'd to the resolved runtime cap (DB_SERVER_MAX_CLIENTS or the
     * WORKER_MAX_CLIENTS default), same allocation style as worker.c's operators[] array. Set
     * max_clients BEFORE any fail path so operator_shutdown() (which bounds its cleanup loop on it)
     * never scans past what was actually requested, even on a partial-init failure below. */
    op->max_clients = max_clients;
    op->clients      = calloc(max_clients, sizeof(client_t));
    if(!op->clients)
    {
        EML_ERROR(LOG_TAG, "init: clients allocation failed (max_clients=%u)", (unsigned)max_clients);
        goto fail;
    }

    /* Initialize spsc ring */
    op->ring = spsc_ring_init(ring_capacity);
    if(!op->ring)
    {
        EML_ERROR(LOG_TAG, "init: ring alloc failed");
        goto fail;
    }

    /* Initialize operator's reactor */
    if(reactor_init(&op->reactor) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] reactor_init failed", op->id);
        goto fail;
    }

    /* Start and register timer fd to reactor */
    if(_operator_timer_init(op) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "[op %d] timer init failed", op->id);
        goto fail;
    }

    /* Start and register wakeup fd to reactor */
    if(_operator_wakeup_init(op) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "[op %d] wakeup init failed", op->id);
        goto fail;
    }

    /* Client parser allocation (db_http_parser_init) is deliberately NOT done here. It used to run in
     * this loop, on the BOOT thread — a real NUMA first-touch violation (MEMORY_MODEL.md §4.3): every
     * write to op->clients[i].http_parser faults that page onto the boot thread's node, not this
     * operator's eventual pinned node. It now runs in operator_thread(), right after this operator
     * pins itself, and reports back via op->status (see OPERATOR_STATUS_INITIALIZING/_INIT_FAILED). */

    /* Ring/reactor/timer/wakeup are live; the operator is ready for its own thread to finish setup. */
    op->status = OPERATOR_STATUS_INITIALIZING;

    return STATUS_SUCCESS;

fail:
    operator_shutdown(op);
    return STATUS_FAILURE;
}

/**
 * @brief Operator thread main: initialize reactor/timer, process events.
 *
 * Event sources:
 *  - wakeup eventfd: drain and accept new client FDs from the mailbox ring
 *  - timerfd: periodic housekeeping (timeouts)
 *  - client sockets: read/parse HTTP and drive request lifecycle
 */
void* operator_thread(void* arg)
{
    operator_t* op = (operator_t*)arg;
    if(!op)
    {
        EML_ERROR(LOG_TAG, "thread: invalid operator");
        return NULL;
    }

    /* Pin this operator to its own core (slot id+1; the listener owns slot 0)
     * so it stays cache-hot and never migrates under load. Non-fatal. */
    srv_affinity_pin_self("operator", (int)op->id + 1);

    /* First-touch the response arena from THIS pinned thread, not the boot thread that called
     * operator_init() — MEMORY_MODEL.md §4.3. memset() is the actual touch that faults pages onto
     * this core's NUMA node; arena_bind() just publishes the thread-local pointer/cap afterward. */
    memset(op->resp_arena, 0, sizeof(op->resp_arena));
    db_app_response_arena_bind(op->resp_arena, sizeof(op->resp_arena));

    /* Client parser allocation — moved here from operator_init() (§4.3, same NUMA reasoning as the
     * arena above): every db_http_parser_init() write to op->clients[i].http_parser now lands on
     * THIS pinned thread's node instead of the boot thread's. The stream gate (§9.4) lets POST
     * /api/app/files bypass body buffering into the upload pump. */
    int init_failed = 0;
    for(size_t cli_idx = 0; cli_idx < op->max_clients; cli_idx++)
    {
        if(db_http_parser_init(&op->clients[cli_idx].http_parser) != DB_http_status_OK ||
           db_http_parser_set_stream_gate(op->clients[cli_idx].http_parser, upload_stream_gate, NULL) != DB_http_status_OK)
        {
            EML_ERROR(LOG_TAG, "[op %d] thread: client %zu parser init failed", op->id, cli_idx);
            init_failed = 1;
            break;
        }
    }

    /* Publish the outcome with a CAS, not a plain store: worker_run() (still on the boot thread) may
     * have already given up waiting and asked every operator to shut down (operator_request_shutdown()
     * — a plain atomic_store of SHUTDOWN) WHILE this loop was still running. A plain store here would
     * silently clobber that SHUTDOWN back to ACTIVE/INIT_FAILED, the main loop below would start
     * anyway, and worker_destroy()'s pthread_join() on this thread would hang forever waiting for a
     * shutdown signal that already came and went. The CAS only succeeds if status is still
     * INITIALIZING; on failure `expected` comes back as whatever it actually is (SHUTDOWN), and this
     * thread honors that instead of overwriting it. */
    operator_status_t expected = OPERATOR_STATUS_INITIALIZING;
    operator_status_t target   = init_failed ? OPERATOR_STATUS_INIT_FAILED : OPERATOR_STATUS_ACTIVE;
    if(!atomic_compare_exchange_strong(&op->status, &expected, target))
    {
        EML_INFO(LOG_TAG, "[op %d] thread: shutdown requested during init (status=%d) — exiting without serving", op->id,
                 (int)expected);
        return NULL;
    }
    if(init_failed)
    {
        return NULL;
    }

    EML_INFO(LOG_TAG, "[op %d] thread starting", op->id);

    /* Main loop */
    /**
     * Run while ACTIVE or FULL. FULL will not receive new clients, reject if requested by worker, and will just proceed with active clients
     * until a slot frees up.
     */
    while(op->status == OPERATOR_STATUS_ACTIVE || op->status == OPERATOR_STATUS_FULL)
    {
        /* Reactor loops over epoll fds, calling the registered handler */
        int out_fd = -1;
        if(reactor_run(&op->reactor, &out_fd) != STATUS_SUCCESS)
        {
            /* Check if reactor signaled to close the fd */
            if(out_fd != -1)
            {
#ifdef DEBUG
                EML_DBG(LOG_TAG, "[op %d] reactor requested close for fd %d", op->id, out_fd);
#endif
                /* shuts down the client and removes from operator's reactor */
                _operator_remove_client_by_fd(op, out_fd);
                continue;
            }
            EML_ERROR(LOG_TAG, "[op %d] reactor_run failed", op->id);
        }
    }

    /* The loop exited because SHUTDOWN was requested. Do NOT free resources
     * here: worker_destroy() joins this thread first and then calls
     * operator_shutdown(). Destroying ring/reactor/fds from under a thread that
     * a moment ago was using them (or that another thread is about to free) is a
     * use-after-free. The thread's only job now is to return. */
    EML_INFO(LOG_TAG, "[op %d] thread exiting", op->id);
    return NULL;
}

void operator_request_shutdown(operator_t* op)
{
    if(op == NULL)
    {
        return;
    }

    /* Publish SHUTDOWN first (atomic) so the woken thread observes it... */
    atomic_store(&op->status, OPERATOR_STATUS_SHUTDOWN);

    /* ...then kick the thread out of epoll_wait so the run loop re-checks the
     * status and returns. Best-effort: the loop condition also polls status. */
    if(op->wakeup_ctx.fd != -1)
    {
        uint64_t one = 1U;
        ssize_t  w   = write(op->wakeup_ctx.fd, &one, sizeof one);
        (void)w;
    }
}

void operator_shutdown(operator_t* op)
{
    if(op == NULL)
    {
        EML_ERROR(LOG_TAG, "_shutdown: invalid input");
        return;
    }

    int shutdown_id = op->id; /* keep for the log line below (id is zeroed next) */

    /* Set status to SHUTDOWN */
    atomic_store(&op->status, OPERATOR_STATUS_SHUTDOWN);

    /* Set id to 0 */
    op->id = 0;

    /* Clean ring */
    spsc_ring_destroy(&op->ring);

    /* Clean wakeup fd and its context */
    if(op->wakeup_ctx.fd != -1)
    {
        socket_shutdown_and_close(op->wakeup_ctx.fd);
        op->wakeup_ctx.fd = -1;
    }

    /* Clean timer fd */
    if(op->timer_fd != -1)
    {
        socket_shutdown_and_close(op->timer_fd);
        op->timer_fd = -1;
    }

    /* Clean the clients and their parser handles, then free the calloc'd slot array itself. */
    if(op->clients)
    {
        for(size_t i = 0; i < op->max_clients; i++)
        {
            client_shutdown(&op->clients[i]);
            db_http_parser_kill(op->clients[i].http_parser);
            op->clients[i].http_parser = NULL;
        }
        free(op->clients);
        op->clients = NULL;
    }
    op->max_clients = 0u;

    /* Clean reactor */
    reactor_shutdown(&op->reactor);

    /* Reset active/queued clients counts */
    atomic_store(&op->active_clients, 0);
    atomic_store(&op->queued_clients, 0);

    EML_INFO(LOG_TAG, "[op %d] shutdown complete", shutdown_id);
}

/*****************************************************************************************************************************************
 * PRIVATE FUNCTION DEFINITIONS
 *****************************************************************************************************************************************
 */

static int _operator_handle_wakeup_event(int fd, fd_ctx_t* ctx)
{
    /* The wakeup event is triggered by the listener
    and it is time to store a new client */
    operator_t* op = (operator_t*)ctx->owner;

    /* Drain wakeup fd */
    socket_drain(fd);

    /* Loop over all possible new fds on the operator's spsc ring while it is active */
    int client_fd = -1;
    while(spsc_ring_pop(op->ring, &client_fd) == 0)
    {
        /* Dequeued: no longer "waiting in the ring" for dispatch-load purposes, regardless of
         * whether _operator_add_client below succeeds — either way it's no longer queued. */
        atomic_fetch_sub_explicit(&op->queued_clients, 1U, memory_order_relaxed);

        if(op->status != OPERATOR_STATUS_ACTIVE)
        {
            EML_ERROR(LOG_TAG, "[op %d] wakeup: operator not active", op->id);
            return STATUS_FAILURE;
        }

#ifdef DEBUG
        EML_DBG(LOG_TAG, "[op %d] wakeup, adding new client on fd %d", op->id, client_fd);
#endif /* DEBUG */
        if(_operator_add_client(op, client_fd) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "[op %d] failed to add client %d, client closed.", op->id, client_fd);
            return STATUS_FAILURE;
        }
    }

    return STATUS_SUCCESS;
}

static int _operator_handle_client_event(int fd, fd_ctx_t* ctx)
{
    if(!ctx || !ctx->owner)
    {
        EML_ERROR(LOG_TAG, "client event: invalid context");
        return STATUS_FAILURE;
    }

    operator_t* op = (operator_t*)ctx->owner;

    /* Calculate client slot from context pointer */
    /* ctx is a member of client_t, so we can retrieve the container
    by jumping straight to the client_t structure */
    client_t* cli = (client_t*)(void*)((char*)ctx - offsetof(client_t, ctx));

    /* Sanity check */
    if(cli->ctx.fd != fd)
    {
        EML_ERROR(LOG_TAG, "[op %d] client event: fd mismatch (ctx=%d, event=%d)", op->id, cli->ctx.fd, fd);
        return STATUS_FAILURE;
    }

    if(!cli->is_busy)
    {
        EML_ERROR(LOG_TAG, "[op %d] client event: slot not busy for fd %d", op->id, fd);
        return STATUS_FAILURE;
    }

    /* A draining client is registered for EPOLLOUT, not EPOLLIN (response_writer.c's EPOLLOUT parking,
     * §9.2 end state) — this event means "the socket can accept more bytes now", not "there's a new
     * request to parse", so it resumes the parked send instead of calling client_handle(). */
    int rc = cli->draining ? response_writer_resume(cli) : client_handle(cli, op->id);
    if(rc != STATUS_SUCCESS)
    {
        _operator_remove_client_by_fd(op, cli->ctx.fd);
    }
    return STATUS_SUCCESS;
}

static int _operator_handle_timer_event(int fd, fd_ctx_t* ctx)
{
    operator_t* op = (operator_t*)ctx->owner;

    if(socket_drain(fd) == -1)
    {
        /* EAGAIN here is benign: the timerfd is level-triggered, so a read() can
         * legitimately race an epoll_wait() readiness snapshot and find nothing left
         * (e.g. another ready fd in the same batch was handled first and the interval
         * hadn't re-armed yet). That is NOT an operator failure — treat it as "nothing
         * to do this cycle" and keep going, instead of reporting reactor_run() failed
         * and spamming the log once per timer period forever. A non-EAGAIN errno on a
         * timerfd read (fd closed under us, bad fd, ...) is still a genuine error. */
        if(errno == EAGAIN) /* == EWOULDBLOCK on Linux */
        {
            return STATUS_SUCCESS;
        }
        EML_PERR(LOG_TAG, "[op %d] timer read failed", op->id);
        return STATUS_FAILURE;
    }

    _operator_timer_update(op);
    _clean_clients(op);
    return STATUS_SUCCESS;
}

static client_t* _get_available_client_slot(operator_t* op)
{
    for(uint32_t i = 0; i < op->max_clients; ++i)
    {
        if(!op->clients[i].is_busy)
        {
            return &op->clients[i];
        }
    }
    EML_WARN(LOG_TAG, "[op %d] no available client slot", op->id);
    return NULL; /* invalid */
}

static int _operator_add_client(operator_t* op, int client_fd)
{
    /* Get empty client slot */
    client_t* free_slot = _get_available_client_slot(op);
    if(!free_slot)
    {
        EML_ERROR(LOG_TAG, "[op %d] no free client slot found for fd %d", op->id, client_fd);
        _operator_status_update(op);
        return STATUS_FAILURE;
    }

    /* Set client's socket settings */
    if(socket_client_init(&client_fd) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] socket_client_init failed for fd %d", op->id, client_fd);
        return STATUS_FAILURE;
    }

    /* populate fd context */
    fd_ctx_t* ctx          = &free_slot->ctx;
    /* Set client fd */
    free_slot->ctx.fd      = client_fd;
    /* Set owner to this operator */
    free_slot->ctx.owner   = op;
    /* Set client's handler */
    free_slot->ctx.handler = _operator_handle_client_event;

    /* Register new client context with reactor */
    if(reactor_add_in_client(&op->reactor, client_fd, ctx) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] reactor_add_in_client failed for fd %d", op->id, client_fd);
        client_shutdown(free_slot);
        return STATUS_FAILURE;
    }

    /* populate client's slot */
    memset(free_slot->buf, 0, sizeof(free_slot->buf));
    memset(&free_slot->http_request, 0, sizeof(free_slot->http_request));
    free_slot->buf_idx           = 0u;
    free_slot->connection_policy = 0u;
    /* This is the API path's fd-adoption point (client_adopt_fd() is the equivalent for the upload
     * pool) — capture the peer once here rather than at accept() time in listener.c, because the fd
     * crosses into this thread over the SPSC dispatch ring (worker_dispatch_to_operator), which only
     * carries a bare int; a connected socket's peer address is stable for the fd's whole lifetime, so
     * reading it here is exactly equivalent (see socket_get_peer_ipv4()'s doc). */
    (void)socket_get_peer_ipv4(client_fd, &free_slot->remote_ip_be, &free_slot->remote_port_be);
    /* Set client's last activity to now */
    free_slot->last_activity     = (uint64_t)time_helper_get_now();
    /* Set client's request count */
    free_slot->request_count     = 0u;
    /* Mark client's slot as busy */
    free_slot->is_busy           = (uint8_t)1U;

    /* Increase this operator's active client count by 1 */
    atomic_fetch_add_explicit(&op->active_clients, 1U, memory_order_relaxed);

#ifdef DEBUG
    unsigned active_clients = atomic_load_explicit(&op->active_clients, memory_order_relaxed);
    EML_DBG(LOG_TAG, "[op %u] added client fd %d (active=%u)", op->id, client_fd, active_clients);
#endif /* DEBUG */

    /* Update operator's state */
    _operator_state_update(op);

    return STATUS_SUCCESS;
}

static int _operator_remove_client_by_idx(operator_t* op, unsigned int idx)
{
    /* Delete client from reactor with retries */
    int retries = 0;
    while(reactor_del(&op->reactor, op->clients[idx].ctx.fd) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] reactor_del failed fd %d (attempt %d/%d)", op->id, op->clients[idx].ctx.fd, retries + 1,
                 REACTOR_DEL_MAX_RETRIES);

        if(++retries >= REACTOR_DEL_MAX_RETRIES)
        {
            EML_ERROR(LOG_TAG, "[op %d] reactor_del failed permanently for fd %d, proceeding with shutdown", op->id,
                      op->clients[idx].ctx.fd);
            break;
        }
    }

    /* Shutdown client */
    client_shutdown(&op->clients[idx]);

    /* Decrease this operator's active client count by 1 */
    atomic_fetch_sub_explicit(&op->active_clients, 1U, memory_order_relaxed);

    return STATUS_SUCCESS;
}

static int _operator_remove_client_by_fd(operator_t* op, int client_fd)
{
    /* find client by fd */
    unsigned int idx;
    for(idx = 0; idx < op->max_clients; ++idx)
    {
        if(op->clients[idx].is_busy && op->clients[idx].ctx.fd == client_fd)
        {
            break;
        }
    }

    if(idx >= op->max_clients)
    {
        EML_ERROR(LOG_TAG, "[op %d] _remove_client_by_fd: no client slot found for fd %d", op->id, client_fd);
        return STATUS_FAILURE;
    }

    /* Delete client from reactor with retries */
    int retries = 0;
    while(reactor_del(&op->reactor, op->clients[idx].ctx.fd) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] _remove_client_by_fd: reactor_del failed fd %d (attempt %d/%d)", op->id, op->clients[idx].ctx.fd,
                 retries + 1, REACTOR_DEL_MAX_RETRIES);

        if(++retries >= REACTOR_DEL_MAX_RETRIES)
        {
            EML_ERROR(LOG_TAG, "[op %d] _remove_client_by_fd: reactor_del failed permanently for fd %d, proceeding with shutdown", op->id,
                      op->clients[idx].ctx.fd);
            break;
        }
    }

    /* Shutdown client */
    client_shutdown(&op->clients[idx]);

    /* Decrease this operator's active client count by 1 */
    atomic_fetch_sub_explicit(&op->active_clients, 1U, memory_order_relaxed);

#ifdef DEBUG
    unsigned active_clients = atomic_load_explicit(&op->active_clients, memory_order_relaxed);
    EML_DBG(LOG_TAG, "[op %u] removed client fd %d (active=%u)", op->id, client_fd, active_clients);
#endif /* DEBUG */

    _operator_state_update(op);

    return STATUS_SUCCESS;
}

static void _operator_state_update(operator_t* op)
{
    _operator_timer_update(op);

    _operator_status_update(op);
}

static int _operator_timer_init(operator_t* op)
{
    /* Create timer file descriptor */
    op->timer_fd = time_helper_init();
    if(op->timer_fd == -1)
    {
        EML_PERR(LOG_TAG, "[op %d] timerfd_create failed", op->id);
        return STATUS_FAILURE;
    }

    /* Setup context — op->timer_ctx is a struct field (operator.h), not a heap allocation: it lives
     * exactly as long as op itself, so there is nothing to free on any shutdown path. */
    fd_ctx_t* timer_ctx = &op->timer_ctx;
    timer_ctx->fd       = op->timer_fd;
    timer_ctx->owner    = op;
    timer_ctx->handler  = _operator_handle_timer_event;
    op->timer_period    = OPERATOR_TIMER_PERIOD_LONG;

    /* Start timer with idle frequency */
    if(time_helper_set(op->timer_fd, op->timer_period /* [s] */, 0 /* [ns] */) == -1)
    {
        EML_PERR(LOG_TAG, "[op %d] time_helper_set failed", op->id);
        goto fail_timer;
    }

    if(reactor_add_in(&op->reactor, op->timer_fd, timer_ctx) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] reactor_add_in failed for timer", op->id);
        goto fail_timer;
    }

    return STATUS_SUCCESS;

fail_timer:
    /* timer_ctx is op->timer_ctx now (a struct field) — nothing to free. op->timer_fd stays open
     * here; operator_init()'s own failure path always routes through operator_shutdown(), which
     * closes it (op->timer_fd != -1 still holds since we never touch it on this path). */
    return STATUS_FAILURE;
}

static int _operator_wakeup_init(operator_t* op)
{
    fd_ctx_t* wkp_ctx = &op->wakeup_ctx;
    /* Create wakeup eventfd */
    wkp_ctx->fd       = eventfd(0, EFD_SEMAPHORE | EFD_NONBLOCK);
    if(wkp_ctx->fd == -1)
    {
        EML_PERR(LOG_TAG, "[op %d] init: eventfd creation failed", op->id);
        goto fail;
    }

    /* Set owner */
    wkp_ctx->owner   = op;
    /* Set handler*/
    wkp_ctx->handler = _operator_handle_wakeup_event;
    /* Add context to reactor */
    if(reactor_add_in(&op->reactor, wkp_ctx->fd, wkp_ctx) != STATUS_SUCCESS)
    {
        EML_PERR(LOG_TAG, "[op %d] reactor_add_in wakeup failed", op->id);
        goto fail;
    }
    return STATUS_SUCCESS;

fail:
    return STATUS_FAILURE;
}

static void _operator_status_update(operator_t* op)
{
    /* Get active clients no */
    unsigned int active_clients = atomic_load_explicit(&op->active_clients, memory_order_relaxed);

    /* Update operator status */
    if(active_clients >= op->max_clients && op->status != OPERATOR_STATUS_FULL)
    {
        op->status = OPERATOR_STATUS_FULL;
#ifdef DEBUG
        EML_DBG(LOG_TAG, "[op %d] status set to FULL", op->id);
#endif /* DEBUG */
    }

    if(active_clients < op->max_clients && op->status != OPERATOR_STATUS_ACTIVE)
    {
        op->status = OPERATOR_STATUS_ACTIVE;
#ifdef DEBUG
        EML_DBG(LOG_TAG, "[op %d] status set to ACTIVE", op->id);
#endif /* DEBUG */
    }
}

static void _operator_timer_update(operator_t* op)
{
    /* Get active clients no */
    unsigned int active_clients = atomic_load_explicit(&op->active_clients, memory_order_relaxed);

    /* Check if operator's housekeeping timer has to be updated */

    /* For present clients and long period, set period to short */
    if(active_clients > 0 && op->timer_period != OPERATOR_TIMER_PERIOD_SHORT)
    {
        _operator_set_timer(op, OPERATOR_TIMER_PERIOD_SHORT);
#ifdef DEBUG
        EML_DBG(LOG_TAG, "[op %d] switched timer to short period", op->id);
#endif /* DEBUG */
    }

    /* For absent clients and short period, set period to long */
    if(active_clients == 0 && op->timer_period != OPERATOR_TIMER_PERIOD_LONG)
    {
        _operator_set_timer(op, OPERATOR_TIMER_PERIOD_LONG);
#ifdef DEBUG
        EML_DBG(LOG_TAG, "[op %d] switched timer to long period", op->id);
#endif /* DEBUG */
    }
}

static void _clean_clients(operator_t* op)
{
    uint64_t now = (uint64_t)time_helper_get_now();
    /* Must scan every slot, not just indices below active_clients: client removal
     * (client.c's client_shutdown()) frees a slot by clearing is_busy without compacting the
     * array, so a live client can sit at an index >= active_clients (e.g. slot 0 removed while
     * slot 1 stays busy drops active_clients to 1, and — worse — active_clients can shrink again
     * mid-scan as this very loop removes an earlier slot, cutting the old bound short before it
     * ever reached a later, still-expired slot in the same pass). op->max_clients (64 by default,
     * DB_SERVER_MAX_CLIENTS-overridable) per operator is small enough that a full bounded scan every
     * housekeeping tick is cheap (docs/DB_APP_MAINTENANCE.md's timeout-scanning review). */
    for(unsigned int cli_idx = 0; cli_idx < op->max_clients; cli_idx++)
    {
        if(!op->clients[cli_idx].is_busy) continue;

        uint64_t timeout = (op->clients[cli_idx].request_count == 0) ? WORKER_CLIENT_TIMEOUT_SHORT : WORKER_CLIENT_TIMEOUT_LONG;
        if((now - op->clients[cli_idx].last_activity) > timeout)
        {
#ifdef DEBUG
            EML_DBG(LOG_TAG, "[op %d] timeout closing idle fd %d (last=%lu, now=%lu, to=%lu)", op->id, op->clients[cli_idx].ctx.fd,
                    op->clients[cli_idx].last_activity, now, timeout);
#endif
            _operator_remove_client_by_idx(op, cli_idx);
            continue;
        }
    }
}

static void _operator_set_timer(operator_t* op, uint32_t period)
{
    /* check always timer socket */
    if(op->timer_fd < 0)
    {
        EML_ERROR(LOG_TAG, "[op %d] set_timer: invalid timer fd", op->id);
        return;
    }

    /* Check if not "resetted to same" by mistake */
    if(op->timer_period == period) return;

    /* Set timer */
    time_helper_set(op->timer_fd, period, 0);
    op->timer_period = period;
}
