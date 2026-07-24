/**
 * @file listener.c
 * @brief TCP listener implementation for accepting incoming client connections.
 *
 * This module manages the server's listening sockets across multiple address families (IPv4/IPv6). It uses a reactor pattern to monitor
 * accept events and forwards new client connections to the worker pipeline for processing.
 *
 * @author  Roman Horshkov <124358264+RomanHorshkov@users.noreply.github.com>
 * @date    2025
 */

#define _GNU_SOURCE

#include <db_server/core/listener/listener.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <db_server/core/config_core.h>
#include <emlog.h>
#include <db_server/core/reactor.h>
#include <db_server/utils/affinity.h>
#include <db_server/utils/socket_helper.h>
#include <db_server/core/worker/worker.h>
#include <db_server/core/worker/upload_worker.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG "srv_listener"

/*****************************************************************************************************************************************
 * PRIVATE STRUCTURED VARIABLES
 *****************************************************************************************************************************************
 */

/**
 * @brief Internal listener state structure.
 *
 * Manages the lifecycle of listening sockets, reactor instance, and pipeline integration for accepting and forwarding client connections.
 */
typedef struct
{
    listener_status_t status;                                         /**< Current listener status */
    reactor_t         reactor;                                        /**< Reactor for monitoring accept events */
    char              spec[108];                                      /**< API listen spec: a TCP port ("3490") OR a unix path ("/run/…") */
    char              upload_spec[108];                               /**< Upload listen spec (unix path); "" = uploads handled in operators */
    char              unix_paths[2][108];                             /**< bound AF_UNIX paths, unlinked on stop() */
    uint8_t           unix_paths_no;                                  /**< how many entries of unix_paths[] are live */
    int               sockets_fds[SERVER_CORE_MAX_LISTENING_SOCKETS]; /**< Array of listening socket FDs */
    uint8_t           socket_is_upload[SERVER_CORE_MAX_LISTENING_SOCKETS]; /**< per-fd: 1 = upload listener → the pool; 0 = API → operators */
    uint32_t          active_sockets_no;                              /**< Number of active listening sockets */
    // pipeline_t *pipeline;                                        /**< Pointer to worker pipeline */
} listener_t;

static listener_t _listener = {0};

/**
 * @brief Shutdown-signal eventfd (SIGTERM/SIGINT self-pipe trick). File-scope, not a listener_t
 * member: the signal handler below is only async-signal-safe if it touches nothing but this raw fd —
 * write(2) is on the POSIX async-signal-safe list, struct field access through a pointer chain is
 * not guaranteed to be. -1 when no handler is installed (dev/test binaries that never call
 * _install_shutdown_signal_handler()).
 */
static volatile int _shutdown_efd = -1;

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

/**
 * @brief Initialize listening sockets for the given port.
 *
 * Creates and binds TCP listening sockets for all available address families (IPv4 and IPv6), configures socket options, and stores file
 * descriptors in the listener state.
 *
 * @param[in] port Port number to bind to (as string).
 * @retval STATUS_SUCCESS At least one listening socket was successfully created.
 * @retval STATUS_FAILURE No listening sockets could be initialized.
 */
static int _init_listening_sockets(const char* port, uint8_t is_upload);

/**
 * @brief Register all listening sockets with the reactor.
 *
 * Allocates context structures for each listening socket and registers them with the reactor for monitoring EPOLLIN events (incoming
 * connections).
 *
 * @retval STATUS_SUCCESS All listening sockets registered successfully.
 * @retval STATUS_FAILURE Failed to register one or more sockets.
 */
static int _register_listening_sockets(void);

/**
 * @brief Handle accept events on a listening socket.
 *
 * Called by the reactor when a new connection is available. Accepts the client connection, initializes the client socket, and pushes it to
 * the worker pipeline.
 *
 * @param[in] fd       File descriptor of the listening socket.
 * @param[in] ctx      Context associated with the listening socket.
 * @retval STATUS_SUCCESS Client connection accepted and queued successfully.
 * @retval STATUS_FAILURE Failed to accept or queue the connection.
 */
static int _handle_listen_event(int fd, fd_ctx_t* ctx);

/**
 * @brief accept()/accept4() failed with EMFILE/ENFILE (this process's fd table is exhausted). Logs
 *        once per exhaustion streak (not once per epoll spin — the listening socket stays readable,
 *        level-triggered, until something ELSE frees an fd, which could be seconds away under real
 *        exhaustion) and sleeps briefly so the listener thread backs off instead of busy-spinning
 *        epoll_wait() at 100% CPU with no way to service the ready event.
 * @param what Short label for the log line ("API accept" / "upload accept").
 */
static void _backoff_on_fd_exhaustion(const char* what);

/**
 * @brief Handle accept events on the UPLOAD listening socket.
 *
 * Accepts the connection and hands it to the dedicated upload worker pool. On pool saturation, answers a minimal
 * 503 and closes — this is the per-server upload concurrency cap and can never starve the API operators.
 *
 * @param[in] fd  File descriptor of the upload listening socket.
 * @param[in] ctx Context associated with the listening socket.
 * @retval STATUS_SUCCESS Connection accepted and queued.
 * @retval STATUS_FAILURE Failed to accept, or the pool rejected the connection.
 */
static int _handle_upload_listen_event(int fd, fd_ctx_t* ctx);

/**
 * @brief Stop the listener and close all listening sockets.
 *
 * Closes all active listening socket file descriptors and resets the listener state.
 *
 * @param[in,out] l Pointer to the listener structure.
 */
static void _stop_listener(listener_t* l);

/**
 * @brief Reactor callback for the shutdown eventfd: drains it and flips the listener to SHUTDOWN so
 *        listener_run()'s loop exits on its next status check — the same "stop accepting NEW
 *        connections, then unwind" contract LISTENER_STATUS_SHUTDOWN already documented but nothing
 *        ever actually triggered (there was no SIGTERM/SIGINT handling at all before this).
 */
static int _handle_shutdown_event(int fd, fd_ctx_t* ctx);

/**
 * @brief The actual signal handler (SIGTERM/SIGINT). Async-signal-safe by construction: touches
 *        nothing but a raw fd via write(2) (self-pipe/eventfd trick) — no logging, no struct access,
 *        no malloc. The reactor thread observes the write via _handle_shutdown_event above.
 */
static void _sigterm_handler(int sig);

/**
 * @brief Create the shutdown eventfd, register it with the listener's reactor, and install the
 *        SIGTERM/SIGINT handlers. Called once, at the end of a successful listener_init()/
 *        listener_init_activated(). Non-fatal on failure (logged; the server still runs, just without
 *        a graceful signal-triggered drain — matches this project's general "log and continue" stance
 *        on strictly-additive hardening, never regressing a listener that otherwise initialized fine).
 */
static void _install_shutdown_signal_handler(listener_t* l);

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int listener_init(const char* api_spec, const char* upload_spec)
{
    if(!api_spec || api_spec[0] == '\0')
    {
        EML_ERROR(LOG_TAG, "listener_init: invalid input");
        return STATUS_FAILURE;
    }

    strncpy(_listener.spec, api_spec, sizeof(_listener.spec) - 1);
    if(upload_spec && upload_spec[0] != '\0')
    {
        strncpy(_listener.upload_spec, upload_spec, sizeof(_listener.upload_spec) - 1);
    }

    /* Init listener's reactor */
    if(reactor_init(&_listener.reactor) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener_init: reactor_init failed");
        return STATUS_FAILURE;
    }

    /* Initialize the API listening socket(s) */
    if(_init_listening_sockets(_listener.spec, 0u) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener_init: API socket init failed");
        reactor_shutdown(&_listener.reactor);
        return STATUS_FAILURE;
    }

    /* Initialize the dedicated UPLOAD listening socket, if configured. Uploads
     * accepted here go to the upload worker pool instead of the API operators,
     * so a slow upload can never head-of-line-block API traffic. */
    if(_listener.upload_spec[0] != '\0')
    {
        if(_init_listening_sockets(_listener.upload_spec, 1u) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "listener_init: upload socket init failed");
            _stop_listener(&_listener);
            reactor_shutdown(&_listener.reactor);
            return STATUS_FAILURE;
        }
    }

    /* register listening sockets to reactor */
    if(_register_listening_sockets() != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener_init: register sockets failed");
        _stop_listener(&_listener);
        reactor_shutdown(&_listener.reactor);
        return STATUS_FAILURE;
    }

    _install_shutdown_signal_handler(&_listener);

    _listener.status = LISTENER_STATUS_ACTIVE;
    return STATUS_SUCCESS;
}

int listener_init_activated(const sd_listen_set_t* fds)
{
    if(!fds || fds->api_fd < 0)
    {
        EML_ERROR(LOG_TAG, "listener_init_activated: no API socket");
        return STATUS_FAILURE;
    }

    if(reactor_init(&_listener.reactor) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener_init_activated: reactor_init failed");
        return STATUS_FAILURE;
    }

    /* Adopt the systemd-provided fds directly. They are already listening, non-blocking, and CLOEXEC
     * (sd_take_listen_fds validated them). We record NO unix_paths[] entry: systemd created the socket
     * files and owns their removal (RemoveOnStop=yes) — the backend must never unlink them. */
    _listener.socket_is_upload[_listener.active_sockets_no] = 0u;
    _listener.sockets_fds[_listener.active_sockets_no++]    = fds->api_fd;
    if(fds->upload_fd >= 0)
    {
        _listener.socket_is_upload[_listener.active_sockets_no] = 1u;
        _listener.sockets_fds[_listener.active_sockets_no++]    = fds->upload_fd;
    }

    if(_register_listening_sockets() != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener_init_activated: register sockets failed");
        _stop_listener(&_listener);
        reactor_shutdown(&_listener.reactor);
        return STATUS_FAILURE;
    }

    EML_INFO(LOG_TAG, "listener: socket-activated (api fd=%d, upload fd=%d) — no bind/chmod/unlink", fds->api_fd, fds->upload_fd);

    _install_shutdown_signal_handler(&_listener);

    _listener.status = LISTENER_STATUS_ACTIVE;
    return STATUS_SUCCESS;
}

uint8_t listener_upload_active(void)
{
    for(uint32_t i = 0; i < _listener.active_sockets_no; ++i)
    {
        if(_listener.socket_is_upload[i]) return 1u;
    }
    return 0u;
}

void* listener_run(void* arg)
{
    (void)arg;

    /* The listener/dispatcher owns core 0 (slot 0); operators own 1.. . */
    srv_affinity_pin_self("listener", 0);

    while(_listener.status == LISTENER_STATUS_ACTIVE)
    {
        if(reactor_run(&_listener.reactor, NULL) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "reactor_run failed");
        }
    }

    _stop_listener(&_listener);
    reactor_shutdown(&_listener.reactor);
    return NULL;
}

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

/**
 * @brief Bind an AF_UNIX stream listener at @p path — the production nginx↔backend transport.
 *
 * A unix socket removes the loopback TCP port entirely (no backend port to bind, expose, or scan). The socket is created 0660 so nginx
 * (www-data, a member of the home_server group) can connect, while nothing off-box ever can. A stale socket from a previous run is
 * unlinked first. The accepted fds flow through the exact same byte pipeline as TCP — accept() never reads the peer address.
 */
static int _init_unix_socket(const char* path, uint8_t is_upload)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if(fd == -1)
    {
        EML_PERR(LOG_TAG, "listener: AF_UNIX socket() failed");
        return STATUS_FAILURE;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof addr);
    addr.sun_family = AF_UNIX;
    if(strlen(path) >= sizeof addr.sun_path)
    {
        EML_ERROR(LOG_TAG, "listener: unix socket path too long (%s)", path);
        close(fd);
        return STATUS_FAILURE;
    }
    strncpy(addr.sun_path, path, sizeof addr.sun_path - 1);

    (void)unlink(path); /* a stale socket from a prior run is not an error */

    if(bind(fd, (const struct sockaddr*)&addr, (socklen_t)sizeof addr) == -1)
    {
        EML_PERR(LOG_TAG, "listener: bind(%s) failed", path);
        close(fd);
        return STATUS_FAILURE;
    }
    /* 0660: owner (home_server) + group (home_server, which nginx's www-data joins) — never world. */
    if(chmod(path, 0660) == -1)
    {
        EML_PERR(LOG_TAG, "listener: chmod(%s, 0660) failed", path);
        close(fd);
        (void)unlink(path);
        return STATUS_FAILURE;
    }
    if(listen(fd, SERVER_CORE_MAX_PENDING_SOCKETS_PER_LISTENER) == -1)
    {
        EML_PERR(LOG_TAG, "listener: listen(%s) failed", path);
        close(fd);
        (void)unlink(path);
        return STATUS_FAILURE;
    }
    if(socket_set_non_blocking(&fd) != STATUS_SUCCESS)
    {
        close(fd);
        (void)unlink(path);
        return STATUS_FAILURE;
    }

    _listener.socket_is_upload[_listener.active_sockets_no] = is_upload;
    _listener.sockets_fds[_listener.active_sockets_no++]    = fd;
    if(_listener.unix_paths_no < (uint8_t)(sizeof(_listener.unix_paths) / sizeof(_listener.unix_paths[0])))
    {
        strncpy(_listener.unix_paths[_listener.unix_paths_no], path, sizeof(_listener.unix_paths[0]) - 1);
        _listener.unix_paths_no++;
    }
    EML_INFO(LOG_TAG, "listener: bound AF_UNIX %s (0660, no TCP port) — %s", path, is_upload ? "UPLOAD pool" : "API operators");
    return STATUS_SUCCESS;
}

static int _init_listening_sockets(const char* port, uint8_t is_upload)
{
    struct addrinfo  hints;
    struct addrinfo* ai = NULL;

    /* A spec that begins with '/' is a unix socket PATH (the production transport);
     * anything else is a TCP port (dev, tests, direct-LAN trials). */
    if(port[0] == '/')
    {
        return _init_unix_socket(port, is_upload);
    }

    if(socket_listener_set_hints(&hints) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener: hints setup failed");
        return STATUS_FAILURE;
    }

    /* Security default: bind LOOPBACK ONLY. In production nginx faces the
     * network and proxies to us on 127.0.0.1 (SECURITY.md §1 — the C backend
     * is never directly exposed). DB_SERVER_BIND overrides the host for
     * deliberate direct access (e.g. "0.0.0.0" for a LAN dev trial without
     * nginx); doing so is an explicit, logged choice, never the default. */
    const char* bind_host = getenv("DB_SERVER_BIND");
    if(!bind_host || bind_host[0] == '\0')
    {
        bind_host = "127.0.0.1";
    }
    if(strcmp(bind_host, "127.0.0.1") != 0 && strcmp(bind_host, "::1") != 0 && strcmp(bind_host, "localhost") != 0)
    {
        EML_WARN(LOG_TAG, "listener: binding NON-LOOPBACK host '%s' — the backend is directly network-exposed (nginx should front it)",
                 bind_host);
    }
    EML_INFO(LOG_TAG, "listener: binding %s:%s", bind_host, port);

    const int gai_rc = getaddrinfo(bind_host, port, &hints, &ai);
    if(gai_rc != 0)
    {
        EML_ERROR(LOG_TAG, "listener: getaddrinfo(%s:%s) failed: %s", bind_host, port, gai_strerror(gai_rc));
        return STATUS_FAILURE;
    }

    for(const struct addrinfo* cur = ai; cur != NULL; cur = cur->ai_next)
    {
        if(_listener.active_sockets_no >= SERVER_CORE_MAX_LISTENING_SOCKETS)
        {
            EML_WARN(LOG_TAG, "listener: max listening sockets reached (%d)", SERVER_CORE_MAX_LISTENING_SOCKETS);
            break;
        }

        int fd = socket(cur->ai_family, cur->ai_socktype, cur->ai_protocol);
        if(fd == -1)
        {
            EML_PERR(LOG_TAG, "listener: socket creation failed");
            continue;
        }

        if(socket_listener_init(&fd, &cur->ai_family) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "listener: socket_listener_init failed");
            close(fd);
            continue;
        }

        if(bind(fd, cur->ai_addr, cur->ai_addrlen) == -1)
        {
            EML_PERR(LOG_TAG, "listener: bind failed");
            close(fd);
            continue;
        }

        if(listen(fd, SERVER_CORE_MAX_PENDING_SOCKETS_PER_LISTENER) == -1)
        {
            EML_PERR(LOG_TAG, "listener: listen failed");
            close(fd);
            continue;
        }

        _listener.socket_is_upload[_listener.active_sockets_no] = is_upload;
        _listener.sockets_fds[_listener.active_sockets_no++]    = fd;

#ifdef DEBUG
        char        ip_str[INET6_ADDRSTRLEN];
        void*       addr  = NULL;
        const char* ipver = "";
        if(cur->ai_family == AF_INET)
        {
            struct sockaddr_in* ipv4 = (struct sockaddr_in*)cur->ai_addr;
            addr                     = &(ipv4->sin_addr);
            ipver                    = "IPv4";
        }
        else if(cur->ai_family == AF_INET6)
        {
            struct sockaddr_in6* ipv6 = (struct sockaddr_in6*)cur->ai_addr;
            addr                      = &(ipv6->sin6_addr);
            ipver                     = "IPv6";
        }
        if(addr)
        {
            inet_ntop(cur->ai_family, addr, ip_str, sizeof(ip_str));
            EML_DBG(LOG_TAG, "listening on %s:%s", ip_str, port);
        }
        else
        {
            EML_DBG(LOG_TAG, "listening socket created (%s)", ipver);
        }
#endif /* DEBUG */
    }

    freeaddrinfo(ai);
    return (_listener.active_sockets_no > 0) ? STATUS_SUCCESS : STATUS_FAILURE;
}

static int _register_listening_sockets(void)
{
    for(uint32_t i = 0; i < _listener.active_sockets_no; ++i)
    {
        int       fd  = _listener.sockets_fds[i];
        fd_ctx_t* ctx = calloc(1, sizeof(*ctx));
        if(!ctx)
        {
            EML_ERROR(LOG_TAG, "listener: context allocation failed");
            return STATUS_FAILURE;
        }
        ctx->fd      = fd;
        ctx->owner   = &_listener;
        ctx->handler = _listener.socket_is_upload[i] ? _handle_upload_listen_event : _handle_listen_event;

        if(reactor_add_in(&_listener.reactor, fd, ctx) != STATUS_SUCCESS)
        {
            EML_PERR(LOG_TAG, "listener: reactor_add_in failed for fd %d", fd);
            free(ctx);
            return STATUS_FAILURE;
        }
    }

    return STATUS_SUCCESS;
}

/* Set once accept()/accept4() reports EMFILE/ENFILE, cleared the next time either listener's accept
 * succeeds — the "log once per streak" latch _backoff_on_fd_exhaustion() relies on. */
static volatile sig_atomic_t _fd_exhaustion_warned = 0;

static void _backoff_on_fd_exhaustion(const char* what)
{
    if(!_fd_exhaustion_warned)
    {
        EML_WARN(LOG_TAG, "%s: process fd table exhausted (EMFILE/ENFILE) — backing off until fds free up", what);
        _fd_exhaustion_warned = 1;
    }
    /* Bounded sleep: the listening socket stays EPOLLIN-ready (level-triggered) with nothing this
     * thread can do about it, so returning immediately would busy-spin epoll_wait() at 100% CPU for
     * as long as the exhaustion lasts. 10ms caps that to near-zero without meaningfully delaying
     * accepts once fds free up again. */
    struct timespec backoff = {.tv_sec = 0, .tv_nsec = 10000000L};
    nanosleep(&backoff, NULL);
}

static int _handle_listen_event(int fd, fd_ctx_t* ctx)
{
    (void)ctx;
#ifdef DEBUG
    EML_DBG(LOG_TAG, "listen event on fd %d", fd);
#endif /* DEBUG */

    int client_fd = accept(fd, NULL, NULL);
    if(client_fd < 0)
    {
        if(errno == EMFILE || errno == ENFILE)
        {
            _backoff_on_fd_exhaustion("API accept");
            return STATUS_SUCCESS; /* transient — retried on the next epoll_wait, not a reactor error */
        }
        EML_PERR(LOG_TAG, "accept failed");
        return STATUS_FAILURE;
    }
    _fd_exhaustion_warned = 0;

    if(worker_dispatch_to_operator(client_fd) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "worker_dispatch_to_operator failed");
        goto fail;
    }

    // if(pipeline_push(client_fd) != STATUS_SUCCESS)
    // {
    //     EML_PERR(LOG_TAG, "pipeline_push failed");
    //     socket_shutdown_and_close(client_fd);
    //     return STATUS_FAILURE;
    // }

    return STATUS_SUCCESS;

fail:
    socket_shutdown_and_close(client_fd);
    return STATUS_FAILURE;
}

/* Prebuilt saturation response — sent in one non-blocking write so the listener never waits on a rejected peer. */
static const char _UPLOAD_BUSY_503[] = "HTTP/1.1 503 Service Unavailable\r\n"
                                       "Content-Type: application/json\r\n"
                                       "Content-Length: 23\r\n"
                                       "Retry-After: 10\r\n"
                                       "Connection: close\r\n"
                                       "\r\n"
                                       "{\"error\":\"upload_busy\"}";

static int _handle_upload_listen_event(int fd, fd_ctx_t* ctx)
{
    (void)ctx;

    int client_fd = accept4(fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if(client_fd < 0)
    {
        /* EWOULDBLOCK == EAGAIN on Linux (testing both trips -Wlogical-op). */
        if(errno == EAGAIN)
        {
            return STATUS_SUCCESS; /* spurious wakeup / already drained */
        }
        if(errno == EMFILE || errno == ENFILE)
        {
            _backoff_on_fd_exhaustion("upload accept");
            return STATUS_SUCCESS; /* transient — retried on the next epoll_wait, not a reactor error */
        }
        EML_PERR(LOG_TAG, "upload accept failed");
        return STATUS_FAILURE;
    }
    _fd_exhaustion_warned = 0;

    if(upload_worker_dispatch(client_fd) == 0)
    {
        return STATUS_SUCCESS; /* queued; the pool owns the fd now */
    }

    /* Pool saturated: answer 503 without ever blocking the listener, then close. */
    (void)send(client_fd, _UPLOAD_BUSY_503, sizeof _UPLOAD_BUSY_503 - 1u, MSG_NOSIGNAL | MSG_DONTWAIT);
    socket_shutdown_and_close(client_fd);
    return STATUS_SUCCESS;
}

static int _handle_shutdown_event(int fd, fd_ctx_t* ctx)
{
    (void)ctx;
    socket_drain(fd);
    EML_WARN(LOG_TAG, "shutdown signal received — draining: no new connections will be accepted");
    _listener.status = LISTENER_STATUS_SHUTDOWN;
    return STATUS_SUCCESS;
}

static void _sigterm_handler(int sig)
{
    (void)sig;
    /* Async-signal-safe: write(2) on a raw fd, nothing else. A signal arriving before the eventfd
     * exists (can't happen — the handler is only installed after the eventfd is created and
     * registered) or after _stop_listener() already closed it (possible on a second SIGTERM racing
     * teardown) both degrade to a harmless EBADF, ignored here for the same async-signal-safety
     * reason nothing else in this handler checks a return value. */
    int fd = _shutdown_efd;
    if(fd >= 0)
    {
        uint64_t one = 1U;
        ssize_t  w   = write(fd, &one, sizeof one);
        (void)w;
    }
}

static void _install_shutdown_signal_handler(listener_t* l)
{
    int efd = eventfd(0, EFD_NONBLOCK);
    if(efd == -1)
    {
        EML_PERR(LOG_TAG, "listener: shutdown eventfd creation failed — SIGTERM/SIGINT will terminate abruptly");
        return;
    }

    fd_ctx_t* ctx = calloc(1, sizeof(*ctx));
    if(!ctx)
    {
        EML_ERROR(LOG_TAG, "listener: shutdown context allocation failed — SIGTERM/SIGINT will terminate abruptly");
        close(efd);
        return;
    }
    ctx->fd      = efd;
    ctx->owner   = l;
    ctx->handler = _handle_shutdown_event;

    if(reactor_add_in(&l->reactor, efd, ctx) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "listener: shutdown eventfd registration failed — SIGTERM/SIGINT will terminate abruptly");
        free(ctx);
        close(efd);
        return;
    }

    _shutdown_efd = efd;

    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = _sigterm_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART; /* don't make every blocking libc call in other threads handle EINTR */
    (void)sigaction(SIGTERM, &sa, NULL);
    (void)sigaction(SIGINT, &sa, NULL);

    EML_INFO(LOG_TAG, "listener: SIGTERM/SIGINT install a graceful drain (stop accepting, finish in-flight)");
}

static void _stop_listener(listener_t* l)
{
    if(!l) return;

    /* Restore default disposition and stop routing the signal through a (soon-to-be-closed) fd —
     * a second SIGTERM after this point terminates the process immediately, which is the correct
     * fallback once the graceful path has already run (or never got the chance to). */
    struct sigaction dfl;
    memset(&dfl, 0, sizeof dfl);
    dfl.sa_handler = SIG_DFL;
    sigemptyset(&dfl.sa_mask);
    (void)sigaction(SIGTERM, &dfl, NULL);
    (void)sigaction(SIGINT, &dfl, NULL);
    if(_shutdown_efd >= 0)
    {
        close(_shutdown_efd);
        _shutdown_efd = -1;
    }

    for(uint32_t i = 0; i < l->active_sockets_no; ++i)
    {
        if(l->sockets_fds[i] > 0)
        {
            socket_shutdown_and_close(l->sockets_fds[i]);
            l->sockets_fds[i] = -1;
        }
    }
    l->active_sockets_no = 0;

    /* Remove any AF_UNIX socket files so a restart binds cleanly (bind also
     * unlinks a stale one, but leaving them around is untidy). */
    for(uint8_t i = 0; i < l->unix_paths_no; ++i)
    {
        if(l->unix_paths[i][0] != '\0')
        {
            (void)unlink(l->unix_paths[i]);
            l->unix_paths[i][0] = '\0';
        }
    }
    l->unix_paths_no = 0;
}
