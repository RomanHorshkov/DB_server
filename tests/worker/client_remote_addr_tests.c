/**
 * @file client_remote_addr_tests.c
 *
 * @brief Tests for the client-IP capture wiring: client.c's client_adopt_fd() (the upload-pool fd
 *        adoption path) and operator.c's _operator_add_client() (the API operator path, reached only
 *        through the real accept -> worker_dispatch_to_operator SPSC ring -> operator-thread wakeup
 *        handoff — there is no direct call to test here, by design).
 *
 * Before this wiring, client.c's _client_store_request() hardcoded http_request.remote_ip_be/
 * remote_port_be to 0 for every request — there was zero per-connection client-IP visibility anywhere
 * in this server. socket_get_peer_ipv4() (socket_helper_tests.c) is the primitive; these tests prove
 * BOTH real call sites that must invoke it actually populate client_t's remote_ip_be/remote_port_be from
 * a genuine loopback TCP connection, end to end through the real object graph (real operator thread,
 * real epoll reactor, real SPSC ring) for the operator path, matching this repo's existing white-box
 * test style (see operator_lifecycle_tests.c).
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

#define _GNU_SOURCE /* pthread_timedjoin_np — must precede every system include */

/*****************************************************************************************************************************************
 * INCLUDES
 *****************************************************************************************************************************************
 */
/* cmocka needs these four BEFORE it — keep this order. */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <stdint.h>

#include <cmocka.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <DB_http/DB_http.h>

#include <db_server/core/worker/operator/client/client.h>
#include <db_server/core/worker/operator/operator.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define TEST_RING_CAPACITY 16u
#define TEST_TIMEOUT_S     2

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

/** @brief Bind+listen a loopback TCP socket on an OS-chosen ephemeral port, connect a second socket to
 *         it, and accept() the connection — mirrors what listener.c's real accept() sees. Returns the
 *         accepted (server-side) fd; @p out_client_fd and @p out_client_port_be receive the connecting
 *         (client-side) socket's fd and bound local port (network byte order). */
static int  _make_loopback_pair(int* out_client_fd, uint16_t* out_client_port_be);
static int  _bounded_join(pthread_t thread);

static void test_client_adopt_fd_captures_ipv4_peer_address(void** state);
static void test_client_adopt_fd_unix_peer_leaves_sentinel_zero(void** state);
static void test_operator_add_client_captures_real_peer_address_via_ring(void** state);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static int _make_loopback_pair(int* out_client_fd, uint16_t* out_client_port_be)
{
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert_true(listen_fd >= 0);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    assert_int_equal(bind(listen_fd, (struct sockaddr*)&addr, sizeof addr), 0);
    assert_int_equal(listen(listen_fd, 1), 0);

    socklen_t addr_len = sizeof addr;
    assert_int_equal(getsockname(listen_fd, (struct sockaddr*)&addr, &addr_len), 0);
    const uint16_t listen_port = addr.sin_port;

    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    assert_true(client_fd >= 0);

    struct sockaddr_in connect_addr;
    memset(&connect_addr, 0, sizeof connect_addr);
    connect_addr.sin_family      = AF_INET;
    connect_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    connect_addr.sin_port        = listen_port;
    assert_int_equal(connect(client_fd, (struct sockaddr*)&connect_addr, sizeof connect_addr), 0);

    struct sockaddr_in client_local;
    socklen_t          client_local_len = sizeof client_local;
    assert_int_equal(getsockname(client_fd, (struct sockaddr*)&client_local, &client_local_len), 0);

    int server_fd = accept(listen_fd, NULL, NULL);
    assert_true(server_fd >= 0);

    close(listen_fd);

    *out_client_fd      = client_fd;
    *out_client_port_be = client_local.sin_port;
    return server_fd;
}

static int _bounded_join(pthread_t thread)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += TEST_TIMEOUT_S;
    return pthread_timedjoin_np(thread, NULL, &deadline);
}

/** @brief The upload-pool adoption path (upload_worker.c's _drive_upload() calls client_adopt_fd()
 *         exactly like this): a real loopback connection's server-side fd, adopted onto a client_t,
 *         must land in cli->remote_ip_be/remote_port_be — not the old hardcoded-0 sentinel. */
static void test_client_adopt_fd_captures_ipv4_peer_address(void** state)
{
    (void)state;

    int      client_fd;
    uint16_t client_port_be;
    int      server_fd = _make_loopback_pair(&client_fd, &client_port_be);

    DB_http_parser_t* parser = NULL;
    assert_int_equal(db_http_parser_init(&parser), DB_http_status_OK);

    client_t cli;
    memset(&cli, 0, sizeof cli);
    cli.ctx.fd      = -1;
    cli.http_parser = parser;

    client_adopt_fd(&cli, server_fd);

    assert_int_equal(cli.ctx.fd, server_fd);
    assert_int_equal(cli.remote_ip_be, htonl(INADDR_LOOPBACK));
    assert_int_equal(cli.remote_port_be, client_port_be);

    client_release_fd(&cli); /* closes server_fd */
    db_http_parser_kill(parser);
    close(client_fd);
}

/** @brief AF_UNIX (the production nginx transport, adopted by the same client_adopt_fd() when uploads
 *         run over a unix socket) has no IPv4 representation — must leave the documented 0 sentinel. */
static void test_client_adopt_fd_unix_peer_leaves_sentinel_zero(void** state)
{
    (void)state;

    int sv[2];
    assert_int_equal(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    DB_http_parser_t* parser = NULL;
    assert_int_equal(db_http_parser_init(&parser), DB_http_status_OK);

    client_t cli;
    memset(&cli, 0, sizeof cli);
    cli.ctx.fd      = -1;
    cli.http_parser = parser;

    client_adopt_fd(&cli, sv[0]);

    assert_int_equal(cli.remote_ip_be, 0u);
    assert_int_equal(cli.remote_port_be, 0u);

    client_release_fd(&cli); /* closes sv[0] */
    db_http_parser_kill(parser);
    close(sv[1]);
}

/** @brief Integration-level: the real API path. A real operator thread (real epoll reactor, real SPSC
 *         ring) receives a real accepted loopback fd exactly the way worker_dispatch_to_operator()
 *         hands one over, and _operator_add_client() must populate that client slot's remote_ip_be/
 *         remote_port_be from the genuine peer — proving the accept -> ring -> operator-thread-wakeup ->
 *         slot-population wiring, not just the client_adopt_fd() primitive in isolation. */
static void test_operator_add_client_captures_real_peer_address_via_ring(void** state)
{
    (void)state;

    operator_t op;
    memset(&op, 0, sizeof op);
    assert_int_equal(operator_init(&op, 3u, TEST_RING_CAPACITY, WORKER_MAX_CLIENTS), STATUS_SUCCESS);

    pthread_t thread;
    assert_int_equal(pthread_create(&thread, NULL, operator_thread, &op), 0);

    /* Bounded poll for the operator to leave INITIALIZING (mirrors operator_lifecycle_tests.c). */
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += TEST_TIMEOUT_S;
    operator_status_t st;
    while((st = op.status) == OPERATOR_STATUS_INITIALIZING)
    {
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        assert_true(now.tv_sec < deadline.tv_sec);
        struct timespec poll_interval = {.tv_sec = 0, .tv_nsec = 100000L};
        nanosleep(&poll_interval, NULL);
    }
    assert_int_equal(st, OPERATOR_STATUS_ACTIVE);

    int      client_fd;
    uint16_t client_port_be;
    int      server_fd = _make_loopback_pair(&client_fd, &client_port_be);

    /* Exactly what worker_dispatch_to_operator() does: push the fd onto the operator's SPSC ring, then
     * write its wakeup eventfd so the operator's own epoll loop notices it. */
    assert_int_equal(spsc_ring_push(op.ring, server_fd), 0);
    uint64_t wakeup_var = 1U;
    assert_int_equal(write(op.wakeup_ctx.fd, &wakeup_var, sizeof wakeup_var), (ssize_t)sizeof wakeup_var);

    /* Bounded poll for the client to land in a busy slot. */
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    deadline.tv_sec += TEST_TIMEOUT_S;
    client_t* found = NULL;
    while(!found)
    {
        for(size_t i = 0; i < WORKER_MAX_CLIENTS; ++i)
        {
            if(op.clients[i].is_busy && op.clients[i].ctx.fd == server_fd)
            {
                found = &op.clients[i];
                break;
            }
        }
        if(found) break;

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        assert_true(now.tv_sec < deadline.tv_sec);
        struct timespec poll_interval = {.tv_sec = 0, .tv_nsec = 1000000L};
        nanosleep(&poll_interval, NULL);
    }

    assert_int_equal(found->remote_ip_be, htonl(INADDR_LOOPBACK));
    assert_int_equal(found->remote_port_be, client_port_be);

    operator_request_shutdown(&op);
    assert_int_equal(_bounded_join(thread), 0);
    operator_shutdown(&op); /* closes server_fd via client_shutdown() */

    close(client_fd);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_client_adopt_fd_captures_ipv4_peer_address),
        cmocka_unit_test(test_client_adopt_fd_unix_peer_leaves_sentinel_zero),
        cmocka_unit_test(test_operator_add_client_captures_real_peer_address_via_ring),
    };
    return cmocka_run_group_tests_name("client_remote_addr", tests, NULL, NULL);
}
