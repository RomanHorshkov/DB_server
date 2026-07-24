/**
 * @file socket_helper_tests.c
 *
 * @brief Unit tests for socket_helper.c's socket_get_peer_ipv4() — the peer-address capture that
 *        replaces the previous "listener.c/client.c never learn the client's IP" gap (remote_ip_be /
 *        remote_port_be were hardcoded to 0 in client.c's _client_store_request()).
 *
 * socket_get_peer_ipv4() deliberately reads the peer off an already-connected fd via getpeername()
 * rather than requiring an accept()-time sockaddr: the fd crosses into an operator/upload-worker thread
 * over a plain-int SPSC ring / queue, so there is nowhere to carry a sockaddr through that handoff. A
 * connected socket's peer address is stable for the fd's whole lifetime, so reading it here (at
 * fd-adoption time, in client.c/operator.c) is exactly equivalent to reading it at accept() time — these
 * tests exercise that equivalence directly over a real loopback TCP connection.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

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
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <db_server/core/config.h>
#include <db_server/utils/socket_helper.h>

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

/** @brief Bind+listen a loopback TCP socket on an OS-chosen ephemeral port, connect a second socket to
 *         it, and accept() the connection — mirrors what listener.c's real accept() sees, without
 *         needing the full server binary. Returns the accepted (server-side) fd; @p out_client_fd and
 *         @p out_client_port_be receive the connecting (client-side) socket's fd and bound local port
 *         (network byte order) so the test can assert socket_get_peer_ipv4() reports exactly that. */
static int _make_loopback_pair(int* out_client_fd, uint16_t* out_client_port_be);

static void test_get_peer_ipv4_reports_real_loopback_peer(void** state);
static void test_get_peer_ipv4_unix_peer_leaves_sentinel_zero(void** state);
static void test_get_peer_ipv4_null_outputs_fail(void** state);
static void test_get_peer_ipv4_closed_fd_leaves_sentinel_zero(void** state);

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
    addr.sin_port        = 0; /* OS-chosen ephemeral port */
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

    *out_client_fd       = client_fd;
    *out_client_port_be  = client_local.sin_port;
    return server_fd;
}

/** @brief The property the whole client-IP arc exists for: a real loopback connection's server-side fd
 *         reports the exact peer address/port the connecting socket used, in network byte order. */
static void test_get_peer_ipv4_reports_real_loopback_peer(void** state)
{
    (void)state;

    int      client_fd;
    uint16_t client_port_be;
    int      server_fd = _make_loopback_pair(&client_fd, &client_port_be);

    uint32_t ip_be   = 0xdeadbeefu;
    uint16_t port_be = 0xdeadu;
    assert_int_equal(socket_get_peer_ipv4(server_fd, &ip_be, &port_be), STATUS_SUCCESS);

    assert_int_equal(ip_be, htonl(INADDR_LOOPBACK));
    assert_int_equal(port_be, client_port_be);

    close(server_fd);
    close(client_fd);
}

/** @brief AF_UNIX has no IPv4 representation — DB_http_request_t.remote_ip_be is a uint32_t by
 *         contract, so a unix-domain peer (the production nginx transport) must leave the 0 sentinel,
 *         not a garbage cast of sockaddr_un. This is the exact case listener.c's unix-socket path hits
 *         on every real production connection. */
static void test_get_peer_ipv4_unix_peer_leaves_sentinel_zero(void** state)
{
    (void)state;

    int sv[2];
    assert_int_equal(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    uint32_t ip_be   = 0xdeadbeefu;
    uint16_t port_be = 0xdeadu;
    assert_int_equal(socket_get_peer_ipv4(sv[0], &ip_be, &port_be), STATUS_SUCCESS);

    assert_int_equal(ip_be, 0u);
    assert_int_equal(port_be, 0u);

    close(sv[0]);
    close(sv[1]);
}

static void test_get_peer_ipv4_null_outputs_fail(void** state)
{
    (void)state;

    uint32_t ip_be   = 0u;
    uint16_t port_be = 0u;
    assert_int_equal(socket_get_peer_ipv4(0, NULL, &port_be), STATUS_FAILURE);
    assert_int_equal(socket_get_peer_ipv4(0, &ip_be, NULL), STATUS_FAILURE);
}

/** @brief getpeername() on a bad/closed fd is a normal, expected case (a racing close, a test double) —
 *         not a reason to fail the connection, so the sentinel stays 0 and the call still reports
 *         success (best-effort metadata, per socket_get_peer_ipv4()'s doc). */
static void test_get_peer_ipv4_closed_fd_leaves_sentinel_zero(void** state)
{
    (void)state;

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert_true(fd >= 0);
    close(fd); /* now guaranteed invalid — never connected, and closed besides */

    uint32_t ip_be   = 0xdeadbeefu;
    uint16_t port_be = 0xdeadu;
    assert_int_equal(socket_get_peer_ipv4(fd, &ip_be, &port_be), STATUS_SUCCESS);
    assert_int_equal(ip_be, 0u);
    assert_int_equal(port_be, 0u);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_get_peer_ipv4_reports_real_loopback_peer),
        cmocka_unit_test(test_get_peer_ipv4_unix_peer_leaves_sentinel_zero),
        cmocka_unit_test(test_get_peer_ipv4_null_outputs_fail),
        cmocka_unit_test(test_get_peer_ipv4_closed_fd_leaves_sentinel_zero),
    };
    return cmocka_run_group_tests_name("socket_helper_peer_ipv4", tests, NULL, NULL);
}
