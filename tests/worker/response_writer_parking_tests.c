/**
 * @file response_writer_parking_tests.c
 *
 * @brief Unit tests for response_writer.c's EPOLLOUT parking (§9.2 end state): a slow/backpressured
 *        client must not block the operator thread — the send parks (cli->draining, fd re-armed for
 *        EPOLLOUT via the real reactor) and response_writer_resume() finishes it once the peer drains.
 *
 * Backpressure is forced deterministically, not via timing: a real AF_UNIX socketpair with SO_SNDBUF
 * clamped tiny on the "client" side, sent a response bigger than that buffer, with the test-controlled
 * peer never reading — guaranteed EAGAIN, no race, no sleep-and-hope.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

#define _GNU_SOURCE /* memmem — must precede every system include */

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

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <db_server/core/worker/operator/client/response_writer.h>
#include <db_server/core/worker/operator/operator.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

/* Small enough that a several-KB body can never fit in one write(); the test never reads from the
 * peer end, so every write past this fills the kernel buffer and EAGAINs — deterministic backpressure. */
#define TINY_SNDBUF  256

#define BODY_LEN     20000u /* comfortably under DB_HTTP_MAX_BUFFER_LEN_B (32 KiB) with headers */
#define RING_CAP     16u

/*****************************************************************************************************************************************
 * PRIVATE STRUCTURED TYPEDEFS
 *****************************************************************************************************************************************
 */

typedef struct
{
    operator_t op;
    client_t   cli;
    int        peer_fd; /* the test's end of the socketpair — the "far side" of cli.ctx.fd */
    char*      body;
} fixture_t;

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS PROTOTYPES
 *****************************************************************************************************************************************
 */

static int  _setup(void** state);
static int  _teardown(void** state);
static ssize_t _read_spin(int fd, char* buf, size_t cap);

static void test_send_completes_synchronously_when_socket_has_room(void** state);
static void test_send_parks_on_backpressure_then_resume_finishes_it_intact(void** state);
static void test_resume_without_draining_is_rejected(void** state);
static void test_park_close_policy_deferred_until_resume_completes(void** state);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static int _setup(void** state)
{
    fixture_t* f = calloc(1, sizeof(*f));
    assert_non_null(f);

    assert_int_equal(operator_init(&f->op, 0u, RING_CAP, WORKER_MAX_CLIENTS), STATUS_SUCCESS);

    int sv[2];
    assert_int_equal(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    /* cli.ctx.fd is the "server" side response_writer.c writes to; peer_fd is what the test reads to
     * simulate a client draining its TCP receive buffer (or doesn't, to force backpressure). Both
     * non-blocking: peer_fd's reads must never block on "buffer temporarily empty" (which read() can't
     * distinguish from EOF while the socket stays open — nobody closes it mid-test), only on a real
     * EAGAIN the test explicitly checks for. */
    int flags0 = fcntl(sv[0], F_GETFL, 0);
    assert_int_not_equal(fcntl(sv[0], F_SETFL, flags0 | O_NONBLOCK), -1);
    int flags1 = fcntl(sv[1], F_GETFL, 0);
    assert_int_not_equal(fcntl(sv[1], F_SETFL, flags1 | O_NONBLOCK), -1);

    int tiny = TINY_SNDBUF;
    assert_int_equal(setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &tiny, sizeof tiny), 0);

    memset(&f->cli, 0, sizeof f->cli);
    f->cli.ctx.fd            = sv[0];
    f->cli.ctx.owner         = &f->op;
    f->cli.is_busy            = 1u;
    f->cli.connection_policy = (uint8_t)HTTP_CONNECTION_KEEP_ALIVE;
    assert_int_equal(reactor_add_in_client(&f->op.reactor, sv[0], &f->cli.ctx), STATUS_SUCCESS);

    f->peer_fd = sv[1];
    f->body    = malloc(BODY_LEN);
    assert_non_null(f->body);
    memset(f->body, 'A', BODY_LEN);

    *state = f;
    return 0;
}

static int _teardown(void** state)
{
    fixture_t* f = *state;
    if(f)
    {
        if(f->cli.ctx.fd >= 0)
        {
            close(f->cli.ctx.fd);
        }
        close(f->peer_fd);
        operator_shutdown(&f->op);
        free(f->body);
        free(f);
    }
    return 0;
}

/** @brief Spin-retry a non-blocking read for bytes the test KNOWS are coming (already written on the
 *         other end before this is called) — bounded, so a genuine regression fails loudly instead of
 *         hanging the binary the way a blocking read() would if the expected bytes never showed up. */
static ssize_t _read_spin(int fd, char* buf, size_t cap)
{
    for(int tries = 0; tries < 100000; tries++)
    {
        ssize_t n = read(fd, buf, cap);
        if(n > 0)
        {
            return n;
        }
        if(n < 0 && errno == EAGAIN)
        {
            continue;
        }
        return n; /* real error, or EOF (0) */
    }
    return -1;
}

/** @brief Baseline: a response that fits comfortably under TCP/kernel buffering completes inside the
 *         single response_writer_send() call — no parking, draining stays 0. */
static void test_send_completes_synchronously_when_socket_has_room(void** state)
{
    fixture_t* f = *state;

    /* Real SO_SNDBUF is still there even though it's clamped small — but the kernel typically doubles
     * requested SO_SNDBUF and a tiny request still has room for a small body. Use a body far smaller
     * than TINY_SNDBUF to keep this test's premise (no backpressure) robust across kernels. */
    DB_app_response_t res;
    memset(&res, 0, sizeof res);
    res.status       = 200u;
    res.content_type = "text/plain";
    res.body         = "ok";
    res.body_len     = 2u;

    assert_int_equal(response_writer_send(&f->cli, &res), STATUS_SUCCESS);
    assert_int_equal(f->cli.draining, 0u);

    char received[256];
    ssize_t n = _read_spin(f->peer_fd, received, sizeof received);
    assert_true(n > 0);
    assert_non_null(memmem(received, (size_t)n, "200", 3));
    assert_non_null(memmem(received, (size_t)n, "ok", 2));
}

/** @brief The core property: a response too big for the (clamped) socket buffer parks instead of
 *         blocking, and resuming after the peer drains delivers every byte, byte-for-byte, exactly
 *         once — no truncation, no duplication, no corruption across the park/resume boundary. */
static void test_send_parks_on_backpressure_then_resume_finishes_it_intact(void** state)
{
    fixture_t* f = *state;

    DB_app_response_t res;
    memset(&res, 0, sizeof res);
    res.status       = 200u;
    res.content_type = "text/plain";
    res.body         = f->body;
    res.body_len     = BODY_LEN;

    assert_int_equal(response_writer_send(&f->cli, &res), STATUS_SUCCESS);
    assert_int_equal(f->cli.draining, 1u); /* the whole point: did NOT block waiting for the peer */
    assert_true(f->cli.send_off < f->cli.send_len);
    assert_int_equal(f->cli.send_len, f->cli.send_off + (f->cli.send_len - f->cli.send_off)); /* sanity */

    /* Drain the peer AND keep calling resume until it's actually done — resume() only writes what
     * currently fits; on a real reactor this loop is "wait for the next EPOLLOUT", here it's direct. */
    char*  received     = malloc(f->cli.send_len + 64u); /* headers + body; over-allocate for headers */
    size_t total_to_read = 0u;
    /* We don't know the exact header length up front from the test side; read in a loop draining
     * whatever is available while resuming, until resume() reports done (draining flips back to 0). */
    int    resume_rc = STATUS_SUCCESS;
    size_t rounds     = 0u;
    while(f->cli.draining)
    {
        /* Drain whatever the kernel is currently holding so the next write() in resume() can make
         * progress — mirrors "the peer's TCP receive window opened up, EPOLLOUT fires". */
        char   chunk[4096];
        ssize_t n = _read_spin(f->peer_fd, chunk, sizeof chunk);
        assert_true(n > 0);
        memcpy(received + total_to_read, chunk, (size_t)n);
        total_to_read += (size_t)n;

        resume_rc = response_writer_resume(&f->cli);
        assert_int_equal(resume_rc, STATUS_SUCCESS); /* keep-alive: finishing must not signal removal */

        assert_true(++rounds < 10000u); /* guard against an infinite loop if the property under test breaks */
    }

    /* Read out whatever's left buffered that we haven't consumed yet — EAGAIN (not EOF: nobody closes
     * the socket mid-test) means there's genuinely nothing more, we're done. */
    for(;;)
    {
        char   chunk[4096];
        ssize_t n = read(f->peer_fd, chunk, sizeof chunk);
        if(n > 0)
        {
            memcpy(received + total_to_read, chunk, (size_t)n);
            total_to_read += (size_t)n;
            continue;
        }
        if(n < 0 && errno == EAGAIN)
        {
            break;
        }
        fail_msg("unexpected read() outcome draining leftovers: n=%zd errno=%d", n, errno);
    }

    assert_int_equal(f->cli.draining, 0u);
    assert_int_equal(f->cli.send_off, 0u);
    assert_int_equal(f->cli.send_len, 0u);

    /* The body — the last BODY_LEN bytes received — must be exactly what was sent, unmangled. */
    assert_true(total_to_read >= BODY_LEN);
    assert_memory_equal(received + (total_to_read - BODY_LEN), f->body, BODY_LEN);

    free(received);
}

static void test_resume_without_draining_is_rejected(void** state)
{
    fixture_t* f = *state;
    assert_int_equal(f->cli.draining, 0u);
    assert_int_equal(response_writer_resume(&f->cli), STATUS_FAILURE);
}

/** @brief A response that must close (Connection: close, e.g. response_writer_error()'s contract) but
 *         parks first: the close must NOT happen until the drain actually finishes — proven here by
 *         checking response_writer_resume()'s return only flips to STATUS_FAILURE (the "now remove
 *         this client" signal) once every byte is actually gone. */
static void test_park_close_policy_deferred_until_resume_completes(void** state)
{
    fixture_t* f              = *state;
    f->cli.connection_policy = (uint8_t)HTTP_CONNECTION_CLOSE;

    DB_app_response_t res;
    memset(&res, 0, sizeof res);
    res.status       = 500u;
    res.content_type = "text/plain";
    res.body         = f->body;
    res.body_len     = BODY_LEN;

    assert_int_equal(response_writer_send(&f->cli, &res), STATUS_SUCCESS); /* parked, not yet "close" */
    assert_int_equal(f->cli.draining, 1u);

    int rc = STATUS_SUCCESS;
    while(f->cli.draining)
    {
        char   chunk[4096];
        ssize_t n = _read_spin(f->peer_fd, chunk, sizeof chunk);
        assert_true(n > 0);
        rc = response_writer_resume(&f->cli);
        if(f->cli.draining)
        {
            assert_int_equal(rc, STATUS_SUCCESS); /* still parked — never a premature close signal */
        }
    }
    /* Fully drained now: the deferred close policy finally surfaces. */
    assert_int_equal(rc, STATUS_FAILURE);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup_teardown(test_send_completes_synchronously_when_socket_has_room, _setup, _teardown),
        cmocka_unit_test_setup_teardown(test_send_parks_on_backpressure_then_resume_finishes_it_intact, _setup, _teardown),
        cmocka_unit_test_setup_teardown(test_resume_without_draining_is_rejected, _setup, _teardown),
        cmocka_unit_test_setup_teardown(test_park_close_policy_deferred_until_resume_completes, _setup, _teardown),
    };
    return cmocka_run_group_tests_name("response_writer_parking", tests, NULL, NULL);
}
