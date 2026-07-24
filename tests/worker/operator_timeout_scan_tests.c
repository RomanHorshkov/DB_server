/**
 * @file operator_timeout_scan_tests.c
 *
 * @brief Unit test for operator.c's _clean_clients() (the timerfd housekeeping callback,
 *        _operator_handle_timer_event()) — the idle-client timeout scan must cover every client slot,
 *        not just indices below active_clients.
 *
 * client.c's client_shutdown() frees a slot by clearing is_busy WITHOUT compacting the clients[]
 * array, so a live client can end up sitting at an index >= active_clients: if slot 0 is removed while
 * slot 1 stays busy, active_clients drops to 1. The original _clean_clients() looped
 * `for(cli_idx = 0; cli_idx < op->active_clients; cli_idx++)`, re-reading active_clients as the bound
 * on every iteration — so a SINGLE timer tick that finds slot 0 expired and removes it (active_clients
 * 2 -> 1) would stop before ever reaching slot 1 in that same pass, even though slot 1 was ALSO
 * expired. A resource-exhaustion bug: an idle client past index 0 could escape cleanup indefinitely
 * (docs/DB_APP_MAINTENANCE.md's timeout-scanning review; docs/MEMORY_MODEL.md §4.4). The fix scans all
 * WORKER_MAX_CLIENTS slots and tests is_busy directly.
 *
 * This test runs entirely single-threaded (operator_init() only — no operator_thread() pthread is ever
 * started) so there is no race with a real reactor loop also polling the same timerfd: it forges two
 * expired client slots directly, force-fires the operator's real timerfd, and invokes the real timer
 * handler function pointer exactly once, then asserts BOTH slots were reclaimed in that one pass.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

#define _GNU_SOURCE

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

#include <poll.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <db_server/core/reactor.h>
#include <db_server/core/worker/operator/client/client.h>
#include <db_server/core/worker/operator/operator.h>
#include <db_server/utils/time_helper.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define TEST_RING_CAPACITY 16u

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

static void test_timeout_scan_reclaims_slot_past_active_clients_bound(void** state);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

/** @brief Force @p op into the exact state the doc's bug report describes — two busy, already-expired
 *         client slots, active_clients=2 — then fire the real timer handler once and prove both slots
 *         (not just slot 0) get reclaimed in that single pass. */
static void test_timeout_scan_reclaims_slot_past_active_clients_bound(void** state)
{
    (void)state;

    operator_t op;
    memset(&op, 0, sizeof op);
    assert_int_equal(operator_init(&op, 9u, TEST_RING_CAPACITY, WORKER_MAX_CLIENTS), STATUS_SUCCESS);

    /* Two real fds (socketpair halves) so reactor_del()/socket_shutdown_and_close() inside the removal
     * path operate on genuine, registered descriptors instead of hitting the (harmless but noisy)
     * reactor_del-failed retry path. */
    int sv0[2], sv1[2];
    assert_int_equal(socketpair(AF_UNIX, SOCK_STREAM, 0, sv0), 0);
    assert_int_equal(socketpair(AF_UNIX, SOCK_STREAM, 0, sv1), 0);

    op.clients[0].is_busy       = 1u;
    op.clients[0].ctx.fd        = sv0[0];
    op.clients[0].ctx.owner     = &op;
    op.clients[0].last_activity = 0u; /* deep past -> expired under either timeout bucket */
    op.clients[0].request_count = 0u;
    assert_int_equal(reactor_add_in_client(&op.reactor, sv0[0], &op.clients[0].ctx), STATUS_SUCCESS);

    op.clients[1].is_busy       = 1u;
    op.clients[1].ctx.fd        = sv1[0];
    op.clients[1].ctx.owner     = &op;
    op.clients[1].last_activity = 0u; /* also expired — this is the slot the old bound skipped */
    op.clients[1].request_count = 0u;
    assert_int_equal(reactor_add_in_client(&op.reactor, sv1[0], &op.clients[1].ctx), STATUS_SUCCESS);

    atomic_store(&op.active_clients, 2u);

    /* Force-fire the real timerfd almost immediately (1ms) instead of waiting out the real 15s/60s
     * housekeeping period, then call the real handler function pointer directly — single-threaded, no
     * operator_thread() running, so this is not a race with anything. poll() on the timerfd itself
     * (rather than a fixed sleep) waits for it to genuinely be readable before invoking the handler:
     * _operator_handle_timer_event() treats an unexpired timerfd (EAGAIN) as a benign no-op and returns
     * STATUS_SUCCESS WITHOUT calling _clean_clients() at all, so a sleep that's merely "usually long
     * enough" can pass the handler's return-value check while silently skipping the cleanup this test
     * exists to prove — poll() removes that race entirely instead of guessing a safe constant. */
    assert_int_equal(time_helper_set(op.timer_fd, 0u, 1000000u), 0);
    struct pollfd pfd = {.fd = op.timer_fd, .events = POLLIN};
    assert_int_equal(poll(&pfd, 1, 2000), 1);
    assert_true(pfd.revents & POLLIN);

    assert_int_equal(op.timer_ctx.handler(op.timer_ctx.fd, &op.timer_ctx), STATUS_SUCCESS);

    /* The property under test: BOTH slots reclaimed in the one pass, not just slot 0. Under the old
     * `cli_idx < op->active_clients` bound, active_clients dropping 2->1 when slot 0 was removed would
     * end the loop before it ever looked at slot 1, leaving it busy forever. */
    assert_int_equal(op.clients[0].is_busy, 0u);
    assert_int_equal(op.clients[1].is_busy, 0u);
    assert_int_equal(atomic_load(&op.active_clients), 0u);

    close(sv0[1]);
    close(sv1[1]);
    operator_shutdown(&op);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_timeout_scan_reclaims_slot_past_active_clients_bound),
    };
    return cmocka_run_group_tests_name("operator_timeout_scan", tests, NULL, NULL);
}
