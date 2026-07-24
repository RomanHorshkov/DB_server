/**
 * @file dispatch_load_balance_tests.c
 *
 * @brief Unit test for worker.c's _least_loaded_operator() — dispatch load must account for queued
 *        (ring-pushed, not-yet-dequeued) clients, not just active_clients.
 *
 * Before worker.c tracked queued_clients, the dispatcher's "least loaded operator" scan read only
 * active_clients. An operator whose SPSC ring already held several accepted-but-undrained connections
 * could still report active_clients==0 and therefore look completely idle, so the dispatcher kept
 * routing every new connection onto that SAME operator until its ring filled and pushes started
 * failing outright — even while a genuinely idle sibling operator sat unused
 * (docs/DB_APP_MAINTENANCE.md's dispatch-accounting review; docs/MEMORY_MODEL.md §4.4). The fix adds
 * an atomic queued_clients counter (incremented on ring push, decremented on pop) and uses
 * active+queued as the effective load.
 *
 * This test proves the fix through the real PUBLIC worker.h API — worker_init()/
 * worker_dispatch_to_operator()/worker_destroy() — deliberately WITHOUT calling worker_run(): with no
 * operator thread ever started, nothing ever pops an operator's ring, so active_clients stays 0 for
 * every operator for the whole test and queued_clients is the ONLY signal that can possibly steer
 * dispatch. That isolates exactly the property under test: whether queued load is counted at all.
 *
 * Math for the fixed-point scenario below (WORKER_MAX_CLIENTS=64 -> blind-assignment threshold =
 * WORKER_MAX_CLIENTS/10 = 6, DB_SERVER_RING_CAPACITY=8, cpu_count=3 -> operators=2): dispatch fills
 * operator 0 with pushes until its queued load reaches the blind threshold (6), at which point operator
 * 1 (still at 0) wins every subsequent comparison ahead of operator 0, so load alternates between the
 * two operators from there on, distributing all 16 dispatches evenly across BOTH 8-slot rings (8 each)
 * before either ring is full. Under the old active_clients-only accounting, every one of the 16
 * dispatches would target operator 0 alone (active is always 0 for both operators, so the first
 * operator under the blind threshold — operator 0 — is returned immediately every single call), and
 * the 9th push would fail outright once operator 0's 8-slot ring filled, with operator 1 still
 * completely untouched.
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

#include <stdlib.h>

#include <db_server/core/config.h>
#include <db_server/core/worker/worker.h>

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

static void test_dispatch_spreads_across_operators_via_queued_load(void** state);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static void test_dispatch_spreads_across_operators_via_queued_load(void** state)
{
    (void)state;

    /* Deterministic sizing: cpu_count=3 -> 2 operators (cpu_count-1); an explicit env override would
     * otherwise beat the parameter, so make sure none is set. */
    unsetenv("DB_SERVER_WORKERS");
    setenv("DB_SERVER_RING_CAPACITY", "8", 1);

    assert_int_equal(worker_init(3u), STATUS_SUCCESS);
    assert_int_equal(worker_get_operators_count(), 2u);

    /* worker_run() is deliberately never called: no operator thread exists to pop the ring, so
     * active_clients is 0 for both operators for the whole test and every dispatch decision is driven
     * purely by queued_clients. */

    int successes = 0;
    for(int i = 0; i < 16; ++i)
    {
        /* Dummy fd values: worker_dispatch_to_operator() only pushes the int onto the ring (no
         * operator thread ever reads/uses it in this test), so any non-negative value works. Offset
         * away from 0/1/2 purely to avoid visual confusion with real stdio fds. */
        if(worker_dispatch_to_operator(1000 + i) == STATUS_SUCCESS)
        {
            ++successes;
        }
    }
    /* All 16 must land somewhere: 2 operators x 8-slot ring = exactly 16 slots of total capacity. Under
     * the pre-fix active-only accounting every dispatch would target operator 0 alone and the 9th push
     * (its ring's 9th slot) would fail — this assertion is what catches that regression. */
    assert_int_equal(successes, 16);

    /* Both rings are now completely full (8 queued each) -- one more dispatch must fail because there
     * is genuinely no room left anywhere, not because of a broken load-balancing blind spot. */
    assert_int_equal(worker_dispatch_to_operator(9999), STATUS_FAILURE);

    worker_destroy();
    unsetenv("DB_SERVER_RING_CAPACITY");
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_dispatch_spreads_across_operators_via_queued_load),
    };
    return cmocka_run_group_tests_name("dispatch_load_balance", tests, NULL, NULL);
}
