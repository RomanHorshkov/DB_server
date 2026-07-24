/**
 * @file affinity_tests.c
 *
 * @brief Unit tests for affinity.c's srv_affinity_online_cpus() — the worker-pool sizing input that
 *        must reflect the process's *effective* CPU affinity mask (sched_getaffinity(2)), not the
 *        host's raw online-CPU count (sysconf(_SC_NPROCESSORS_ONLN)).
 *
 * Before core.c's _core_detect_cpu_count() switched to srv_affinity_online_cpus() (see
 * app/src/core/core.c), the worker pool was sized from sysconf(_SC_NPROCESSORS_ONLN) directly — which
 * reports the HOST's total CPU count and ignores any cgroup/taskset/container/systemd AllowedCPUs=
 * restriction narrowing what this process is actually allowed to run on. Under such a restriction the
 * server could size far more operators than legal CPUs, wrapping many threads onto the same real core
 * (docs/DB_APP_MAINTENANCE.md's CPU/NUMA review; docs/MEMORY_MODEL.md §4.4).
 *
 * These tests exercise srv_affinity_online_cpus() directly against a REAL, narrowed affinity mask —
 * restricting this test process's own CPU set via sched_setaffinity(2) is functionally identical to
 * running the whole test binary under `taskset -c <n>`, just hermetic (no external tool dependency,
 * no assumption about which CPU IDs a CI runner happens to expose) and restored afterward so it can't
 * leak into any other test in the same binary.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE /* CPU_SET macros, sched_getaffinity/sched_setaffinity */
#endif

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

#include <sched.h>
#include <unistd.h>

#include <db_server/utils/affinity.h>

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

static void test_online_cpus_reflects_narrowed_affinity_mask_of_two(void** state);
static void test_online_cpus_reflects_narrowed_affinity_mask_of_one(void** state);
static void test_online_cpus_never_returns_less_than_one(void** state);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

/** @brief Restrict the calling process to exactly the first @p n CPUs present in its ORIGINAL allowed
 *         set (not raw CPU IDs 0..n-1, which may not all be legal under an already-restrictive
 *         environment such as a CI container) — mirrors what `taskset -c <ids>` does from outside the
 *         process, just performed on our own pid. Returns the built mask via @p out_mask so the caller
 *         can restore the original set afterward. */
static void _restrict_self_to_first_n_allowed(const cpu_set_t* original, int n, cpu_set_t* out_mask)
{
    CPU_ZERO(out_mask);
    int found = 0;
    for(size_t cpu = 0u; cpu < (size_t)CPU_SETSIZE && found < n; ++cpu)
    {
        if(CPU_ISSET(cpu, original))
        {
            CPU_SET(cpu, out_mask);
            ++found;
        }
    }
    assert_int_equal(found, n); /* test host must have at least n allowed CPUs */
    assert_int_equal(sched_setaffinity(0, sizeof(*out_mask), out_mask), 0);
}

/** @brief Narrowing this process's affinity mask to 2 CPUs must make srv_affinity_online_cpus()
 *         report exactly 2 — regardless of how many CPUs the host actually has (this test's own
 *         narrowed mask stands in for a cgroup/taskset/AllowedCPUs= restriction; nproc(1)/sysconf()
 *         would still report the host's full count, which is precisely the bug this fixes). */
static void test_online_cpus_reflects_narrowed_affinity_mask_of_two(void** state)
{
    (void)state;

    cpu_set_t original;
    CPU_ZERO(&original);
    assert_int_equal(sched_getaffinity(0, sizeof original, &original), 0);
    if(CPU_COUNT(&original) < 2)
    {
        skip(); /* host itself doesn't offer 2 allowed CPUs to restrict into */
        return;
    }

    cpu_set_t narrowed;
    _restrict_self_to_first_n_allowed(&original, 2, &narrowed);

    assert_int_equal(srv_affinity_online_cpus(), 2);

    /* Restore — must not leak a narrowed mask into any other test sharing this process. */
    assert_int_equal(sched_setaffinity(0, sizeof original, &original), 0);
}

/** @brief Same property at n=1 — the single-CPU-container/VM case docs/MEMORY_MODEL.md §4.4 calls out
 *         explicitly ("1 CPU -> one operator, pinning is a no-op"). */
static void test_online_cpus_reflects_narrowed_affinity_mask_of_one(void** state)
{
    (void)state;

    cpu_set_t original;
    CPU_ZERO(&original);
    assert_int_equal(sched_getaffinity(0, sizeof original, &original), 0);

    cpu_set_t narrowed;
    _restrict_self_to_first_n_allowed(&original, 1, &narrowed);

    assert_int_equal(srv_affinity_online_cpus(), 1);

    assert_int_equal(sched_setaffinity(0, sizeof original, &original), 0);
}

/** @brief The documented fallback contract: srv_affinity_online_cpus() must never return < 1 (affinity.h).
 *         Under a normal, unrestricted process this is just the sanity check that the real (non-mocked)
 *         call returns a positive, plausible value — sched_getaffinity() cannot be made to fail from
 *         user space with valid arguments on a live pid, so the sysconf() fallback branch itself is not
 *         independently reachable without either mocking the syscall or refactoring it behind a seam;
 *         this test instead pins down the observable contract every caller (core.c's
 *         _core_detect_cpu_count()) actually relies on. */
static void test_online_cpus_never_returns_less_than_one(void** state)
{
    (void)state;
    assert_true(srv_affinity_online_cpus() >= 1);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test(test_online_cpus_reflects_narrowed_affinity_mask_of_two),
        cmocka_unit_test(test_online_cpus_reflects_narrowed_affinity_mask_of_one),
        cmocka_unit_test(test_online_cpus_never_returns_less_than_one),
    };
    return cmocka_run_group_tests_name("affinity", tests, NULL, NULL);
}
