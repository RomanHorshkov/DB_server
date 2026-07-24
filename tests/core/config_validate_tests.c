/**
 * @file config_validate_tests.c
 *
 * @brief Unit tests for config_validate.c's config_validate_startup() — the startup gate that fills the
 *        gap core.c's server_init() used to leave completely open (no validation of the listen spec(s)
 *        passed on argv, nor of the DB_SERVER_WORKERS / DB_SERVER_RING_CAPACITY env overrides, before
 *        threads/sockets/DB_app all started spinning up).
 *
 * Each rejection path is exercised directly against config_validate_startup() (the public entry point)
 * rather than the private per-field helpers, so these tests track the real contract callers rely on.
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

#include <db_server/core/config_core.h>
#include <db_server/core/config_validate.h>

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

static int  _setup_clears_env(void** state);
static void test_null_and_empty_specs_are_valid(void** state);
static void test_valid_tcp_port_accepted(void** state);
static void test_port_zero_rejected(void** state);
static void test_port_out_of_range_rejected(void** state);
static void test_non_numeric_port_rejected(void** state);
static void test_valid_unix_path_accepted(void** state);
static void test_unix_path_missing_parent_dir_rejected(void** state);
static void test_unix_path_too_long_rejected(void** state);
static void test_upload_spec_validated_independently_of_api_spec(void** state);
static void test_workers_env_unset_is_valid(void** state);
static void test_workers_env_all_accepted(void** state);
static void test_workers_env_valid_integer_accepted(void** state);
static void test_workers_env_zero_rejected(void** state);
static void test_workers_env_out_of_range_rejected(void** state);
static void test_workers_env_non_numeric_rejected(void** state);
static void test_ring_capacity_env_unset_is_valid(void** state);
static void test_ring_capacity_env_valid_power_of_two_accepted(void** state);
static void test_ring_capacity_env_non_power_of_two_rejected(void** state);
static void test_ring_capacity_env_out_of_range_rejected(void** state);
static void test_max_clients_env_unset_is_valid(void** state);
static void test_max_clients_env_valid_integer_accepted(void** state);
static void test_max_clients_env_out_of_range_rejected(void** state);
static void test_max_clients_env_non_numeric_rejected(void** state);
static void test_upload_workers_env_unset_is_valid(void** state);
static void test_upload_workers_env_valid_integer_accepted(void** state);
static void test_upload_workers_env_zero_rejected(void** state);
static void test_upload_workers_env_out_of_range_rejected(void** state);
static void test_upload_workers_env_non_numeric_rejected(void** state);
static void test_upload_queue_depth_env_unset_is_valid(void** state);
static void test_upload_queue_depth_env_valid_integer_accepted(void** state);
static void test_upload_queue_depth_env_zero_rejected(void** state);
static void test_upload_queue_depth_env_out_of_range_rejected(void** state);
static void test_upload_queue_depth_env_non_numeric_rejected(void** state);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static int _setup_clears_env(void** state)
{
    (void)state;
    unsetenv("DB_SERVER_WORKERS");
    unsetenv("DB_SERVER_RING_CAPACITY");
    unsetenv("DB_SERVER_MAX_CLIENTS");
    unsetenv("DB_SERVER_UPLOAD_WORKERS");
    unsetenv("DB_SERVER_UPLOAD_QUEUE_DEPTH");
    return 0;
}

static void test_null_and_empty_specs_are_valid(void** state)
{
    (void)state;
    /* Legitimate: socket activation supplies neither; upload_spec is always optional. */
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    assert_int_equal(config_validate_startup("", ""), STATUS_SUCCESS);
}

static void test_valid_tcp_port_accepted(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup("3490", NULL), STATUS_SUCCESS);
    assert_int_equal(config_validate_startup("1", NULL), STATUS_SUCCESS);
    assert_int_equal(config_validate_startup("65535", NULL), STATUS_SUCCESS);
}

static void test_port_zero_rejected(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup("0", NULL), STATUS_FAILURE);
}

static void test_port_out_of_range_rejected(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup("65536", NULL), STATUS_FAILURE);
    assert_int_equal(config_validate_startup("999999", NULL), STATUS_FAILURE);
}

static void test_non_numeric_port_rejected(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup("abc", NULL), STATUS_FAILURE);
    assert_int_equal(config_validate_startup("34x9", NULL), STATUS_FAILURE);
    assert_int_equal(config_validate_startup("-1", NULL), STATUS_FAILURE);
}

static void test_valid_unix_path_accepted(void** state)
{
    (void)state;
    /* /tmp is guaranteed to exist wherever this test suite runs. */
    assert_int_equal(config_validate_startup("/tmp/db_server_test_api.sock", NULL), STATUS_SUCCESS);
}

static void test_unix_path_missing_parent_dir_rejected(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup("/this/dir/almost-certainly/does-not/exist/api.sock", NULL), STATUS_FAILURE);
}

static void test_unix_path_too_long_rejected(void** state)
{
    (void)state;
    /* sockaddr_un.sun_path is 108 bytes on Linux; well past that regardless of parent-dir existence. */
    char long_path[200];
    long_path[0] = '/';
    for(size_t i = 1; i < sizeof long_path - 1u; ++i)
    {
        long_path[i] = 'a';
    }
    long_path[sizeof long_path - 1u] = '\0';

    assert_int_equal(config_validate_startup(long_path, NULL), STATUS_FAILURE);
}

static void test_upload_spec_validated_independently_of_api_spec(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup("3490", "99999"), STATUS_FAILURE);
    assert_int_equal(config_validate_startup("99999", "3491"), STATUS_FAILURE);
    assert_int_equal(config_validate_startup("3490", "3491"), STATUS_SUCCESS);
}

static void test_workers_env_unset_is_valid(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_workers_env_all_accepted(void** state)
{
    (void)state;
    setenv("DB_SERVER_WORKERS", "all", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_WORKERS", "ALL", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_workers_env_valid_integer_accepted(void** state)
{
    (void)state;
    setenv("DB_SERVER_WORKERS", "1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_WORKERS", "255", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_workers_env_zero_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_WORKERS", "0", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_workers_env_out_of_range_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_WORKERS", "256", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_WORKERS", "-1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_workers_env_non_numeric_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_WORKERS", "banana", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_ring_capacity_env_unset_is_valid(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_ring_capacity_env_valid_power_of_two_accepted(void** state)
{
    (void)state;
    setenv("DB_SERVER_RING_CAPACITY", "8", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_RING_CAPACITY", "1024", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_RING_CAPACITY", "32", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_ring_capacity_env_non_power_of_two_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_RING_CAPACITY", "10", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_ring_capacity_env_out_of_range_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_RING_CAPACITY", "4", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_RING_CAPACITY", "2048", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_max_clients_env_unset_is_valid(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_max_clients_env_valid_integer_accepted(void** state)
{
    (void)state;
    setenv("DB_SERVER_MAX_CLIENTS", "8", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_MAX_CLIENTS", "255", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_MAX_CLIENTS", "128", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_max_clients_env_out_of_range_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_MAX_CLIENTS", "7", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_MAX_CLIENTS", "256", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_MAX_CLIENTS", "0", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_MAX_CLIENTS", "-1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_max_clients_env_non_numeric_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_MAX_CLIENTS", "banana", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_upload_workers_env_unset_is_valid(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_upload_workers_env_valid_integer_accepted(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_WORKERS", "1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_UPLOAD_WORKERS", "16", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_upload_workers_env_zero_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_WORKERS", "0", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_upload_workers_env_out_of_range_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_WORKERS", "17", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_UPLOAD_WORKERS", "-1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_upload_workers_env_non_numeric_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_WORKERS", "banana", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_upload_queue_depth_env_unset_is_valid(void** state)
{
    (void)state;
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_upload_queue_depth_env_valid_integer_accepted(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_QUEUE_DEPTH", "1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
    setenv("DB_SERVER_UPLOAD_QUEUE_DEPTH", "32", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_SUCCESS);
}

static void test_upload_queue_depth_env_zero_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_QUEUE_DEPTH", "0", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_upload_queue_depth_env_out_of_range_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_QUEUE_DEPTH", "33", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
    setenv("DB_SERVER_UPLOAD_QUEUE_DEPTH", "-1", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

static void test_upload_queue_depth_env_non_numeric_rejected(void** state)
{
    (void)state;
    setenv("DB_SERVER_UPLOAD_QUEUE_DEPTH", "banana", 1);
    assert_int_equal(config_validate_startup(NULL, NULL), STATUS_FAILURE);
}

int main(void)
{
    const struct CMUnitTest tests[] = {
        cmocka_unit_test_setup(test_null_and_empty_specs_are_valid, _setup_clears_env),
        cmocka_unit_test_setup(test_valid_tcp_port_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_port_zero_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_port_out_of_range_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_non_numeric_port_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_valid_unix_path_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_unix_path_missing_parent_dir_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_unix_path_too_long_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_spec_validated_independently_of_api_spec, _setup_clears_env),
        cmocka_unit_test_setup(test_workers_env_unset_is_valid, _setup_clears_env),
        cmocka_unit_test_setup(test_workers_env_all_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_workers_env_valid_integer_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_workers_env_zero_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_workers_env_out_of_range_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_workers_env_non_numeric_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_ring_capacity_env_unset_is_valid, _setup_clears_env),
        cmocka_unit_test_setup(test_ring_capacity_env_valid_power_of_two_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_ring_capacity_env_non_power_of_two_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_ring_capacity_env_out_of_range_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_max_clients_env_unset_is_valid, _setup_clears_env),
        cmocka_unit_test_setup(test_max_clients_env_valid_integer_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_max_clients_env_out_of_range_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_max_clients_env_non_numeric_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_workers_env_unset_is_valid, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_workers_env_valid_integer_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_workers_env_zero_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_workers_env_out_of_range_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_workers_env_non_numeric_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_queue_depth_env_unset_is_valid, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_queue_depth_env_valid_integer_accepted, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_queue_depth_env_zero_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_queue_depth_env_out_of_range_rejected, _setup_clears_env),
        cmocka_unit_test_setup(test_upload_queue_depth_env_non_numeric_rejected, _setup_clears_env),
    };
    return cmocka_run_group_tests_name("config_validate", tests, NULL, NULL);
}
