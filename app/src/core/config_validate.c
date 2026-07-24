/**
 * @file config_validate.c
 * @brief Startup config validation (see config_validate.h).
 */
#include <db_server/core/config_validate.h>

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <db_server/core/config_core.h>
#include <emlog.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG "srv_config_validate"

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

static int _validate_tcp_port(const char* label, const char* port_str);
static int _validate_unix_path(const char* label, const char* path);
static int _validate_listen_spec(const char* label, const char* spec);
static int _validate_workers_env(void);
static int _validate_ring_capacity_env(void);
static int _validate_upload_workers_env(void);
static int _validate_upload_queue_depth_env(void);

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int config_validate_startup(const char* api_spec, const char* upload_spec)
{
    int valid = 1;

    if(_validate_listen_spec("api_spec", api_spec) != STATUS_SUCCESS)
    {
        valid = 0;
    }
    if(_validate_listen_spec("upload_spec", upload_spec) != STATUS_SUCCESS)
    {
        valid = 0;
    }
    if(_validate_workers_env() != STATUS_SUCCESS)
    {
        valid = 0;
    }
    if(_validate_ring_capacity_env() != STATUS_SUCCESS)
    {
        valid = 0;
    }
    if(_validate_upload_workers_env() != STATUS_SUCCESS)
    {
        valid = 0;
    }
    if(_validate_upload_queue_depth_env() != STATUS_SUCCESS)
    {
        valid = 0;
    }

    return valid ? STATUS_SUCCESS : STATUS_FAILURE;
}

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

/** @brief A spec that begins with '/' is a unix path, anything else a TCP port — same rule listener.c's
 *         _init_listening_sockets() uses to pick a transport. NULL/"" is valid: not every caller
 *         configures both specs (socket activation supplies neither; upload_spec is optional). */
static int _validate_listen_spec(const char* label, const char* spec)
{
    if(!spec || spec[0] == '\0')
    {
        return STATUS_SUCCESS;
    }
    if(spec[0] == '/')
    {
        return _validate_unix_path(label, spec);
    }
    return _validate_tcp_port(label, spec);
}

static int _validate_tcp_port(const char* label, const char* port_str)
{
    const size_t len      = strlen(port_str);
    int          well_formed = (len > 0u && len <= 5u);
    for(size_t i = 0; well_formed && i < len; ++i)
    {
        well_formed = isdigit((unsigned char)port_str[i]) != 0;
    }

    long val = -1;
    if(well_formed)
    {
        errno          = 0;
        char* end      = NULL;
        val            = strtol(port_str, &end, 10);
        /* Port 0 asks the kernel to pick an ephemeral port — not a meaningful value for a fixed
         * listen spec this server is told to bind, so it is rejected alongside the 1..65535 range. */
        well_formed    = (errno == 0 && end != port_str && *end == '\0' && val >= 1 && val <= 65535);
    }

    if(!well_formed)
    {
        EML_ERROR(LOG_TAG, "%s: '%s' is not a valid TCP port (want 1..65535)", label, port_str);
        return STATUS_FAILURE;
    }
    return STATUS_SUCCESS;
}

static int _validate_unix_path(const char* label, const char* path)
{
    struct sockaddr_un su;
    const size_t       max_len = sizeof su.sun_path;
    const size_t       len     = strlen(path);

    /* Same bound _init_unix_socket() enforces at bind time (sockaddr_un.sun_path) — checked here too so
     * an oversized path fails at startup with a clear reason instead of a bind()-time one. */
    if(len == 0u || len >= max_len)
    {
        EML_ERROR(LOG_TAG, "%s: unix socket path empty or too long (max %zu bytes): '%s'", label, max_len - 1u, path);
        return STATUS_FAILURE;
    }

    char parent[sizeof su.sun_path];
    strncpy(parent, path, sizeof parent - 1u);
    parent[sizeof parent - 1u] = '\0';

    char* last_slash = strrchr(parent, '/');
    if(last_slash == parent)
    {
        /* path is "/name" — parent is the filesystem root */
        last_slash[1] = '\0';
    }
    else
    {
        *last_slash = '\0';
    }

    struct stat st;
    if(stat(parent, &st) == -1 || !S_ISDIR(st.st_mode))
    {
        EML_ERROR(LOG_TAG, "%s: parent directory '%s' of unix socket path '%s' does not exist", label, parent, path);
        return STATUS_FAILURE;
    }

    return STATUS_SUCCESS;
}

/** @brief Bounds mirror worker.c's _compute_operator_count() (1..255, or "all"/"ALL") — keep both in
 *         sync if either changes. worker.c's own parse falls back to auto on a bad value; this runs
 *         first and turns that same bad value into a startup failure instead. */
static int _validate_workers_env(void)
{
    const char* env = getenv("DB_SERVER_WORKERS");
    if(!env || env[0] == '\0')
    {
        return STATUS_SUCCESS;
    }
    if(strcmp(env, "all") == 0 || strcmp(env, "ALL") == 0)
    {
        return STATUS_SUCCESS;
    }

    errno          = 0;
    char*     end  = NULL;
    const long val = strtol(env, &end, 10);
    if(errno != 0 || end == env || *end != '\0' || val < 1 || val > 255)
    {
        EML_ERROR(LOG_TAG, "DB_SERVER_WORKERS='%s' is invalid (want 'all' or an integer 1..255)", env);
        return STATUS_FAILURE;
    }
    return STATUS_SUCCESS;
}

/** @brief Bounds mirror worker.c's _compute_ring_capacity() (power of two, 8..1024) — keep both in
 *         sync if either changes. */
static int _validate_ring_capacity_env(void)
{
    const char* env = getenv("DB_SERVER_RING_CAPACITY");
    if(!env || env[0] == '\0')
    {
        return STATUS_SUCCESS;
    }

    errno          = 0;
    char*     end  = NULL;
    const long val = strtol(env, &end, 10);
    if(errno != 0 || end == env || *end != '\0' || val < 8 || val > 1024 || (val & (val - 1)) != 0)
    {
        EML_ERROR(LOG_TAG, "DB_SERVER_RING_CAPACITY='%s' is invalid (want a power of two in 8..1024)", env);
        return STATUS_FAILURE;
    }
    return STATUS_SUCCESS;
}

/** @brief Bound mirrors core.c's _compute_upload_worker_count() (1..16) — keep both in sync if either
 *         changes. */
static int _validate_upload_workers_env(void)
{
    const char* env = getenv("DB_SERVER_UPLOAD_WORKERS");
    if(!env || env[0] == '\0')
    {
        return STATUS_SUCCESS;
    }

    errno          = 0;
    char*     end  = NULL;
    const long val = strtol(env, &end, 10);
    if(errno != 0 || end == env || *end != '\0' || val < 1 || val > 16)
    {
        EML_ERROR(LOG_TAG, "DB_SERVER_UPLOAD_WORKERS='%s' is invalid (want an integer 1..16)", env);
        return STATUS_FAILURE;
    }
    return STATUS_SUCCESS;
}

/** @brief Bound mirrors core.c's _compute_upload_queue_depth() (1..32) — keep both in sync if either
 *         changes. */
static int _validate_upload_queue_depth_env(void)
{
    const char* env = getenv("DB_SERVER_UPLOAD_QUEUE_DEPTH");
    if(!env || env[0] == '\0')
    {
        return STATUS_SUCCESS;
    }

    errno          = 0;
    char*     end  = NULL;
    const long val = strtol(env, &end, 10);
    if(errno != 0 || end == env || *end != '\0' || val < 1 || val > 32)
    {
        EML_ERROR(LOG_TAG, "DB_SERVER_UPLOAD_QUEUE_DEPTH='%s' is invalid (want an integer 1..32)", env);
        return STATUS_FAILURE;
    }
    return STATUS_SUCCESS;
}
