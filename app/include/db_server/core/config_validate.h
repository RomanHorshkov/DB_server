/**
 * @file config_validate.h
 * @brief Startup config validation: fail fast on an invalid listen spec or env override.
 */
#ifndef SERVER_CONFIG_VALIDATE_H
#define SERVER_CONFIG_VALIDATE_H

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DECLARATIONS
 *****************************************************************************************************************************************
 */

/**
 * @brief Validate every runtime-configurable input this server reads before touching a socket, thread,
 *        or the filesystem, and fail fast (clear EML_ERROR — which reaches stderr, see emlog's default
 *        sink) on the first invalid one instead of surfacing later as an obscure bind()/getaddrinfo()
 *        failure or a silently-downgraded thread pool.
 *
 * Covers: @p api_spec / @p upload_spec (a TCP port 1..65535 or a unix path whose parent directory
 * exists and fits sockaddr_un.sun_path), and the DB_SERVER_WORKERS / DB_SERVER_RING_CAPACITY env
 * overrides (bounds mirrored from worker.c's _compute_operator_count() / _compute_ring_capacity() —
 * keep both in sync).
 *
 * Deliberately NOT covered: DB_SERVER_BIND (listener.c already validates it with an intentional
 * warn-not-reject policy — binding non-loopback is a supported opt-in, not a mistake to block), and
 * systemd socket-activation's LISTEN_FDS/LISTEN_PID/LISTEN_FDNAMES (owned and validated by
 * sd_activation.c — not operator-facing config in the same sense).
 *
 * @param[in] api_spec     Same value server_init() passes to listener_init(). NULL/"" is valid (the
 *                         socket-activated production path never supplies one).
 * @param[in] upload_spec  Same value server_init() passes to listener_init(). NULL/"" is valid (no
 *                         dedicated upload listener configured).
 * @retval STATUS_SUCCESS Every configured value is valid.
 * @retval STATUS_FAILURE At least one value is invalid; the specific reason was already logged.
 */
int config_validate_startup(const char* api_spec, const char* upload_spec);

#endif /* SERVER_CONFIG_VALIDATE_H */
