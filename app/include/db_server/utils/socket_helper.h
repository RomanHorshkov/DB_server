/**
 * @file socket_helper.h
 * @brief Helper functions for socket configuration and management.
 *
 * Provides wrappers for setting socket options such as non-blocking mode, disabling Nagle's algorithm, and initializing listener/client
 * sockets.
 *
 * @author  Roman Horshkov
 * @date    2025-05-11
 */

#ifndef SERVER_SOCKET_HELPER_H
#define SERVER_SOCKET_HELPER_H
#include <stddef.h>
#include <stdint.h>    /* int64_t, int32_t */
#include <sys/types.h> /* ssize_t */

/* Forward declarations: */
struct addrinfo;

/**
 * @brief Set a socket to non-blocking mode.
 *
 * Uses fcntl() to set the O_NONBLOCK flag on the given file descriptor.
 *
 * @param socket_fd  File descriptor of the socket.
 * @retval  0  Success.
 * @retval -1 Failure (see log for details).
 */
int socket_set_non_blocking(const int* socket_fd);

/**
 * @brief Disable Nagle's algorithm (TCP_NODELAY) on a socket.
 *
 * Sets TCP_NODELAY to 1 for lower latency on the given TCP socket.
 *
 * @param socket_fd  File descriptor of the socket.
 * @retval  0  Success.
 * @retval -1 Failure (see log for details).
 */
int socket_disable_nagle(const int* socket_fd);

/**
 * @brief Enable address reuse on a socket.
 *
 * Sets the SO_REUSEADDR option on the provided socket file descriptor, allowing the server to restart without waiting for old sockets to
 * time out.
 *
 * @param socket_fd  Pointer to the socket file descriptor.
 * @retval  0  Success.
 * @retval -1 Failure (setsockopt failed).
 */
int socket_set_reusability(const int* socket_fd);

/**
 * @brief Enable fast restart on a socket by setting SO_LINGER.
 *
 * Configures the socket to discard unsent data and close immediately on shutdown, which is suitable for listener sockets.
 *
 * @param socket_fd  Pointer to the socket file descriptor.
 * @retval  0  Success.
 * @retval -1 Failure (setsockopt failed).
 */
int socket_set_restartability(const int* socket_fd);

/**
 * @brief Set recommended socket options in the addrinfo hints structure.
 *
 * Initializes the provided hints structure for getaddrinfo() with recommended values for dual-stack TCP listening sockets.
 *
 * @param hints  Pointer to the addrinfo structure to initialize.
 * @retval  0  Success.
 * @retval -1 Failure (invalid pointer).
 */
int socket_listener_set_hints(struct addrinfo* hints);

int64_t socket_drain(const int fd);

void socket_shutdown_and_close(int fd);

/**
 * @brief Initialize a listener socket with recommended options.
 *
 * Sets SO_REUSEADDR, SO_LINGER, and O_NONBLOCK on the provided socket, and restricts IPv6 sockets to IPv6-only if required.
 *
 * @param listen_fd  File descriptor of the listener socket.
 * @param ai_family  Pointer to the address family (AF_INET/AF_INET6).
 * @retval  0  Success.
 * @retval -1 Failure (see log for details).
 */
int socket_listener_init(const int* listen_fd, const int32_t* ai_family);

/**
 * @brief Initialize a client socket with recommended options.
 *
 * Sets non-blocking mode and disables Nagle's algorithm for the client socket.
 *
 * @param client_fd  File descriptor of the client socket.
 * @retval  0  Success.
 * @retval -1 Failure (see log for details).
 */
int socket_client_init(const int* client_fd);

/**
 * @brief Read the connected peer's IPv4 address/port off an accepted socket, network byte order.
 *
 * Works on any already-connected fd — there is no accept()-time-only requirement, since a connected
 * socket's peer address is stable for the fd's whole lifetime (getpeername() returns the same answer
 * whether it's called right after accept() or later, from a different thread, once the fd has crossed
 * into an operator/worker). This is how the caller propagates the real client address without needing
 * to carry a sockaddr through the (fd-only) SPSC dispatch ring.
 *
 * A non-IPv4 peer (AF_UNIX — the production nginx transport — or AF_INET6) is not a failure: the
 * DB_http_request_t contract (@c remote_ip_be) is a @c uint32_t, IPv4-only by design, so both outputs
 * are left at the documented 0 "unknown" sentinel. getpeername() failing (bad/closed fd) is handled the
 * same way — best-effort metadata, never a reason to drop the connection.
 *
 * @param[in]  fd            Connected socket fd.
 * @param[out] out_ip_be     Receives the peer's IPv4 address in network byte order, or 0.
 * @param[out] out_port_be   Receives the peer's TCP port in network byte order, or 0.
 * @retval STATUS_SUCCESS Always, unless an output pointer is NULL.
 * @retval STATUS_FAILURE @p out_ip_be or @p out_port_be is NULL.
 */
int socket_get_peer_ipv4(int fd, uint32_t* out_ip_be, uint16_t* out_port_be);

ssize_t socket_read_nonblocking(int fd, void* buf, size_t count);

#endif /* SERVER_SOCKET_HELPER_H */
