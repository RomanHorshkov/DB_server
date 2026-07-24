/**
 * @file socket_helper.c
 * @brief Helper functions for socket configuration and management.
 *
 * Implements wrappers for setting socket options such as non-blocking mode, disabling Nagle's algorithm, and initializing listener/client
 * sockets.
 *
 * @author  Roman Horshkov
 * @date    2025-05-11
 */

#ifndef _POSIX_C_SOURCE
#    define _POSIX_C_SOURCE 200112L
#endif
#define _GNU_SOURCE /* for accept4 */

#include <db_server/utils/socket_helper.h>

#include <arpa/inet.h>  /* inet_ntop(), struct sockaddr_in, struct sockaddr_in6 */
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>      /* IPPROTO_TCP, getaddrinfo(), addrinfo, gai_strerror() */
#include <netinet/tcp.h>
#include <stddef.h>
#include <string.h>     /* strerror() */
#include <sys/socket.h> /* fcntl(), setsockopt() */
#include <sys/types.h>
#include <unistd.h>

#include <db_server/core/config_core.h> /* STATUS_SUCCESS, STATUS_FAILURE */
#include <emlog.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG "socket_helper"

/*****************************************************************************************************************************************
 * PRIVATE ENUMERATED VARIABLES
 *****************************************************************************************************************************************
 */
/* None */

/*****************************************************************************************************************************************
 * PRIVATE STRUCTURED TYPES
 *****************************************************************************************************************************************
 */
/* None */

/*****************************************************************************************************************************************
 * PRIVATE VARIABLES DEFINITIONS
 *****************************************************************************************************************************************
 */
/* None */

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS PROTOTYPES
 *****************************************************************************************************************************************
 */
/* None */

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int socket_set_non_blocking(const int* socket_fd)
{
    if(socket_fd == NULL)
    {
        EML_ERROR(LOG_TAG, "_set_non_blocking: invalid input");
        goto fail;
    }

    /* Get current flags */
    int flags = fcntl(*socket_fd, F_GETFL, 0);
    if(flags == -1)
    {
        EML_PERR(LOG_TAG, "_set_non_blocking: fcntl(F_GETFL) failed: %s", strerror(errno));
        goto fail;
    }

    /* Set O_NONBLOCK flag */
    if(fcntl(*socket_fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        EML_PERR(LOG_TAG, "_set_non_blocking: fcntl(F_SETFL) failed: %s", strerror(errno));
        goto fail;
    }

    return STATUS_SUCCESS;

fail:
    return STATUS_FAILURE;
}

int socket_set_reusability(const int* socket_fd)
{
    if(!socket_fd)
    {
        EML_ERROR(LOG_TAG, "_set_reusability: invalid input");
        return STATUS_FAILURE;
    }

    /* Allow reuse of address (critical for restarts) */
    int yes = 1;

    if(setsockopt(*socket_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) != -1)
    {
        return STATUS_SUCCESS;
    }
    else
    {
        EML_PERR(LOG_TAG, "_set_reusability failed");
    }

    return STATUS_FAILURE;
}

int socket_set_restartability(const int* socket_fd)
{
    if(!socket_fd)
    {
        EML_ERROR(LOG_TAG, "_set_restartability: invalid input");
        return STATUS_FAILURE;
    }
    /* Options for restart:
     * Just kill it. Don’t wait. Don’t flush data. These options are ok for listeners.
     */
    struct linger sl;
    sl.l_onoff  = 1;
    sl.l_linger = 0;
    if(setsockopt(*socket_fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl)) != -1)
    {
        return STATUS_SUCCESS;
    }
    else
    {
        EML_PERR(LOG_TAG, "_set_restartability failed");
    }

    return STATUS_FAILURE;
}

int socket_disable_nagle(const int* socket_fd)
{
    int one = 1;

    if(!socket_fd)
    {
        EML_ERROR(LOG_TAG, "_disable_nagle: invalid input");
        return STATUS_FAILURE;
    }

    if(setsockopt(*socket_fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != -1)
    {
        return STATUS_SUCCESS;
    }
    else
    {
        EML_PERR(LOG_TAG, "_disable_nagle failed");
    }
    return STATUS_FAILURE;
}

int socket_listener_set_hints(struct addrinfo* hints)
{
    if(hints)
    {
        /* make sure hints setted to 0 */
        memset(hints, 0, sizeof(struct addrinfo));

        /* AF_UNSPEC allows IPv4 or IPv6 */
        hints->ai_family = AF_UNSPEC;

        /* The sockettype will automatically set the ai_protocol to
        IPPROTO_TCP 6 with SOCK_STREAM or IPPROTO_UDP 17 with SOCK_DGRAM */
        hints->ai_socktype = SOCK_STREAM;  // TCP

        /* Use my IP automatically */
        hints->ai_flags = AI_PASSIVE;

        return STATUS_SUCCESS;
    }

    return STATUS_FAILURE;
}

ssize_t socket_read_nonblocking(int fd, void* buf, size_t buf_len)
{
    if(fd < 0 || !buf || buf_len == 0)
    {
        EML_ERROR(LOG_TAG, "_read_nonblocking: invalid input");
        return -1;
    }

    static int retry_max   = 3;
    int        retry_count = 0;

retry:
{
    ssize_t red_bytes = read(fd, buf, buf_len);

    /* return immediately if read() succeeded */
    if(red_bytes > 0) return red_bytes;

    /* return failure if connection closed */
    if(red_bytes == 0) return STATUS_FAILURE;

    if(errno == EAGAIN) /* == EWOULDBLOCK on Linux */
    {
        /* No data available right now */
        EML_WARN(LOG_TAG, "_read_nonblocking: read with no data");
        return STATUS_SUCCESS;
    }

    if(errno == EINTR)
    {
        /* retry immediately */
        if(++retry_count < retry_max)
            goto retry;
        else
        {
            EML_ERROR(LOG_TAG, "socket_read_nonblocking: read() interrupted too many times");
            return STATUS_FAILURE;
        }
    }
}  // retry

    /* An actual error occurred */
    EML_ERROR(LOG_TAG, "socket_read_nonblocking: read() failed");
    return STATUS_FAILURE;
}

int64_t socket_drain(const int fd)
{
    uint64_t n;
    ssize_t  r = read(fd, &n, sizeof n);
    return (r == sizeof n) ? (int64_t)n : -1;
}

void socket_shutdown_and_close(int fd)
{
    /* shutdown(SHUT_RDWR) is a TCP notion (force the FIN/RST, wake a blocked
     * peer). On an AF_UNIX socket it buys nothing over close(), and because the
     * production sockets are pathname unix sockets passed by systemd, AppArmor
     * mediates the shutdown as a FILE op needing a 'd' permission on the socket
     * path (unavailable as a file-rule mode → an audit denial every close). So
     * shutdown ONLY IP sockets (the dev/direct-run fallback); unix just closes. */
    int       domain = 0;
    socklen_t dlen   = sizeof(domain);
    if(getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &domain, &dlen) == 0 && (domain == AF_INET || domain == AF_INET6))
    {
        /* send FIN – graceful half-close; no more receptions or transmissions */
        shutdown(fd, SHUT_RDWR);
    }

    /* Close the socket */
    close(fd);
}

int socket_listener_init(const int* listen_fd, const int32_t* ai_family)
{
    /* Return variable */
    int res = STATUS_FAILURE;

    /* Check input */
    if(listen_fd == NULL || ai_family == NULL)
    {
        EML_ERROR(LOG_TAG, "_listener_init: invalid input");
    }

    /* Set sockets reusability */
    else if(socket_set_reusability(listen_fd) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "_listener_init: _reusability failed.");
    }

    /* Set sockets restartability */
    else if(socket_set_restartability(listen_fd) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "_listener_init: _restartability failed.");
    }

    /* Set non-blocking mode */
    else if(socket_set_non_blocking(listen_fd) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "_listener_init: failed to set non-blocking");
    }

    /* Everything went ok */
    res = STATUS_SUCCESS;

    /* restrict the socket to only ipv6 if socket for ipv6 */
    if(*ai_family == AF_INET6)
    {
        /* yes value */
        int yes = 1;

        /* Check if restriction successfully occurred */
        if(setsockopt(*listen_fd, IPPROTO_IPV6, IPV6_V6ONLY, &yes, sizeof(yes)) != STATUS_SUCCESS)
        {
            /* if restriction fails set return variable to failure */
            res = STATUS_FAILURE;
            EML_ERROR(LOG_TAG, "Socket ipv6 opts failed: %s\n", strerror(errno));
        }
    }

    return res;
}

int socket_client_init(const int* client_fd)
{
    if(client_fd == NULL)
    {
        EML_ERROR(LOG_TAG, "_client_init: invalid input");
        return STATUS_FAILURE;
    }

    /* Set non-blocking mode */
    if(socket_set_non_blocking(client_fd) != STATUS_SUCCESS)
    {
        EML_ERROR(LOG_TAG, "_client_init: failed to set non-blocking");
        return STATUS_FAILURE;
    }

    /* TCP_NODELAY and the TCP-RST SO_LINGER dance are TCP-only. In production the
     * peer is nginx over an AF_UNIX socket (no Nagle, no RST semantics), where
     * setsockopt(IPPROTO_TCP, …) fails EOPNOTSUPP. Apply those options ONLY to
     * IP sockets (the dev/direct-run fallback); skip them for AF_UNIX. */
    int       domain = 0;
    socklen_t dlen   = sizeof(domain);
    if(getsockopt(*client_fd, SOL_SOCKET, SO_DOMAIN, &domain, &dlen) == -1)
    {
        EML_PERR(LOG_TAG, "_client_init: failed to read SO_DOMAIN");
        return STATUS_FAILURE;
    }

    if(domain == AF_INET || domain == AF_INET6)
    {
        /* Disable Nagle's algorithm */
        if(socket_disable_nagle(client_fd) != STATUS_SUCCESS)
        {
            EML_ERROR(LOG_TAG, "_client_init: failed to disable Nagle");
            return STATUS_FAILURE;
        }

        /* Accepted sockets INHERIT SO_LINGER from the listener on Linux; the
         * listener uses {on, 0} (RST on close) for fast restarts. A client socket must close gracefully or the response written just before
         * close() is discarded with the RST — reset to the default. */
        struct linger sl = {.l_onoff = 0, .l_linger = 0};
        if(setsockopt(*client_fd, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl)) == -1)
        {
            EML_PERR(LOG_TAG, "_client_init: failed to reset SO_LINGER");
            return STATUS_FAILURE;
        }
    }

    return STATUS_SUCCESS;
}

int socket_get_peer_ipv4(int fd, uint32_t* out_ip_be, uint16_t* out_port_be)
{
    if(!out_ip_be || !out_port_be)
    {
        EML_ERROR(LOG_TAG, "_get_peer_ipv4: invalid input");
        return STATUS_FAILURE;
    }

    *out_ip_be   = 0u;
    *out_port_be = 0u;

    struct sockaddr_storage peer;
    socklen_t               peer_len = sizeof peer;
    if(getpeername(fd, (struct sockaddr*)&peer, &peer_len) == -1)
    {
        /* Not fatal: an unconnected/closed/racing fd just leaves the 0 sentinel — see the header doc. */
        return STATUS_SUCCESS;
    }

    if(peer.ss_family != AF_INET)
    {
        /* AF_UNIX (production transport) or AF_INET6: no uint32_t representation, sentinel stays. */
        return STATUS_SUCCESS;
    }

    const struct sockaddr_in* sin = (const struct sockaddr_in*)&peer;
    *out_ip_be                    = sin->sin_addr.s_addr; /* already network byte order */
    *out_port_be                  = sin->sin_port;        /* already network byte order */
    return STATUS_SUCCESS;
}

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */
/* None */
