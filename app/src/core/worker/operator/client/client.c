/**
 * @file client.c
 * @brief Per-connection I/O and HTTP parsing loop executed inside an operator.
 *
 * Each client owns one cumulative receive buffer and one DB_http parser handle. DB_http stores zero-copy string views into the cumulative
 * buffer, so this layer appends new bytes, feeds only the new byte count, and keeps the buffer stable while the parsed request snapshot is
 * inspected by higher layers.
 */
#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif /* _GNU_SOURCE */

#include <db_server/core/worker/operator/client/client.h>

#include <errno.h>
#include <string.h>
#include <unistd.h>

#include <DB_http/DB_http.h>
#include <emlog.h>

#include <db_server/core/worker/operator/client/response_writer.h>
#include <db_server/core/worker/operator/client/router/router.h>
#include <db_server/core/worker/operator/client/upload_pump.h>
#include <db_server/utils/socket_helper.h>
#include <db_server/utils/time_helper.h>

#include <sys/socket.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define LOG_TAG "srv_client"

/*****************************************************************************************************************************************
 * PRIVATE FUNCTION DECLARATIONS
 *****************************************************************************************************************************************
 */

static void _client_reset_message(client_t* cli, int clear_stored_request);
static int  _client_store_request(client_t* cli, const DB_http_request_t* req, uint8_t thread_id);

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int client_handle(client_t* cli, uint8_t thread_id)
{
    if(!cli || !cli->http_parser || cli->ctx.fd < 0)
    {
        EML_ERROR(LOG_TAG, "client_handle: invalid input");
        return STATUS_FAILURE;
    }

    for(;;)
    {
        if(cli->buf_idx >= DB_HTTP_MAX_BUFFER_LEN_B)
        {
            EML_ERROR(LOG_TAG, "fd %d: receive buffer limit reached", cli->ctx.fd);
            goto fail;
        }

        const size_t writable   = DB_HTTP_MAX_BUFFER_LEN_B - cli->buf_idx;
        ssize_t      read_bytes = read(cli->ctx.fd, cli->buf + cli->buf_idx, writable);

        if(read_bytes > 0)
        {
            DB_http_request_t* parsed_req = NULL;
            const size_t       new_bytes  = (size_t)read_bytes;

            cli->last_activity  = (uint64_t)time_helper_get_now();
            cli->buf_idx       += new_bytes;

#ifdef DEBUG
            EML_DBG(LOG_TAG, "fd %d received %zu bytes, buffered=%zu", cli->ctx.fd, new_bytes, cli->buf_idx);
#endif

            DB_http_status_t parse_status = db_http_parser_exec(cli->http_parser, cli->buf, new_bytes, &parsed_req);

            /* §9.4: the stream gate claimed this request at headers-complete — the upload pump owns the body from here.
             * Uploads are one-shot: a response has been sent on every pump path, and the connection closes either way —
             * forced here (not left to whatever the ORIGINAL request's Connection header said) so that decision is
             * correct and visible to response_writer_resume() too, if this response's send ends up parking. */
            if(parse_status == DB_http_status_HeadersComplete_Stream)
            {
                cli->connection_policy = (uint8_t)HTTP_CONNECTION_CLOSE;
                (void)client_upload_pump(cli, thread_id, parsed_req);
                if(cli->draining)
                {
                    /* Parked mid-send (response_writer.c) — response_writer_resume() closes once the drain
                     * finishes; do NOT reset cli->buf/goto fail out from under it while it's still in flight. */
                    return STATUS_SUCCESS;
                }
                goto fail; /* the fail label is also the clean-close path (see HTTP_CONNECTION_CLOSE below) */
            }

            if(parse_status != DB_http_status_OK)
            {
                EML_ERROR(LOG_TAG, "fd %d: HTTP parse failed with status %d", cli->ctx.fd, parse_status);
                /* Terse answer on the wire (413 for an over-declared body, 400 for everything else — including rejected
                 * pipelining and chunked bodies), loud detail above; connection closes (response_writer_error() sets
                 * that itself now, immediately or deferred to response_writer_resume() if this parks). */
                (void)response_writer_error(cli, parse_status == DB_http_status_ContentTooLarge ? 413u : 400u);
                if(cli->draining)
                {
                    return STATUS_SUCCESS;
                }
                goto fail;
            }

            if(!parsed_req)
            {
                EML_ERROR(LOG_TAG, "fd %d: DB_http returned no request DTO", cli->ctx.fd);
                goto fail;
            }

            if(parsed_req->message_complete)
            {
                if(_client_store_request(cli, parsed_req, thread_id) != STATUS_SUCCESS)
                {
                    goto fail;
                }

#ifdef DEBUG
                EML_DBG(LOG_TAG, "fd %d parsed request: method=%u path=%.*s headers=%u body=%zu policy=%u", cli->ctx.fd,
                        (unsigned)cli->http_request.method, (int)cli->http_request.path.n,
                        cli->http_request.path.p ? cli->http_request.path.p : "", (unsigned)cli->http_request.header_count,
                        cli->http_request.body.n, (unsigned)cli->http_request.connection_policy);
#endif

                /* §9.2 sequence: dispatch runs db_app_run() while the request
                 * views into cli->buf are alive, then serializes the response
                 * INTO cli->buf. The parser is cleared only afterwards. */
                int dispatch_rc = srv_router_dispatch(cli);

                db_http_parser_clear(cli->http_parser);
                cli->buf_idx = 0u;
                memset(&cli->http_request, 0, sizeof(cli->http_request));

                if(dispatch_rc != STATUS_SUCCESS)
                {
                    goto fail;
                }
                if(cli->draining)
                {
                    /* response_writer.c parked this fd on EPOLLOUT (§9.2 end state) — it, not this
                     * function, decides close-vs-keep-alive once the send actually finishes
                     * (response_writer_resume()). Nothing else to do on this dispatch. */
                    return STATUS_SUCCESS;
                }
                if(cli->connection_policy == (uint8_t)HTTP_CONNECTION_CLOSE)
                {
                    /* Clean close after a fully sent response. */
                    goto fail;
                }

                return STATUS_SUCCESS;
            }

            continue;
        }

        if(read_bytes == 0)
        {
            EML_INFO(LOG_TAG, "fd %d: peer closed connection", cli->ctx.fd);
            goto fail;
        }

        if(errno == EINTR)
        {
            continue;
        }

        if(errno == EAGAIN) /* == EWOULDBLOCK on Linux */
        {
            return STATUS_SUCCESS;
        }

        EML_PERR(LOG_TAG, "fd %d: read failed", cli->ctx.fd);
        goto fail;
    }

fail:
    _client_reset_message(cli, 1);
    return STATUS_FAILURE;
}

void client_adopt_fd(client_t* cli, int fd)
{
    if(!cli)
    {
        return;
    }

    /* One place for all per-connection reset: buffer, request snapshot, counters, parser, fd ownership. */
    _client_reset_message(cli, 1);
    if(cli->http_parser)
    {
        db_http_parser_clear(cli->http_parser);
    }

    cli->buf_idx           = 0u;
    cli->connection_policy = 0u;
    cli->request_count     = 0u;
    cli->is_busy           = 1u;
    cli->ctx.fd            = fd;
    cli->ctx.owner         = NULL;
    cli->ctx.handler       = NULL;
    cli->last_activity     = (uint64_t)time_helper_get_now();

    /* Captured once here (this is the upload-pool adoption path — the operator's own
     * _operator_add_client() does the equivalent capture for the API path). */
    (void)socket_get_peer_ipv4(fd, &cli->remote_ip_be, &cli->remote_port_be);
}

void client_release_fd(client_t* cli)
{
    if(!cli)
    {
        return;
    }

    if(cli->ctx.fd >= 0)
    {
        socket_shutdown_and_close(cli->ctx.fd);
    }
    if(cli->http_parser)
    {
        db_http_parser_clear(cli->http_parser);
    }
    _client_reset_message(cli, 1);

    cli->ctx.fd            = -1;
    cli->ctx.owner         = NULL;
    cli->ctx.handler       = NULL;
    cli->is_busy           = 0u;
    cli->connection_policy = 0u;
    cli->last_activity     = 0u;
    cli->request_count     = 0u;
    cli->remote_ip_be      = 0u;
    cli->remote_port_be    = 0u;
}

void client_shutdown(client_t* cli)
{
    if(!cli)
    {
        EML_ERROR(LOG_TAG, "shutdown: invalid input");
        return;
    }

    if(cli->is_busy)
    {
        _client_reset_message(cli, 1);

        socket_shutdown_and_close(cli->ctx.fd);
        cli->ctx.fd      = -1;
        cli->ctx.owner   = NULL;
        cli->ctx.handler = NULL;

        cli->is_busy           = 0u;
        cli->connection_policy = 0u;
        cli->last_activity     = 0u;
        cli->request_count     = 0u;
        cli->remote_ip_be      = 0u;
        cli->remote_port_be    = 0u;
    }
}

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static int _client_store_request(client_t* cli, const DB_http_request_t* req, uint8_t thread_id)
{
    if(!cli || !req || !req->message_complete)
    {
        EML_ERROR(LOG_TAG, "store_request: invalid input");
        return STATUS_FAILURE;
    }

    cli->http_request           = *req;
    cli->http_request.thread_id = thread_id;
    cli->http_request.timestamp = cli->last_activity;

    /* Connection-lifetime fact, captured once at adopt time (client_adopt_fd() / the operator's
     * _operator_add_client()) — every message on this connection carries the same peer address. */
    cli->http_request.remote_ip_be   = cli->remote_ip_be;
    cli->http_request.remote_port_be = cli->remote_port_be;

    cli->connection_policy = cli->http_request.connection_policy;
    cli->request_count++;

    return STATUS_SUCCESS;
}

static void _client_reset_message(client_t* cli, int clear_stored_request)
{
    if(!cli)
    {
        return;
    }

    if(cli->http_parser)
    {
        db_http_parser_clear(cli->http_parser);
    }

    cli->buf_idx = 0u;
    memset(cli->buf, 0, sizeof(cli->buf));

    /* Defensive: normally already 0 by the time this runs (a client reaches client_handle()'s fail
     * label only when NOT draining — the reactor dispatch routes a draining client to
     * response_writer_resume() instead — and a graceful drain completion clears these itself). Reset
     * here too so an abrupt disconnect mid-drain (EPOLLRDHUP/HUP/ERR -> client_shutdown() -> here)
     * can never leave stale send state behind for the next connection this slot serves. */
    cli->draining = 0u;
    cli->send_off = 0u;
    cli->send_len = 0u;

    if(clear_stored_request)
    {
        memset(&cli->http_request, 0, sizeof(cli->http_request));
    }
}
