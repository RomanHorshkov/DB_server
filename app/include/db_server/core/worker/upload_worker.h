/**
 * @file upload_worker.h
 * @brief Dedicated upload worker pool — isolates upload execution from API operators.
 *
 * The problem (proven by fleet-test): the upload pump `poll()`s the calling
 * thread until the transfer finishes, so an upload run inside an API operator
 * blocks every other request on that operator (a slow upload can make /ping time
 * out). This pool runs the FULL upload lifecycle (parse headers → authorize →
 * pump → commit → 201) on its own small set of threads, fed by a dedicated
 * AF_UNIX socket. API operators never touch an upload fd, so a stalled upload
 * changes API latency only slightly and can never stall /ping.
 *
 * The pump code is reused verbatim (client_handle → client_upload_pump); it
 * simply runs on an upload worker instead of an operator. See
 * DB_server/README.md.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */
#ifndef DB_SERVER_CORE_WORKER_UPLOAD_WORKER_H
#define DB_SERVER_CORE_WORKER_UPLOAD_WORKER_H

/*****************************************************************************************************************************************
 * PUBLIC INCLUDES
 *****************************************************************************************************************************************
 */
#include <stdint.h>

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS PROTOTYPES
 *****************************************************************************************************************************************
 */

/**
 * @brief Start the upload worker pool.
 * @param[in] n_workers      Number of upload threads (a small fixed pool; uploads are I/O-bound).
 * @param[in] worker_no_base First DB_app transaction slot for this pool — MUST NOT overlap the operators'
 *                           slots [0, ops). Workers use worker_no_base + i, so db_app_init() must have been
 *                           called with (ops + n_workers) slots.
 * @param[in] queue_depth    Bounded queue depth beyond the busy workers (1..the pool's physical max).
 *                           Past (n_workers + queue_depth) concurrent uploads, dispatch rejects (503).
 * @return 0 on success, -1 on failure.
 */
int upload_workers_init(uint8_t n_workers, uint8_t worker_no_base, uint16_t queue_depth);

/**
 * @brief Hand an accepted upload connection to the pool.
 * @param[in] fd The accepted upload socket fd (ownership transfers to the pool on success).
 * @return 0 if queued; -1 if the pool is saturated (the caller must answer 503 + close — backpressure).
 */
int upload_worker_dispatch(int fd);

/** @brief Stop the pool: wake all workers, drain, join. Idempotent. */
void upload_workers_shutdown(void);

/** @brief Number of running upload workers (0 if the pool was never started). */
uint8_t upload_workers_count(void);

#endif /* DB_SERVER_CORE_WORKER_UPLOAD_WORKER_H */
