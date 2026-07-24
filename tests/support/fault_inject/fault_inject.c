/**
 * @file fault_inject.c
 *
 * @brief LD_PRELOAD fault-injection shim for DB_server's OWN direct filesystem syscalls plus, since
 *        interposition is process-wide, everything the statically-linked libdb_app.a (and, through it,
 *        libdb_lmdb.a) does in the same process — intercepts open/open64/openat/openat64, close, read,
 *        and rename, and fails ONE (or, with PERSIST, every subsequent) matching call with a configured
 *        errno.
 *
 * This is the sibling of DB_lmdb/tests/support/fault_inject/fault_inject.c, which wraps the
 * write/fsync/msync syscalls a real LMDB commit issues. That harness lives in a DIFFERENT layer: this
 * one targets the filesystem operations OUTSIDE DB_lmdb that a request driven through the real
 * db_server binary can reach — investigation (see docs/HARDENING_TODO.md) found DB_server's OWN source
 * makes very few direct fs calls on the upload/download path (close()+read() on a blob fd
 * db_app_folder_zip_open() hands it, in app/src/core/worker/operator/client/folder_zip_pump.c); the
 * open()/rename() for upload-commit staging live one layer down, in DB_lmdb's files_stage_open/promote
 * (already covered by DB_lmdb's own harness) reached via DB_app's app/src/db_app/files/upload.c
 * (close()/fsync() there are also DB_app's own direct calls). Because LD_PRELOAD interposes at the
 * process's dynamic-symbol level, arming this shim against the real db_server binary reaches ALL of
 * that in one running process — the tests built on top of it (tests/fault_injection_test.sh) verify
 * DB_server's OWN observable behavior (a clean HTTP error response, no crash, no fd leak, no hang) when
 * a failure lands anywhere on that path, which is the property this project actually needs proven.
 *
 * Unlike DB_lmdb's harness (a cmocka test binary that self-re-execs itself with the shim on
 * LD_PRELOAD), this project's fault-injection tests drive a REAL, SEPARATELY LAUNCHED db_server
 * process from a shell script (this project's tests/ shell-script convention — see spsc_dispatch_load_test.sh), so there is no
 * self-re-exec dance here: the shell script simply sets LD_PRELOAD and the arming env vars before
 * starting the binary. See tests/fault_injection_test.sh for the harness build + arm/disarm protocol.
 *
 * Arming protocol (all via environment variables, read fresh on every intercepted call, so a single
 * long-lived server process can be armed/fired/re-armed across many requests without restarting):
 *
 *   DB_SERVER_FI_TARGET   which call to fail: "open" | "openat" | "close" | "read" | "rename" | "any"
 *                         (matches every intercepted call, including the open64/openat64 aliases).
 *                         Unset or empty disables interception entirely (every call passes through).
 *   DB_SERVER_FI_AFTER    unsigned count of MATCHING calls to let through before the failure (0 = fail
 *                         the very next matching call).
 *   DB_SERVER_FI_ERRNO    "EACCES" | "EPERM" | "EMFILE" | "ENFILE" | "EXDEV" | "ENOENT" | "EIO"
 *                         (defaults to EIO if unrecognized).
 *   DB_SERVER_FI_GEN      arbitrary token; changing it re-arms (resets the internal call counter and
 *                         the one-shot latch) without needing a fresh process.
 *   DB_SERVER_FI_PERSIST  "1" to keep failing every matching call from the Nth one onward instead of
 *                         just the one (default: exactly one failure per arm/generation).
 *   DB_SERVER_FI_PATH_SUBSTR  Optional. When set, an open/openat/rename/renameat call additionally
 *                         only matches if ITS pathname (for renameat, the OLD path — the tmp name in
 *                         every write-then-rename-atomic caller this project has, per
 *                         db_lmdb_fs_write_file_atomic()) contains this substring. Unset = match by
 *                         call type alone, same as DB_lmdb's harness. close/read have no pathname to
 *                         filter on and ignore this variable entirely.
 *
 * NOTE on rename() vs renameat(): DB_lmdb's blob-promotion path (database/files/files.c) calls the RAW
 * syscall(SYS_renameat2, ...) directly (its own file header explains why: atomic no-clobber semantics
 * without depending on a libc wrapper), which — on any system where SYS_renameat2 is defined at compile
 * time, true of every mainstream Linux target — bypasses LD_PRELOAD symbol interposition entirely; no
 * userspace shim can intercept a raw syscall() call. This shim CAN still reach every renameat() the
 * project actually calls through the libc symbol: db_lmdb_fs_write_file_atomic()'s tmp-then-rename
 * (database/core/fsops.c, used by the schema manifest writer at every fresh LMDB env open — a boot-time,
 * no-auth-needed instance of the exact same crash-safe write-then-rename pattern the upload-commit path
 * uses) and files.c's OWN portable #else fallback (systems without SYS_renameat2). This project's tests
 * target the former (see tests/fault_injection_startup_test.sh) and document the syscall() gap rather
 * than silently pretending to cover it.
 *
 * @author  Roman Horshkov <github.com/RomanHorshkov>
 * @date    jul 2026
 * (c) 2026
 */

#ifndef _GNU_SOURCE
#    define _GNU_SOURCE
#endif

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

/*****************************************************************************************************************************************
 * PRIVATE DEFINES
 *****************************************************************************************************************************************
 */

#define _FI_GEN_MAX 64u

/*****************************************************************************************************************************************
 * PRIVATE VARIABLES
 *****************************************************************************************************************************************
 */

static pthread_mutex_t g_fi_lock                 = PTHREAD_MUTEX_INITIALIZER;
static char            g_fi_last_gen[_FI_GEN_MAX] = "";
static unsigned        g_fi_call_count            = 0u;
static int             g_fi_fired                 = 0;

/* Real libc entry points, resolved lazily via dlsym(RTLD_NEXT, ...) on first use. */
static int (*real_open)(const char*, int, ...)         = NULL;
static int (*real_open64)(const char*, int, ...)       = NULL;
static int (*real_openat)(int, const char*, int, ...)  = NULL;
static int (*real_openat64)(int, const char*, int, ...) = NULL;
static int (*real_close)(int)                          = NULL;
static ssize_t (*real_read)(int, void*, size_t)        = NULL;
static int (*real_rename)(const char*, const char*)    = NULL;
static int (*real_renameat)(int, const char*, int, const char*) = NULL;

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS PROTOTYPES
 *****************************************************************************************************************************************
 */

static int fi_errno_from_name(const char* name);
static int fi_should_fail(const char* target_name);
static int fi_should_fail_path(const char* target_name, const char* pathname);

/*****************************************************************************************************************************************
 * PRIVATE FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

static int fi_errno_from_name(const char* name)
{
    if(name == NULL) return EIO;
    if(strcmp(name, "EACCES") == 0) return EACCES;
    if(strcmp(name, "EPERM") == 0) return EPERM;
    if(strcmp(name, "EMFILE") == 0) return EMFILE;
    if(strcmp(name, "ENFILE") == 0) return ENFILE;
    if(strcmp(name, "EXDEV") == 0) return EXDEV;
    if(strcmp(name, "ENOENT") == 0) return ENOENT;
    if(strcmp(name, "EIO") == 0) return EIO;
    return EIO;
}

/** @brief Decide, under the lock, whether THIS call (of @p target_name) should fail.
 *  @return the errno to inject, or 0 if the call must pass through untouched. */
static int fi_should_fail(const char* target_name)
{
    return fi_should_fail_path(target_name, NULL);
}

/** @brief Same as fi_should_fail(), plus an optional DB_SERVER_FI_PATH_SUBSTR filter — @p pathname is
 *         only consulted when the caller has one (open/openat/rename/renameat); NULL disables the
 *         filter regardless of whether the env var is set (close/read calls). */
static int fi_should_fail_path(const char* target_name, const char* pathname)
{
    const char* target;
    const char* gen;
    const char* after_s;
    const char* persist_s;
    const char* errno_s;
    const char* path_substr;
    unsigned    after_n;
    int         persist;
    int         inject_errno = 0;

    target = getenv("DB_SERVER_FI_TARGET");
    if(target == NULL || target[0] == '\0')
    {
        return 0;
    }
    if(strcmp(target, "any") != 0 && strcmp(target, target_name) != 0)
    {
        return 0;
    }

    path_substr = getenv("DB_SERVER_FI_PATH_SUBSTR");
    if(pathname != NULL && path_substr != NULL && path_substr[0] != '\0' && strstr(pathname, path_substr) == NULL)
    {
        return 0;
    }

    gen       = getenv("DB_SERVER_FI_GEN");
    after_s   = getenv("DB_SERVER_FI_AFTER");
    persist_s = getenv("DB_SERVER_FI_PERSIST");
    errno_s   = getenv("DB_SERVER_FI_ERRNO");
    after_n   = (after_s != NULL) ? (unsigned)strtoul(after_s, NULL, 10) : 0u;
    persist   = (persist_s != NULL && strcmp(persist_s, "1") == 0);

    pthread_mutex_lock(&g_fi_lock);

    if(gen == NULL) gen = "";
    if(strncmp(gen, g_fi_last_gen, _FI_GEN_MAX - 1u) != 0)
    {
        strncpy(g_fi_last_gen, gen, _FI_GEN_MAX - 1u);
        g_fi_last_gen[_FI_GEN_MAX - 1u] = '\0';
        g_fi_call_count                = 0u;
        g_fi_fired                     = 0;
    }

    if(g_fi_fired && !persist)
    {
        pthread_mutex_unlock(&g_fi_lock);
        return 0;
    }

    if(g_fi_call_count == after_n)
    {
        g_fi_fired   = 1;
        inject_errno = fi_errno_from_name(errno_s);
    }
    else
    {
        ++g_fi_call_count;
    }

    pthread_mutex_unlock(&g_fi_lock);
    return inject_errno;
}

/*****************************************************************************************************************************************
 * PUBLIC FUNCTIONS DEFINITIONS
 *****************************************************************************************************************************************
 */

int open(const char* pathname, int flags, ...)
{
    mode_t  mode        = 0;
    va_list ap;
    if(flags & (O_CREAT
#ifdef O_TMPFILE
                | O_TMPFILE
#endif
                ))
    {
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }

    if(real_open == NULL) real_open = dlsym(RTLD_NEXT, "open");
    int fail_errno = fi_should_fail_path("open", pathname);
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    return real_open(pathname, flags, mode);
}

int open64(const char* pathname, int flags, ...)
{
    mode_t  mode = 0;
    va_list ap;
    if(flags & (O_CREAT
#ifdef O_TMPFILE
                | O_TMPFILE
#endif
                ))
    {
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }

    if(real_open64 == NULL) real_open64 = dlsym(RTLD_NEXT, "open64");
    int fail_errno = fi_should_fail_path("open", pathname);
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    /* Some libc builds route open64() itself back through the plain "open" symbol; fall back if this
     * process's libc never resolves a distinct "open64" (mirrors fault_inject.c's pwrite64 fallback in
     * DB_lmdb's harness). */
    if(real_open64 != NULL) return real_open64(pathname, flags, mode);
    if(real_open == NULL) real_open = dlsym(RTLD_NEXT, "open");
    return real_open(pathname, flags, mode);
}

int openat(int dirfd, const char* pathname, int flags, ...)
{
    mode_t  mode = 0;
    va_list ap;
    if(flags & (O_CREAT
#ifdef O_TMPFILE
                | O_TMPFILE
#endif
                ))
    {
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }

    if(real_openat == NULL) real_openat = dlsym(RTLD_NEXT, "openat");
    int fail_errno = fi_should_fail_path("openat", pathname);
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    return real_openat(dirfd, pathname, flags, mode);
}

int openat64(int dirfd, const char* pathname, int flags, ...)
{
    mode_t  mode = 0;
    va_list ap;
    if(flags & (O_CREAT
#ifdef O_TMPFILE
                | O_TMPFILE
#endif
                ))
    {
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }

    if(real_openat64 == NULL) real_openat64 = dlsym(RTLD_NEXT, "openat64");
    int fail_errno = fi_should_fail_path("openat", pathname);
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    if(real_openat64 != NULL) return real_openat64(dirfd, pathname, flags, mode);
    if(real_openat == NULL) real_openat = dlsym(RTLD_NEXT, "openat");
    return real_openat(dirfd, pathname, flags, mode);
}

int close(int fd)
{
    if(real_close == NULL) real_close = dlsym(RTLD_NEXT, "close");
    int fail_errno = fi_should_fail("close");
    if(fail_errno != 0)
    {
        /* A REAL close(2) failure (EIO flushing on some filesystems, EBADF, ...) still releases the fd
         * kernel-side in every case that matters here (Linux never leaves an fd allocated after a
         * close() that returns EBADF/EINTR-once-retried/EIO) -- so, to inject a genuinely observable
         * "close failed" condition without ALSO leaking the real fd out from under the caller (which
         * would corrupt this shim's own process, not just the callee under test), still call through to
         * the real close() and then report the injected failure on top of it. This matches what a real
         * close()-time EIO looks like: the descriptor is gone either way, only the return value/errno
         * differ. */
        real_close(fd);
        errno = fail_errno;
        return -1;
    }
    return real_close(fd);
}

ssize_t read(int fd, void* buf, size_t count)
{
    if(real_read == NULL) real_read = dlsym(RTLD_NEXT, "read");
    int fail_errno = fi_should_fail("read");
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    return real_read(fd, buf, count);
}

int rename(const char* oldpath, const char* newpath)
{
    if(real_rename == NULL) real_rename = dlsym(RTLD_NEXT, "rename");
    int fail_errno = fi_should_fail_path("rename", oldpath);
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    return real_rename(oldpath, newpath);
}

int renameat(int olddirfd, const char* oldpath, int newdirfd, const char* newpath)
{
    if(real_renameat == NULL) real_renameat = dlsym(RTLD_NEXT, "renameat");
    int fail_errno = fi_should_fail_path("renameat", oldpath);
    if(fail_errno != 0)
    {
        errno = fail_errno;
        return -1;
    }
    return real_renameat(olddirfd, oldpath, newdirfd, newpath);
}
