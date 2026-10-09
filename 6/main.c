#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#define PORT              8080
#define RBUF              4096
#define MAXCONN           16384
#define MAXEV             256
#define IDLE_SEC          15
#define MAXCACHE          (512 * 1024)
#define ARENA_SZ          (256UL << 20)
#define TSIZE             4096
#define HEADER_BUFSZ      512

/* Arena: Bump Allocator */
typedef struct {
    char *base;
    size_t cap;
    size_t used;
} Arena;

static void *Arena_alloc(Arena *a, size_t n) {
    n = (n + 15) & ~(size_t)15;
    if (a->used + n > a->cap) return NULL;
    void *p = a->base + a->used;
    a->used += n;
    return p;
}

/* CacheEntry: mmap */
typedef struct {
    uint64_t hash;
    const char *path;
    const char *mime;
    char *mmap_ptr;
    off_t size;
    time_t mtime;
    int gone;
} CacheEntry;

static uint64_t hash_path(const char *path) {
    uint64_t h = 5381;
    for (const char *p = path; *p; p++)
        h = ((h << 5) + h) ^ (unsigned char)*p;
    return h;
}

static const char *mime_type(const char *path) {
    const char *dot = strrchr(path, '.');
    if (!dot) return "application/octet-stream";
    if (strcmp(dot, ".html") == 0 || strcmp(dot, ".htm") == 0) return "text/html; charset=utf-8";
    if (strcmp(dot, ".css") == 0) return "text/css; charset=utf-8";
    if (strcmp(dot, ".js") == 0) return "text/javascript";
    if (strcmp(dot, ".png") == 0) return "image/png";
    if (strcmp(dot, ".jpg") == 0 || strcmp(dot, ".jpeg") == 0) return "image/jpeg";
    if (strcmp(dot, ".gif") == 0) return "image/gif";
    if (strcmp(dot, ".svg") == 0) return "image/svg+xml";
    if (strcmp(dot, ".txt") == 0) return "text/plain; charset=utf-8";
    return "application/octet-stream";
}

/* Conn: Connection pool pre-allocated */
typedef struct Conn {
    int fd, ffd;
    int keep_alive;
    time_t last_activity;
    char *rbuf;
    size_t rlen;
    const char *resp_p[2];
    size_t resp_l[2];
    off_t foff, fsize;
    char hdr[HEADER_BUFSZ];
    struct Conn *next;
} Conn;

/* Thread: Worker */
typedef struct {
    int epfd, hw;
    Conn *conns;
    Conn *free_list;
    Arena arena;
    CacheEntry *cache_table;
} Thread;

static __thread time_t thread_now;

static void Thread_init(Thread *t) {
    t->conns = mmap(NULL, sizeof(Conn) * MAXCONN, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    t->arena.base = mmap(NULL, ARENA_SZ, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    t->arena.cap = ARENA_SZ;
    t->cache_table = calloc(TSIZE, sizeof(CacheEntry));
    t->epfd = epoll_create1(EPOLL_CLOEXEC);
    
    if (t->conns == MAP_FAILED || t->arena.base == MAP_FAILED || !t->cache_table) {
        perror("mmap");
        exit(1);
    }

    for (int i = 0; i < MAXCONN; i++) {
        t->conns[i].rbuf = Arena_alloc(&t->arena, RBUF);
        t->conns[i].ffd = -1;
        t->conns[i].fd = -1;
        t->conns[i].next = (i + 1 < MAXCONN) ? &t->conns[i + 1] : NULL;
    }
    t->free_list = &t->conns[0];
}

static void serve_file(Thread *t, Conn *c, const char *path, int head) {
    uint64_t h = hash_path(path);
    CacheEntry *e = &t->cache_table[h % TSIZE];

    while (e->path && !(e->hash == h && strcmp(e->path, path) == 0))
        e = &t->cache_table[(e - t->cache_table + 1) % TSIZE];

    struct stat st;
    int fd = open(path, O_RDONLY | O_CLOEXEC);

    if (fd < 0 || fstat(fd, &st) < 0 || !S_ISREG(st.st_mode)) {
        if (fd >= 0) close(fd);
        int len = snprintf(c->hdr, HEADER_BUFSZ,
                          "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n"
                          "Content-Length: 9\r\nConnection: close\r\n\r\n");
        c->resp_p[0] = c->hdr;
        c->resp_l[0] = len;
        c->resp_l[1] = 0;
        c->keep_alive = 0;
        return;
    }

    if (st.st_size <= MAXCACHE && !e->path) {
        char *mapped = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (mapped != MAP_FAILED) {
            e->hash = h;
            e->path = path;
            e->mime = mime_type(path);
            e->mmap_ptr = mapped;
            e->size = st.st_size;
            close(fd);

            int hlen = snprintf(c->hdr, HEADER_BUFSZ,
                               "HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
                               "Content-Length: %lld\r\nConnection: keep-alive\r\n\r\n",
                               e->mime, (long long)e->size);
            c->resp_p[0] = c->hdr;
            c->resp_l[0] = hlen;
            c->resp_p[1] = head ? NULL : e->mmap_ptr;
            c->resp_l[1] = head ? 0 : (size_t)e->size;
            c->keep_alive = 1;
            return;
        }
    }

    int hlen = snprintf(c->hdr, HEADER_BUFSZ,
                       "HTTP/1.1 200 OK\r\nContent-Type: %s\r\n"
                       "Content-Length: %lld\r\nConnection: keep-alive\r\n\r\n",
                       mime_type(path), (long long)st.st_size);
    c->resp_p[0] = c->hdr;
    c->resp_l[0] = hlen;
    c->resp_l[1] = 0;
    c->ffd = fd;
    c->foff = 0;
    c->fsize = head ? 0 : st.st_size;
    c->keep_alive = 1;
}

static int flush_response(Conn *c) {
    while (c->resp_l[0] || c->resp_l[1]) {
        struct iovec iv[2];
        int n = 0;
        if (c->resp_l[0]) { iv[n].iov_base = (void *)c->resp_p[0]; iv[n].iov_len = c->resp_l[0]; n++; }
        if (c->resp_l[1]) { iv[n].iov_base = (void *)c->resp_p[1]; iv[n].iov_len = c->resp_l[1]; n++; }

        struct msghdr msg = {0};
        msg.msg_iov = iv;
        msg.msg_iovlen = n;

        ssize_t w = sendmsg(c->fd, &msg, MSG_NOSIGNAL | (c->foff < c->fsize ? MSG_MORE : 0));
        if (w < 0) return errno == EAGAIN ? 0 : -1;
        for (int i = 0; i < 2; i++) {
            size_t k = (size_t)w < c->resp_l[i] ? (size_t)w : c->resp_l[i];
            c->resp_p[i] += k;
            c->resp_l[i] -= k;
            w -= (ssize_t)k;
        }
    }

    while (c->foff < c->fsize) {
        ssize_t w = sendfile(c->fd, c->ffd, &c->foff, c->fsize - c->foff);
        if (w < 0) return errno == EAGAIN ? 0 : -1;
        if (w == 0) return -1;
    }
    if (c->ffd >= 0) { close(c->ffd); c->ffd = -1; }
    return 1;
}

#define PENDING(c) ((c)->resp_l[0] || (c)->resp_l[1] || (c)->foff < (c)->fsize)

static void drive_conn(Thread *t, Conn *c) {
    c->last_activity = thread_now;
    for (;;) {
        if (PENDING(c)) {
            int r = flush_response(c);
            if (r < 0 || (r == 1 && !c->keep_alive)) goto close_conn;
            if (r == 0) return;
        }

        const char *end = memmem(c->rbuf, c->rlen, "\r\n\r\n", 4);
        if (!end) {
            if (c->rlen >= RBUF) { c->keep_alive = 0; goto close_conn; }
            ssize_t n = read(c->fd, c->rbuf + c->rlen, RBUF - c->rlen);
            if (n > 0) { c->rlen += n; continue; }
            if (n < 0 && errno == EAGAIN) return;
            goto close_conn;
        }

        size_t used = (end + 4) - c->rbuf;
        char *uri_start = memchr(c->rbuf, '/', used);
        if (!uri_start) goto close_conn;
        char *uri_end = memchr(uri_start, ' ', end - uri_start);
        if (!uri_end) goto close_conn;

        size_t uri_len = uri_end - uri_start;
        char path[512];
        if (uri_len == 1) strcpy(path, "index.html");
        else { memcpy(path, uri_start + 1, uri_len - 1); path[uri_len - 1] = 0; }

        serve_file(t, c, path, 0);
        c->rlen -= used;
        memmove(c->rbuf, c->rbuf + used, c->rlen);
    }

close_conn:
    if (c->fd >= 0) close(c->fd);
    if (c->ffd >= 0) close(c->ffd);
    c->fd = -1;
    c->ffd = -1;
    c->next = t->free_list;
    t->free_list = c;
}

static void *worker_thread(void *arg) {
    Thread *t = (Thread *)arg;
    Thread_init(t);

    int lfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    setsockopt(lfd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    int defer = 5;
    setsockopt(lfd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &defer, sizeof(defer));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(lfd, (struct sockaddr *)&addr, sizeof(addr));
    listen(lfd, 4096);

    struct epoll_event ev = {.events = EPOLLIN, .data.ptr = NULL};
    epoll_ctl(t->epfd, EPOLL_CTL_ADD, lfd, &ev);

    struct epoll_event events[MAXEV];
    for (;;) {
        int n = epoll_wait(t->epfd, events, MAXEV, 1000);
        thread_now = time(NULL);

        for (int i = 0; i < n; i++) {
            Conn *c = (Conn *)events[i].data.ptr;
            if (!c) {
                for (;;) {
                    int fd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (fd < 0) break;
                    Conn *nc = t->free_list;
                    if (nc) t->free_list = nc->next;
                    else if (t->hw < MAXCONN) nc = &t->conns[t->hw++];
                    else { close(fd); continue; }
                    nc->fd = fd;
                    nc->ffd = -1;
                    nc->rlen = 0;
                    nc->keep_alive = 1;
                    nc->last_activity = thread_now;
                    struct epoll_event ce = {.events = EPOLLIN | EPOLLOUT | EPOLLET, .data.ptr = nc};
                    epoll_ctl(t->epfd, EPOLL_CTL_ADD, fd, &ce);
                    drive_conn(t, nc);
                }
            } else if (c->fd >= 0 && !(events[i].events & (EPOLLERR | EPOLLHUP))) {
                drive_conn(t, c);
            }
        }
    }
    return NULL;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    if (argc > 1) chdir(argv[1]);

    int num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    if (num_threads < 1) num_threads = 1;
    if (num_threads > 64) num_threads = 64;

    Thread *threads = calloc(num_threads, sizeof(Thread));
    for (int i = 1; i < num_threads; i++) {
        pthread_t tid;
        pthread_create(&tid, NULL, worker_thread, &threads[i]);
        pthread_detach(tid);
    }

    printf("Servidor optimizado puerto %d, %d threads\n", PORT, num_threads);
    worker_thread(&threads[0]);
    return 0;
}
