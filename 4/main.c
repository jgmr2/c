#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <pthread.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/sendfile.h>
#include <sys/resource.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#define PORT      8080
#define RBUF      4096              /* máx. tamaño de cabeceras de petición */
#define MAXCONN   16384             /* conexiones por hilo */
#define MAXEV     256
#define IDLE_SEC  15                /* timeout keep-alive / clientes lentos */
#define MAXCACHE  (512 * 1024)      /* archivos <= esto se sirven desde RAM */
#define ARENA_SZ  (256UL << 20)     /* arena de caché por hilo (virtual) */
#define TSIZE     4096              /* slots de la tabla hash (potencia de 2) */

/* ---------- arena: bump allocator, nunca se libera individualmente ---------- */
typedef struct { char *base; size_t cap, used; } Arena;

static void *aalloc(Arena *a, size_t n) {
    n = (n + 15) & ~(size_t)15;
    if (a->used + n > a->cap) return NULL;
    void *p = a->base + a->used;
    a->used += n;
    return p;
}

/* ---------- estructuras ---------- */
typedef struct Conn {
    int fd, ffd, keep, rlen;
    time_t last;
    const char *p[2];               /* segmentos en memoria pendientes (cabecera, cuerpo) */
    size_t l[2];
    off_t foff, fsize;              /* archivo grande pendiente (sendfile) */
    struct Conn *next;              /* lista libre */
    char hdr[384];                  /* todo lo anterior se pone en 0 al aceptar */
    char rbuf[RBUF];
} Conn;

typedef struct {
    uint64_t h;
    char *key, *hdr, *body;
    size_t hlen, blen;
    off_t size;
    time_t mtime, checked;
    int gone;
} Ent;

typedef struct {
    int ep, hw, count;
    Conn *conns, *freel;            /* arena de conexiones + lista libre */
    Arena ar;                       /* arena de la caché de archivos */
    Ent *tab;
} Th;

static __thread time_t now;

/* ---------- MIME ---------- */
static const struct { const char *ext, *type; } MT[] = {
    {".html", "text/html; charset=utf-8"}, {".htm", "text/html; charset=utf-8"},
    {".css", "text/css; charset=utf-8"},   {".js", "text/javascript; charset=utf-8"},
    {".mjs", "text/javascript; charset=utf-8"}, {".json", "application/json"},
    {".txt", "text/plain; charset=utf-8"}, {".svg", "image/svg+xml"},
    {".png", "image/png"}, {".jpg", "image/jpeg"}, {".jpeg", "image/jpeg"},
    {".gif", "image/gif"}, {".webp", "image/webp"}, {".ico", "image/x-icon"},
    {".woff2", "font/woff2"}, {".woff", "font/woff"},
    {".wasm", "application/wasm"}, {".pdf", "application/pdf"},
};

static const char *mime(const char *p) {
    const char *e = strrchr(p, '.');
    if (e)
        for (size_t i = 0; i < sizeof MT / sizeof *MT; i++)
            if (!strcasecmp(e, MT[i].ext)) return MT[i].type;
    return "application/octet-stream";
}

static int mkhdr(char *b, size_t n, const char *type, off_t size, int keep) {
    return snprintf(b, n,
        "HTTP/1.1 200 OK\r\nContent-Type: %s\r\nContent-Length: %lld\r\n"
        "Connection: %s\r\n\r\n", type, (long long)size, keep ? "keep-alive" : "close");
}

static void err(Conn *c, const char *st) {
    int n = snprintf(c->hdr, sizeof c->hdr,
        "HTTP/1.1 %s\r\nContent-Type: text/plain\r\nContent-Length: %zu\r\n"
        "Connection: close\r\n\r\n%s\n", st, strlen(st) + 1, st);
    c->p[0] = c->hdr; c->l[0] = n; c->l[1] = 0;
    c->keep = 0;
}

static void closec(Th *t, Conn *c) {
    if (c->fd < 0) return;
    close(c->fd);                   /* close() lo saca del epoll */
    c->fd = -1;
    if (c->ffd >= 0) { close(c->ffd); c->ffd = -1; }
    c->next = t->freel;
    t->freel = c;
}

/* ---------- resolver archivo: caché en RAM o sendfile ---------- */
static void serve(Th *t, Conn *c, const char *path, int head) {
    uint64_t h = 1469598103934665603ULL;
    for (const char *s = path; *s; s++) h = (h ^ (uint8_t)*s) * 1099511628211ULL;

    Ent *e = &t->tab[h & (TSIZE - 1)];
    while (e->key && !(e->h == h && !strcmp(e->key, path)))
        e = &t->tab[(e - t->tab + 1) & (TSIZE - 1)];

    struct stat st;
    int fd;

    if (e->key && !e->gone) {
        if (e->checked == now) goto hit;            /* validado este segundo */
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode) &&
            st.st_mtime == e->mtime && st.st_size == e->size) {
            e->checked = now;
            goto hit;
        }
    }

    fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0 || fstat(fd, &st) < 0 || !S_ISREG(st.st_mode)) {
        if (fd >= 0) close(fd);
        if (e->key) e->gone = 1;
        err(c, "404 Not Found");
        return;
    }

    if (st.st_size <= MAXCACHE && (e->key || t->count < TSIZE * 3 / 4)) {
        char *k = e->key ? e->key : aalloc(&t->ar, strlen(path) + 1);
        char *buf = k ? aalloc(&t->ar, 320 + (size_t)st.st_size) : NULL;
        if (buf) {
            if (!e->key) { strcpy(k, path); e->key = k; e->h = h; t->count++; }
            int hl = mkhdr(buf, 320, mime(path), st.st_size, 1);
            char *body = buf + hl;
            size_t got = 0; ssize_t r;
            while (got < (size_t)st.st_size &&
                   (r = read(fd, body + got, st.st_size - got)) > 0) got += r;
            close(fd);
            if (got != (size_t)st.st_size) { e->gone = 1; err(c, "500 Internal Server Error"); return; }
            e->hdr = buf; e->hlen = hl; e->body = body; e->blen = got;
            e->size = st.st_size; e->mtime = st.st_mtime; e->checked = now; e->gone = 0;
            goto hit;
        }
    }

    /* archivo grande o arena llena: cabecera + sendfile (zero-copy) */
    if (e->key) e->gone = 1;
    {
        int hl = mkhdr(c->hdr, sizeof c->hdr, mime(path), st.st_size, c->keep);
        c->p[0] = c->hdr; c->l[0] = hl; c->l[1] = 0;
        if (head || st.st_size == 0) close(fd);
        else { c->ffd = fd; c->foff = 0; c->fsize = st.st_size; }
    }
    return;

hit:
    if (c->keep) { c->p[0] = e->hdr; c->l[0] = e->hlen; }
    else {
        int hl = mkhdr(c->hdr, sizeof c->hdr, mime(path), e->size, 0);
        c->p[0] = c->hdr; c->l[0] = hl;
    }
    c->p[1] = e->body;
    c->l[1] = head ? 0 : e->blen;
}

/* ---------- parseo de una petición completa (req terminada en \0) ---------- */
static void respond(Th *t, Conn *c, char *req) {
    int head = 0;
    c->l[0] = c->l[1] = 0;

    if (!strncmp(req, "GET ", 4)) req += 4;
    else if (!strncmp(req, "HEAD ", 5)) { head = 1; req += 5; }
    else { err(c, "405 Method Not Allowed"); return; }

    char *sp = strchr(req, ' ');
    if (!sp) { err(c, "400 Bad Request"); return; }
    *sp = 0;
    char *ver = sp + 1;
    int v11 = !strncmp(ver, "HTTP/1.1", 8);
    if (!v11 && strncmp(ver, "HTTP/1.0", 8)) { err(c, "505 HTTP Version Not Supported"); return; }

    const char *hb = strstr(ver, "\r\n");
    if (!hb) hb = "";
    c->keep = v11 ? !strcasestr(hb, "\r\nconnection: close")
                  : !!strcasestr(hb, "\r\nconnection: keep-alive");

    if (*req != '/') { err(c, "400 Bad Request"); return; }
    req++;
    size_t ul = strcspn(req, "?#");
    if (ul > 400) { err(c, "414 URI Too Long"); return; }

    char path[512];
    memcpy(path, req, ul);
    path[ul] = 0;
    /* sin dotfiles, sin "..", sin "//", sin "\" */
    if (path[0] == '.' || strstr(path, "/.") || strstr(path, "//") || strchr(path, '\\')) {
        err(c, "403 Forbidden");
        return;
    }
    if (ul == 0 || path[ul - 1] == '/') strcpy(path + ul, "index.html");

    serve(t, c, path, head);
}

static int parse(Th *t, Conn *c) {
    char *end = memmem(c->rbuf, c->rlen, "\r\n\r\n", 4);
    if (!end) return 0;
    size_t used = (end + 4) - c->rbuf;
    *end = 0;
    respond(t, c, c->rbuf);
    c->rlen -= used;                                /* soporta pipelining */
    memmove(c->rbuf, c->rbuf + used, c->rlen);
    return 1;
}

/* ---------- escritura: sendmsg (cabecera+cuerpo juntos) + sendfile ---------- */
#define PENDING(c) ((c)->l[0] || (c)->l[1] || (c)->foff < (c)->fsize)

static int flush(Conn *c) {                         /* 1 listo, 0 bloqueado, -1 error */
    while (c->l[0] || c->l[1]) {
        struct iovec iv[2]; int n = 0;
        for (int i = 0; i < 2; i++)
            if (c->l[i]) { iv[n].iov_base = (void *)c->p[i]; iv[n].iov_len = c->l[i]; n++; }
        struct msghdr m = {0};
        m.msg_iov = iv; m.msg_iovlen = n;
        ssize_t w = sendmsg(c->fd, &m, MSG_NOSIGNAL | (c->foff < c->fsize ? MSG_MORE : 0));
        if (w < 0) {
            if (errno == EINTR) continue;
            return errno == EAGAIN ? 0 : -1;
        }
        for (int i = 0; i < 2; i++) {
            size_t k = (size_t)w < c->l[i] ? (size_t)w : c->l[i];
            c->p[i] += k; c->l[i] -= k; w -= (ssize_t)k;
        }
    }
    while (c->foff < c->fsize) {
        ssize_t w = sendfile(c->fd, c->ffd, &c->foff, c->fsize - c->foff);
        if (w < 0) {
            if (errno == EINTR) continue;
            return errno == EAGAIN ? 0 : -1;
        }
        if (w == 0) return -1;                      /* archivo truncado */
    }
    if (c->ffd >= 0) { close(c->ffd); c->ffd = -1; }
    c->foff = c->fsize = 0;
    return 1;
}

/* ---------- máquina de estados por conexión (edge-triggered) ---------- */
static void drive(Th *t, Conn *c) {
    c->last = now;
    for (;;) {
        if (PENDING(c)) {
            int r = flush(c);
            if (r <= 0) { if (r < 0) closec(t, c); return; }
            if (!c->keep) { closec(t, c); return; }
        }
        if (parse(t, c)) continue;
        if (c->rlen >= RBUF) { err(c, "431 Request Header Fields Too Large"); continue; }

        ssize_t n = read(c->fd, c->rbuf + c->rlen, RBUF - c->rlen);
        if (n > 0) { c->rlen += n; continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && errno == EAGAIN) return;       /* esperar siguiente evento */
        closec(t, c);                               /* EOF o error */
        return;
    }
}

static void sweep(Th *t) {
    for (int i = 0; i < t->hw; i++) {
        Conn *c = &t->conns[i];
        if (c->fd >= 0 && now - c->last > IDLE_SEC) closec(t, c);
    }
}

/* ---------- hilo de evento ---------- */
static void *run(void *arg) {
    Th *t = arg;

    t->conns = mmap(NULL, sizeof(Conn) * MAXCONN, PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    t->ar.base = mmap(NULL, ARENA_SZ, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    t->ar.cap = ARENA_SZ;
    t->tab = calloc(TSIZE, sizeof(Ent));
    if (t->conns == MAP_FAILED || t->ar.base == MAP_FAILED || !t->tab) { perror("mem"); exit(1); }

    int lfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int one = 1, defer = 5;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one);
    setsockopt(lfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);     /* heredado por accept */
    setsockopt(lfd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &defer, sizeof defer);

    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port = htons(PORT);
    a.sin_addr.s_addr = INADDR_ANY;
    if (bind(lfd, (struct sockaddr *)&a, sizeof a) < 0) { perror("bind"); exit(1); }
    if (listen(lfd, 4096) < 0) { perror("listen"); exit(1); }

    t->ep = epoll_create1(EPOLL_CLOEXEC);
    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = NULL };
    epoll_ctl(t->ep, EPOLL_CTL_ADD, lfd, &ev);

    struct epoll_event evs[MAXEV];
    time_t lastsweep = 0;

    for (;;) {
        int n = epoll_wait(t->ep, evs, MAXEV, 1000);
        now = time(NULL);

        for (int i = 0; i < n; i++) {
            Conn *c = evs[i].data.ptr;

            if (!c) {                                   /* listener: aceptar todo */
                for (;;) {
                    int fd = accept4(lfd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (fd < 0) { if (errno == EINTR) continue; break; }

                    Conn *nc = t->freel;
                    if (nc) t->freel = nc->next;
                    else if (t->hw < MAXCONN) nc = &t->conns[t->hw++];
                    else { close(fd); continue; }

                    memset(nc, 0, offsetof(Conn, hdr));
                    nc->fd = fd; nc->ffd = -1; nc->keep = 1; nc->last = now;

                    struct epoll_event ce = { .events = EPOLLIN | EPOLLOUT | EPOLLET, .data.ptr = nc };
                    if (epoll_ctl(t->ep, EPOLL_CTL_ADD, fd, &ce) < 0) { closec(t, nc); continue; }
                    drive(t, nc);                       /* con DEFER_ACCEPT ya hay datos */
                }
                continue;
            }

            if (c->fd < 0) continue;
            if (evs[i].events & (EPOLLERR | EPOLLHUP)) { closec(t, c); continue; }
            drive(t, c);
        }

        if (now != lastsweep) { lastsweep = now; sweep(t); }
    }
    return NULL;
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    if (argc > 1 && chdir(argv[1])) { perror("chdir"); return 1; }

    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) { rl.rlim_cur = rl.rlim_max; setrlimit(RLIMIT_NOFILE, &rl); }

    int n = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (n < 1) n = 1;
    if (n > 64) n = 64;

    Th *ths = calloc(n, sizeof *ths);
    for (int i = 1; i < n; i++) {
        pthread_t tid;
        pthread_create(&tid, NULL, run, &ths[i]);
        pthread_detach(tid);
    }

    printf("Puerto %d, %d hilos epoll\n", PORT, n);
    fflush(stdout);
    run(&ths[0]);
    return 0;
}
