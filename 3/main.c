#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <fcntl.h>

#define PORT 8080
#define BUF 4096

void handle(int c) {
    char buf[BUF], path[BUF];
    int n = recv(c, buf, BUF-1, 0);
    if (n < 0) { close(c); return; }
    buf[n] = 0;
    
    char *p = buf;
    while (*p != ' ') p++;
    p++;
    char *end = p;
    while (*end != ' ') end++;
    
    strncpy(path, p, end - p);
    path[end - p] = 0;
    
    if (!strcmp(path, "/")) strcpy(path, "index.html");
    else if (path[0] == '/') memmove(path, path+1, strlen(path));
    
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        char *err = "HTTP/1.1 404 Not Found\r\n\r\n404";
        send(c, err, strlen(err), 0);
    } else {
        struct stat s;
        fstat(fd, &s);
        char h[256];
        sprintf(h, "HTTP/1.1 200 OK\r\nContent-Length: %ld\r\n\r\n", s.st_size);
        send(c, h, strlen(h), 0);
        
        char b[BUF];
        int r;
        while ((r = read(fd, b, BUF)) > 0) send(c, b, r, 0);
        close(fd);
    }
    close(c);
}

int main() {
    int s = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    struct sockaddr_in a = {
        .sin_family = AF_INET,
        .sin_port = htons(PORT),
        .sin_addr.s_addr = INADDR_ANY
    };
    
    bind(s, (struct sockaddr*)&a, sizeof(a));
    listen(s, 10);
    printf("Servidor en puerto %d\n", PORT);
    
    while (1) {
        struct sockaddr_in c_addr;
        socklen_t c_len = sizeof(c_addr);
        int c = accept(s, (struct sockaddr*)&c_addr, &c_len);
        if (c >= 0) handle(c);
    }
}
