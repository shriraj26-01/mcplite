/*
 * MCP Bridge — stdio ↔ Unix socket relay with reconnect.
 * Compile: gcc -O2 -o mcp-bridge bridge.c
 * Usage:   mcp-bridge <server_name>
 * Memory:  ~500 KB
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <poll.h>
#include <errno.h>
#include <sys/stat.h>
#include <time.h>

#define BUF_SIZE 65536
#define MAX_RETRIES 10
#define RETRY_BASE_MS 500

static char socket_dir[256];

static void init_socket_dir(void) {
    const char *env = getenv("MCP_ORCH_SOCKET_DIR");
    if (env) { strncpy(socket_dir, env, sizeof(socket_dir)-1); return; }
    const char *xdg = getenv("XDG_RUNTIME_DIR");
    if (xdg) { snprintf(socket_dir, sizeof(socket_dir), "%s/mcp-orchestrator", xdg); return; }
    snprintf(socket_dir, sizeof(socket_dir), "/run/user/%d/mcp-orchestrator", getuid());
}

static int connect_socket(const char *server) {
    char path[256];
    snprintf(path, sizeof(path), "%s/%s.sock", socket_dir, server);

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int wait_and_connect(const char *server) {
    /* Try connecting with exponential backoff */
    for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
        int fd = connect_socket(server);
        if (fd >= 0) return fd;

        /* Start orchestrator on first failure */
        if (attempt == 0) {
            system("python3 \"$(dirname \"$(realpath /proc/self/exe 2>/dev/null || echo .)\")\"/orchestrator.py start 2>/dev/null");
        }

        int delay_ms = RETRY_BASE_MS * (1 << (attempt > 4 ? 4 : attempt)); /* cap at 8s */
        usleep(delay_ms * 1000);
    }
    return -1;
}

/* Relay loop: returns 0 if stdin EOF (normal exit), 1 if socket died (should reconnect) */
static int relay(int sock) {
    struct pollfd fds[2];
    fds[0].fd = STDIN_FILENO;
    fds[0].events = POLLIN;
    fds[1].fd = sock;
    fds[1].events = POLLIN;

    char buf[BUF_SIZE];
    while (1) {
        int ret = poll(fds, 2, 30000); /* 30s timeout for keepalive check */
        if (ret < 0) {
            if (errno == EINTR) continue;
            return 1;
        }
        if (ret == 0) continue; /* timeout, just loop (connection still alive) */

        /* stdin → socket */
        if (fds[0].revents & POLLIN) {
            ssize_t n = read(STDIN_FILENO, buf, BUF_SIZE);
            if (n <= 0) return 0; /* stdin closed = kiro exited, normal */
            ssize_t written = write(sock, buf, n);
            if (written != n) return 1; /* socket write failed, reconnect */
        }
        if (fds[0].revents & (POLLHUP | POLLERR)) return 0; /* stdin done */

        /* socket → stdout */
        if (fds[1].revents & POLLIN) {
            ssize_t n = read(sock, buf, BUF_SIZE);
            if (n <= 0) return 1; /* socket closed, reconnect */
            if (write(STDOUT_FILENO, buf, n) != n) return 0; /* stdout broken */
        }
        if (fds[1].revents & (POLLHUP | POLLERR)) return 1; /* socket died, reconnect */
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: mcp-bridge <server_name>\n");
        return 1;
    }
    init_socket_dir();
    const char *server = argv[1];

    while (1) {
        int sock = wait_and_connect(server);
        if (sock < 0) {
            fprintf(stderr, "[mcp-bridge] Cannot connect to %s after %d retries\n", server, MAX_RETRIES);
            return 1;
        }

        int result = relay(sock);
        close(sock);

        if (result == 0) {
            /* Normal exit (stdin closed = kiro exited) */
            return 0;
        }

        /* Socket died — reconnect after brief pause */
        usleep(200000); /* 200ms */
    }
}
