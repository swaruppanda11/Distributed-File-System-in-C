// dfs.c
// Minimal DFS server: listens on given port and stores/serves chunk files in given directory.
// Usage: ./dfs <dirpath> <port>
//
// Supported commands over TCP (text lines ending in \n):
// - PUT <chunkname> <len>\n<data>   -> stores chunk in <dirpath>/<chunkname>
// - LIST\n  -> returns each chunk filename line then "END\n"
// - GET <chunkname>\n -> returns "OK <len>\n" then data if present, or "ERR\n"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <errno.h>
#include <arpa/inet.h>
#include <dirent.h>

#define BACKLOG 8
#define BUF 8192

static char storedir[1024];

static void usage() {
    fprintf(stderr, "Usage: dfs <dirpath> <port>\n");
    exit(1);
}

static int ensure_dir(const char *d) {
    struct stat st;
    if (stat(d, &st) == 0) return 0;
    if (mkdir(d, 0755) == 0) return 0;
    return -1;
}

static int handle_client(int cfd) {
    FILE *f = fdopen(cfd, "r+");
    if (!f) { close(cfd); return -1; }
    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        // trim
        char *p = line;
        while (*p && (*p=='\r' || *p=='\n')) p++;
        // parse
        if (strncmp(line, "PUT ", 4) == 0) {
            char name[512]; unsigned long len;
            if (sscanf(line+4, "%511s %lu", name, &len) >= 1) {
                // read len bytes from the socket
                char path[1400];
                snprintf(path, sizeof(path), "%s/%s", storedir, name);
                FILE *wf = fopen(path, "wb");
                if (!wf) {
                    fprintf(f, "ERR\n");
                    fflush(f);
                    // consume len bytes regardless
                    size_t toread = len;
                    char tmp[1024];
                    while (toread) {
                        size_t r = fread(tmp,1, (toread>sizeof(tmp)?sizeof(tmp):toread), f);
                        if (r==0) break;
                        toread -= r;
                    }
                    continue;
                }
                size_t left = len;
                while (left) {
                    char buf[BUF];
                    size_t want = left > sizeof(buf) ? sizeof(buf) : left;
                    size_t r = fread(buf, 1, want, f);
                    if (r==0) break;
                    fwrite(buf,1,r,wf);
                    left -= r;
                }
                fclose(wf);
                fprintf(f, "OK\n");
                fflush(f);
            } else {
                fprintf(f, "ERR\n"); fflush(f);
            }
        } else if (strncmp(line, "LIST", 4)==0) {
            // enumerate files in storedir
            DIR *d = opendir(storedir);
            if (d) {
                struct dirent *ent;
                while ((ent = readdir(d)) != NULL) {
                    if (ent->d_type == DT_REG || ent->d_type == DT_LNK || ent->d_type==DT_UNKNOWN) {
                        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..")==0) continue;
                        fprintf(f, "%s\n", ent->d_name);
                    }
                }
                closedir(d);
            }
            fprintf(f, "END\n");
            fflush(f);
        } else if (strncmp(line, "GET ", 4) == 0) {
            char name[512];
            if (sscanf(line+4, "%511s", name) == 1) {
                char path[1400];
                snprintf(path, sizeof(path), "%s/%s", storedir, name);
                FILE *rf = fopen(path, "rb");
                if (!rf) {
                    fprintf(f, "ERR\n"); fflush(f);
                } else {
                    // get size
                    fseek(rf, 0, SEEK_END);
                    long len = ftell(rf);
                    fseek(rf, 0, SEEK_SET);
                    fprintf(f, "OK %ld\n", len); fflush(f);
                    char buf[BUF];
                    while (1) {
                        size_t r = fread(buf,1,sizeof(buf),rf);
                        if (r==0) break;
                        fwrite(buf,1,r,f);
                        fflush(f);
                    }
                    fclose(rf);
                }
            } else {
                fprintf(f, "ERR\n"); fflush(f);
            }
        } else {
            // ignore/unknown
            fprintf(f, "ERR\n"); fflush(f);
        }
        // we finish after single command (dfc closes or expects short-living connections)
        break;
    }
    fclose(f);
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) usage();
    strncpy(storedir, argv[1], sizeof(storedir)-1);
    int port = atoi(argv[2]);
    if (ensure_dir(storedir) != 0) {
        fprintf(stderr, "Cannot create or access directory %s\n", storedir);
        return 1;
    }
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }
    int opt=1; setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in sa;
    memset(&sa,0,sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = INADDR_ANY;
    sa.sin_port = htons(port);
    if (bind(sock, (struct sockaddr*)&sa, sizeof(sa)) < 0) { perror("bind"); return 1; }
    if (listen(sock, BACKLOG) < 0) { perror("listen"); return 1; }
    // simple single-process accept loop
    while (1) {
        struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
        int c = accept(sock, (struct sockaddr*)&cli, &clilen);
        if (c < 0) continue;
        // make a short-living handler - fork to support concurrency
        pid_t pid = fork();
        if (pid == 0) {
            close(sock);
            handle_client(c);
            close(c);
            exit(0);
        } else if (pid > 0) {
            close(c);
            // continue
        } else {
            // fork failed, handle inline
            handle_client(c);
            close(c);
        }
    }
    close(sock);
    return 0;
}
