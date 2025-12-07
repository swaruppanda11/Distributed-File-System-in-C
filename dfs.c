#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <dirent.h>

#define BUFFER_SIZE 8192
#define MAX_FILENAME 256

typedef struct {
    int client_sock;
    char *storage_dir;
} thread_args_t;

// Function to ensure directory exists
void ensure_directory(const char *path) {
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        mkdir(path, 0755);
    }
}

// Handle LIST command - send back list of files in storage directory
void handle_list(int client_sock, const char *storage_dir) {
    DIR *dir = opendir(storage_dir);
    if (!dir) {
        char *error_msg = "ERROR\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    // Send success header
    send(client_sock, "OK\n", 3, 0);

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] != '.') {  // Skip hidden files
            char file_info[BUFFER_SIZE];
            snprintf(file_info, sizeof(file_info), "%s\n", entry->d_name);
            send(client_sock, file_info, strlen(file_info), 0);
        }
    }
    
    send(client_sock, "END\n", 4, 0);
    closedir(dir);
}

// Handle PUT command - receive and store file chunk
void handle_put(int client_sock, const char *storage_dir) {
    char filename[MAX_FILENAME];
    long file_size;
    
    // Receive filename length
    int filename_len;
    if (recv(client_sock, &filename_len, sizeof(filename_len), 0) <= 0) {
        return;
    }
    
    // Receive filename
    if (recv(client_sock, filename, filename_len, 0) <= 0) {
        return;
    }
    filename[filename_len] = '\0';
    
    // Receive file size
    if (recv(client_sock, &file_size, sizeof(file_size), 0) <= 0) {
        return;
    }
    
    // Create full path
    char filepath[BUFFER_SIZE];
    snprintf(filepath, sizeof(filepath), "%s/%s", storage_dir, filename);
    
    // Open file for writing
    FILE *fp = fopen(filepath, "wb");
    if (!fp) {
        char *error_msg = "ERROR\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }
    
    // Send acknowledgment
    send(client_sock, "OK\n", 3, 0);
    
    // Receive file data
    char buffer[BUFFER_SIZE];
    long bytes_received = 0;
    
    while (bytes_received < file_size) {
        long to_receive = (file_size - bytes_received) > BUFFER_SIZE ? 
                          BUFFER_SIZE : (file_size - bytes_received);
        ssize_t n = recv(client_sock, buffer, to_receive, 0);
        if (n <= 0) break;
        
        fwrite(buffer, 1, n, fp);
        bytes_received += n;
    }
    
    fclose(fp);
    
    if (bytes_received == file_size) {
        send(client_sock, "SUCCESS\n", 8, 0);
    } else {
        send(client_sock, "FAILED\n", 7, 0);
    }
}

// Handle GET command - send file chunk to client
void handle_get(int client_sock, const char *storage_dir) {
    char filename[MAX_FILENAME];
    
    // Receive filename length
    int filename_len;
    if (recv(client_sock, &filename_len, sizeof(filename_len), 0) <= 0) {
        return;
    }
    
    // Receive filename
    if (recv(client_sock, filename, filename_len, 0) <= 0) {
        return;
    }
    filename[filename_len] = '\0';
    
    // Create full path
    char filepath[BUFFER_SIZE];
    snprintf(filepath, sizeof(filepath), "%s/%s", storage_dir, filename);
    
    // Check if file exists and get size
    struct stat st;
    if (stat(filepath, &st) != 0) {
        send(client_sock, "NOTFOUND\n", 9, 0);
        return;
    }
    
    long file_size = st.st_size;
    
    // Open file for reading
    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        send(client_sock, "ERROR\n", 6, 0);
        return;
    }
    
    // Send OK and file size
    send(client_sock, "OK\n", 3, 0);
    send(client_sock, &file_size, sizeof(file_size), 0);
    
    // Send file data
    char buffer[BUFFER_SIZE];
    size_t bytes_read;
    
    while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
        send(client_sock, buffer, bytes_read, 0);
    }
    
    fclose(fp);
}

// Client handler thread
void *handle_client(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    int client_sock = args->client_sock;
    char *storage_dir = args->storage_dir;
    
    char command[16];
    ssize_t n = recv(client_sock, command, sizeof(command) - 1, 0);
    
    if (n > 0) {
        command[n] = '\0';
        
        // Remove newline if present
        char *newline = strchr(command, '\n');
        if (newline) *newline = '\0';
        
        if (strcmp(command, "LIST") == 0) {
            handle_list(client_sock, storage_dir);
        } else if (strcmp(command, "PUT") == 0) {
            handle_put(client_sock, storage_dir);
        } else if (strcmp(command, "GET") == 0) {
            handle_get(client_sock, storage_dir);
        }
    }
    
    close(client_sock);
    free(args);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <storage_directory> <port>\n", argv[0]);
        exit(1);
    }
    
    char *storage_dir = argv[1];
    int port = atoi(argv[2]);
    
    // Ensure storage directory exists
    ensure_directory(storage_dir);
    
    // Create socket
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        perror("socket");
        exit(1);
    }
    
    // Set socket options to reuse address
    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Bind to port
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    
    if (bind(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_sock);
        exit(1);
    }
    
    // Listen for connections
    if (listen(server_sock, 10) < 0) {
        perror("listen");
        close(server_sock);
        exit(1);
    }
    
    // Accept connections in a loop
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_len);
        if (client_sock < 0) {
            continue;
        }
        
        // Create thread to handle client
        thread_args_t *args = malloc(sizeof(thread_args_t));
        args->client_sock = client_sock;
        args->storage_dir = storage_dir;
        
        pthread_t thread;
        pthread_create(&thread, NULL, handle_client, args);
        pthread_detach(thread);
    }
    
    close(server_sock);
    return 0;
}#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <errno.h>
#include <dirent.h>

#define BUFFER_SIZE 8192
#define MAX_FILENAME 256

typedef struct {
    int client_sock;
    char *storage_dir;
} thread_args_t;

// Function to ensure directory exists
void ensure_directory(const char *path) {
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        mkdir(path, 0755);
    }
}

// Handle LIST command - send back list of files in storage directory
void handle_list(int client_sock, const char *storage_dir) {
    DIR *dir = opendir(storage_dir);
    if (!dir) {
        char *error_msg = "ERROR\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }

    // Send success header
    send(client_sock, "OK\n", 3, 0);

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] != '.') {  // Skip hidden files
            char file_info[BUFFER_SIZE];
            snprintf(file_info, sizeof(file_info), "%s\n", entry->d_name);
            send(client_sock, file_info, strlen(file_info), 0);
        }
    }
    
    send(client_sock, "END\n", 4, 0);
    closedir(dir);
}

// Handle PUT command - receive and store file chunk
void handle_put(int client_sock, const char *storage_dir) {
    char filename[MAX_FILENAME];
    long file_size;
    
    // Receive filename length
    int filename_len;
    if (recv(client_sock, &filename_len, sizeof(filename_len), 0) <= 0) {
        return;
    }
    
    // Receive filename
    if (recv(client_sock, filename, filename_len, 0) <= 0) {
        return;
    }
    filename[filename_len] = '\0';
    
    // Receive file size
    if (recv(client_sock, &file_size, sizeof(file_size), 0) <= 0) {
        return;
    }
    
    // Create full path
    char filepath[BUFFER_SIZE];
    snprintf(filepath, sizeof(filepath), "%s/%s", storage_dir, filename);
    
    // Open file for writing
    FILE *fp = fopen(filepath, "wb");
    if (!fp) {
        char *error_msg = "ERROR\n";
        send(client_sock, error_msg, strlen(error_msg), 0);
        return;
    }
    
    // Send acknowledgment
    send(client_sock, "OK\n", 3, 0);
    
    // Receive file data
    char buffer[BUFFER_SIZE];
    long bytes_received = 0;
    
    while (bytes_received < file_size) {
        long to_receive = (file_size - bytes_received) > BUFFER_SIZE ? 
                          BUFFER_SIZE : (file_size - bytes_received);
        ssize_t n = recv(client_sock, buffer, to_receive, 0);
        if (n <= 0) break;
        
        fwrite(buffer, 1, n, fp);
        bytes_received += n;
    }
    
    fclose(fp);
    
    if (bytes_received == file_size) {
        send(client_sock, "SUCCESS\n", 8, 0);
    } else {
        send(client_sock, "FAILED\n", 7, 0);
    }
}

// Handle GET command - send file chunk to client
void handle_get(int client_sock, const char *storage_dir) {
    char filename[MAX_FILENAME];
    
    // Receive filename length
    int filename_len;
    if (recv(client_sock, &filename_len, sizeof(filename_len), 0) <= 0) {
        return;
    }
    
    // Receive filename
    if (recv(client_sock, filename, filename_len, 0) <= 0) {
        return;
    }
    filename[filename_len] = '\0';
    
    // Create full path
    char filepath[BUFFER_SIZE];
    snprintf(filepath, sizeof(filepath), "%s/%s", storage_dir, filename);
    
    // Check if file exists and get size
    struct stat st;
    if (stat(filepath, &st) != 0) {
        send(client_sock, "NOTFOUND\n", 9, 0);
        return;
    }
    
    long file_size = st.st_size;
    
    // Open file for reading
    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        send(client_sock, "ERROR\n", 6, 0);
        return;
    }
    
    // Send OK and file size
    send(client_sock, "OK\n", 3, 0);
    send(client_sock, &file_size, sizeof(file_size), 0);
    
    // Send file data
    char buffer[BUFFER_SIZE];
    size_t bytes_read;
    
    while ((bytes_read = fread(buffer, 1, BUFFER_SIZE, fp)) > 0) {
        send(client_sock, buffer, bytes_read, 0);
    }
    
    fclose(fp);
}

// Client handler thread
void *handle_client(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    int client_sock = args->client_sock;
    char *storage_dir = args->storage_dir;
    
    char command[16];
    ssize_t n = recv(client_sock, command, sizeof(command) - 1, 0);
    
    if (n > 0) {
        command[n] = '\0';
        
        // Remove newline if present
        char *newline = strchr(command, '\n');
        if (newline) *newline = '\0';
        
        if (strcmp(command, "LIST") == 0) {
            handle_list(client_sock, storage_dir);
        } else if (strcmp(command, "PUT") == 0) {
            handle_put(client_sock, storage_dir);
        } else if (strcmp(command, "GET") == 0) {
            handle_get(client_sock, storage_dir);
        }
    }
    
    close(client_sock);
    free(args);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <storage_directory> <port>\n", argv[0]);
        exit(1);
    }
    
    char *storage_dir = argv[1];
    int port = atoi(argv[2]);
    
    // Ensure storage directory exists
    ensure_directory(storage_dir);
    
    // Create socket
    int server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock < 0) {
        perror("socket");
        exit(1);
    }
    
    // Set socket options to reuse address
    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // Bind to port
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);
    
    if (bind(server_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(server_sock);
        exit(1);
    }
    
    // Listen for connections
    if (listen(server_sock, 10) < 0) {
        perror("listen");
        close(server_sock);
        exit(1);
    }
    
    // Accept connections in a loop
    while (1) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        
        int client_sock = accept(server_sock, (struct sockaddr *)&client_addr, &client_len);
        if (client_sock < 0) {
            continue;
        }
        
        // Create thread to handle client
        thread_args_t *args = malloc(sizeof(thread_args_t));
        args->client_sock = client_sock;
        args->storage_dir = storage_dir;
        
        pthread_t thread;
        pthread_create(&thread, NULL, handle_client, args);
        pthread_detach(thread);
    }
    
    close(server_sock);
    return 0;
}