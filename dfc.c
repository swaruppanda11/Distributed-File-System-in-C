#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>

#define BUFFER_SIZE 8192
#define MAX_SERVERS 10
#define MAX_FILENAME 256
#define CONFIG_FILE "dfc.conf"
#define CONNECT_TIMEOUT 1

// Function declarations from dfc_maps.c
int compute_hash_mod(const char *str, int mod);
void get_chunks_for_server(int hash_mod, int server_num, int chunks[2]);
void get_distribution_map(const char *filename, int num_servers, int servers[][2]);

typedef struct {
    char name[32];
    char host[64];
    int port;
    int available;  // 1 if server responded, 0 otherwise
} server_t;

server_t servers[MAX_SERVERS];
int num_servers = 0;

// Parse configuration file
int parse_config(const char *config_path) {
    FILE *fp = fopen(config_path, "r");
    if (!fp) {
        fprintf(stderr, "Error: Cannot open config file %s\n", config_path);
        return -1;
    }
    
    char line[256];
    num_servers = 0;
    
    while (fgets(line, sizeof(line), fp) && num_servers < MAX_SERVERS) {
        char *token = strtok(line, " \t\n");
        if (!token || strcmp(token, "server") != 0) continue;
        
        char *name = strtok(NULL, " \t\n");
        char *address = strtok(NULL, " \t\n:");
        char *port_str = strtok(NULL, " \t\n:");
        
        if (name && address && port_str) {
            strncpy(servers[num_servers].name, name, sizeof(servers[num_servers].name) - 1);
            strncpy(servers[num_servers].host, address, sizeof(servers[num_servers].host) - 1);
            servers[num_servers].port = atoi(port_str);
            servers[num_servers].available = 0;
            num_servers++;
        }
    }
    
    fclose(fp);
    return num_servers;
}

// Connect to server with timeout
int connect_with_timeout(const char *host, int port, int timeout_sec) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    
    // Set socket to non-blocking
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    
    struct sockaddr_in server_addr = {0};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &server_addr.sin_addr);
    
    if (connect(sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(sock);
        return -1;
    }
    
    return sock;
}

// Check which servers are available
void check_server_availability() {
    for (int i = 0; i < num_servers; i++) {
        int sock = connect_with_timeout(servers[i].host, servers[i].port, CONNECT_TIMEOUT);
        if (sock >= 0) {
            servers[i].available = 1;
            close(sock);
        } else {
            servers[i].available = 0;
        }
    }
}

// Handle LIST command
void handle_list() {
    check_server_availability();
    
    // Track which files we've seen and on which servers
    typedef struct {
        char name[MAX_FILENAME];
        int chunk_servers[4][MAX_SERVERS];  // chunk_servers[chunk_num-1][server_idx] = 1 if present
        int chunks_found[4];  // chunks_found[chunk_num-1] = count of servers with this chunk
    } file_info_t;
    
    file_info_t files[100];
    int file_count = 0;
    
    // Query each server
    for (int i = 0; i < num_servers; i++) {
        if (!servers[i].available) continue;
        
        int sock = connect_with_timeout(servers[i].host, servers[i].port, CONNECT_TIMEOUT);
        if (sock < 0) continue;
        
        send(sock, "LIST\n", 5, 0);
        
        char buffer[BUFFER_SIZE];
        char response[BUFFER_SIZE * 10] = {0};
        ssize_t total = 0;
        
        // Receive response
        while (1) {
            ssize_t n = recv(sock, buffer, BUFFER_SIZE - 1, 0);
            if (n <= 0) break;
            buffer[n] = '\0';
            strncat(response, buffer, sizeof(response) - strlen(response) - 1);
            if (strstr(response, "END\n")) break;
        }
        
        close(sock);
        
        // Parse response - each line is a chunk filename like "file.txt.1"
        char *line = strtok(response, "\n");
        while (line) {
            if (strcmp(line, "OK") == 0 || strcmp(line, "END") == 0) {
                line = strtok(NULL, "\n");
                continue;
            }
            
            // Extract base filename and chunk number
            char filename[MAX_FILENAME];
            strncpy(filename, line, sizeof(filename) - 1);
            
            // Find the last dot to get chunk number
            char *last_dot = strrchr(filename, '.');
            if (last_dot && *(last_dot + 1) >= '1' && *(last_dot + 1) <= '4') {
                int chunk_num = *(last_dot + 1) - '0';
                *last_dot = '\0';  // Remove .chunk_num from filename
                
                // Find or create file entry
                int file_idx = -1;
                for (int j = 0; j < file_count; j++) {
                    if (strcmp(files[j].name, filename) == 0) {
                        file_idx = j;
                        break;
                    }
                }
                
                if (file_idx == -1 && file_count < 100) {
                    file_idx = file_count++;
                    strncpy(files[file_idx].name, filename, sizeof(files[file_idx].name) - 1);
                    memset(files[file_idx].chunk_servers, 0, sizeof(files[file_idx].chunk_servers));
                    memset(files[file_idx].chunks_found, 0, sizeof(files[file_idx].chunks_found));
                }
                
                if (file_idx >= 0) {
                    files[file_idx].chunk_servers[chunk_num - 1][i] = 1;
                    files[file_idx].chunks_found[chunk_num - 1]++;
                }
            }
            
            line = strtok(NULL, "\n");
        }
    }
    
    // Print results
    for (int i = 0; i < file_count; i++) {
        // Check if file is complete (need at least 3 different chunks)
        int unique_chunks = 0;
        for (int j = 0; j < 4; j++) {
            if (files[i].chunks_found[j] > 0) {
                unique_chunks++;
            }
        }
        
        if (unique_chunks >= 3) {
            printf("%s\n", files[i].name);
        } else {
            printf("%s [incomplete]\n", files[i].name);
        }
    }
}

// Split file into chunks and store in memory
typedef struct {
    unsigned char *data;
    long size;
} chunk_t;

void split_file(const char *filepath, chunk_t chunks[4]) {
    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        fprintf(stderr, "Error: Cannot open file %s\n", filepath);
        return;
    }
    
    // Get file size
    fseek(fp, 0, SEEK_END);
    long file_size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    
    // Calculate chunk size
    long base_chunk_size = file_size / 4;
    long remainder = file_size % 4;
    
    // Read and split file
    for (int i = 0; i < 4; i++) {
        long chunk_size = base_chunk_size + (i < remainder ? 1 : 0);
        chunks[i].data = malloc(chunk_size);
        chunks[i].size = chunk_size;
        fread(chunks[i].data, 1, chunk_size, fp);
    }
    
    fclose(fp);
}

// Handle PUT command
void handle_put(const char *filepath) {
    // Extract filename from path
    const char *filename = strrchr(filepath, '/');
    if (filename) {
        filename++;
    } else {
        filename = filepath;
    }
    
    // Check file exists
    struct stat st;
    if (stat(filepath, &st) != 0) {
        fprintf(stderr, "Error: File %s not found\n", filepath);
        return;
    }
    
    check_server_availability();
    
    // Count available servers
    int available_count = 0;
    for (int i = 0; i < num_servers; i++) {
        if (servers[i].available) available_count++;
    }
    
    // Need at least 3 servers for redundancy
    if (available_count < 3) {
        printf("%s put failed\n", filename);
        return;
    }
    
    // Split file into chunks
    chunk_t chunks[4];
    split_file(filepath, chunks);
    
    // Get distribution map
    int distribution[MAX_SERVERS][2];
    get_distribution_map(filename, num_servers, distribution);
    
    // Upload chunks to servers
    for (int i = 0; i < num_servers; i++) {
        if (!servers[i].available) continue;
        
        int chunk1 = distribution[i][0];
        int chunk2 = distribution[i][1];
        
        // Upload first chunk
        int sock = connect_with_timeout(servers[i].host, servers[i].port, CONNECT_TIMEOUT);
        if (sock >= 0) {
            send(sock, "PUT\n", 4, 0);
            
            // Send filename with chunk number
            char chunk_filename[MAX_FILENAME];
            snprintf(chunk_filename, sizeof(chunk_filename), "%s.%d", filename, chunk1);
            int name_len = strlen(chunk_filename);
            
            send(sock, &name_len, sizeof(name_len), 0);
            send(sock, chunk_filename, name_len, 0);
            send(sock, &chunks[chunk1 - 1].size, sizeof(long), 0);
            
            // Wait for OK
            char ack[16];
            recv(sock, ack, sizeof(ack), 0);
            
            // Send data
            send(sock, chunks[chunk1 - 1].data, chunks[chunk1 - 1].size, 0);
            
            // Wait for confirmation
            recv(sock, ack, sizeof(ack), 0);
            close(sock);
        }
        
        // Upload second chunk
        sock = connect_with_timeout(servers[i].host, servers[i].port, CONNECT_TIMEOUT);
        if (sock >= 0) {
            send(sock, "PUT\n", 4, 0);
            
            char chunk_filename[MAX_FILENAME];
            snprintf(chunk_filename, sizeof(chunk_filename), "%s.%d", filename, chunk2);
            int name_len = strlen(chunk_filename);
            
            send(sock, &name_len, sizeof(name_len), 0);
            send(sock, chunk_filename, name_len, 0);
            send(sock, &chunks[chunk2 - 1].size, sizeof(long), 0);
            
            char ack[16];
            recv(sock, ack, sizeof(ack), 0);
            
            send(sock, chunks[chunk2 - 1].data, chunks[chunk2 - 1].size, 0);
            recv(sock, ack, sizeof(ack), 0);
            close(sock);
        }
    }
    
    // Free chunks
    for (int i = 0; i < 4; i++) {
        free(chunks[i].data);
    }
}

// Handle GET command
void handle_get(const char *filename) {
    check_server_availability();
    
    // Get distribution map
    int distribution[MAX_SERVERS][2];
    get_distribution_map(filename, num_servers, distribution);
    
    // Track which chunks we've retrieved
    chunk_t chunks[4];
    int chunks_retrieved[4] = {0};
    
    for (int i = 0; i < 4; i++) {
        chunks[i].data = NULL;
        chunks[i].size = 0;
    }
    
    // Try to retrieve all chunks
    for (int i = 0; i < num_servers; i++) {
        if (!servers[i].available) continue;
        
        int chunk1 = distribution[i][0];
        int chunk2 = distribution[i][1];
        
        // Try to get first chunk if we don't have it
        if (!chunks_retrieved[chunk1 - 1]) {
            int sock = connect_with_timeout(servers[i].host, servers[i].port, CONNECT_TIMEOUT);
            if (sock >= 0) {
                send(sock, "GET\n", 4, 0);
                
                char chunk_filename[MAX_FILENAME];
                snprintf(chunk_filename, sizeof(chunk_filename), "%s.%d", filename, chunk1);
                int name_len = strlen(chunk_filename);
                
                send(sock, &name_len, sizeof(name_len), 0);
                send(sock, chunk_filename, name_len, 0);
                
                char response[16];
                recv(sock, response, sizeof(response), 0);
                
                if (strstr(response, "OK")) {
                    long file_size;
                    recv(sock, &file_size, sizeof(file_size), 0);
                    
                    chunks[chunk1 - 1].data = malloc(file_size);
                    chunks[chunk1 - 1].size = file_size;
                    
                    long bytes_received = 0;
                    while (bytes_received < file_size) {
                        ssize_t n = recv(sock, chunks[chunk1 - 1].data + bytes_received, 
                                        file_size - bytes_received, 0);
                        if (n <= 0) break;
                        bytes_received += n;
                    }
                    
                    if (bytes_received == file_size) {
                        chunks_retrieved[chunk1 - 1] = 1;
                    }
                }
                
                close(sock);
            }
        }
        
        // Try to get second chunk if we don't have it
        if (!chunks_retrieved[chunk2 - 1]) {
            int sock = connect_with_timeout(servers[i].host, servers[i].port, CONNECT_TIMEOUT);
            if (sock >= 0) {
                send(sock, "GET\n", 4, 0);
                
                char chunk_filename[MAX_FILENAME];
                snprintf(chunk_filename, sizeof(chunk_filename), "%s.%d", filename, chunk2);
                int name_len = strlen(chunk_filename);
                
                send(sock, &name_len, sizeof(name_len), 0);
                send(sock, chunk_filename, name_len, 0);
                
                char response[16];
                recv(sock, response, sizeof(response), 0);
                
                if (strstr(response, "OK")) {
                    long file_size;
                    recv(sock, &file_size, sizeof(file_size), 0);
                    
                    chunks[chunk2 - 1].data = malloc(file_size);
                    chunks[chunk2 - 1].size = file_size;
                    
                    long bytes_received = 0;
                    while (bytes_received < file_size) {
                        ssize_t n = recv(sock, chunks[chunk2 - 1].data + bytes_received, 
                                        file_size - bytes_received, 0);
                        if (n <= 0) break;
                        bytes_received += n;
                    }
                    
                    if (bytes_received == file_size) {
                        chunks_retrieved[chunk2 - 1] = 1;
                    }
                }
                
                close(sock);
            }
        }
    }
    
    // Check if we have enough chunks (at least 3)
    int chunks_count = 0;
    for (int i = 0; i < 4; i++) {
        if (chunks_retrieved[i]) chunks_count++;
    }
    
    if (chunks_count < 3) {
        printf("%s is incomplete\n", filename);
        for (int i = 0; i < 4; i++) {
            if (chunks[i].data) free(chunks[i].data);
        }
        return;
    }
    
    // Reconstruct file
    FILE *fp = fopen(filename, "wb");
    if (!fp) {
        fprintf(stderr, "Error: Cannot create file %s\n", filename);
        for (int i = 0; i < 4; i++) {
            if (chunks[i].data) free(chunks[i].data);
        }
        return;
    }
    
    for (int i = 0; i < 4; i++) {
        if (chunks_retrieved[i]) {
            fwrite(chunks[i].data, 1, chunks[i].size, fp);
        }
    }
    
    fclose(fp);
    
    // Free memory
    for (int i = 0; i < 4; i++) {
        if (chunks[i].data) free(chunks[i].data);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <command> [filename]\n", argv[0]);
        fprintf(stderr, "Commands: list, get <filename>, put <filename>\n");
        exit(1);
    }
    
    // Parse config
    if (parse_config(CONFIG_FILE) <= 0) {
        fprintf(stderr, "Error: Failed to parse config file\n");
        exit(1);
    }
    
    char *command = argv[1];
    
    if (strcmp(command, "list") == 0) {
        handle_list();
    } else if (strcmp(command, "get") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Error: get command requires filename\n");
            exit(1);
        }
        handle_get(argv[2]);
    } else if (strcmp(command, "put") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Error: put command requires filename\n");
            exit(1);
        }
        handle_put(argv[2]);
    } else {
        fprintf(stderr, "Error: Unknown command %s\n", command);
        exit(1);
    }
    
    return 0;
}