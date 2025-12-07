#include <stdio.h>
#include <string.h>
#include <openssl/md5.h>

// Compute MD5 hash of a string and return the modulo
int compute_hash_mod(const char *str, int mod) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5((unsigned char*)str, strlen(str), digest);
    
    // Convert first 4 bytes to an integer
    unsigned int hash_val = 0;
    for (int i = 0; i < 4; i++) {
        hash_val = (hash_val << 8) | digest[i];
    }
    
    return hash_val % mod;
}

// Get which chunks should be stored on which server
// server_num is 0-3 (for dfs1-dfs4)
// Returns an array of 2 chunk numbers (1-4)
void get_chunks_for_server(int hash_mod, int server_num, int chunks[2]) {
    // Distribution table based on hash_mod value
    // x=0: dfs1=(1,2), dfs2=(2,3), dfs3=(3,4), dfs4=(4,1)
    // x=1: dfs1=(4,1), dfs2=(1,2), dfs3=(2,3), dfs4=(3,4)
    // x=2: dfs1=(3,4), dfs2=(4,1), dfs3=(1,2), dfs4=(2,3)
    // x=3: dfs1=(2,3), dfs2=(3,4), dfs3=(4,1), dfs4=(1,2)
    
    int table[4][4][2] = {
        {{1,2}, {2,3}, {3,4}, {4,1}},  // x=0
        {{4,1}, {1,2}, {2,3}, {3,4}},  // x=1
        {{3,4}, {4,1}, {1,2}, {2,3}},  // x=2
        {{2,3}, {3,4}, {4,1}, {1,2}}   // x=3
    };
    
    chunks[0] = table[hash_mod][server_num][0];
    chunks[1] = table[hash_mod][server_num][1];
}

// Determine which servers have which chunks for a given file
// Returns a 2D array: servers[server_idx][0] = chunk1, servers[server_idx][1] = chunk2
void get_distribution_map(const char *filename, int num_servers, int servers[][2]) {
    int hash_mod = compute_hash_mod(filename, num_servers);
    
    for (int i = 0; i < num_servers; i++) {
        get_chunks_for_server(hash_mod, i, servers[i]);
    }
}