#include <fstream>
#include <map>
#include <string>
#include <stack>
#include <unistd.h> // For fsync
#include <cstdio>   // For fileno (if using FILE*) or use platform-specific handles

#include "Page.h"

class BufferPool {
    std::fstream file;
    std::map<uint32_t, std::vector<char>> cache;
    std::stack<uint32_t> free_list;
    uint32_t next_page_id = 0;

public:
    BufferPool(std::string path) {
        file.open(path, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            std::ofstream create(path, std::ios::binary);
            file.open(path, std::ios::in | std::ios::out | std::ios::binary);
        }
        // Calculate next_page_id based on file size
        file.seekg(0, std::ios::end);
        next_page_id = file.tellg() / PAGE_SIZE;

        if (next_page_id == 0) {
            // Reserve Page 0 for MetaData immediately
            next_page_id = 1; 
            std::vector<char> empty_meta(PAGE_SIZE, 0);
            file.seekp(0);
            file.write(empty_meta.data(), PAGE_SIZE);
            file.flush();
        }
    }

    char* getPage(uint32_t id) {
        if (cache.count(id)) return cache[id].data();
        
        std::vector<char> buffer(PAGE_SIZE, 0);
        file.seekg(id * PAGE_SIZE);
        file.read(buffer.data(), PAGE_SIZE);
        cache[id] = std::move(buffer);
        return cache[id].data();
    }

    uint32_t allocatePage() {
        uint32_t id = next_page_id++;
        std::vector<char> buffer(PAGE_SIZE, 0);
        PageHeader* h = (PageHeader*)buffer.data();
        h->page_id = id;
        h->free_space_offset = PAGE_SIZE;
        cache[id] = std::move(buffer);
        flushPage(id);
        return id;
    }

    void flushPage(uint32_t id) {
        if (!cache.count(id)) return;

        file.seekp(id * PAGE_SIZE);
        file.write(cache[id].data(), PAGE_SIZE);
        
        // 1. Move data from App to OS
        file.flush(); 

        // 2. Move data from OS to Physical Disk
        // Note: This requires getting the file descriptor from the fstream
        // For many compilers, this trick works, but it's technically platform-specific
        int fd = -1;
    #ifdef __linux__
        // A common way to get FD from fstream in Linux environments
        struct MyFileBuf : std::filebuf { int fd() { return _M_file.fd(); } };
        // This is getting advanced; a simpler way is using open/write/fsync (C-style)
        // or just calling a system command for this learning exercise:
    #endif

        // Professional DBs often use open() and write() syscalls directly 
        // instead of fstream to make fsync easier to call.
    }
};