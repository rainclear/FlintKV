#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <fstream>
#include <map>
#include <string>
#include <stack>
#include <vector>
#include <unistd.h>
#include <cstdio>

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
        file.seekg(0, std::ios::end);
        next_page_id = file.tellg() / PAGE_SIZE;

        if (next_page_id == 0) {
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
        file.flush(); 
    }
};

#endif // BUFFERPOOL_H