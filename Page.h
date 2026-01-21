#ifndef PAGE_H
#define PAGE_H

#include <cstdint>
#include <cstring>
#include <vector>

#pragma pack(push, 1)
struct PageHeader {
    uint32_t page_id;
    uint32_t parent_id;         // Needed for Bottom-Up Splitting
    uint32_t next_sibling;      // Needed for Horizontal Range Scans
    uint32_t lower_bound_child; // The pointer for values < entries[0].key
    bool is_leaf;
    uint32_t num_slots;
    uint32_t free_space_offset;
};

// Internal nodes store Key + Child PageID
struct IndexEntry {
    char key[16]; // Simplified fixed-length keys for the split logic
    uint32_t child_page_id;
};
#pragma pack(pop)

const size_t PAGE_SIZE = 4096;

#endif // PAGE_H