#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <iostream>
#include <vector>
#include <string>
#include <optional>
#include <cassert>
#include <cstring>
#include "BufferPool.h"
#include "Page.h"

class BPlusTree {
private:
    BufferPool pool;
    uint32_t root_id;

    struct Slot {
        uint16_t offset;
        uint16_t length;
    };

    void updateMetaPage() {
        char* meta_data = pool.getPage(0);
        std::memcpy(meta_data, &root_id, sizeof(uint32_t));
        pool.flushPage(0);
    }

    int findSlotBinary(char* page_data, const std::string& key) {
        PageHeader* h = (PageHeader*)page_data;
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));

        int low = 0;
        int high = (int)h->num_slots - 1;
        int result_idx = h->num_slots;

        while (low <= high) {
            int mid = low + (high - low) / 2;
            char* record_ptr = page_data + slots[mid].offset;
            uint8_t kLen = (uint8_t)(*record_ptr);
            std::string current_key(record_ptr + 1, kLen);

            if (current_key == key) return mid;
            if (current_key < key) {
                low = mid + 1;
            } else {
                result_idx = mid;
                high = mid - 1;
            }
        }
        return result_idx;
    }

    void defragmentPage(uint32_t page_id) {
        char* page_data = pool.getPage(page_id);
        PageHeader* h = (PageHeader*)page_data;
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));

        std::vector<char> temp_buffer(PAGE_SIZE, 0);
        uint32_t current_offset = PAGE_SIZE;

        for (uint32_t i = 0; i < h->num_slots; ++i) {
            char* old_rec_ptr = page_data + slots[i].offset;
            uint8_t kLen = (uint8_t)old_rec_ptr[0];
            uint8_t vLen = (uint8_t)old_rec_ptr[1 + kLen];
            uint32_t rec_size = 2 + kLen + vLen;

            current_offset -= rec_size;
            std::memcpy(temp_buffer.data() + current_offset, old_rec_ptr, rec_size);
            slots[i].offset = (uint16_t)current_offset;
            slots[i].length = (uint16_t)rec_size;
        }

        uint32_t data_area_start = sizeof(PageHeader) + (h->num_slots * sizeof(Slot));
        std::memset(page_data + data_area_start, 0, PAGE_SIZE - data_area_start);
        
        uint32_t data_size = PAGE_SIZE - current_offset;
        if (data_size > 0) {
            std::memcpy(page_data + current_offset, temp_buffer.data() + current_offset, data_size);
        }
        h->free_space_offset = (uint16_t)current_offset;
    }

    void splitInternal(uint32_t node_id) {
        uint32_t new_node_id = pool.allocatePage();
        char* old_data = pool.getPage(node_id);
        char* new_data = pool.getPage(new_node_id);
        
        PageHeader* old_h = (PageHeader*)old_data;
        PageHeader* new_h = (PageHeader*)new_data;
        new_h->is_leaf = false;
        new_h->parent_id = old_h->parent_id;
        new_h->num_slots = 0;

        IndexEntry* old_entries = (IndexEntry*)(old_data + sizeof(PageHeader));
        IndexEntry* new_entries = (IndexEntry*)(new_data + sizeof(PageHeader));
        
        uint32_t mid = old_h->num_slots / 2;
        std::string promotion_key = old_entries[mid].key;

        // Set lower_bound_child for the new node and update its parent pointer
        new_h->lower_bound_child = old_entries[mid].child_page_id;
        PageHeader* child_h = (PageHeader*)pool.getPage(new_h->lower_bound_child);
        child_h->parent_id = new_node_id;
        pool.flushPage(new_h->lower_bound_child);

        // Move subsequent entries directly to avoid recursion
        uint32_t move_count = 0;
        for (uint32_t i = mid + 1; i < old_h->num_slots; ++i) {
            std::memcpy(&new_entries[move_count], &old_entries[i], sizeof(IndexEntry));
            
            PageHeader* moved_child_h = (PageHeader*)pool.getPage(old_entries[i].child_page_id);
            moved_child_h->parent_id = new_node_id;
            pool.flushPage(old_entries[i].child_page_id);
            move_count++;
        }

        new_h->num_slots = move_count;
        old_h->num_slots = mid; 

        pool.flushPage(node_id);
        pool.flushPage(new_node_id);

        if (node_id == root_id) {
            createNewRoot(node_id, new_node_id, promotion_key);
        } else {
            insertIntoInternal(old_h->parent_id, promotion_key, new_node_id);
        }
    }

    void insertIntoInternal(uint32_t parent_id, const std::string& key, uint32_t child_id) {
        char* data = pool.getPage(parent_id);
        PageHeader* h = (PageHeader*)data;
        size_t max_entries = (PAGE_SIZE - sizeof(PageHeader)) / sizeof(IndexEntry);

        if (h->num_slots >= max_entries) {
            splitInternal(parent_id);
            // After split, we would normally find which new page to insert into.
            // For now, propagation happens via promotion.
            return; 
        }

        IndexEntry* entries = (IndexEntry*)(data + sizeof(PageHeader));
        IndexEntry& new_entry = entries[h->num_slots];
        
        std::memset(new_entry.key, 0, 16); // Sanity check: Clear buffer
        std::strncpy(new_entry.key, key.c_str(), 15);
        new_entry.child_page_id = child_id;

        h->num_slots++;
        pool.flushPage(parent_id);
    }

    void createNewRoot(uint32_t left_child_id, uint32_t right_child_id, const std::string& key) {
        uint32_t new_root_id = pool.allocatePage();
        char* root_data = pool.getPage(new_root_id);
        PageHeader* root_h = (PageHeader*)root_data;
        
        root_h->is_leaf = false;
        root_h->num_slots = 1;
        root_h->lower_bound_child = left_child_id;

        IndexEntry* entries = (IndexEntry*)(root_data + sizeof(PageHeader));
        std::memset(entries[0].key, 0, 16);
        std::strncpy(entries[0].key, key.c_str(), 15);
        entries[0].child_page_id = right_child_id;
        
        ((PageHeader*)pool.getPage(left_child_id))->parent_id = new_root_id;
        ((PageHeader*)pool.getPage(right_child_id))->parent_id = new_root_id;
        pool.flushPage(left_child_id);
        pool.flushPage(right_child_id);

        root_id = new_root_id;
        updateMetaPage();
    }

    void insertIntoLeaf(uint32_t leaf_id, const std::string& key, const std::string& value) {
        char* page_data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)page_data;
        int slot_idx = findSlotBinary(page_data, key);
        
        size_t entry_size = key.size() + value.size() + 2;
        size_t slot_array_end = sizeof(PageHeader) + (h->num_slots + 1) * sizeof(Slot);

        if (h->free_space_offset < entry_size || (h->free_space_offset - entry_size) < slot_array_end) {
            return; // Managed by split logic in put()
        }

        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));
        if (slot_idx < (int)h->num_slots) {
            std::memmove(&slots[slot_idx + 1], &slots[slot_idx], (h->num_slots - slot_idx) * sizeof(Slot));
        }

        h->free_space_offset -= entry_size;
        char* data_ptr = page_data + h->free_space_offset;
        data_ptr[0] = (uint8_t)key.size();
        std::memcpy(data_ptr + 1, key.data(), key.size());
        data_ptr[1 + key.size()] = (uint8_t)value.size();
        std::memcpy(data_ptr + 2 + key.size(), value.data(), value.size());

        slots[slot_idx].offset = (uint16_t)h->free_space_offset;
        slots[slot_idx].length = (uint16_t)entry_size;
        h->num_slots++;
        pool.flushPage(leaf_id);
    }

    void splitLeaf(uint32_t old_leaf_id, const std::string& key, const std::string& value) {
        uint32_t new_leaf_id = pool.allocatePage();
        char* old_data = pool.getPage(old_leaf_id);
        PageHeader* old_h = (PageHeader*)old_data;
        PageHeader* new_h = (PageHeader*)pool.getPage(new_leaf_id);

        new_h->is_leaf = true;
        new_h->parent_id = old_h->parent_id;
        new_h->next_sibling = old_h->next_sibling;
        old_h->next_sibling = new_leaf_id;

        Slot* old_slots = (Slot*)(old_data + sizeof(PageHeader));
        uint32_t mid = old_h->num_slots / 2;
        char* sep_rec_ptr = old_data + old_slots[mid].offset;
        std::string mid_key(sep_rec_ptr + 1, (uint8_t)sep_rec_ptr[0]);

        for (uint32_t i = mid; i < old_h->num_slots; ++i) {
            char* rec_ptr = old_data + old_slots[i].offset;
            uint8_t kLen = (uint8_t)rec_ptr[0];
            uint8_t vLen = (uint8_t)rec_ptr[1 + kLen];
            insertIntoLeaf(new_leaf_id, std::string(rec_ptr + 1, kLen), std::string(rec_ptr + 2 + kLen, vLen));
        }

        old_h->num_slots = mid;
        defragmentPage(old_leaf_id);

        if (key < mid_key) insertIntoLeaf(old_leaf_id, key, value);
        else insertIntoLeaf(new_leaf_id, key, value);

        if (old_leaf_id == root_id) createNewRoot(old_leaf_id, new_leaf_id, mid_key);
        else insertIntoInternal(old_h->parent_id, mid_key, new_leaf_id);
    }

public:
    BPlusTree() : pool("db.bin") {
        char* meta_data = pool.getPage(0);
        root_id = *reinterpret_cast<uint32_t*>(meta_data);
        if (root_id == 0) {
            root_id = pool.allocatePage();
            PageHeader* h = (PageHeader*)pool.getPage(root_id);
            h->is_leaf = true;
            updateMetaPage();
        }
    }

    uint32_t findLeaf(uint32_t node_id, const std::string& key) {
        char* page_data = pool.getPage(node_id);
        PageHeader* h = (PageHeader*)page_data;
        if (h->is_leaf) return node_id;

        IndexEntry* entries = (IndexEntry*)(page_data + sizeof(PageHeader));
        if (key < std::string(entries[0].key)) return findLeaf(h->lower_bound_child, key);

        for (int i = (int)h->num_slots - 1; i >= 0; --i) {
            if (key >= std::string(entries[i].key)) return findLeaf(entries[i].child_page_id, key);
        }
        return findLeaf(entries[0].child_page_id, key);
    }

    void put(const std::string& key, const std::string& value) {
        // 1. Enforce Key Length (Internal node constraint)
        assert(key.length() <= 15 && "Key length exceeds limit of 15");

        // 2. Enforce Total Record Size (Slotted Page constraint)
        // We reserve roughly 100 bytes for headers and slot metadata
        const size_t MAX_RECORD_SIZE = PAGE_SIZE - 100;
        size_t entry_size = key.length() + value.length() + 2;

        if (entry_size > MAX_RECORD_SIZE) {
            std::cerr << "Error: Record too large (" << entry_size
                    << " bytes). Max allowed is " << MAX_RECORD_SIZE << " bytes." << std::endl;
            return;
        }

        uint32_t leaf_id = findLeaf(root_id, key);
        PageHeader* h = (PageHeader*)pool.getPage(leaf_id);
        size_t needed = sizeof(PageHeader) + (h->num_slots + 1) * sizeof(Slot) + entry_size;

        if (h->free_space_offset < needed) splitLeaf(leaf_id, key, value);
        else insertIntoLeaf(leaf_id, key, value);
    }

    std::optional<std::string> get(const std::string& key) {
        uint32_t leaf_id = findLeaf(root_id, key);
        char* page_data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)page_data;
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));
        int idx = findSlotBinary(page_data, key);
        if (idx < (int)h->num_slots) {
            char* rec = page_data + slots[idx].offset;
            uint8_t kLen = (uint8_t)*rec;
            if (std::string(rec + 1, kLen) == key) {
                uint8_t vLen = (uint8_t)rec[1 + kLen];
                return std::string(rec + 2 + kLen, vLen);
            }
        }
        return std::nullopt;
    }

    std::vector<std::pair<std::string, std::string>> rangeScan(const std::string& start, const std::string& end) {
        std::vector<std::pair<std::string, std::string>> res;
        uint32_t curr = findLeaf(root_id, start);
        while (curr != 0) {
            char* data = pool.getPage(curr);
            PageHeader* h = (PageHeader*)data;
            Slot* slots = (Slot*)(data + sizeof(PageHeader));
            for (uint32_t i = findSlotBinary(data, start); i < h->num_slots; ++i) {
                char* rec = data + slots[i].offset;
                uint8_t kLen = (uint8_t)*rec;
                std::string k(rec + 1, kLen);
                if (k > end) return res;
                uint8_t vLen = (uint8_t)rec[1 + kLen];
                res.push_back({k, std::string(rec + 2 + kLen, vLen)});
            }
            curr = h->next_sibling;
        }
        return res;
    }

    bool remove(const std::string& key) {
        uint32_t leaf_id = findLeaf(root_id, key);
        char* data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)data;
        int idx = findSlotBinary(data, key);
        if (idx >= (int)h->num_slots) return false;
        Slot* slots = (Slot*)(data + sizeof(PageHeader));
        char* rec = data + slots[idx].offset;
        if (std::string(rec + 1, (uint8_t)*rec) != key) return false;
        if (idx < (int)h->num_slots - 1) {
            std::memmove(&slots[idx], &slots[idx + 1], (h->num_slots - idx - 1) * sizeof(Slot));
        }
        h->num_slots--;
        defragmentPage(leaf_id);
        pool.flushPage(leaf_id);
        return true;
    }
};

#endif