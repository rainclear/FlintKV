#include <iostream>
#include <optional>
#include "BufferPool.h"

class BPlusTree {
    BufferPool pool;
    uint32_t root_id;
    struct Slot {
        uint16_t offset;
        uint16_t length;
    };

    void updateMetaPage() {
        char* meta_data = pool.getPage(0);
        // Assuming the first 4 bytes of Page 0 store the root_id
        std::memcpy(meta_data, &root_id, sizeof(uint32_t));
        pool.flushPage(0);
    }    

    int findSlotBinary(char* page_data, const std::string& key) {
        PageHeader* h = (PageHeader*)page_data;
        assert(h->num_slots < 500);
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));

        int low = 0;
        int high = (int)h->num_slots - 1;
        int result_idx = h->num_slots; // Default to "end"

        while (low <= high) {
            int mid = low + (high - low) / 2;
            
            // Navigate to the record using the offset in the slot
            char* record_ptr = page_data + slots[mid].offset;
            uint8_t kLen = (uint8_t)(*record_ptr);
            std::string current_key(record_ptr + 1, kLen);

            if (current_key == key) {
                return mid; // Exact match
            } else if (current_key < key) {
                low = mid + 1;
            } else {
                result_idx = mid; // Potential insertion point
                high = mid - 1;
            }
        }
        return result_idx; 
    }

public:
    BPlusTree() : pool("db.bin") {
        char* meta_data = pool.getPage(0);
        // Use the first 4 bytes of Page 0 to store the root_id
        root_id = *reinterpret_cast<uint32_t*>(meta_data);

        if (root_id == 0) { // Fresh DB
            root_id = pool.allocatePage(); // This will likely return 1
            PageHeader* h = (PageHeader*)pool.getPage(root_id);
            h->is_leaf = true;
            
            // Save new root_id to Meta Page
            *reinterpret_cast<uint32_t*>(meta_data) = root_id;
            pool.flushPage(0);
            pool.flushPage(root_id);
        }
    }

    // 1. TRAVERSAL LOOP
    uint32_t findLeaf(uint32_t node_id, const std::string& key) {
        char* page_data = pool.getPage(node_id);
        PageHeader* h = (PageHeader*)page_data;
        if (h->is_leaf) return node_id;

        IndexEntry* entries = (IndexEntry*)(page_data + sizeof(PageHeader));
        
        // If key is less than the very first separator, go to the leftmost child
        if (key < std::string(entries[0].key)) {
            return findLeaf(h->lower_bound_child, key);
        }

        // Otherwise, find the rightmost entry that is <= key
        for (int i = (int)h->num_slots - 1; i >= 0; --i) {
            if (key >= std::string(entries[i].key)) {
                return findLeaf(entries[i].child_page_id, key);
            }
        }
        return findLeaf(entries[0].child_page_id, key); // Fallback
    }

    // 2. SPLIT FUNCTION (Leaf)
    void splitLeaf(uint32_t old_leaf_id, const std::string& key, const std::string& value) {
        uint32_t new_leaf_id = pool.allocatePage(); 
        
        char* old_data = pool.getPage(old_leaf_id);
        char* new_data = pool.getPage(new_leaf_id);
        PageHeader* old_h = (PageHeader*)old_data;
        PageHeader* new_h = (PageHeader*)new_data;

        new_h->is_leaf = true;
        new_h->parent_id = old_h->parent_id;
        new_h->next_sibling = old_h->next_sibling;
        old_h->next_sibling = new_leaf_id;

        Slot* old_slots = (Slot*)(old_data + sizeof(PageHeader));
        uint32_t mid = old_h->num_slots / 2;
        uint32_t num_to_move = old_h->num_slots - mid;

        // Use the first key of the NEW page as the separator for the parent
        char* sep_rec_ptr = old_data + old_slots[mid].offset;
        std::string mid_key(sep_rec_ptr + 1, (uint8_t)sep_rec_ptr[0]);

        // Move records to the new page
        for (uint32_t i = mid; i < old_h->num_slots; ++i) {
            char* rec_ptr = old_data + old_slots[i].offset;
            uint8_t kLen = (uint8_t)rec_ptr[0];
            uint8_t vLen = (uint8_t)rec_ptr[1 + kLen];
            std::string k(rec_ptr + 1, kLen);
            std::string v(rec_ptr + 2 + kLen, vLen);
            
            // Re-insert into the new leaf (simplest way to handle offsets)
            insertIntoLeaf(new_leaf_id, k, v);
        }

        // Shrink the old leaf
        old_h->num_slots = mid;
        defragmentPage(old_leaf_id); // Reclaim the space from moved records

        // Decide where the NEW record (the one that triggered the split) goes
        if (key < mid_key) {
            insertIntoLeaf(old_leaf_id, key, value);
        } else {
            insertIntoLeaf(new_leaf_id, key, value);
        }

        // Update Parent
        if (old_leaf_id == root_id) {
            createNewRoot(old_leaf_id, new_leaf_id, mid_key);
        } else {
            insertIntoInternal(old_h->parent_id, mid_key, new_leaf_id);
        }
    }

    void defragmentPage(uint32_t page_id) {
        char* page_data = pool.getPage(page_id);
        PageHeader* h = (PageHeader*)page_data;
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));

        // Create a temporary buffer to hold the records
        std::vector<char> temp_buffer(PAGE_SIZE, 0);
        uint32_t current_offset = PAGE_SIZE;

        // Copy records one by one to the end of the temp buffer and update offsets
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

        // Copy the reorganized data area back to the actual page
        uint32_t data_area_start = sizeof(PageHeader) + (h->num_slots * sizeof(Slot));
        uint32_t data_size = PAGE_SIZE - current_offset;
        
        // Zero out the old data area first
        std::memset(page_data + data_area_start, 0, PAGE_SIZE - data_area_start);
        
        // Copy back the compacted records
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

        IndexEntry* old_entries = (IndexEntry*)(old_data + sizeof(PageHeader));
        uint32_t mid = old_h->num_slots / 2;
        
        // The key at 'mid' is promoted to the parent.
        std::string promotion_key = old_entries[mid].key;

        // CRITICAL FIX: Set the lower_bound_child for the new internal node.
        new_h->lower_bound_child = old_entries[mid].child_page_id;

        // Update parent pointer of the child that moved to lower_bound_child.
        char* mid_child_ptr = pool.getPage(new_h->lower_bound_child);
        ((PageHeader*)mid_child_ptr)->parent_id = new_node_id;
        pool.flushPage(new_h->lower_bound_child);

        // Move subsequent entries to the new node.
        for (uint32_t i = mid + 1; i < old_h->num_slots; ++i) {
            insertIntoInternal(new_node_id, old_entries[i].key, old_entries[i].child_page_id);
            
            char* child_ptr = pool.getPage(old_entries[i].child_page_id);
            ((PageHeader*)child_ptr)->parent_id = new_node_id;
            pool.flushPage(old_entries[i].child_page_id);
        }

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

        // Calculate max capacity: (4096 - HeaderSize) / EntrySize
        size_t max_entries = (PAGE_SIZE - sizeof(PageHeader)) / sizeof(IndexEntry);

        if (h->num_slots >= max_entries) {
            splitInternal(parent_id);
            // Note: For a production engine, you'd then determine which 
            // side of the split the new key belongs in.
            return; 
        }

        IndexEntry* entries = (IndexEntry*)(data + sizeof(PageHeader));
        IndexEntry& new_entry = entries[h->num_slots];
        
        std::memset(new_entry.key, 0, 16);
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
        root_h->lower_bound_child = left_child_id; // Store the left side!

        IndexEntry* entries = (IndexEntry*)(root_data + sizeof(PageHeader));
        std::memset(entries[0].key, 0, 16);
        std::strncpy(entries[0].key, key.c_str(), 15);
        entries[0].child_page_id = right_child_id; // Store the right side
        
        // Crucial: Update children's parent pointers
        ((PageHeader*)pool.getPage(left_child_id))->parent_id = new_root_id;
        ((PageHeader*)pool.getPage(right_child_id))->parent_id = new_root_id;

        root_id = new_root_id;
        updateMetaPage(); // Persist the new root_id to Page 0
    }

    void insertIntoLeaf(uint32_t leaf_id, const std::string& key, const std::string& value) {
        char* page_data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)page_data;

        int slot_idx = findSlotBinary(page_data, key);
        
        // Check if key already exists for update (optional but recommended)
        if (slot_idx < (int)h->num_slots) {
            Slot* slots = (Slot*)(page_data + sizeof(PageHeader));
            char* rec_ptr = page_data + slots[slot_idx].offset;
            if (std::string(rec_ptr + 1, (uint8_t)*rec_ptr) == key) {
                // Basic update: for simplicity, we'll return, but real DBs 
                // would handle variable-length updates here.
                return; 
            }
        }

        size_t entry_size = key.size() + value.size() + 2; // [kLen][key][vLen][val]
        size_t slot_array_end = sizeof(PageHeader) + (h->num_slots + 1) * sizeof(Slot);

        // CRITICAL BOUNDARY CHECK: Ensure slot array doesn't hit data
        if (h->free_space_offset < entry_size || (h->free_space_offset - entry_size) < slot_array_end) {
            // This should technically be caught by the split logic in put(), 
            // but this check prevents a crash if logic fails elsewhere.
            std::cerr << "CRITICAL ERROR: Page overflow in insertIntoLeaf" << std::endl;
            return;
        }

        // 1. Shift Slot Array to make room for the new Slot at slot_idx
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));
        if (slot_idx < (int)h->num_slots) {
            std::memmove(&slots[slot_idx + 1], &slots[slot_idx], (h->num_slots - slot_idx) * sizeof(Slot));
        }

        // 2. Write Data to the bottom of the page
        h->free_space_offset -= entry_size;
        char* data_ptr = page_data + h->free_space_offset;
        
        data_ptr[0] = (uint8_t)key.size();
        std::memcpy(data_ptr + 1, key.data(), key.size());
        data_ptr[1 + key.size()] = (uint8_t)value.size();
        std::memcpy(data_ptr + 2 + key.size(), value.data(), value.size());

        // 3. Update the Slot entry
        slots[slot_idx].offset = (uint16_t)h->free_space_offset;
        slots[slot_idx].length = (uint16_t)entry_size;

        h->num_slots++;
        pool.flushPage(leaf_id);
    }

    void put(const std::string& key, const std::string& value) {
        uint32_t leaf_id = findLeaf(root_id, key);
        char* page_data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)page_data;

        // Check if we need to split
        if (h->free_space_offset < (sizeof(PageHeader) + (h->num_slots + 1) * sizeof(Slot) + key.length() + value.length() + 2)) {
            splitLeaf(leaf_id, key, value);
        } else {
            insertIntoLeaf(leaf_id, key, value);
        }
    }

    std::optional<std::string> get(const std::string& key) {
        uint32_t leaf_id = findLeaf(root_id, key);
        char* page_data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)page_data;
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));

        int idx = findSlotBinary(page_data, key);
        if (idx < (int)h->num_slots) {
            char* record_ptr = page_data + slots[idx].offset;
            
            // Read Key (Length-prefixed)
            uint8_t kLen = (uint8_t)(*record_ptr);
            std::string found_key(record_ptr + 1, kLen);

            if (found_key == key) {
                // Read Value (Length-prefixed, located after key)
                char* v_ptr = record_ptr + 1 + kLen;
                uint8_t vLen = (uint8_t)(*v_ptr);
                return std::string(v_ptr + 1, vLen);
            }
        }
        return std::nullopt;
    }

    std::vector<std::pair<std::string, std::string>> rangeScan(const std::string& start_key, const std::string& end_key) {
        std::vector<std::pair<std::string, std::string>> results;
        uint32_t current_page_id = findLeaf(root_id, start_key);

        while (current_page_id != 0) {
            char* page_data = pool.getPage(current_page_id);
            PageHeader* h = (PageHeader*)page_data;
            Slot* slots = (Slot*)(page_data + sizeof(PageHeader));

            // Find starting index in this page (first key >= start_key)
            int start_idx = findSlotBinary(page_data, start_key);
            std::cout << "Range Scan started at Page: " << current_page_id << std::endl;

            for (uint32_t i = start_idx; i < h->num_slots; ++i) {
                char* record_ptr = page_data + slots[i].offset;
                uint8_t kLen = (uint8_t)(*record_ptr);
                std::string key(record_ptr + 1, kLen);

                if (key > end_key) return results; // Stop if we passed the end of the range

                char* v_ptr = record_ptr + 1 + kLen;
                uint8_t vLen = (uint8_t)(*v_ptr);
                results.push_back({key, std::string(v_ptr + 1, vLen)});
            }
            
            // Move to the next sibling page
            current_page_id = h->next_sibling;
        }
        return results;
    }

    bool remove(const std::string& key) {
        uint32_t leaf_id = findLeaf(root_id, key);
        char* page_data = pool.getPage(leaf_id);
        PageHeader* h = (PageHeader*)page_data;
        
        int slot_idx = findSlotBinary(page_data, key);
        
        // Check if the key actually exists in this slot
        if (slot_idx >= (int)h->num_slots) return false;
        
        Slot* slots = (Slot*)(page_data + sizeof(PageHeader));
        char* record_ptr = page_data + slots[slot_idx].offset;
        uint8_t kLen = (uint8_t)(*record_ptr);
        std::string current_key(record_ptr + 1, kLen);
        
        if (current_key != key) return false; // Key not found

        // 1. Remove the slot by shifting all subsequent slots left
        if (slot_idx < (int)h->num_slots - 1) {
            std::memmove(&slots[slot_idx], &slots[slot_idx + 1], 
                        (h->num_slots - slot_idx - 1) * sizeof(Slot));
        }
        
        h->num_slots--;

        // 2. Reclaim the space
        // We don't just shift the data (which is hard); we rebuild the page
        defragmentPage(leaf_id);
        
        pool.flushPage(leaf_id);
        return true;
    }
};