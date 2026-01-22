#include <iostream>
#include <vector>
#include <string>
#include <assert.h>
#include <iomanip>
#include "FlintKV.h"

int main() {
    // 1. Initialize Engine
    // Note: To start fresh, manually delete 'db.bin' before running.
    BPlusTree db;
    const int TEST_COUNT = 1000;

    std::cout << "--- Phase 1: Sequential Insertion (Persistence & Splitting) ---" << std::endl;
    for (int i = 1; i <= TEST_COUNT; ++i) {
        // We use padded keys (e.g., "key0001") to ensure lexicographical order matches numerical
        std::string key = "key" + std::to_string(i + 10000).substr(1); 
        std::string val = "val" + std::to_string(i);
        db.put(key, val);
        
        if (i % 200 == 0) std::cout << "Inserted " << i << " records..." << std::endl;
    }

    std::cout << "\n--- Phase 2: Point Lookups (Traversal Accuracy) ---" << std::endl;
    int found_count = 0;
    for (int i = 1; i <= TEST_COUNT; ++i) {
        std::string key = "key" + std::to_string(i + 10000).substr(1);
        auto result = db.get(key);
        if (result && *result == ("val" + std::to_string(i))) {
            found_count++;
            // std::cout << "Key " << key << " is found." << std::endl;
        } else {
            std::cerr << "Error: Key " << key << " not found or value mismatch!" << std::endl;
        }
    }
    std::cout << "Successfully retrieved " << found_count << "/" << TEST_COUNT << " records." << std::endl;

    std::cout << "\n--- Phase 3: Range Scan (Sibling Linking) ---" << std::endl;
    // Scanning a small range in the middle
    std::string start = "key0490"; // Matches "key" + (490 + 10000).substr(1)
    std::string end   = "key0510"; // Matches "key" + (510 + 10000).substr(1)
    auto results = db.rangeScan(start, end);

    std::cout << "Range scan results for [" << start << " to " << end << "]:" << std::endl;
    for (const auto& pair : results) {
        std::cout << "  " << pair.first << " => " << pair.second << std::endl;
    }

    // Validation: Check if rangeScan returned the expected number of items (21)
    if (results.size() == 21) {
        std::cout << "SUCCESS: Range scan returned correct number of items." << std::endl;
    } else {
        std::cout << "FAILURE: Range scan returned " << results.size() << " items instead of 21." << std::endl;
    }

    std::cout << "\n--- Phase 4: Non-Existent Key Test ---" << std::endl;
    auto missing = db.get("key99999");
    if (!missing) {
        std::cout << "SUCCESS: Non-existent key correctly returned nullopt." << std::endl;
    }

    std::cout << "\n--- Phase 5: Deletion Test ---" << std::endl;
    // Delete even-numbered keys
    int delete_count = 0;
    for (int i = 2; i <= TEST_COUNT; i += 2) {
        std::string key = "key" + std::to_string(i + 10000).substr(1);
        if (db.remove(key)) delete_count++;
    }
    std::cout << "Deleted " << delete_count << " records." << std::endl;

    // Verify: Odd should exist, Even should be nullopt
    bool delete_success = true;
    for (int i = 1; i <= TEST_COUNT; ++i) {
        std::string key = "key" + std::to_string(i + 10000).substr(1);
        auto result = db.get(key);
        if (i % 2 == 0) {
            if (result) delete_success = false; // Should be gone
        } else {
            if (!result) delete_success = false; // Should still be there
        }
    }
    std::cout << (delete_success ? "SUCCESS: Deletion verified." : "FAILURE: Deletion state inconsistent.") << std::endl;

    return 0;
}