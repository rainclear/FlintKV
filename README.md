# FlintKV ☄️

**FlintKV** is a high-performance, disk-persistent Key-Value (KV) storage engine implemented in C++. It utilizes a custom **B+ Tree** index and a **Buffer Pool Manager** to provide efficient data storage and retrieval with a minimal memory footprint.

The name "Flint" represents the engine's goal: being small, sparky, and an essential tool for building larger storage systems.

## 🚀 Features

* **Disk-Based Persistence:** All data is serialized to a binary file (`db.bin`), ensuring data survives application restarts.
* **B+ Tree Indexing:** Optimized for both point lookups ($O(\log n)$) and high-speed range scans.
* **Buffer Pool Management:** Implements an in-memory page cache to minimize expensive disk I/O operations.
* **Slotted-Page Architecture:** Manages variable-length records within fixed-size 4KB pages to maximize space utilization.
* **Horizontal Leaf Linking:** Supports efficient range queries by traversing sibling pointers at the leaf level.
* **Lazy Deletion:** Supports record removal with automated page defragmentation to reclaim space.


## ⚠️ Current Limitations

* **Fixed Internal Key Length:** Keys in internal nodes are capped at **15 characters** to optimize traversal speed through fixed-length memory alignment.
* **Maximum Record Size:** Total record size (Key + Value) cannot exceed **~4KB**.
    * **Note:** FlintKV currently does not support "Overflow Pages." If you need to store large blobs (images, large text), it is recommended to store the file path as the value and keep the actual data on the external filesystem.
* **Single Threaded:** The engine does not currently implement latches or locks; it is intended for single-threaded usage.


---

## 🛠 Architecture

### 1. Storage Layout
FlintKV organizes data into fixed-size **4096-byte pages**.
- **Metadata Page (Page 0):** Stores the current `root_id` and engine state.
- **Internal Nodes:** Act as separators/routers, guiding the search to the correct leaf.
- **Leaf Nodes:** Store actual KV pairs. Each leaf maintains a `next_sibling` ID, creating a linked list for range scans.

### 2. Slotted Pages
To handle variable-length keys and values without fragmentation, each page uses a **Slotted-Page** design. Headers and slots grow from the top down, while actual record data grows from the bottom up.



### 3. Buffer Pool Manager
The Buffer Pool caches pages in a `std::map`. When a page is modified, it is marked as "dirty" and eventually flushed back to the physical disk. This allows the engine to handle datasets much larger than the available RAM.

---

## 💻 Getting Started

### Prerequisites
- A C++17 compatible compiler (GCC 7+, Clang 5+, or MSVC 2017+).

### Compilation
Compile the engine along with the provided test suite:

```bash
g++ main.cpp -o flint_test
```

```c++
#include "BPlusTree.h"

int main() {
    BPlusTree db; // Initializes or opens db.bin

    // Insert a record
    db.put("user_1", "Alice");

    // Retrieve a record
    auto val = db.get("user_1");
    if (val) std::cout << "Found: " << *val << std::endl;

    // Perform a Range Scan
    auto results = db.rangeScan("user_1", "user_9");

    // Delete a record
    db.remove("user_1");

    return 0;
}
```

