#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <iostream>
#include <vector>
#include <string>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <algorithm>
#include <cstdint>
#include <functional>

// Reserved marker for deletions in an LSM-style system
const std::string TOMBSTONE = "<<TOMBSTONE_MARKER>>";

class SkipNode {
public:
    std::string key;
    std::string value;
    std::vector<SkipNode*> next;

    SkipNode(std::string k, std::string v, int level) 
        : key(k), value(v), next(level + 1, nullptr) {}
};

class SkipList {
private:
    int max_level;
    float probability;
    int current_level;
    SkipNode* head;
    size_t element_count;

    int randomLevel() {
        int lvl = 0;
        while ((float)std::rand() / RAND_MAX < probability && lvl < max_level - 1) {
            lvl++;
        }
        return lvl;
    }

public:
    // Initializing with your preferred max_level of 24
    SkipList(int max_lvl = 24, float p = 0.5) 
        : max_level(max_lvl), probability(p), current_level(0), element_count(0) {
        std::srand(std::time(0));
        head = new SkipNode("", "", max_level);
    }

    ~SkipList() {
        // Simple teardown (in production, use smart pointers or recursive delete)
        SkipNode* curr = head->next[0];
        while (curr) {
            SkipNode* temp = curr;
            curr = curr->next[0];
            delete temp;
        }
        delete head;
    }

    void put(std::string key, std::string value) {
        std::vector<SkipNode*> update(max_level, nullptr);
        SkipNode* curr = head;

        for (int i = current_level; i >= 0; i--) {
            while (curr->next[i] != nullptr && curr->next[i]->key < key) {
                curr = curr->next[i];
            }
            update[i] = curr;
        }

        curr = curr->next[0];

        if (curr != nullptr && curr->key == key) {
            curr->value = value;
        } else {
            int rLevel = randomLevel();
            if (rLevel > current_level) {
                for (int i = current_level + 1; i <= rLevel; i++) {
                    update[i] = head;
                }
                current_level = rLevel;
            }

            SkipNode* newNode = new SkipNode(key, value, rLevel);
            for (int i = 0; i <= rLevel; i++) {
                newNode->next[i] = update[i]->next[i];
                update[i]->next[i] = newNode;
            }
            element_count++;
        }
    }

    void remove(std::string key) {
        put(key, TOMBSTONE);
    }

    std::string get(std::string key) {
        SkipNode* curr = head;
        for (int i = current_level; i >= 0; i--) {
            while (curr->next[i] != nullptr && curr->next[i]->key < key) {
                curr = curr->next[i];
            }
        }
        curr = curr->next[0];
        if (curr && curr->key == key) {
            return (curr->value == TOMBSTONE) ? "Not Found" : curr->value;
        }
        return "Not Found";
    }

    void flush(const std::string& filename) {
        std::ofstream out(filename, std::ios::binary);
        SkipNode* curr = head->next[0];
        while (curr) {
            uint16_t kLen = curr->key.length();
            uint16_t vLen = curr->value.length();
            out.write((char*)&kLen, sizeof(kLen));
            out.write(curr->key.data(), kLen);
            out.write((char*)&vLen, sizeof(vLen));
            out.write(curr->value.data(), vLen);
            curr = curr->next[0];
        }
        out.close();
    }

    size_t size() const { return element_count; }

    // Standalone Static Helper for Disk-to-Disk Streaming Compaction
    static void compactFiles(const std::string& fileOld, const std::string& fileNewer, const std::string& fileOut) {
        std::ifstream inOld(fileOld, std::ios::binary);
        std::ifstream inNewer(fileNewer, std::ios::binary);
        std::ofstream out(fileOut, std::ios::binary);

        auto readNext = [](std::ifstream& in, std::string& k, std::string& v) -> bool {
            uint16_t kLen, vLen;
            if (!in.read((char*)&kLen, sizeof(kLen))) return false;
            k.resize(kLen); in.read(&k[0], kLen);
            if (!in.read((char*)&vLen, sizeof(vLen))) return false;
            v.resize(vLen); in.read(&v[0], vLen);
            return true;
        };

        std::string kOld, vOld, kNewer, vNewer;
        bool hasOld = readNext(inOld, kOld, vOld);
        bool hasNewer = readNext(inNewer, kNewer, vNewer);

        while (hasOld || hasNewer) {
            bool useNewer = false;
            if (hasOld && hasNewer) {
                if (kNewer <= kOld) useNewer = true;
            } else if (hasNewer) {
                useNewer = true;
            }

            if (useNewer) {
                if (vNewer != TOMBSTONE) {
                    uint16_t kL = kNewer.length(), vL = vNewer.length();
                    out.write((char*)&kL, sizeof(kL)); out.write(kNewer.data(), kL);
                    out.write((char*)&vL, sizeof(vL)); out.write(vNewer.data(), vL);
                }
                if (hasOld && kOld == kNewer) hasOld = readNext(inOld, kOld, vOld); // Deduplicate
                hasNewer = readNext(inNewer, kNewer, vNewer);
            } else {
                if (vOld != TOMBSTONE) {
                    uint16_t kL = kOld.length(), vL = vOld.length();
                    out.write((char*)&kL, sizeof(kL)); out.write(kOld.data(), kL);
                    out.write((char*)&vL, sizeof(vL)); out.write(vOld.data(), vL);
                }
                hasOld = readNext(inOld, kOld, vOld);
            }
        }
    }

    std::vector<std::pair<std::string, std::string>> rangeScan(std::string start, std::string end) {
        std::vector<std::pair<std::string, std::string>> results;
        SkipNode* curr = head;

        // 1. Drop down to the start position (standard Skip List search)
        for (int i = current_level; i >= 0; i--) {
            while (curr->next[i] != nullptr && curr->next[i]->key < start) {
                curr = curr->next[i];
            }
        }
        curr = curr->next[0]; // Move to the first actual node >= start

        // 2. Linear scan along Level 0 until we hit the end key
        while (curr != nullptr && curr->key <= end) {
            if (curr->value != TOMBSTONE) {
                results.push_back({curr->key, curr->value});
            }
            curr = curr->next[0];
        }
        return results;
    }    
};

class FlintQuery {
private:
    SkipList& db;
    std::string start_key = "";
    std::string end_key = "\xff";
    int limit_val = -1;
    std::vector<std::function<bool(const std::string&, const std::string&)>> filters;

public:
    FlintQuery(SkipList& database) : db(database) {}

    FlintQuery& select(std::string start, std::string end) {
        start_key = start;
        end_key = end;
        return *this;
    }

    FlintQuery& where(std::function<bool(const std::string&, const std::string&)> predicate) {
        filters.push_back(predicate);
        return *this;
    }

    FlintQuery& limit(int n) {
        limit_val = n;
        return *this;
    }

    std::vector<std::pair<std::string, std::string>> execute() {
        auto raw = db.rangeScan(start_key, end_key);
        std::vector<std::pair<std::string, std::string>> final_results;

        for (auto& pair : raw) {
            if (limit_val >= 0 && (int)final_results.size() >= limit_val) break;

            bool match = true;
            for (auto& f : filters) {
                if (!f(pair.first, pair.second)) {
                    match = false;
                    break;
                }
            }
            if (match) final_results.push_back(pair);
        }
        return final_results;
    }
};

#include <map>

// A simple structure to represent our joined "Row"
struct JoinedResult {
    std::string key;
    std::string user_info;
    std::string order_info;
};

std::vector<JoinedResult> joinDicts(SkipList& users, SkipList& orders, std::string start_id, std::string end_id) {
    std::vector<JoinedResult> final_report;

    // 1. Get a range of users
    auto user_list = users.rangeScan(start_id, end_id);

    // 2. For each user, "Probe" the orders dictionary
    for (auto& user_pair : user_list) {
        std::string order_data = orders.get(user_pair.first); // O(log n) lookup
        
        if (order_data != "Not Found") {
            final_report.push_back({
                user_pair.first, 
                user_pair.second, 
                order_data
            });
        }
    }
    return final_report;
}

#endif // SKIPLIST_H