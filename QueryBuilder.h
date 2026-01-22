#ifndef QUERY_BUILDER_H
#define QUERY_BUILDER_H

#include "BPlusTree.h"
#include <functional>
#include <algorithm>

class QueryBuilder {
private:
    BPlusTree& db;
    std::string start_key = "";
    std::string end_key = "\xff";
    int limit_val = -1; // -1 means no limit
    bool sort_descending = false;
    std::vector<std::function<bool(const std::string&, const std::string&)>> filters;

public:
    QueryBuilder(BPlusTree& database) : db(database) {}

    QueryBuilder& range(const std::string& start, const std::string& end) {
        start_key = start;
        end_key = end;
        return *this;
    }

    QueryBuilder& where(std::function<bool(const std::string&, const std::string&)> predicate) {
        filters.push_back(predicate);
        return *this;
    }

    // New: Limit the number of results
    QueryBuilder& limit(int n) {
        limit_val = n;
        return *this;
    }

    // New: Reverse the order
    QueryBuilder& desc() {
        sort_descending = true;
        return *this;
    }

    std::vector<std::pair<std::string, std::string>> execute() {
        auto results = db.rangeScan(start_key, end_key);
        
        // 1. Filter
        if (!filters.empty()) {
            std::vector<std::pair<std::string, std::string>> filtered;
            for (const auto& pair : results) {
                bool match = true;
                for (auto& f : filters) if (!f(pair.first, pair.second)) { match = false; break; }
                if (match) filtered.push_back(pair);
            }
            results = std::move(filtered);
        }

        // 2. Sort (B+ Tree is already sorted ASC, so we only handle DESC)
        if (sort_descending) {
            std::reverse(results.begin(), results.end());
        }

        // 3. Limit
        if (limit_val >= 0 && limit_val < (int)results.size()) {
            results.resize(limit_val);
        }

        return results;
    }
};

#endif