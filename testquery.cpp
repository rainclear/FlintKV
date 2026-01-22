#include "QueryBuilder.h"

int main() {
    BPlusTree db;
    QueryBuilder query(db);

    // Get the top 3 users whose IDs start with 'u'
    auto top_users = query
        .range("u", "v")
        .where([](const std::string& k, const std::string& v) {
            return v.length() > 0; // Ensure value isn't empty
        })
        .desc()   // Z-A order
        .limit(3) // Only give me the first 3
        .execute();

    for (auto& user : top_users) {
        std::cout << user.first << " -> " << user.second << std::endl;
    }

    std::cout << "Test Query Done." << std::endl;

    return 0;
}