#pragma once
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <variant>
#include <optional>

// A cell value can be a string or integer
using Value = std::variant<int, std::string>;

// A row is an ordered list of values
using Row = std::vector<Value>;

// Column definition
struct Column {
    std::string name;
    std::string type; // "INT" or "TEXT"
};

// Schema for a table
struct Schema {
    std::string tableName;
    std::vector<Column> columns;
};

// Result of a query
struct QueryResult {
    bool success;
    std::string message;
    std::vector<std::string> headers;
    std::vector<Row> rows;
};

// WHERE condition
struct Condition {
    std::string column;
    std::string op;   // =, !=, <, >, <=, >=
    std::string value;
};

// Utility: convert Value to string
inline std::string valueToString(const Value& v) {
    return std::visit([](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, int>)
            return std::to_string(arg);
        else
            return arg;
    }, v);
}
