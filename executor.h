#pragma once
#include "types.h"
#include "storage.h"
#include "parser.h"
#include <map>
#include <algorithm>
#include <stdexcept>
#include <iomanip>
#include <sstream>

// ─── In-memory index: column_name -> sorted map of value -> row_indices ───────
using Index = std::map<std::string, std::map<std::string, std::vector<size_t>>>;

class Executor {
public:
    explicit Executor(const std::string& dataDir = "./data")
        : storage_(dataDir) {}

    QueryResult execute(const std::string& rawQuery) {
        // Strip trailing semicolon/whitespace
        std::string q = rawQuery;
        while (!q.empty() && (q.back() == ';' || std::isspace(q.back()))) q.pop_back();
        if (q.empty()) return {true, "", {}, {}};

        try {
            Parser parser;
            ParsedCommand cmd = parser.parse(q);
            return std::visit([this](auto&& c) { return run(c); }, cmd);
        } catch (const std::exception& e) {
            return {false, std::string("Error: ") + e.what(), {}, {}};
        }
    }

private:
    StorageEngine storage_;
    // Per-table in-memory index cache
    std::map<std::string, Index> indexCache_;

    // ── CREATE TABLE ─────────────────────────────────────────────────────────
    QueryResult run(const CreateTableCmd& cmd) {
        if (storage_.tableExists(cmd.tableName))
            return {false, "Table '" + cmd.tableName + "' already exists.", {}, {}};
        Schema schema{cmd.tableName, cmd.columns};
        storage_.saveSchema(schema);
        buildIndex(cmd.tableName);
        return {true, "Table '" + cmd.tableName + "' created with " +
                std::to_string(cmd.columns.size()) + " column(s).", {}, {}};
    }

    // ── INSERT ────────────────────────────────────────────────────────────────
    QueryResult run(const InsertCmd& cmd) {
        auto schema = requireSchema(cmd.tableName);
        if (cmd.values.size() != schema.columns.size())
            return {false, "Column count mismatch: expected " +
                std::to_string(schema.columns.size()) + ", got " +
                std::to_string(cmd.values.size()), {}, {}};

        Row row;
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            if (schema.columns[i].type == "INT") {
                try { row.push_back(std::stoi(cmd.values[i])); }
                catch (...) { return {false, "Column '" + schema.columns[i].name +
                    "' expects INT, got '" + cmd.values[i] + "'", {}, {}}; }
            } else {
                row.push_back(cmd.values[i]);
            }
        }

        storage_.appendRow(cmd.tableName, row);
        invalidateIndex(cmd.tableName);
        return {true, "1 row inserted into '" + cmd.tableName + "'.", {}, {}};
    }

    // ── SELECT ────────────────────────────────────────────────────────────────
    QueryResult run(const SelectCmd& cmd) {
        auto schema = requireSchema(cmd.tableName);
        auto allRows = storage_.loadRows(cmd.tableName, schema);

        // Filter
        std::vector<Row> filtered;
        for (const auto& row : allRows) {
            if (!cmd.where || matchesCondition(row, schema, *cmd.where))
                filtered.push_back(row);
        }

        // ORDER BY
        if (cmd.orderBy) {
            int colIdx = colIndex(schema, *cmd.orderBy);
            if (colIdx < 0) return {false, "Unknown column: " + *cmd.orderBy, {}, {}};
            bool desc = cmd.orderDesc;
            std::stable_sort(filtered.begin(), filtered.end(),
                [&](const Row& a, const Row& b) {
                    auto& av = a[colIdx]; auto& bv = b[colIdx];
                    if (std::holds_alternative<int>(av) && std::holds_alternative<int>(bv))
                        return desc ? std::get<int>(av) > std::get<int>(bv)
                                    : std::get<int>(av) < std::get<int>(bv);
                    return desc ? valueToString(av) > valueToString(bv)
                                : valueToString(av) < valueToString(bv);
                });
        }

        // Project columns
        std::vector<int> colIndices;
        std::vector<std::string> headers;
        if (cmd.columns.empty()) {
            for (size_t i = 0; i < schema.columns.size(); ++i) {
                colIndices.push_back((int)i);
                headers.push_back(schema.columns[i].name);
            }
        } else {
            for (const auto& col : cmd.columns) {
                int idx = colIndex(schema, col);
                if (idx < 0) return {false, "Unknown column: " + col, {}, {}};
                colIndices.push_back(idx);
                headers.push_back(col);
            }
        }

        std::vector<Row> result;
        for (const auto& row : filtered) {
            Row projected;
            for (int idx : colIndices) projected.push_back(row[idx]);
            result.push_back(projected);
        }

        return {true, std::to_string(result.size()) + " row(s) found.", headers, result};
    }

    // ── DELETE ────────────────────────────────────────────────────────────────
    QueryResult run(const DeleteCmd& cmd) {
        auto schema = requireSchema(cmd.tableName);
        auto allRows = storage_.loadRows(cmd.tableName, schema);

        std::vector<Row> keep;
        int deleted = 0;
        for (const auto& row : allRows) {
            if (cmd.where && matchesCondition(row, schema, *cmd.where)) {
                ++deleted;
            } else {
                keep.push_back(row);
            }
        }

        storage_.saveAllRows(cmd.tableName, keep, schema);
        invalidateIndex(cmd.tableName);
        return {true, std::to_string(deleted) + " row(s) deleted from '" + cmd.tableName + "'.", {}, {}};
    }

    // ── DROP TABLE ────────────────────────────────────────────────────────────
    QueryResult run(const DropTableCmd& cmd) {
        if (!storage_.tableExists(cmd.tableName))
            return {false, "Table '" + cmd.tableName + "' does not exist.", {}, {}};
        storage_.dropTable(cmd.tableName);
        indexCache_.erase(cmd.tableName);
        return {true, "Table '" + cmd.tableName + "' dropped.", {}, {}};
    }

    // ── SHOW TABLES ───────────────────────────────────────────────────────────
    QueryResult run(const ShowTablesCmd&) {
        auto tables = storage_.listTables();
        std::vector<Row> rows;
        for (const auto& t : tables) rows.push_back({t});
        return {true, std::to_string(tables.size()) + " table(s).", {"table_name"}, rows};
    }

    // ── DESCRIBE ──────────────────────────────────────────────────────────────
    QueryResult run(const DescribeCmd& cmd) {
        auto schema = requireSchema(cmd.tableName);
        std::vector<Row> rows;
        for (size_t i = 0; i < schema.columns.size(); ++i) {
            rows.push_back({
                std::to_string(i + 1),
                schema.columns[i].name,
                schema.columns[i].type
            });
        }
        return {true, "", {"#", "column", "type"}, rows};
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    Schema requireSchema(const std::string& tableName) {
        auto s = storage_.loadSchema(tableName);
        if (!s) throw std::runtime_error("Table '" + tableName + "' does not exist.");
        return *s;
    }

    int colIndex(const Schema& schema, const std::string& name) {
        for (size_t i = 0; i < schema.columns.size(); ++i)
            if (schema.columns[i].name == name) return (int)i;
        return -1;
    }

    bool matchesCondition(const Row& row, const Schema& schema, const Condition& cond) {
        int idx = colIndex(schema, cond.column);
        if (idx < 0) return false;

        const Value& cellVal = row[idx];
        std::string cellStr = valueToString(cellVal);

        // Try numeric comparison first
        bool bothNumeric = false;
        int cellInt = 0, condInt = 0;
        try {
            cellInt = std::get<int>(cellVal);
            condInt = std::stoi(cond.value);
            bothNumeric = true;
        } catch (...) {}

        if (bothNumeric) {
            if (cond.op == "=")  return cellInt == condInt;
            if (cond.op == "!=") return cellInt != condInt;
            if (cond.op == "<")  return cellInt <  condInt;
            if (cond.op == ">")  return cellInt >  condInt;
            if (cond.op == "<=") return cellInt <= condInt;
            if (cond.op == ">=") return cellInt >= condInt;
        } else {
            if (cond.op == "=")  return cellStr == cond.value;
            if (cond.op == "!=") return cellStr != cond.value;
            if (cond.op == "<")  return cellStr <  cond.value;
            if (cond.op == ">")  return cellStr >  cond.value;
            if (cond.op == "<=") return cellStr <= cond.value;
            if (cond.op == ">=") return cellStr >= cond.value;
        }
        return false;
    }

    // Simple index build (for future use / fast lookups)
    void buildIndex(const std::string& tableName) {
        auto schema = storage_.loadSchema(tableName);
        if (!schema) return;
        auto rows = storage_.loadRows(tableName, *schema);
        Index idx;
        for (size_t r = 0; r < rows.size(); ++r) {
            for (size_t c = 0; c < schema->columns.size(); ++c) {
                idx[schema->columns[c].name][valueToString(rows[r][c])].push_back(r);
            }
        }
        indexCache_[tableName] = std::move(idx);
    }

    void invalidateIndex(const std::string& tableName) {
        indexCache_.erase(tableName);
    }
};
