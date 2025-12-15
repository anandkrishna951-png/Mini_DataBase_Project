#pragma once
#include "types.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <filesystem>

namespace fs = std::filesystem;

class StorageEngine {
public:
    explicit StorageEngine(const std::string& dataDir = "./data")
        : dataDir_(dataDir) {
        fs::create_directories(dataDir_);
    }

    // Save schema to <table>.schema
    bool saveSchema(const Schema& schema) {
        std::ofstream f(schemaPath(schema.tableName));
        if (!f) return false;
        f << schema.tableName << "\n";
        for (const auto& col : schema.columns)
            f << col.name << "," << col.type << "\n";
        return true;
    }

    // Load schema from <table>.schema
    std::optional<Schema> loadSchema(const std::string& tableName) {
        std::ifstream f(schemaPath(tableName));
        if (!f) return std::nullopt;
        Schema schema;
        std::string line;
        std::getline(f, schema.tableName);
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            auto comma = line.find(',');
            if (comma == std::string::npos) continue;
            Column col;
            col.name = line.substr(0, comma);
            col.type = line.substr(comma + 1);
            schema.columns.push_back(col);
        }
        return schema;
    }

    // Append a row to <table>.db
    bool appendRow(const std::string& tableName, const Row& row) {
        std::ofstream f(dataPath(tableName), std::ios::app);
        if (!f) return false;
        for (size_t i = 0; i < row.size(); ++i) {
            if (i > 0) f << "|";
            f << escape(valueToString(row[i]));
        }
        f << "\n";
        return true;
    }

    // Load all rows from <table>.db
    std::vector<Row> loadRows(const std::string& tableName, const Schema& schema) {
        std::vector<Row> rows;
        std::ifstream f(dataPath(tableName));
        if (!f) return rows;
        std::string line;
        while (std::getline(f, line)) {
            if (line.empty()) continue;
            Row row = parseLine(line, schema);
            rows.push_back(row);
        }
        return rows;
    }

    // Overwrite all rows (used by DELETE)
    bool saveAllRows(const std::string& tableName, const std::vector<Row>& rows, const Schema& schema) {
        std::ofstream f(dataPath(tableName), std::ios::trunc);
        if (!f) return false;
        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0) f << "|";
                f << escape(valueToString(row[i]));
            }
            f << "\n";
        }
        return true;
    }

    bool tableExists(const std::string& tableName) {
        return fs::exists(schemaPath(tableName));
    }

    // List all tables
    std::vector<std::string> listTables() {
        std::vector<std::string> tables;
        for (const auto& entry : fs::directory_iterator(dataDir_)) {
            if (entry.path().extension() == ".schema") {
                tables.push_back(entry.path().stem().string());
            }
        }
        return tables;
    }

    bool dropTable(const std::string& tableName) {
        bool ok = true;
        if (fs::exists(schemaPath(tableName))) ok &= fs::remove(schemaPath(tableName));
        if (fs::exists(dataPath(tableName)))   ok &= fs::remove(dataPath(tableName));
        return ok;
    }

private:
    std::string dataDir_;

    std::string schemaPath(const std::string& t) { return dataDir_ + "/" + t + ".schema"; }
    std::string dataPath(const std::string& t)   { return dataDir_ + "/" + t + ".db"; }

    // Escape pipe characters in values
    std::string escape(const std::string& s) {
        std::string out;
        for (char c : s) {
            if (c == '|') out += "\\|";
            else if (c == '\\') out += "\\\\";
            else out += c;
        }
        return out;
    }

    std::string unescape(const std::string& s) {
        std::string out;
        for (size_t i = 0; i < s.size(); ++i) {
            if (s[i] == '\\' && i + 1 < s.size()) {
                ++i;
                out += s[i];
            } else {
                out += s[i];
            }
        }
        return out;
    }

    Row parseLine(const std::string& line, const Schema& schema) {
        Row row;
        std::vector<std::string> tokens;
        std::string cur;
        for (size_t i = 0; i < line.size(); ++i) {
            if (line[i] == '\\' && i + 1 < line.size()) {
                cur += line[++i];
            } else if (line[i] == '|') {
                tokens.push_back(cur);
                cur.clear();
            } else {
                cur += line[i];
            }
        }
        tokens.push_back(cur);

        for (size_t i = 0; i < schema.columns.size() && i < tokens.size(); ++i) {
            const std::string& raw = tokens[i];
            if (schema.columns[i].type == "INT") {
                try { row.push_back(std::stoi(raw)); }
                catch (...) { row.push_back(0); }
            } else {
                row.push_back(raw);
            }
        }
        return row;
    }
};
