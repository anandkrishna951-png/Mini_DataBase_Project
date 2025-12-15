#include "executor.h"
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>

// ─── Pretty-print a query result as an ASCII table ────────────────────────────

void printResult(const QueryResult& result) {
    if (!result.success) {
        std::cerr << "\033[1;31m" << result.message << "\033[0m\n";
        return;
    }

    if (!result.headers.empty()) {
        // Compute column widths
        std::vector<size_t> widths(result.headers.size());
        for (size_t i = 0; i < result.headers.size(); ++i)
            widths[i] = result.headers[i].size();

        for (const auto& row : result.rows)
            for (size_t i = 0; i < row.size() && i < widths.size(); ++i)
                widths[i] = std::max(widths[i], valueToString(row[i]).size());

        // Top border
        std::cout << "+";
        for (size_t w : widths) std::cout << std::string(w + 2, '-') << "+";
        std::cout << "\n";

        // Header row
        std::cout << "|";
        for (size_t i = 0; i < result.headers.size(); ++i)
            std::cout << " \033[1m" << std::left << std::setw((int)widths[i])
                      << result.headers[i] << "\033[0m |";
        std::cout << "\n";

        // Header separator
        std::cout << "+";
        for (size_t w : widths) std::cout << std::string(w + 2, '=') << "+";
        std::cout << "\n";

        // Data rows
        for (const auto& row : result.rows) {
            std::cout << "|";
            for (size_t i = 0; i < row.size() && i < widths.size(); ++i)
                std::cout << " " << std::left << std::setw((int)widths[i])
                          << valueToString(row[i]) << " |";
            std::cout << "\n";
        }

        // Bottom border
        std::cout << "+";
        for (size_t w : widths) std::cout << std::string(w + 2, '-') << "+";
        std::cout << "\n";
    }

    if (!result.message.empty())
        std::cout << "\033[1;32m" << result.message << "\033[0m\n";
}

// ─── Print help ───────────────────────────────────────────────────────────────

void printHelp() {
    std::cout << R"(
  MiniDB — supported commands
  ─────────────────────────────────────────────────────────────────
  CREATE TABLE name (col1 INT, col2 TEXT, ...)
  INSERT INTO name VALUES (val1, 'val2', ...)
  SELECT * FROM name [WHERE col op val] [ORDER BY col [DESC]]
  SELECT col1, col2 FROM name [WHERE col op val]
  DELETE FROM name [WHERE col op val]
  DROP TABLE name
  SHOW TABLES
  DESCRIBE name
  ─────────────────────────────────────────────────────────────────
  Operators: =  !=  <  >  <=  >=
  Types:     INT   TEXT
  Commands are case-insensitive.  String values use single quotes.
  ─────────────────────────────────────────────────────────────────
  Type .help for this menu, .quit to exit, .clear to clear screen.

)";
}

// ─── Multi-line input: collect until ; or blank continuation ──────────────────

std::string readQuery(bool multiline) {
    std::string line, full;
    while (true) {
        if (full.empty())
            std::cout << "\033[1;34mminidb>\033[0m ";
        else
            std::cout << "     -> ";

        if (!std::getline(std::cin, line)) return ".quit";

        // Trim
        auto start = line.find_first_not_of(" \t\r");
        auto end   = line.find_last_not_of(" \t\r");
        if (start == std::string::npos) {
            if (!full.empty()) break;
            continue;
        }
        line = line.substr(start, end - start + 1);

        if (!full.empty()) full += " ";
        full += line;

        if (!multiline) break;
        if (full.back() == ';' || full.back() == ')') break;
    }
    return full;
}

// ─── REPL ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    std::string dataDir = (argc > 1) ? argv[1] : "./data";
    Executor executor(dataDir);

    std::cout << "\033[1;36m";
    std::cout << " ╔══════════════════════════════╗\n";
    std::cout << " ║   MiniDB v1.0  — SQL Engine  ║\n";
    std::cout << " ╚══════════════════════════════╝\n";
    std::cout << "\033[0m";
    std::cout << " Data directory: " << dataDir << "\n";
    std::cout << " Type .help for commands, .quit to exit.\n\n";

    while (true) {
        std::string query = readQuery(true);

        if (query.empty()) continue;

        // Meta-commands
        if (query == ".quit" || query == ".exit") {
            std::cout << "Bye!\n";
            break;
        }
        if (query == ".help") { printHelp(); continue; }
        if (query == ".clear") { std::cout << "\033[2J\033[H"; continue; }

        auto result = executor.execute(query);
        printResult(result);
        std::cout << "\n";
    }

    return 0;
}
