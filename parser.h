#pragma once
#include "types.h"
#include <sstream>
#include <algorithm>
#include <stdexcept>

// Parsed representations of each command

struct CreateTableCmd {
    std::string tableName;
    std::vector<Column> columns;
};

struct InsertCmd {
    std::string tableName;
    std::vector<std::string> values;
};

struct SelectCmd {
    std::string tableName;
    std::vector<std::string> columns; // empty = SELECT *
    std::optional<Condition> where;
    std::optional<std::string> orderBy;
    bool orderDesc = false;
};

struct DeleteCmd {
    std::string tableName;
    std::optional<Condition> where;
};

struct DropTableCmd {
    std::string tableName;
};

struct ShowTablesCmd {};
struct DescribeCmd { std::string tableName; };

using ParsedCommand = std::variant<
    CreateTableCmd,
    InsertCmd,
    SelectCmd,
    DeleteCmd,
    DropTableCmd,
    ShowTablesCmd,
    DescribeCmd
>;

// ─── Tokenizer ────────────────────────────────────────────────────────────────

class Tokenizer {
public:
    explicit Tokenizer(const std::string& input) : input_(input), pos_(0) {}

    std::vector<std::string> tokenize() {
        std::vector<std::string> tokens;
        while (pos_ < input_.size()) {
            skipWhitespace();
            if (pos_ >= input_.size()) break;

            char c = input_[pos_];
            if (c == '\'') {
                tokens.push_back(readString());
            } else if (c == '(' || c == ')' || c == ',' || c == ';') {
                tokens.push_back(std::string(1, c));
                ++pos_;
            } else if (c == '<' || c == '>' || c == '!' || c == '=') {
                tokens.push_back(readOp());
            } else {
                tokens.push_back(readWord());
            }
        }
        return tokens;
    }

private:
    std::string input_;
    size_t pos_;

    void skipWhitespace() {
        while (pos_ < input_.size() && std::isspace(input_[pos_])) ++pos_;
    }

    std::string readWord() {
        size_t start = pos_;
        while (pos_ < input_.size() && !std::isspace(input_[pos_]) &&
               input_[pos_] != ',' && input_[pos_] != '(' &&
               input_[pos_] != ')' && input_[pos_] != '\'' &&
               input_[pos_] != ';') {
            ++pos_;
        }
        return input_.substr(start, pos_ - start);
    }

    std::string readString() {
        ++pos_; // skip opening '
        std::string s;
        while (pos_ < input_.size() && input_[pos_] != '\'') {
            if (input_[pos_] == '\\' && pos_ + 1 < input_.size()) {
                ++pos_;
                s += input_[pos_++];
            } else {
                s += input_[pos_++];
            }
        }
        ++pos_; // skip closing '
        return s;
    }

    std::string readOp() {
        std::string op(1, input_[pos_++]);
        if (pos_ < input_.size() && input_[pos_] == '=') {
            op += input_[pos_++];
        }
        return op;
    }
};

// ─── Parser ───────────────────────────────────────────────────────────────────

class Parser {
public:
    ParsedCommand parse(const std::string& query) {
        Tokenizer tok(query);
        tokens_ = tok.tokenize();
        pos_ = 0;

        std::string cmd = upperToken(peek());
        advance();

        if (cmd == "CREATE") return parseCreate();
        if (cmd == "INSERT") return parseInsert();
        if (cmd == "SELECT") return parseSelect();
        if (cmd == "DELETE") return parseDelete();
        if (cmd == "DROP")   return parseDrop();
        if (cmd == "SHOW")   return parseShow();
        if (cmd == "DESCRIBE" || cmd == "DESC") return parseDescribe();

        throw std::runtime_error("Unknown command: " + cmd);
    }

private:
    std::vector<std::string> tokens_;
    size_t pos_;

    std::string peek(int offset = 0) {
        size_t idx = pos_ + offset;
        return idx < tokens_.size() ? tokens_[idx] : "";
    }

    std::string advance() {
        return pos_ < tokens_.size() ? tokens_[pos_++] : "";
    }

    std::string expect(const std::string& val) {
        std::string t = advance();
        if (upperToken(t) != upperToken(val))
            throw std::runtime_error("Expected '" + val + "', got '" + t + "'");
        return t;
    }

    std::string upperToken(const std::string& s) {
        std::string u = s;
        std::transform(u.begin(), u.end(), u.begin(), ::toupper);
        return u;
    }

    // CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
    CreateTableCmd parseCreate() {
        expect("TABLE");
        CreateTableCmd cmd;
        cmd.tableName = advance();
        expect("(");
        while (peek() != ")" && pos_ < tokens_.size()) {
            Column col;
            col.name = advance();
            col.type = upperToken(advance());
            if (col.type != "INT" && col.type != "TEXT")
                throw std::runtime_error("Unknown type: " + col.type + ". Use INT or TEXT.");
            cmd.columns.push_back(col);
            if (peek() == ",") advance();
        }
        expect(")");
        return cmd;
    }

    // INSERT INTO name VALUES (v1, v2, ...)
    InsertCmd parseInsert() {
        expect("INTO");
        InsertCmd cmd;
        cmd.tableName = advance();
        expect("VALUES");
        expect("(");
        while (peek() != ")" && pos_ < tokens_.size()) {
            cmd.values.push_back(advance());
            if (peek() == ",") advance();
        }
        expect(")");
        return cmd;
    }

    // SELECT col1, col2 FROM name [WHERE ...] [ORDER BY col [DESC]]
    SelectCmd parseSelect() {
        SelectCmd cmd;
        if (peek() == "*") {
            advance();
        } else {
            while (upperToken(peek()) != "FROM" && pos_ < tokens_.size()) {
                cmd.columns.push_back(advance());
                if (peek() == ",") advance();
            }
        }
        expect("FROM");
        cmd.tableName = advance();

        if (upperToken(peek()) == "WHERE") {
            advance();
            cmd.where = parseCondition();
        }
        if (upperToken(peek()) == "ORDER") {
            advance();
            expect("BY");
            cmd.orderBy = advance();
            if (upperToken(peek()) == "DESC") { advance(); cmd.orderDesc = true; }
            else if (upperToken(peek()) == "ASC") { advance(); }
        }
        return cmd;
    }

    // DELETE FROM name [WHERE ...]
    DeleteCmd parseDelete() {
        expect("FROM");
        DeleteCmd cmd;
        cmd.tableName = advance();
        if (upperToken(peek()) == "WHERE") {
            advance();
            cmd.where = parseCondition();
        }
        return cmd;
    }

    DropTableCmd parseDrop() {
        expect("TABLE");
        DropTableCmd cmd;
        cmd.tableName = advance();
        return cmd;
    }

    ShowTablesCmd parseShow() {
        expect("TABLES");
        return ShowTablesCmd{};
    }

    DescribeCmd parseDescribe() {
        DescribeCmd cmd;
        cmd.tableName = advance();
        return cmd;
    }

    Condition parseCondition() {
        Condition c;
        c.column = advance();
        c.op     = advance();
        c.value  = advance();
        return c;
    }
};
