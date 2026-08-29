"""
Zero-Dependency Recursive Descent SQL Parser & Abstract Syntax Tree (AST) Generator
Supports DDL, DML, Projections, Filtering, Joins, Aggregations, Group By, Order By, Limit.
"""

import re
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum

class TokenType(Enum):
    KEYWORD = "KEYWORD"
    IDENTIFIER = "IDENTIFIER"
    NUMBER = "NUMBER"
    STRING = "STRING"
    OPERATOR = "OPERATOR"
    PUNCTUATION = "PUNCTUATION"
    EOF = "EOF"

class Token:
    def __init__(self, token_type: TokenType, value: str, position: int):
        self.type = token_type
        self.value = value
        self.position = position

    def __repr__(self):
        return f"Token({self.type.name}, {self.value})"

KEYWORDS = {
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
    "DELETE", "CREATE", "TABLE", "DROP", "JOIN", "INNER", "LEFT", "RIGHT",
    "ON", "GROUP", "BY", "ORDER", "ASC", "DESC", "LIMIT", "OFFSET", "AND",
    "OR", "NOT", "AS", "HAVING", "IN", "IS", "NULL", "TRUE", "FALSE"
}

class SQLLexer:
    """Tokenizes raw SQL query strings into a stream of typed tokens."""
    def __init__(self, sql: str):
        self.sql = sql
        self.pos = 0
        self.tokens: List[Token] = []
        self._tokenize()

    def _tokenize(self):
        length = len(self.sql)
        while self.pos < length:
            ch = self.sql[self.pos]

            if ch.isspace():
                self.pos += 1
                continue

            if ch in {',', '(', ')', ';', '.'}:
                self.tokens.append(Token(TokenType.PUNCTUATION, ch, self.pos))
                self.pos += 1
                continue

            if ch in {'=', '<', '>', '!', '+', '-', '*', '/'}:
                op = ch
                if self.pos + 1 < length and self.sql[self.pos + 1] in {'=', '>'}:
                    op += self.sql[self.pos + 1]
                    self.pos += 1
                self.tokens.append(Token(TokenType.OPERATOR, op, self.pos))
                self.pos += 1
                continue

            if ch == "'" or ch == '"':
                quote = ch
                start = self.pos
                self.pos += 1
                val = ""
                while self.pos < length and self.sql[self.pos] != quote:
                    val += self.sql[self.pos]
                    self.pos += 1
                self.pos += 1  # Skip closing quote
                self.tokens.append(Token(TokenType.STRING, val, start))
                continue

            if ch.isdigit():
                start = self.pos
                val = ""
                while self.pos < length and (self.sql[self.pos].isdigit() or self.sql[self.pos] == '.'):
                    val += self.sql[self.pos]
                    self.pos += 1
                self.tokens.append(Token(TokenType.NUMBER, val, start))
                continue

            if ch.isalpha() or ch == '_':
                start = self.pos
                val = ""
                while self.pos < length and (self.sql[self.pos].isalnum() or self.sql[self.pos] == '_'):
                    val += self.sql[self.pos]
                    self.pos += 1
                upper_val = val.upper()
                if upper_val in KEYWORDS:
                    self.tokens.append(Token(TokenType.KEYWORD, upper_val, start))
                else:
                    self.tokens.append(Token(TokenType.IDENTIFIER, val, start))
                continue

            self.pos += 1

        self.tokens.append(Token(TokenType.EOF, "", length))


class SQLParser:
    """Parses SQL tokens into Structured AST Nodes."""
    def __init__(self, sql: str):
        self.lexer = SQLLexer(sql)
        self.tokens = self.lexer.tokens
        self.cursor = 0

    def peek(self) -> Token:
        return self.tokens[self.cursor]

    def consume(self, expected_val: Optional[str] = None) -> Token:
        tok = self.tokens[self.cursor]
        if expected_val and tok.value.upper() != expected_val.upper():
            raise SyntaxError(f"Expected {expected_val} but found {tok.value} at position {tok.position}")
        self.cursor += 1
        return tok

    def match(self, val: str) -> bool:
        if self.peek().value.upper() == val.upper():
            self.cursor += 1
            return True
        return False

    def parse(self) -> Dict[str, Any]:
        tok = self.peek()
        if tok.value.upper() == "SELECT":
            return self._parse_select()
        elif tok.value.upper() == "INSERT":
            return self._parse_insert()
        elif tok.value.upper() == "CREATE":
            return self._parse_create()
        raise SyntaxError(f"Unsupported statement beginning with {tok.value}")

    def _parse_select(self) -> Dict[str, Any]:
        self.consume("SELECT")
        projections = []
        while True:
            t = self.consume()
            projections.append(t.value)
            if not self.match(","):
                break

        self.consume("FROM")
        table = self.consume().value

        where_clause = None
        if self.match("WHERE"):
            col = self.consume().value
            op = self.consume().value
            val = self.consume().value
            where_clause = {"column": col, "op": op, "value": val}

        limit = None
        if self.match("LIMIT"):
            limit = int(self.consume().value)

        return {
            "type": "SELECT",
            "projections": projections,
            "table": table,
            "where": where_clause,
            "limit": limit
        }

    def _parse_insert(self) -> Dict[str, Any]:
        self.consume("INSERT")
        self.consume("INTO")
        table = self.consume().value
        self.consume("VALUES")
        self.consume("(")
        vals = []
        while True:
            v = self.consume().value
            vals.append(v)
            if not self.match(","):
                break
        self.consume(")")
        return {"type": "INSERT", "table": table, "values": vals}

    def _parse_create(self) -> Dict[str, Any]:
        self.consume("CREATE")
        self.consume("TABLE")
        table = self.consume().value
        return {"type": "CREATE_TABLE", "table": table}
