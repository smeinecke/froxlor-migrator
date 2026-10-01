from __future__ import annotations

from typing import Any

import pymysql


def _connect(connect_kwargs: dict[str, Any], database: str) -> pymysql.connections.Connection:
    kwargs = dict(connect_kwargs)
    kwargs["database"] = database
    kwargs["charset"] = "utf8mb4"
    kwargs["autocommit"] = True
    kwargs["connect_timeout"] = 30
    return pymysql.connect(**kwargs)


def query(connect_kwargs: dict[str, Any], database: str, sql: str) -> list[list[str]]:
    with _connect(connect_kwargs, database) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
    result: list[list[str]] = []
    for row in rows:
        result.append(["" if value is None else str(value) for value in row])
    return result


def execute(connect_kwargs: dict[str, Any], database: str, sql: str) -> None:
    statements = _iter_mysql_statements(sql)
    if not statements:
        return
    with _connect(connect_kwargs, database) as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)


class _MysqlScriptScanner:
    """Splits a MySQL script on DELIMITER-aware statement boundaries.

    The lexer tracks single/double-quoted strings, backtick identifiers,
    line comments (``-- ``, ``#``) and block comments; statements inside any
    of those are not split. Each state gets its own method so the per-branch
    complexity stays readable."""

    def __init__(self, script: str) -> None:
        self.script = script
        self.i = 0
        self.delimiter = ";"
        self.buffer: list[str] = []
        self.statements: list[str] = []
        self.in_single = False
        self.in_double = False
        self.in_backtick = False
        self.in_line_comment = False
        self.in_block_comment = False

    def _pair(self) -> tuple[str, str]:
        ch = self.script[self.i]
        nxt = self.script[self.i + 1] if self.i + 1 < len(self.script) else ""
        return ch, nxt

    def _inside_quote(self) -> bool:
        return self.in_single or self.in_double or self.in_backtick

    def _inside_string_or_comment(self) -> bool:
        return self._inside_quote() or self.in_line_comment or self.in_block_comment

    def _consume_delimiter_directive(self) -> bool:
        # DELIMITER is only valid at the start of a line (leading whitespace allowed).
        if self._inside_string_or_comment() or not self.script.startswith("DELIMITER ", self.i):
            return False
        line_start = self.script.rfind("\n", 0, self.i) + 1
        if self.script[line_start : self.i].strip():
            return False
        end = self.script.find("\n", self.i)
        if end == -1:
            end = len(self.script)
        self.delimiter = self.script[self.i : end].split(" ", 1)[1].strip() or ";"
        self.i = end + 1
        return True

    def _consume_comment_body(self) -> bool:
        ch, nxt = self._pair()
        if self.in_line_comment:
            self.buffer.append(ch)
            if ch == "\n":
                self.in_line_comment = False
            self.i += 1
            return True
        if self.in_block_comment:
            self.buffer.append(ch)
            if ch == "*" and nxt == "/":
                self.buffer.append("/")
                self.i += 2
                self.in_block_comment = False
            else:
                self.i += 1
            return True
        return False

    def _consume_doubled_quote(self) -> bool:
        # A doubled quote inside a string is a literal quote, not a boundary.
        ch, nxt = self._pair()
        if (self.in_single and ch == "'" and nxt == "'") or (self.in_double and ch == '"' and nxt == '"'):
            self.buffer.append(ch)
            self.buffer.append(nxt)
            self.i += 2
            return True
        return False

    def _consume_comment_opener(self) -> bool:
        if self._inside_quote():
            return False
        ch, nxt = self._pair()
        # MySQL only treats '--' as a comment when followed by whitespace or control.
        if ch == "-" and nxt == "-" and (self.i + 2 >= len(self.script) or self.script[self.i + 2] in " \t\r\n\v\f"):
            self.in_line_comment = True
        elif ch == "#":
            self.in_line_comment = True
        elif ch == "/" and nxt == "*":
            self.in_block_comment = True
        else:
            return False
        self.buffer.append(ch)
        self.i += 1
        return True

    def _toggle_quote_state(self) -> None:
        ch, _ = self._pair()
        if (ch == "'" and not self.in_double and not self.in_backtick) or (ch == '"' and not self.in_single and not self.in_backtick):
            # A quote is escaped only by an odd-length run of preceding backslashes.
            backslashes = 0
            j = self.i - 1
            while j >= 0 and self.script[j] == "\\":
                backslashes += 1
                j -= 1
            if backslashes % 2 == 0:
                if ch == "'":
                    self.in_single = not self.in_single
                else:
                    self.in_double = not self.in_double
        elif ch == "`" and not self.in_single and not self.in_double:
            self.in_backtick = not self.in_backtick

    def _flush_statement(self) -> bool:
        if self._inside_string_or_comment() or not self.delimiter:
            return False
        if not self.script.startswith(self.delimiter, self.i):
            return False
        statement = "".join(self.buffer).strip()
        if statement:
            self.statements.append(statement)
        self.buffer = []
        self.i += len(self.delimiter)
        return True

    def run(self) -> list[str]:
        while self.i < len(self.script):
            if self._consume_delimiter_directive() or self._consume_comment_body() or self._consume_doubled_quote() or self._consume_comment_opener():
                continue
            self._toggle_quote_state()
            if self._flush_statement():
                continue
            self.buffer.append(self.script[self.i])
            self.i += 1

        tail = "".join(self.buffer).strip()
        if tail:
            self.statements.append(tail)
        return self.statements


def _iter_mysql_statements(script: str) -> list[str]:
    return _MysqlScriptScanner(script).run()
