from __future__ import annotations

from pathlib import Path
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


def _iter_mysql_statements(script: str) -> list[str]:
    delimiter = ";"
    statements: list[str] = []
    buffer: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    while i < len(script):
        if script.startswith("DELIMITER ", i) and not (in_single or in_double or in_backtick or in_line_comment or in_block_comment):
            end = script.find("\n", i)
            if end == -1:
                end = len(script)
            delimiter = script[i:end].split(" ", 1)[1].strip() or ";"
            i = end + 1
            continue

        ch = script[i]
        nxt = script[i + 1] if i + 1 < len(script) else ""

        if in_line_comment:
            buffer.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue
        if in_block_comment:
            buffer.append(ch)
            if ch == "*" and nxt == "/":
                buffer.append("/")
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        if not (in_single or in_double or in_backtick):
            if ch == "-" and nxt == "-":
                in_line_comment = True
                buffer.append(ch)
                i += 1
                continue
            if ch == "#":
                in_line_comment = True
                buffer.append(ch)
                i += 1
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                buffer.append(ch)
                i += 1
                continue

        if ch == "'" and not in_double and not in_backtick:
            escaped = i > 0 and script[i - 1] == "\\"
            if not escaped:
                in_single = not in_single
        elif ch == '"' and not in_single and not in_backtick:
            escaped = i > 0 and script[i - 1] == "\\"
            if not escaped:
                in_double = not in_double
        elif ch == "`" and not in_single and not in_double:
            in_backtick = not in_backtick

        if not (in_single or in_double or in_backtick or in_line_comment or in_block_comment):
            if delimiter and script.startswith(delimiter, i):
                statement = "".join(buffer).strip()
                if statement:
                    statements.append(statement)
                buffer = []
                i += len(delimiter)
                continue

        buffer.append(ch)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def import_sql_dump(connect_kwargs: dict[str, Any], database: str, dump_path: str) -> None:
    script = Path(dump_path).read_text(encoding="utf-8", errors="ignore")
    statements = _iter_mysql_statements(script)
    with _connect(connect_kwargs, database) as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
