from __future__ import annotations

from unittest.mock import MagicMock, patch

from froxlor_migrator import mysql_driver


def test_iter_mysql_statements_simple():
    sql = "SELECT 1; SELECT 2;"
    assert mysql_driver._iter_mysql_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_iter_mysql_statements_delimiter_and_comments():
    sql = """
    -- comment line
    SELECT 1; /* block comment; still comment */
    DELIMITER //
    CREATE PROCEDURE sp() BEGIN SELECT 2; END;//
    //
    """
    stmts = mysql_driver._iter_mysql_statements(sql)
    assert any("CREATE PROCEDURE" in s for s in stmts)
    assert any("SELECT 1" in s for s in stmts)


def test_query_uses_connect_and_fetchall():
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1, None), ("a", "b")]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.__enter__.return_value = conn

    with patch.object(mysql_driver, "_connect", return_value=conn) as mock_connect:
        rows = mysql_driver.query({"host": "x"}, "db", "SELECT 1")
        assert rows == [["1", ""], ["a", "b"]]
        mock_connect.assert_called_once_with({"host": "x"}, "db")
        cursor.execute.assert_called_once_with("SELECT 1")


def test_execute_sends_each_statement():
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.__enter__.return_value = conn

    with patch.object(mysql_driver, "_connect", return_value=conn):
        mysql_driver.execute({"host": "x"}, "db", "SELECT 1; SELECT 2;")
        cursor.execute.assert_any_call("SELECT 1")
        cursor.execute.assert_any_call("SELECT 2")


def test_import_sql_dump_reads_file_and_executes(tmp_path):
    dump = tmp_path / "dump.sql"
    dump.write_text("SELECT 1; SELECT 2;")

    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.__enter__.return_value = conn

    with patch.object(mysql_driver, "_connect", return_value=conn):
        mysql_driver.import_sql_dump({"host": "x"}, "db", str(dump))
        cursor.execute.assert_any_call("SELECT 1")
        cursor.execute.assert_any_call("SELECT 2")
