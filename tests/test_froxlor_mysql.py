from __future__ import annotations

from pathlib import Path

from froxlor_migrator.froxlor_mysql import (
    _credential_score,
    _extract_first_sql_root_entry,
    _extract_php_array_body,
    _extract_php_array_value,
    connect_kwargs_from_credentials,
    extract_sql_credentials,
    extract_sql_root_credentials,
    load_local_sql_credentials,
    load_local_sql_root_credentials,
)


def test_credential_score_counts_password_socket_and_port() -> None:
    assert _credential_score({}) == 0
    assert _credential_score({"password": "p"}) == 2
    assert _credential_score({"password": "p", "socket": "/tmp/x"}) == 3
    assert _credential_score({"password": "p", "socket": "/tmp/x", "port": "3306"}) == 4


def test_extract_credentials_from_php_array_body() -> None:
    content = """
    $sql = [
        'user' => 'u',
        'password' => 'p',
        'host' => 'h',
        'port' => '1234',
        'socket' => '/tmp/s',
    ];
    """
    creds = extract_sql_credentials(content)
    assert creds == {"user": "u", "password": "p", "host": "h", "port": "1234", "socket": "/tmp/s"}


def test_extract_sql_root_credentials_old_style() -> None:
    content = """
    $sql_root[0]['user'] = 'root';
    $sql_root[0]['password'] = 'p';
    $sql_root[1]['user'] = '';
    """
    creds = extract_sql_root_credentials(content)
    assert creds["user"] == "root"


def test_extract_sql_root_credentials_new_style() -> None:
    content = """
    $sql_root = [
        '0' => [
            'user' => 'root',
            'password' => 'p',
            'host' => 'h',
        ],
        '1' => [
            'user' => 'admin',
            'password' => 'p2',
        ],
    ];
    """
    creds = extract_sql_root_credentials(content)
    assert creds["user"] == "root"


def test_extract_php_array_helpers() -> None:
    body = "'user' => 'u', 'password' => 'p'"
    assert _extract_php_array_body("$sql = [" + body + "];", "sql") == body
    assert _extract_first_sql_root_entry("'0' => ['user' => 'u'], '1' => ['user' => 'v'],") == "'user' => 'u'"
    assert _extract_php_array_value(body, "user") == "u"
    assert _extract_php_array_value(body, "password") == "p"


def test_connect_kwargs_from_credentials_sets_defaults() -> None:
    creds = {"user": "u", "password": "p"}
    kwargs = connect_kwargs_from_credentials(creds)
    assert kwargs["host"] == "localhost"
    assert kwargs["port"] == 3306


def test_load_local_credentials_prefers_non_root_and_password(tmp_path) -> None:
    path1 = tmp_path / "c1.php"
    path1.write_text("$sql = ['user' => 'root', 'password' => 'p1'];")
    path2 = tmp_path / "c2.php"
    path2.write_text("$sql = ['user' => 'alice', 'password' => 'p2'];")

    creds = load_local_sql_credentials([str(path1), str(path2)])
    assert creds["user"] == "alice"

    # Root only input should still return root
    creds2 = load_local_sql_root_credentials([str(path1)])
    assert creds2["user"] == "root"


def test_load_local_sql_root_credentials_caches_file_reads(tmp_path, monkeypatch) -> None:
    path = tmp_path / "userdata.inc.php"
    path.write_text("$sql_root[0]['user'] = 'root'; $sql_root[0]['password'] = 'p';")

    original_read_text = Path.read_text
    read_calls: list[str] = []

    def tracked_read_text(self, encoding="utf-8", errors="ignore"):
        read_calls.append(str(self))
        return original_read_text(self, encoding=encoding, errors=errors)

    monkeypatch.setattr(Path, "read_text", tracked_read_text)

    creds1 = load_local_sql_root_credentials([str(path)])
    creds2 = load_local_sql_root_credentials([str(path)])

    assert creds1 == creds2
    # Should only read the file once due to caching.
    assert len(read_calls) == 1
