from dataclasses import dataclass, asdict
import sqlite3


DATABASE_NAME = "sqlite_aef_export.db"


@dataclass
class Row:
    task_id: str
    eecu_seconds: float | None
    runtime_seconds: float | None
    status: str
    image_id: str
    year: int
    s3_path: str


def get_connection():
    return sqlite3.connect(DATABASE_NAME)


def init_database():
    cur = get_connection()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS exports(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id VARCHAR,
            eecu_seconds FLOAT,
            runtime_seconds FLOAT,
            status VARCHAR,
            image_id VARCHAR,
            year INTEGER,
            s3_path VARCHAR
        )
    """)


def insert_row(row: Row):
    d = asdict(row)
    columns = ", ".join(d.keys())
    placeholders = ", ".join(["?" for _ in d.values()])
    query = f"INSERT INTO exports ({columns}) VALUES ({placeholders})"

    cur = get_connection()
    cur.execute(query, tuple(d.values()))
    cur.commit()


def update_row(
    task_id: str,
    status: str,
    eecu_seconds: float | None = None,
    runtime_seconds: float | None = None,
):
    query = "UPDATE exports SET status = ?, eecu_seconds = ?, runtime_seconds = ? WHERE task_id = ?"
    cur = get_connection()
    cur.execute(query, (status, eecu_seconds, runtime_seconds, task_id))
    cur.commit()


def get_summary():
    query = """
        SELECT
            status,
            COUNT(*) as count,
            SUM(eecu_seconds) as eecu_seconds,
            AVG(runtime_seconds) as avg_runtime_seconds
        FROM exports
        GROUP BY status;
    """
    conn = get_connection()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query)
    resp = cur.fetchall()
    return [dict(row) for row in resp]
