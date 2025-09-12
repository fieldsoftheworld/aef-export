from dataclasses import dataclass, asdict
import sqlite3


DATABASE_NAME = "sqlite_aef_export.db"


@dataclass
class Row:
    task_id: str
    job_name: str
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
            job_name VARCHAR,
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
