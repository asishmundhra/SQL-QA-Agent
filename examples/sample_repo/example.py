import sqlite3


def run_queries():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE LOWER(email) = 'foo@example.com'")
    cur.execute(
        """
        UPDATE users
        SET last_login = CURRENT_TIMESTAMP
        """
    )
    cur.execute("SELECT id FROM users WHERE id IN (1, 2, 3)")
    return cur.fetchall()


if __name__ == "__main__":
    run_queries()
