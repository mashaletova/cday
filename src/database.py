import sqlite3
import click


class DBConnection:
    def __init__(self, db_path, reset_db=False):
        self.db_path = db_path
        if reset_db: self._delete_db()
        self.c = sqlite3.connect(db_path)

    def _delete_db(self):
        import os
        try:
            os.unlink(self.db_path)
        except FileNotFoundError:
            pass