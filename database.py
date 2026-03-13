import pymssql
import os
from dotenv import load_dotenv

load_dotenv()


class Database:

    def __init__(self):
        self.conn = None

    def db_connect(self):

        try:

            self.conn = pymssql.connect(
                server=os.getenv("SERVER"),
                user=os.getenv("USER"),
                password=os.getenv("PASSWORD"),
                database=os.getenv("DATABASE")
            )

            cursor = self.conn.cursor()

            return True, cursor

        except Exception as e:

            return False, str(e)

    def db_query(self, sql):

        cursor = None

        try:

            status, cursor = self.db_connect()

            if not status:
                return False, cursor

            cursor.execute(sql)

            rows = cursor.fetchall()

            cursor.close()
            self.conn.close()

            return True, rows

        except Exception as e:

            if cursor:
                cursor.close()

            if self.conn:
                self.conn.close()

            return False, str(e)

    def db_update(self, sql):

        cursor = None

        try:

            status, cursor = self.db_connect()

            if not status:
                return False, cursor

            cursor.execute(sql)

            self.conn.commit()

            cursor.close()
            self.conn.close()

            return True, "OK"

        except Exception as e:

            if cursor:
                cursor.close()

            if self.conn:
                self.conn.close()

            return False, str(e)