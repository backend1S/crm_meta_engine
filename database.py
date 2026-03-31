import os
import logging
import pyodbc
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("database")


class Database:
    def __init__(self):
        self.server = os.getenv("SERVER", "").strip()
        self.database = os.getenv("DATABASE", "").strip()
        self.user = os.getenv("USER", "").strip()
        self.password = os.getenv("PASSWORD", "").strip()
        self.port = os.getenv("PORT", "1433").strip()

        self.conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER=tcp:{self.server},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={self.password};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )

    # --------------------------------
    # GET CONNECTION
    # --------------------------------
    def get_conn(self):
        try:
            conn = pyodbc.connect(self.conn_str)
            return conn
        except Exception as e:
            logger.error(f"❌ DB connection failed → {e}")
            return None

    # --------------------------------
    # SELECT QUERY
    # --------------------------------
    def db_query(self, sql, params=None):
        conn = None
        cursor = None

        try:
            conn = self.get_conn()
            if not conn:
                return False, []

            cursor = conn.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            rows = cursor.fetchall()
            return True, rows

        except Exception as e:
            logger.error(f"❌ DB query failed → {e}")
            return False, str(e)

        finally:
            try:
                if cursor:
                    cursor.close()
            except:
                pass

            try:
                if conn:
                    conn.close()
            except:
                pass

    # --------------------------------
    # INSERT / UPDATE / DELETE
    # --------------------------------
    def db_update(self, sql, params=None):
        conn = None
        cursor = None

        try:
            conn = self.get_conn()
            if not conn:
                return False, "DB connection failed"

            cursor = conn.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            conn.commit()
            return True, "OK"

        except Exception as e:
            logger.error(f"❌ DB update failed → {e}")
            return False, str(e)

        finally:
            try:
                if cursor:
                    cursor.close()
            except:
                pass

            try:
                if conn:
                    conn.close()
            except:
                pass

    # --------------------------------
    # EXECUTE MANY
    # --------------------------------
    def db_executemany(self, sql, params_list):
        conn = None
        cursor = None

        try:
            conn = self.get_conn()
            if not conn:
                return False, "DB connection failed"

            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(sql, params_list)

            conn.commit()
            return True, "OK"

        except Exception as e:
            logger.error(f"❌ DB executemany failed → {e}")
            return False, str(e)

        finally:
            try:
                if cursor:
                    cursor.close()
            except:
                pass

            try:
                if conn:
                    conn.close()
            except:
                pass

    # --------------------------------
    # TEST CONNECTION
    # --------------------------------
    def test_connection(self):
        conn = None
        try:
            conn = self.get_conn()
            if conn:
                return True
            return False
        except:
            return False
        finally:
            try:
                if conn:
                    conn.close()
            except:
                pass