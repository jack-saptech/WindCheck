import pymysql as sql
import pandas as pd


class DBData:

    def __init__(self, host, db, user, pwd, auto_conn):
        self.credentials = {"host": host, "db": db, "user": user, "pwd": pwd}
        if auto_conn:
            self.conn = self.connect()
        else:
            self.conn = None

    def connect(self):
        # Get credentials
        host = self.credentials["host"]
        db = self.credentials["db"]
        user = self.credentials["user"]
        pwd = self.credentials["pwd"]

        self.conn = sql.connect(host=host, database=db, user=user, password=pwd)

    def sql_cmd(self, cmd):
        if self.conn is None:
            self.connect()

        df = pd.read_sql(cmd, self.conn)
        return df
