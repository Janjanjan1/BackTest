
from sqlite3 import Connection
import sqlite3
from pandas import to_pickle, read_pickle
import datetime
import os
from dataclasses import dataclass
from enum import Enum
from NEPSEAPI import NEPSE_API


class SQLITE_DTYPE(Enum):
    NULL = 'NULL'
    INTEGER = "INTEGER"
    REAL = "REAL"
    TEXT = "TEXT"
    BLOB = "BLOB"


@dataclass
class Column:
    name: str
    value: str
    dtype: SQLITE_DTYPE
    index: bool = False
    primary_key: bool = False
    not_null: bool = True
    unique: bool = False



class SQLITE_TABLE:
    def __init__(self, table, columns, con: Connection):
        self.table = table
        self.columns = columns
        self.con = con
        self.create()

    def create(self):
        columnStr = "(" + ",".join(
            [f'{i.name} {i.dtype.value} {"PRIMARY KEY DESC" if i.primary_key else ""} {"UNIQUE" if i.unique else ""} {"NOT NULL" if i.not_null else ""} ' for i in self.columns]) + ")"
        cur = self.con.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS {self.table} {columnStr}")
        cur.close()
        return self

    def bulkInsert(self, values):
        count = 0
        value = []
        v = []
        for row in values:
            count += 1
            row = [f'"{i}"' for i in row]
            value.append(f'({",".join(row)})')
            if count == 500:
                v.append(value)
                value = []
                count = 0
        if count != 0:
            v.append(value)
        for i in v:
            self.insert(i)
        return

    def insert(self, values):
        if type(values) == list:
            if type(values[0]) == list:
                return self.bulkInsert(values)
            values = ",".join(values)
        cur = self.con.cursor()
        columns = [i.name for i in self.columns]
        cur.execute(
            f'INSERT OR IGNORE INTO {self.table} {"(" + ",".join(columns) + ")" if self.columns else "" }  VALUES {values}')
        self.con.commit()
        cur.close()
        return self

    def select(self, columns, where=None):
        cur = self.con.cursor()
        cur.execute(
            f"SELECT {','.join(columns)} FROM {self.table} {'WHERE '+ where if where else ''}")
        res = cur.fetchall()
        cur.close()
        return res

    def filter(self, values):
        p_key = list(filter(lambda x: x.primary_key, self.columns))
        valuesAlr = [i[0] for i in self.select([i.value for i in p_key])]
        return [i for i in values if i[p_key[0].value] not in valuesAlr]

class NEPSE_API_SQLITE(NEPSE_API, SQLITE_TABLE):
    SQL_LITE_MAX_INSERT = 500

    def elogger(self, error):
        PATH = f"./{self.table}.elog"
        if os.path.exists(PATH):
            data = read_pickle(PATH)
            data.append(error)
        else:
            data = [error]
        to_pickle(data, PATH)
        return

    def __init__(self, table: str, columns: list, con):
        if type(con) == str:
            con = sqlite3.connect(con)
        elif type(con) != Connection:
            raise TypeError(
                f'con has to be either a str or Connection but is {type(con)}')
        NEPSE_API.__init__(self)
        SQLITE_TABLE.__init__(self, table, columns, con)
        self.table = table
        self.con.execute(
            'PRAGMA journal_mode = WAL')
        self.con.execute('PRAGMA synchronous = NORMAL')

class Securities(NEPSE_API_SQLITE):
    def __init__(self, con):
        super().__init__('Securities', [Column('id', 'id', SQLITE_DTYPE.INTEGER, primary_key=True), Column('symbol', 'symbol', SQLITE_DTYPE.TEXT, True, False, True, True), Column(
            'securityName', 'securityName', SQLITE_DTYPE.TEXT, True, False, True, False), Column('name', 'name', SQLITE_DTYPE.TEXT, True, False, True, True)], con)

class FloorSheet(NEPSE_API_SQLITE):
    def __init__(self, con):
        NEPSE_API_SQLITE.__init__(self, 'floorsheet', [Column('contractID', 'contractId', SQLITE_DTYPE.INTEGER, primary_key=True), Column('securityID', 'stockId', SQLITE_DTYPE.INTEGER, True), Column(
            'dateTime', 'tradeTime', SQLITE_DTYPE.TEXT), Column('quantity', 'contractQuantity', SQLITE_DTYPE.REAL), Column('rate', 'contractRate', SQLITE_DTYPE.REAL), Column('buyerID', 'buyerMemberId', SQLITE_DTYPE.INTEGER), Column('sellerID', 'sellerMemberId', SQLITE_DTYPE.INTEGER)], con)

    def sync(self):
        pass

    def getAllPages(self, date, securityID, size):
        results = []
        page = 0
        while True:
            # start_time = time.time()
            r = self.getFloorsheet(date, securityID, page, size)
            # end_time = time.time()
            # print(
            #     f"time taken: {end_time - start_time} | {securityID} {date} {page}")
            if r.status_code != 200:
                self.elogger({"page": page, "error": r.text,
                              "date": date, "securityID": securityID})
                break
            try:
                j = r.json()
            except:
                self.elogger({"page": page, "error": r.text,
                              "date": date, "securityID": securityID})
                continue
            results.extend(j['floorsheets']['content'])
            if j['floorsheets']['totalPages'] > page+1:
                page += 1
                continue
            else:
                break
        return results

    def getEntireSecurity(self, securityID: str):
        dates = []
        date = datetime.datetime(2023, 4, 6)
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        while date.strftime("%Y-%m-%d") != today:
            dates.append(date.strftime("%Y-%m-%d"))
            date = date + datetime.timedelta(days=1)
            # Not a Saturday
            if date.weekday() > 5:
                continue
        for date in dates:
            r = self.getAllPages(date, securityID, 500)
            # print(f"Security: {securityID} Date: {date} Entries: {len(r)}")
            if len(r) > 0:
                self.bulkInsert([[str(i[key.value]).replace('"', '^').replace(
                    '(', '[').replace(')', ']') for key in self.columns] for i in r])
