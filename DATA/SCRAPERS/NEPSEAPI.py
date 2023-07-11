import os
import sqlite3
import requests
import time
import pywasm
import datetime
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractclassmethod
from tqdm import tqdm
from sqlite3 import Connection
from pandas import to_pickle, read_pickle
from multiprocessing import Pool
from random import shuffle


def append_to_log(message):
    # print(message)
    log_file = "log.txt"
    if not os.path.exists(log_file):
        with open(log_file, "w") as file:
            file.write("Log File\n\n")
    with open(log_file, "a") as file:
        file.write(message + "\n")


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


class NEPSE_API:
    def __init__(self):
        self.baseURL = "https://www.nepalstock.com/"
        self.headers = {
            'authority': 'www.nepalstock.com',
            'content-type': 'application/json',
            'origin': 'https://www.nepalstock.com',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'referer': 'https://www.nepalstock.com/',
            'sec-ch-ua': '"Not.A/Brand";v="8", "Chromium";v="114", "Microsoft Edge";v="114"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.67',
        }
        self.accessToken = ""
        self.refreshToken = ""
        self.tokens = {}
        self.t = pywasm.load('./css.wasm')
        self.prove()
        self.lastProven = time.time()
        self.securities = []

    def tokenGenerate(self, e, accessToken, refreshToken):
        t = self.t
        accessToken = accessToken[:t.exec("cdx", [e["salt1"], e["salt2"], e["salt3"], e["salt4"], e["salt5"]])] + \
            accessToken[t.exec("cdx", [e["salt1"], e["salt2"], e["salt3"], e["salt4"], e["salt5"]]) + 1: t.exec("rdx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]])] + \
            accessToken[t.exec("rdx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]]) + 1: t.exec("bdx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]])] + \
            accessToken[t.exec("bdx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]]) + 1: t.exec("ndx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]])] + \
            accessToken[t.exec("ndx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]]) + 1: t.exec("mdx", [e["salt1"], e["salt2"], e["salt4"], e["salt3"], e["salt5"]])] + \
            accessToken[t.exec("mdx", [e["salt1"], e["salt2"],
                                       e["salt4"], e["salt3"], e["salt5"]]) + 1:]
        refreshToken = refreshToken[:t.exec("cdx", [e["salt2"], e["salt1"], e["salt3"], e["salt5"], e["salt4"]])] + \
            refreshToken[t.exec("cdx", [e["salt2"], e["salt1"], e["salt3"], e["salt5"], e["salt4"]]) + 1: t.exec("rdx", [e["salt2"], e["salt1"], e["salt3"], e["salt4"], e["salt5"]])] + \
            refreshToken[t.exec("rdx", [e["salt2"], e["salt1"], e["salt3"], e["salt4"], e["salt5"]]) + 1: t.exec("bdx", [e["salt2"], e["salt1"], e["salt4"], e["salt3"], e["salt5"]])] + \
            refreshToken[t.exec("bdx", [e["salt2"], e["salt1"], e["salt4"], e["salt3"], e["salt5"]]) + 1: t.exec("ndx", [e["salt2"], e["salt1"], e["salt4"], e["salt3"], e["salt5"]])] + \
            refreshToken[t.exec("ndx", [e["salt2"], e["salt1"], e["salt4"], e["salt3"], e["salt5"]]) + 1: t.exec("mdx", [e["salt2"], e["salt1"], e["salt4"], e["salt3"], e["salt5"]])] + \
            refreshToken[t.exec("mdx", [e["salt2"], e["salt1"],
                                e["salt4"], e["salt3"], e["salt5"]]) + 1:]

        return [accessToken, refreshToken]

    def prove(self):
        r = retry(requests.get, 0,
                  "https://www.nepalstock.com/api/authenticate/prove", headers=self.headers)
        h = r.json()
        self.tokens = h
        self.setTokens(h)
        return self

    def reprove(self):
        r = requests.post('https://www.nepalstock.com/api/authenticate/refresh-token',
                          headers=self.headers, json={'refreshToken': self.refreshToken})
        h = r.json()
        self.tokens = h
        self.setTokens(h)
        return

    def setTokens(self, h):
        self.accessToken, self.refreshToken = self.tokenGenerate(
            h, h['accessToken'], h['refreshToken'])
        self.headers['authorization'] = f"Salter {self.accessToken}"
        return

    def reprove_func(func):
        def wrapper(self, *args, **kw):
            self.reprove()
            res = func(self, *args, **kw)
            return res
        return wrapper

    def prove_func(func):
        def wrapper(self, *args, **kw):
            self.prove()
            res = func(self, *args, **kw)
            return res
        return wrapper

    @reprove_func
    def getSecurities(self):
        r = requests.get(
            'https://www.nepalstock.com/api/nots/security?nonDelisted=true', headers=self.headers)
        return r.json()

    def getMarketStatus(self):
        r = requests.get(
            'https://www.nepalstock.com/api/nots/nepse-data/market-open', headers=self.headers)
        return r.json()

    def getMarketStatusID(self):
        return self.getMarketStatus()['id']

    def get_dummy_data(self):
        # Replace this function with your implementation to retrieve the dummy data
        # and return it as an array or dictionary.
        return [147, 117, 239, 143, 157, 312, 161, 612, 512, 804, 411, 527, 170, 511, 421, 667, 764, 621, 301, 106, 133, 793, 411, 511, 312, 423, 344, 346, 653, 758, 342, 222, 236, 811, 711, 611, 122, 447, 128, 199, 183, 135, 489, 703, 800, 745, 152, 863, 134, 211, 142, 564, 375, 793, 212, 153, 138, 153, 648, 611, 151, 649, 318, 143, 117, 756, 119, 141, 717, 113, 112, 146, 162, 660, 693, 261, 362, 354, 251, 641, 157, 178, 631, 192, 734, 445, 192, 883, 187, 122, 591, 731, 852, 384, 565, 596, 451, 772, 624, 691]

    def get_access_tokens(self):
        return [int(self.tokens[f"salt{i}"]) for i in range(1, 6)]

    def generate_fId(self):
        day = datetime.date.today().day
        dummy_data = self.get_dummy_data()
        marketStatus_id = self.getMarketStatusID()
        access_tokens = self.get_access_tokens()
        i = dummy_data[marketStatus_id] + marketStatus_id + 2 * day
        fID = i + (access_tokens[1 if (i % 10 < 4) else 3] *
                   day) - (access_tokens[(1 if (i % 10 < 4) else 3)-1])
        return fID

    def loadSecurities(self):
        self.securities = self.getSecurities()
        return self

    def getFloorsheet(self, date, securityID, page=0, size=500):
        self.prove()
        r = retry(requests.post, 0, f'https://www.nepalstock.com/api/nots/security/floorsheet/{securityID}?&size={size}&page={page}&businessDate={date}',
                  headers=self.headers, json={'id': self.generate_fId()})
        return r

def retry(func, r_level, *args, **kw,):
    if r_level > 150:
        raise Exception("Failed To Retry")
    try:
        res = func(*args, **kw)
    except Exception as E:
        time.sleep(r_level * 1)
        return retry(func, r_level+1, *args, **kw)
    return res

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

def fsFunc(securityID):
    fs = FloorSheet('./nepse.db')
    print(securityID)
    fs.getEntireSecurity(securityID['id'])
    fs.con.commit()
    fs.con.close()
    return

if __name__ == '__main__':
    n = NEPSE_API()
    securities = n.loadSecurities().securities
    securities.reverse()

    with Pool(7) as p:
        p.map(fsFunc, securities)
