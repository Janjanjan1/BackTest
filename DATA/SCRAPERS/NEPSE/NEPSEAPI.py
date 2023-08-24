import os
import requests
import time
import pywasm
import datetime




def append_to_log(message):
    # print(message)
    log_file = "log.txt"
    if not os.path.exists(log_file):
        with open(log_file, "w") as file:
            file.write("Log File\n\n")
    with open(log_file, "a") as file:
        file.write(message + "\n")



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


    def getCSSWASM(self):
        print("GETTING CSS WASM ")
        if os.path.isfile('./css.wasm'):
            return 
        res = requests.get('https://www.nepalstock.com/assets/prod/css.wasm')
        data = open('./css.wasm', 'w')
        data.write(res.text)
        data.close()
        return 
    
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

