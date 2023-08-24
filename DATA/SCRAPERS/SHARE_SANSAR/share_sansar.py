import toolz
import requests
import pandas as pd
from datetime import datetime, timedelta

CONTENT_SIZE = 500
headers = {
    "authority": "www.sharesansar.com",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "accept-language": "en-US,en;q=0.9",
    # 'cookie': 'XSRF-TOKEN=eyJpdiI6Ik5KK1dsR01vOTlZWVFMS0xlNEFiUEE9PSIsInZhbHVlIjoibTkya3RHSlM1U1NpNTh0bEprL1EzTEdhbEdWUUQ3UU05NjY2VHhtOU1CV3JCeGNiK0N6Y1hUUWdSUzJHVi9VRG55SFVYZVRGbTVLQkhpS1lPY2xZR05KZEZKR3VzZXZac096RE5PS2twZ1BHRmFIWWlRQlBtbjhXSTM3eFUrQlMiLCJtYWMiOiI4NWY5NTA0YjVhZWJkM2FmZDY3MzIxNDBmYTk3ZmFiN2Q0NDMzYjdhOTI2YWVkYmFiNjNiYjc2ZjFkYmJhMmFjIn0%3D; sharesansar_session=eyJpdiI6IjhCdkhvOHdUMkFUbEtWcjJVV1dkcEE9PSIsInZhbHVlIjoiNXI4c0pHM0NhRFhsemoyZy9FaDh2MnJLd3l1a095MitrUGdySjJJVk1Ia05qb0F4MmRxOTJlV0NZWGdydkY3VE1mck8vYXZlYlpoUXJiTzBOdUZTUFg0VGtJM1VSV3lXRXZjR0Y2K0lZNW5IdS9tVlhKYVZsV2VIMjQxajNheXAiLCJtYWMiOiJkZDY1OWYwOWU5MjFmZGNlN2I1NGZiZDdmNTNhMTI4ODE0N2FjN2E0NGY3ZmMwZjY1NzUwNGY2Y2FmZWQxMTViIn0%3D',
    # 'referer': 'https://www.sharesansar.com/floorsheet',
    "sec-ch-ua": '"Chromium";v="116", "Not)A;Brand";v="24", "Microsoft Edge";v="116"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    # 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.54',
    "x-requested-with": "XMLHttpRequest",
}


## set company
def set_company(params={}, company=""):
    params["company"] = company
    return params


def set_date(params={}, date=""):
    params["date"] = date
    return params


get_curried = toolz.curry(requests.get)
get_with_headers = get_curried(headers=headers)
get_floorsheet = lambda params: get_with_headers(
    url="https://www.sharesansar.com/floorsheet", params=params
)
parse_floorsheet_json = lambda json: dict(
    map(lambda json_items: (json_items["transaction"], json_items), json["data"])
)


def get_all_floorsheet_pages(params):
    draw = 1
    start = 0
    end = CONTENT_SIZE
    all_floorsheet_pages = {}
    while start <= end:
        params["draw"] = draw
        params["start"] = start
        response = get_floorsheet(params)
        json_data = response.json()
        all_floorsheet_pages |= parse_floorsheet_json(json_data)
        start += CONTENT_SIZE
        draw += 1
        end = json_data["recordsTotal"]
    return all_floorsheet_pages


def get_floorsheets_date_range(company, from_date="2015-12-31", to_date=datetime.now()):
    params = {
        "draw": "2",
        "columns[0][data]": "DT_Row_Index",
        "columns[0][name]": "",
        "columns[0][searchable]": "false",
        "columns[0][orderable]": "false",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
        "columns[1][data]": "company.symbol",
        "columns[1][name]": "",
        "columns[1][searchable]": "true",
        "columns[1][orderable]": "false",
        "columns[1][search][value]": "",
        "columns[1][search][regex]": "false",
        "columns[2][data]": "transaction",
        "columns[2][name]": "",
        "columns[2][searchable]": "true",
        "columns[2][orderable]": "false",
        "columns[2][search][value]": "",
        "columns[2][search][regex]": "false",
        "columns[3][data]": "buyer",
        "columns[3][name]": "",
        "columns[3][searchable]": "true",
        "columns[3][orderable]": "false",
        "columns[3][search][value]": "",
        "columns[3][search][regex]": "false",
        "columns[4][data]": "seller",
        "columns[4][name]": "",
        "columns[4][searchable]": "true",
        "columns[4][orderable]": "false",
        "columns[4][search][value]": "",
        "columns[4][search][regex]": "false",
        "columns[5][data]": "quantity",
        "columns[5][name]": "",
        "columns[5][searchable]": "true",
        "columns[5][orderable]": "false",
        "columns[5][search][value]": "",
        "columns[5][search][regex]": "false",
        "columns[6][data]": "rate",
        "columns[6][name]": "",
        "columns[6][searchable]": "true",
        "columns[6][orderable]": "false",
        "columns[6][search][value]": "",
        "columns[6][search][regex]": "false",
        "columns[7][data]": "amount",
        "columns[7][name]": "",
        "columns[7][searchable]": "true",
        "columns[7][orderable]": "false",
        "columns[7][search][value]": "",
        "columns[7][search][regex]": "false",
        "columns[8][data]": "date_",
        "columns[8][name]": "",
        "columns[8][searchable]": "true",
        "columns[8][orderable]": "false",
        "columns[8][search][value]": "",
        "columns[8][search][regex]": "false",
        "start": "0",
        "length": CONTENT_SIZE,
        "search[value]": "",
        "search[regex]": "false",
        "company": company,
        "date": "",
        "buyer": "",
        "seller": "",
    }
    from_date = datetime.strptime(from_date, "%Y-%m-%d")
    delta = timedelta(days=1)
    floorsheet_all = {}
    while to_date.strftime("%Y-%m-%d") != from_date.strftime("%Y-%m-%d"):
        params = set_date(params, to_date.strftime("%Y-%m-%d"))
        temp = get_all_floorsheet_pages(params)
        print(f'Date: {to_date.strftime("%Y-%m-%d")} | Length: {len(temp)} ')
        floorsheet_all |= temp
        to_date -= delta
    return floorsheet_all


def main():
    company = "22"
    r = get_floorsheets_date_range(company)
    df = pd.DataFrame(r)
    df.to_excel(f"{company}.xlsx")
    return


main()
