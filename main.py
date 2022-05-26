from pickletools import uint8
import requests
import datetime
import json
from pytz import timezone
import numpy as np
import pandas as pd
from io import StringIO
from dateutil.relativedelta import relativedelta

KST = timezone("Asia/Seoul")


def collect_data_from_row(row, block_list_dict, transaction_list, gas_fee_list, start_date, address_name):
    cur_date = KST.localize(
        datetime.datetime.strptime(row[0][:10], "%Y-%m-%d"))
    if cur_date > start_date:
        date_delta = cur_date - start_date
        days_ind = date_delta.days
        block_list_dict[address_name][days_ind] = block_list_dict[address_name][days_ind] + 1
        block_list_dict["Total"][days_ind] = block_list_dict["Total"][days_ind] + 1
        transaction_list[days_ind] = transaction_list[days_ind] + int(row[1])
        gas_fee_list[days_ind] = gas_fee_list[days_ind] + float(row[2]) - 9.6


def initialize():
    [start_date_raw, start_date, end_date_raw, start_month, end_month,
        addresses, block_list_dict, transaction_list, gas_fee_list] = setup()
    request_link1 = "https://klaytn-proposed-block.s3.ap-northeast-2.amazonaws.com/cypress/"
    request_link2 = "/ProposedBlocks_"

    while start_month <= end_month:
        print(datetime.datetime.strftime(start_month, "%Y-%m"))
        # load all of the csv files for a KGC member, then iterate over
        for member in addresses:
            print(member["name"])
            try:
                response = requests.get(request_link1 + datetime.datetime.strftime(start_month, "%Y%b")
                                        + request_link2 + datetime.datetime.strftime(start_month, "%Y%b") + "_" + member["address"] + ".csv")
                if response.text and response.text[0] != "<":
                    df = pd.read_csv(StringIO(response.text),
                                     usecols=[1, 2, 3])
                    df.apply(lambda row: collect_data_from_row(
                        row, block_list_dict, transaction_list, gas_fee_list, start_date, member["name"]), axis=1)
            except Exception:
                pass
        start_month = start_month + relativedelta(months=1)
    date_range = pd.date_range(start_date_raw, end_date_raw)
    block_df = pd.DataFrame(block_list_dict, index=date_range)
    transaction_df = pd.DataFrame(transaction_list, index=date_range)
    gas_fee_df = pd.DataFrame(gas_fee_list, index=date_range)

    # for now, write to csv files
    block_df.to_csv("block.csv")
    transaction_df.to_csv("transactions.csv")
    gas_fee_df.to_csv("gas_fees.csv")


def update():
    [start_date_raw, start_date, end_date_raw, start_month, end_month,
        addresses, block_list_dict, transaction_list, gas_fee_list] = setup()
    head = {"authority": "api-cypress-v2.scope.klaytn.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "cookie": "connect.sid=s%3ADnToatvPn_oItoyopp4Bh6-Ci6mubFjo.3oAeR3iSKKviMNh1gU9uTPalMim67P47WbvOadgTKA4",
            "referer": "https://scope.klaytn.com/",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-site",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1"}
    request_link1 = "https://api-cypress-v2.scope.klaytn.com/v2/accounts/"
    request_link2 = "/blocks/download?date="
    while start_month <= end_month:
        print(datetime.datetime.strftime(start_month, "%Y-%m"))
        # load all of the csv files for a KGC member, then iterate over
        for member in addresses:
            print(member["name"])
            try:
                response = requests.get(
                    request_link1 + member["address"] + request_link2 + datetime.datetime.strftime(start_month, "%Y-%m"), headers=head)
                if response.text and response.text[0] != "<":
                    df = pd.read_csv(StringIO(response.text),
                                     usecols=[1, 2, 3])
                    df.apply(lambda row: collect_data_from_row(
                        row, block_list_dict, transaction_list, gas_fee_list, start_date, member["name"]), axis=1)
            except Exception:
                pass
        start_month = start_month + relativedelta(months=1)
    date_range = pd.date_range(datetime.datetime(2019, 6, 25), end_date_raw)
    block_df_init = pd.read_csv("block.csv", index_col=0).to_numpy()
    for i in range(len(addresses)):
        block_list_dict[addresses[i]["name"]] = np.concatenate(
            block_df_init[:, i], block_list_dict[addresses[i]["name"]], axis=0)
    block_list_dict["Total"] = np.concatenate(
        block_df_init[:, len(addresses)], block_list_dict["Total"], axis=0)
    block_df = pd.DataFrame(block_list_dict, index=date_range)
    transaction_df = pd.DataFrame(np.concatenate(pd.read_csv(
        "transactions.csv", index_col=0).to_numpy(), transaction_list, axis=0), index=date_range)
    gas_fee_df = pd.DataFrame(np.concatenate(pd.read_csv(
        "gas_fees.csv", index_col=0).to_numpy(), gas_fee_list, axis=0), index=date_range)

    pd.concat([block_df_init, block_df]).to_csv("block.csv")
    pd.concat([transaction_df_init, transaction_df]).to_csv("transactions.csv")
    pd.concat([gas_fee_df_init, gas_fee_df]).to_csv("gas_fees.csv")

    # for now, write to csv files
    block_df.to_csv("block.csv")
    transaction_df.to_csv("transactions.csv")
    gas_fee_df.to_csv("gas_fees.csv")


def setup():
    # initializing dates and times
    f = open("last_updated_date.txt", "r")
    last_updated_date = f.read()
    f.close()
    start_date_raw = datetime.datetime.strptime(last_updated_date, "%Y-%m-%d")
    start_date = KST.localize(start_date_raw)
    kst_time_now = datetime.datetime.now(KST)
    date_delta = kst_time_now-start_date
    end_date_raw = datetime.datetime(
        kst_time_now.year, kst_time_now.month, kst_time_now.day, 23, 59, 59, 999999) - datetime.timedelta(days=1)
    end_date = KST.localize(end_date_raw)

    if end_date < start_date:
        raise Exception("Ran function too early")
    f = open("last_updated_date.txt", "w")
    f.write(datetime.datetime.strftime(kst_time_now, "%Y-%m-%d"))
    f.close()
    start_month = datetime.datetime(start_date.year, start_date.month, 1)
    end_month = datetime.datetime(end_date.year, end_date.month, 1)
    num_days = date_delta.days

    # retrieve names and addresses of KGC
    f = open("address.json")
    addresses = json.load(f)

    # initialize a list for every KGC member of length end_date - start_date with default value 0
    # also initialize two lists of length end_date - start_date
    # one for # of transactions, the other for total gas fee
    # could combined all of these into one including total for the blocks?
    block_list_dict = {}
    for member in addresses:
        block_list_dict[member["name"]] = np.zeros(num_days, dtype=np.uint32)
    block_list_dict["Total"] = np.zeros(num_days, dtype=np.uint32)
    transaction_list = np.zeros(num_days, dtype=[("Transactions", np.uint64)])
    gas_fee_list = np.zeros(num_days, dtype=[("Gas Fees", np.float64)])
    return [start_date_raw, start_date, end_date_raw, start_month, end_month, addresses, block_list_dict, transaction_list, gas_fee_list]


if __name__ == "__main__":
    update()
