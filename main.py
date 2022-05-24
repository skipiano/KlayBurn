import requests
import datetime
import json
from pytz import timezone
import numpy as np
import pandas as pd

if __name__ == "__main__":
    request_link1 = "https://klaytn-proposed-block.s3.ap-northeast-2.amazonaws.com/cypress/"
    request_link2 = "/ProposedBlocks_"

    # initializing dates and times
    f = open("last_updated_date.txt", "r")
    last_updated_date = f.read()
    f.close()
    start_date = datetime.datetime.strptime(last_updated_date, "%Y-%m-%d")
    kst_time_now = datetime.datetime.now(timezone("Asia/Seoul"))
    date_delta = kst_time_now-start_date
    end_date = datetime.datetime(
        kst_time_now.year, kst_time_now.month, kst_time_now.day, 23, 59, 59, 999999) - datetime.timedelta(days=1)
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
    KGC_len = len(addresses)

    # initialize a list for every KGC member of length end_date - start_date with default value 0
    # also initialize two lists of length end_date - start_date
    # one for # of transactions, the other for total gas fee
    # could combined all of these into one including total for the blocks?
    block_list = np.zeros((KGC_len, num_days))
    transaction_list = np.zeros((num_days))
    gas_fee_list = np.zeros((num_days))

    while start_month <= end_month:
        # load all of the csv files for a KGC member, then iterate over

        # response = requests.get(
        #     "https://klaytn-proposed-block.s3.ap-northeast-2.amazonaws.com/cypress/2022Jan/ProposedBlocks_2022Jan_0xed6ee8a1877f9582858dbe2509abb0ac33e5f24e.csv")
        # print(response.text)
