from ast import Str
import requests
import datetime
import json
from pytz import timezone
import numpy as np
import pandas as pd
from io import StringIO
from dateutil.relativedelta import relativedelta

if __name__ == "__main__":
    # initializing dates and times
    f = open("last_updated_date.txt", "r")
    last_updated_date = f.read()
    f.close()
    start_date = timezone(
        "Asia/Seoul").localize(datetime.datetime.strptime(last_updated_date, "%Y-%m-%d"))
    kst_time_now = datetime.datetime.now(timezone("Asia/Seoul"))
    date_delta = kst_time_now-start_date
    end_date = timezone("Asia/Seoul").localize(datetime.datetime(
        kst_time_now.year, kst_time_now.month, kst_time_now.day, 23, 59, 59, 999999) - datetime.timedelta(days=1))
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

    # debug
    print(KGC_len)
    print(addresses[2])

    # initialize a list for every KGC member of length end_date - start_date with default value 0
    # also initialize two lists of length end_date - start_date
    # one for # of transactions, the other for total gas fee
    # could combined all of these into one including total for the blocks?
    block_list = np.zeros((KGC_len, num_days))
    transaction_list = np.zeros((num_days))
    gas_fee_list = np.zeros((num_days))
    request_link1 = "https://klaytn-proposed-block.s3.ap-northeast-2.amazonaws.com/cypress/"
    request_link2 = "/ProposedBlocks_"

    while start_month <= end_month:
        # load all of the csv files for a KGC member, then iterate over
        for i in range(KGC_len):
            response = requests.get(request_link1 + datetime.datetime.strftime(start_month, "%Y%b")
                                    + request_link2 + datetime.datetime.strftime(start_month, "%Y%b") + "_" + addresses[i]['address'] + ".csv")
            if response.text and response.text[0] != "<":
                df = pd.read_csv(StringIO(response.text))

        start_month = start_month + relativedelta(months=1)
