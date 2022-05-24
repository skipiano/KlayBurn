import requests
import datetime
from pytz import timezone

if __name__ == "__main__":
    request_link1 = "https://klaytn-proposed-block.s3.ap-northeast-2.amazonaws.com/cypress/"
    request_link2 = "/ProposedBlocks_"
    # initializing dates and times
    f = open("last_updated_date.txt", "r")
    last_updated_date = f.read()
    f.close()
    start_date = datetime.datetime.strptime(last_updated_date, "%Y-%m-%d")
    kst_time_now = datetime.datetime.now(timezone("Asia/Seoul"))
    end_date = datetime.datetime(
        kst_time_now.year, kst_time_now.month, kst_time_now.day) - datetime.timedelta(days=1)
    f = open("last_updated_date.txt", "w")
    f.write(datetime.datetime.strftime(kst_time_now, "%Y-%m-%d"))
    f.close()
    start_month = datetime.datetime(start_date.year, start_date.month, 1)
    end_month = datetime.datetime(end_date.year, end_date.month, 1)
    # debug
    print(start_date)
    print(end_date)
    print(start_month)
    print(end_month)

    # response = requests.get(
    #     "https://klaytn-proposed-block.s3.ap-northeast-2.amazonaws.com/cypress/2022Jan/ProposedBlocks_2022Jan_0xed6ee8a1877f9582858dbe2509abb0ac33e5f24e.csv")
    # print(response.text)
