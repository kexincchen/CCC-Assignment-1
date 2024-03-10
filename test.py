import json
import pandas as pd
import time
# from pandas.io.json import json_normalize


def convert_to_12h_format(hour):
    if hour == 0 or hour == 24:
        return "am"
    elif hour == 12:
        return "pm"
    elif hour < 12:
        return f"am"
    else:
        return f"PM"


# Start timing
start_time = time.time()

# Load the JSON data
with open("data/twitter-50mb.json") as f:
    data = json.load(f)
    tweets_df = pd.json_normalize(data["rows"])

    tweets_df["doc.data.created_at"] = pd.to_datetime(tweets_df["doc.data.created_at"])

    tweets_df["hour"] = tweets_df["doc.data.created_at"].dt.hour
    tweets_df["day"] = tweets_df["doc.data.created_at"].dt.date

    # happiest hour
    happiest_hour = tweets_df.groupby("hour")["doc.data.sentiment"].sum().idxmax()
    happiest_hour = int(happiest_hour)
    hourly_sentiment_sums = tweets_df.groupby("hour")["doc.data.sentiment"].sum()
    happiest_hour_sum = hourly_sentiment_sums[happiest_hour]

    # happiest day
    happiest_day = tweets_df.groupby("day")["doc.data.sentiment"].sum().idxmax()
    happiest_day_f = happiest_day.strftime(f"%dth %B of %Y")
    daily_sentiment_sums = tweets_df.groupby("day")["doc.data.sentiment"].sum()
    happiest_day_sum = daily_sentiment_sums[happiest_day]

    # most active hour ever
    # 先将数据按照day分开，找到每天最活跃的hour并且记录value counts，然后每天最活跃的hour value counts进行比较，
    # 选出最active hour ever并且输出hour，date，和相应的value count
    # 分组以天和小时为单位，并计算每组的推文数量
    counts_per_day_hour = (
        tweets_df.groupby(["day", "hour"]).size().reset_index(name="counts")
    )
    # 分离日期和小时, 每天最活跃的小时及其推文数量
    max_counts_per_day = counts_per_day_hour.loc[
        counts_per_day_hour.groupby("day")["counts"].idxmax()
    ]
    # 找到所有天中最活跃的小时
    most_active_record = max_counts_per_day.loc[max_counts_per_day["counts"].idxmax()]
    most_active_hour = int(most_active_record["hour"])
    co_day = most_active_record["day"]
    co_day_f = co_day.strftime(f"%dth %B of %Y")
    most_active_count = most_active_record["counts"]

    # most active day ever
    most_active_day = tweets_df["day"].value_counts().idxmax()
    most_active_day_count = tweets_df["day"].value_counts()[most_active_day]
    most_active_day = most_active_day.strftime(f"%dth %B of %Y")

    # End timing
    end_time = time.time()

    # Calculate and print the execution time
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds")

    print(
        f"• the happiest hour ever: {happiest_hour}-"
        + f"{(happiest_hour + 1) % 24}{convert_to_12h_format(happiest_hour+1)} with an overall sentiment score of {happiest_hour_sum:.2f}"
    )

    print(
        f"• the happiest day ever: {happiest_day_f} was the happiest day with an overall sentiment score of {happiest_day_sum:.2f}"
    )

    print(
        f"• the most active hour ever: {most_active_hour}-"
        + f"{(most_active_hour + 1) % 24}{convert_to_12h_format(most_active_hour+1)} on {co_day_f} had the most tweets (#{most_active_count})"
    )  # Replace '...' with the number of tweets if available

    print(
        f"• the most active day ever: {most_active_day} had the most tweets (#{most_active_day_count})"
    )
