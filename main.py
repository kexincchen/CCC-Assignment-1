from mpi4py import MPI
import ijson
import time

# from dateutil import parser
from datetime import datetime
from collections import defaultdict


def process_item(item):
    # 处理单个数据项的逻辑
    print(item)


def process_chunk(data_chunk):
    # 用于处理每个数据片段的函数
    for item in data_chunk:
        process_item(item)


# Start timing
start_time = time.time()

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# filename = "data/one.json"
filename = "data/twitter-50mb.json"

# Initialize data structures for aggregation
sentiment_by_hour = defaultdict(int)
sentiment_by_day = defaultdict(int)
activity_by_hour = defaultdict(int)
activity_by_day = defaultdict(int)


"""
'data/one.json' contains:
1. Normal record
2. No 'value'
3. No 'sentiment'
"""

with open(filename, "rb") as file:
    # Parse and iterate through the array of items under 'rows'
    for item in ijson.items(file, "rows.item"):
        sentiment = item.get("doc", {}).get("data", {}).get("sentiment", 0)
        try:
            sentiment = float(sentiment)
        except TypeError:
            # print(f"ERROR: Invalid sentiment: {sentiment}")
            continue

        created_at = item.get("doc", {}).get("data", {}).get("created_at", "")
        if created_at == "":
            continue
        # date_object = parser.parse(created_at)
        try:
            date_object = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            # print("ERROR: Date format is not correct\n" + created_at)
            continue

        day = date_object.date()
        hour = date_object.hour

        # Aggregate sentiment and activity
        sentiment_by_hour[hour] += sentiment
        sentiment_by_day[day] += sentiment
        activity_by_hour[hour] += 1
        activity_by_day[day] += 1

        # print(sentiment)
        # # print(created_at)
        # print(date_object.date())
        # print(date_object.hour)
        # print()
# process_large_json(filename)


# Identifying peaks
happiest_hour = max(sentiment_by_hour, key=sentiment_by_hour.get)
happiest_day = max(sentiment_by_day, key=sentiment_by_day.get)
most_active_hour = max(activity_by_hour, key=activity_by_hour.get)
most_active_day = max(activity_by_day, key=activity_by_day.get)

# End timing
end_time = time.time()

# Calculate and print the execution time
execution_time = end_time - start_time
print(f"Execution time: {execution_time} seconds")

print("=========SUMMARY=========")
print(f"Happiest hour: {happiest_hour}")
print(f"Happiest day: {happiest_day}")
print(f"Most active hour: {most_active_hour}")
print(f"Most active day: {most_active_day}")

print("Rank: " + str(rank))
print("Size: " + str(size))

# =================================================================
# if rank == 0:
#     # 主节点负责读取JSON文件和分配数据片段的元信息给各个节点
#     # 注意：这里需要一个高效的方法来确定如何分割JSON文件
#     # 示例代码中省略了这一复杂的分割逻辑
#     chunks_info = [...]  # 假设这是数据片段的元信息列表
# else:
#     chunks_info = None

# # 分发数据片段的元信息
# chunk_info = comm.scatter(chunks_info, root=0)

# # 根据元信息，各个节点独立读取和处理自己的数据片段
# # 这可能需要使用文件的seek方法定位到片段的开始位置，然后使用ijson逐项读取和处理数据
# # 示例代码省略了具体的读取逻辑
# data_chunk = read_data_chunk_based_on_info(chunk_info)  # 需要实现这个函数
# process_chunk(data_chunk)
