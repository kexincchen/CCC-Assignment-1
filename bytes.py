from mpi4py import MPI
from collections import defaultdict
import time
import ijson
import json
from datetime import datetime
import os
import re

# DATE_PATTERN = re.compile(r'"created_at":\s*"([^"]+)"')
# SENTIMENT_PATTERN = re.compile(r'"sentiment":\s*(-?\d+\.\d+)')
        
def find_adjustment_backward(filename, position, file_size):
    # with open(filename, "rb") as f:
    #     # if position == 0 or position == file_size:
    #     if position == 0:
    #         return position  # No adjustment needed at the extremes of the file
    #     f.seek(position)
    #     while position > 0:
    #         f.seek(position - 1)
    #         if f.read(1) == "\n":
    #             break
    #         position -= 1
    #     return position
    with open(filename, 'rb') as f:
        f.seek(position)
        if position != 0:  # If not the start of the file, find the start of the next line
            f.readline()  # Read and discard partial line
        adjusted_position = f.tell()  # New position is at the start of the next complete line
    return adjusted_position

def process_file_block(filename, start, end):
    """Process the file block assigned to this MPI process."""
    
    sentiment_by_hour = defaultdict(int)
    sentiment_by_day = defaultdict(int)
    activity_by_hour = defaultdict(int)
    activity_by_day = defaultdict(int)
    
    with open(filename, "rb") as f:
        f.seek(start)
        # valid_json = convert_to_valid_json(f.read(end-start+1))
        # print("READ: ", str(end-start))
        bounded_json = f.read(end-start).decode('utf-8')
        # stripped = bounded_json
        stripped = bounded_json.strip().rstrip("{}]}").rstrip().rstrip(",")
        stripped = "[" + stripped + "]"
        # print("=======STRIP======")
        # print(stripped)
        
        # print("sentiment: " + str(valid_json))
        try:
            # valid_json = json.loads(stripped)
            # print(valid_json.get("id")) 
            # return valid_json
            items = list(ijson.items(stripped, "item"))
            # print(stripped)
            # print("++++++++++++++++++")
            for i, item in enumerate(items):
                process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day)
        # except json.decoder.JSONDecodeError:
        except ijson.common.IncompleteJSONError:
            print("Error: JSON decoding\n" + stripped)
            # print("Error: JSON decoding\n")
            
    
        #     print(item)
        # while f.tell() < end:
        #     line = f.readline()
        #     # Process the line here
        #     print(line)  # Example action
        #     print("++++++++++++++++++")
        #     valid_json = convert_to_valid_json(line)
        #     if valid_json is None:
        #         continue
            # process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day)
        #     process_line(line, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day)
            
    return sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day


# def convert_to_valid_json(bounded_json):
#     stripped = bounded_json.strip().rstrip("\n{}]}").rstrip(",")
#     stripped = "[" + stripped + "]"
#     # print("=======STRIP======")
#     # print(stripped)
    
#     # print("sentiment: " + str(valid_json))
#     try:
#         valid_json = json.loads(stripped)
#         # print(valid_json.get("id")) 
#         return valid_json
#     except json.decoder.JSONDecodeError:
#         print("Error: JSON decoding\n" + stripped)
#         return None

# def process_line(line, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day):
#     # Quick extraction example without full JSON parsing
#     try:
#         created_at_match = DATE_PATTERN.search(line)
#         sentiment_match = SENTIMENT_PATTERN.search(line)
#         if created_at_match: 
#             created_at = created_at_match.group(1)
#             created_at = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
#             # print(created_at)
#         else:
#             return 
#         if sentiment_match:
#             sentiment = float(sentiment_match.group(1))
#             # print(sentiment)
#         else:
#             sentiment = 0
            
#         day = created_at.date()
#         hour = created_at.hour

#         sentiment_by_hour[hour] += sentiment
#         sentiment_by_day[day] += sentiment
#         activity_by_hour[hour] += 1
#         activity_by_day[day] += 1
        
#     except Exception as e:
#         print(f"Error processing line: {e}")


def process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day):
    created_at = item.get("doc", {}).get("data", {}).get("created_at", "")
    try:
        date_object = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        print("Invalid date" + created_at)
        return
    
    sentiment = item.get("doc", {}).get("data", {}).get("sentiment", 0)
    try:
        sentiment = float(sentiment)
    except (TypeError, ValueError):
        sentiment = 0

    day = date_object.date()
    hour = date_object.hour

    sentiment_by_hour[hour] += sentiment
    sentiment_by_day[day] += sentiment
    activity_by_hour[hour] += 1
    activity_by_day[day] += 1


# def process_file_block(filename, start, block_size):
#     """Process the file block assigned to this MPI process."""
#     with open(filename, "r") as f:
#         f.seek(start)
#         block = f.read(block_size)
#         # Process the line here
#         print(block)  # Example action
#         print("++++++++++")
# block_bytes = block.encode('utf-8')

# items = ijson.items(block_bytes, "")
# for i, item in enumerate(items):
#     print(item)
# print(f"     {i}")
# process_item(item, *local_results)


def find_first_line_offset(file_path):
    """
    Find the byte offset of the first line in a file.

    Args:
        file_path (str): Path to the file.

    Returns:
        int: Byte offset of the end of the first line.
    """
    offset = 0
    with open(file_path, "rb") as file:
        while True:
            byte = file.read(1)
            offset += 1
            if byte == b"\n" or byte == b"":
                break
    return offset

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # print("Number of cpu : ", multiprocessing.cpu_count())

    # filename = "data/twitter-1mb.json"
    filename = "data/twitter-50mb.json"
    # filename = "data/one.json"
    # filename = "data/small.json"
    start_time = time.time()

    # print("RANK: " + str(rank))
    # print("SIZE: " + str(size))

    file_size = file_size = os.path.getsize(filename)
    offset = find_first_line_offset(filename)
    # offset = 0
    # make sure no bytes are missed
    block_size = (file_size - offset) // size

    # print("BLOCK_SIZE: " + str(block_size))
    # print("-----------------")

    # Calculate each process's start position
    start_pos = offset + rank * block_size
    end_pos = start_pos + block_size if rank < size - 1 else file_size
    
    if rank > 0:
        start_pos = find_adjustment_backward(filename, start_pos, file_size)
    end_pos = find_adjustment_backward(filename, end_pos, file_size)

    # print("RANK: " + str(rank))
    # print("BLOCK: " + str(block_size))
    # print("START: " + str(start_pos))
    # print("END: " + str(end_pos))
    # print("==================")
    # process_file_block(filename, start_pos, block_size)
    local_results = process_file_block(filename, start_pos, end_pos)
    if rank == 0:
        global_results = [defaultdict(int) for _ in range(4)]
        # global_results[0].update(local_results[0])
        # global_results[1].update(local_results[1])
        # global_results[2].update(local_results[2])
        # global_results[3].update(local_results[3])
        for i in range(1, size):
            local_results = comm.recv(source=i, tag=1)
            for j in range(4):
                for key, value in local_results[j].items():
                    global_results[j][key] += value
    else:
        comm.send(local_results, dest=0, tag=1)

    if rank == 0:
        happiest_hour = max(global_results[0], key=global_results[0].get)
        happiest_day = max(global_results[1], key=global_results[1].get)
        most_active_hour = max(global_results[2], key=global_results[2].get)
        most_active_day = max(global_results[3], key=global_results[3].get)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")

        print("=========SUMMARY=========")
        print(f"The happiest hour ever: {happiest_hour} with a sentiment score of {global_results[0][happiest_hour]}")
        print(f"The happiest day ever: {happiest_day} with a sentiment score of {global_results[1][happiest_day]}")
        print(f"The most active hour ever: {most_active_hour} with {global_results[2][most_active_hour]} tweets")
        print(f"The most active day ever: {most_active_day} with {global_results[3][most_active_day]} tweets")

    # print("-------END-------")


if __name__ == "__main__":
    main()
