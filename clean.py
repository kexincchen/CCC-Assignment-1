from mpi4py import MPI
from collections import defaultdict
import time
from datetime import datetime
import os
import re

DATE_PATTERN = re.compile(r'"created_at":\s*"([^"]+)"')
SENTIMENT_PATTERN = re.compile(r'"sentiment":\s*(?:{"score":\s*)?([-]?\d*\.?\d+)')
        
def find_adjustment_backward(filename, position, file_size):
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
        acc = start 
        while acc < end:
            line = f.readline()
            acc += len(line)
            line = line.decode("utf-8")
            if line.strip() == "":
                break

            process_line(line, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day)
    
    return sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day

def process_line(line, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day):
    # Quick extraction example without full JSON parsing
    try:
        created_at_match = DATE_PATTERN.search(line)
        sentiment_match = SENTIMENT_PATTERN.search(line)
        if created_at_match: 
            created_at = created_at_match.group(1)
            created_at = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return 
        
        if sentiment_match:
            
            sentiment = float(sentiment_match.group(1))
            # print(f"Sentiment: {sentiment} ")
        else:
            sentiment = 0
            
        day = created_at.date()
        hour = created_at.hour

        sentiment_by_hour[hour] += sentiment
        sentiment_by_day[day] += sentiment
        activity_by_hour[hour] += 1
        activity_by_day[day] += 1
        
    except Exception as e:
        print(f"Error processing line: {e}")

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
        file.readline()
        offset = file.tell()
    return offset

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    filename = "twitter-100gb.json"
    start_time = time.time()

    file_size = os.path.getsize(filename)
    offset = find_first_line_offset(filename)

    # make sure no bytes are missed
    block_size = (file_size - offset) // size

    # calculate each process's start position
    start_pos = offset + rank * block_size
    end_pos = start_pos + block_size if rank < size - 1 else file_size
    
    if rank > 0:
        start_pos = find_adjustment_backward(filename, start_pos, file_size)
    end_pos = find_adjustment_backward(filename, end_pos, file_size)
    
    # Process the file block and gather local results at the root process
    local_results = process_file_block(filename, start_pos, end_pos)

    # Gather all local_results at the root process
    all_local_results = comm.gather(local_results, root=0)

    if rank == 0:
        # Initialize global_results with the same structure as local_results but empty
        global_results = [defaultdict(int) for _ in range(4)]
        
        # Combine all local results into global_results
        for other_local_results in all_local_results:
            for j in range(4):
                for key, value in other_local_results[j].items():
                    global_results[j][key] += value
        
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

if __name__ == "__main__":
    main()
