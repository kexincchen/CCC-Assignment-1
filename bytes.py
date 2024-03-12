from mpi4py import MPI
from collections import defaultdict
import time
import ijson
from datetime import datetime
import os
import re

DATE_PATTERN = re.compile(r'"created_at":\s*"([^"]+)"')
SENTIMENT_PATTERN = re.compile(r'"sentiment":\s*(-?\d+\.\d+)')
        
def find_adjustment_backward(filename, position, file_size):
    with open(filename, "rb") as f:
        if position == 0 or position == file_size:
            return position  # No adjustment needed at the extremes of the file
        f.seek(position)
        while position > 0:
            f.seek(position - 1)
            if f.read(1) == "\n":
                break
            position -= 1
        return position

def process_file_block(filename, start, end):
    """Process the file block assigned to this MPI process."""
    with open(filename, "r") as f:
        f.seek(start)
        while f.tell() < end:
            line = f.readline()
            # Process the line here
            print(line)  # Example action
            print("++++++++++++++++++")


def convert_to_valid_json(bounded_json):
    pass


def extract_data_from_json(valid_json):
    pass


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
    # filename = "data/one.json"
    filename = "data/small.json"
    start_time = time.time()

    print("RANK: " + str(rank))
    print("SIZE: " + str(size))

    file_size = file_size = os.path.getsize(filename)
    offset = find_first_line_offset(filename)
    # make sure no bytes are missed
    block_size = (file_size - offset) // size

    print("BLOCK_SIZE: " + str(block_size))
    print("-----------------")

    # Calculate each process's start position
    start_pos = offset + rank * block_size
    end_pos = start_pos + block_size if rank < size - 1 else file_size
    start_pos = find_adjustment_backward(filename, start_pos, file_size)
    end_pos = find_adjustment_backward(filename, end_pos, file_size)

    # process_file_block(filename, start_pos, block_size)
    process_file_block(filename, start_pos, end_pos)

    print("-------END-------")


if __name__ == "__main__":
    main()
