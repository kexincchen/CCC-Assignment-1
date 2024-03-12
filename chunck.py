from mpi4py import MPI
import ijson
from datetime import datetime
from collections import defaultdict
import time

def get_total_rows(filename):
    with open(filename, "rb") as f:
        items = ijson.items(f, "rows.item")
        return sum(1 for _ in items)

def process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day):
    sentiment = item.get("doc", {}).get("data", {}).get("sentiment", 0)
    try:
        sentiment = float(sentiment)
    except (TypeError, ValueError):
        sentiment = 0

    created_at = item.get("doc", {}).get("data", {}).get("created_at", "")
    try:
        date_object = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return

    day = date_object.date()
    hour = date_object.hour

    sentiment_by_hour[hour] += sentiment
    sentiment_by_day[day] += sentiment
    activity_by_hour[hour] += 1
    activity_by_day[day] += 1

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    filename = "data/twitter-50mb.json"
    start_time = time.time()

    if rank == 0:
        total_rows = get_total_rows(filename)
        rows_per_worker = total_rows // (size - 1)
        # print(f"SIZE: {size}")
    else:
        rows_per_worker = None

    rows_per_worker = comm.bcast(rows_per_worker, root=0)

    local_results = [defaultdict(int) for _ in range(4)]  
    # This will hold sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day

    if rank != 0:
        # print(f"Rank: {rank}")
        with open(filename, "rb") as f:
            items = ijson.items(f, "rows.item")
            for i, item in enumerate(items):
                if i // rows_per_worker == rank - 1:
                    # print(f"     {i}")
                    process_item(item, *local_results)
                elif i // rows_per_worker > rank - 1:
                    break

        # Send local results back to the master process
        # print(local_results)
        comm.send(local_results, dest=0, tag=1)

    if rank == 0:
        global_results = [defaultdict(int) for _ in range(4)]
        for i in range(1, size):
            worker_results = comm.recv(source=i, tag=1)
            for j in range(4):
                for key, value in worker_results[j].items():
                    global_results[j][key] += value

        # Calculate and print summary only in the master process
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
