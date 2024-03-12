from mpi4py import MPI
import ijson
import time
from datetime import datetime
from collections import defaultdict

def process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day):
    created_at = item.get("doc", {}).get("data", {}).get("created_at", "")
    try:
        date_object = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return
    
    day = date_object.date()
    hour = date_object.hour
    activity_by_hour[hour] += 1
    activity_by_day[day] += 1
    
    sentiment = item.get("doc", {}).get("data", {}).get("sentiment")
    try:
        sentiment = float(sentiment)
    except (TypeError, ValueError):
        return

    sentiment_by_hour[hour] += sentiment
    sentiment_by_day[day] += sentiment


def distribute_and_process_items(filename, rank, size, comm):
    sentiment_by_hour = defaultdict(int)
    sentiment_by_day = defaultdict(int)
    activity_by_hour = defaultdict(int)
    activity_by_day = defaultdict(int)

    if rank == 0:
        with open(filename, "rb") as file:
            items = list(ijson.items(file, "rows.item"))
            for i, item in enumerate(items):
                target_rank = i % (size - 1) + 1
                if target_rank != 0:
                    comm.send(item, dest=target_rank, tag=0)
                # else:
                #     process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day)
            for target_rank in range(1, size):
                comm.send(None, dest=target_rank, tag=0)
    else:
        while True:
            item = comm.recv(source=0, tag=0)
            if item is None:
                break
            process_item(item, sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day)

    return sentiment_by_hour, sentiment_by_day, activity_by_hour, activity_by_day

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    filename = "data/twitter-50mb.json"

    start_time = time.time()

    local_results = distribute_and_process_items(filename, rank, size, comm)
    
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

if __name__ == "__main__":
    main()
