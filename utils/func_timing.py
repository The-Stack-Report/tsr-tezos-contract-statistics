from datetime import datetime, timedelta
from utils.verbose_timedelta import verbose_timedelta

start_time = False
end_time = False

latest_name = False

def start_timer(name="func timing name"):
    global start_time, end_time, latest_name
    latest_name = name
    start_time = datetime.now()
    print(f"Starting timing function: {name} at time:")
    print(start_time)


def end_timer():
    global start_time, end_time, latest_name
    end_time = datetime.now()
    duration = end_time - start_time
    duration_verbose = verbose_timedelta(duration)
    print(f"Ending timing function: {latest_name} at time:")
    print(f"{start_time} to {end_time} with duration: {duration_verbose}")
