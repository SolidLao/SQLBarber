import time
import os
from functools import wraps
from pathlib import Path

def timing_decorator(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # Access the folder_name from the class instance
        folder_name = self.task_name
        
        # Create the log directory under SQLBarber/logs/<folder_name>
        root_path = Path(__file__).resolve().parents[2]
        log_dir = f"{root_path}/outputs/intermediate/logs/{folder_name}"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Define the log file path
        log_file = os.path.join(log_dir, f"cost.log")

        # Record start time
        start_time = time.time()
        result = func(self, *args, **kwargs)
        end_time = time.time()

        # Calculate the time taken and write it to the log file
        time_taken = (end_time - start_time) / 60
        with open(log_file, "a") as log:
            log.write(f"It takes {time_taken} minutes for {func.__name__}\n")

        return result
    return wrapper