import random
import time


def run_dus1_simulator(delay, callback, stop_event, start_distance=50):
    distance = start_distance
    while not stop_event.is_set():
        distance += random.uniform(-2, 2)
        distance = max(2, min(distance, 400))  # cm bounds
        callback(distance)
        time.sleep(delay)
