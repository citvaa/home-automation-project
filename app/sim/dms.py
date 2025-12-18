import time
import itertools


def run_dms_simulator(delay, callback, stop_event):
    for state in itertools.cycle([False, True]):
        if stop_event.is_set():
            break
        callback(state)
        time.sleep(delay)
