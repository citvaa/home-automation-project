import time
import itertools


def run_ds1_simulator(delay, callback, stop_event):
    # Alternira stanje pritisnuto/nepritisnuto
    for state in itertools.cycle([False, True]):
        if stop_event.is_set():
            break
        callback(state)
        time.sleep(delay)
