import time
import random


def run_pir_simulator(delay, callback, stop_event):
    # Nasumično pali/gasi detekciju kretanja
    state = False
    while not stop_event.is_set():
        # Ređe menjaj stanje
        if random.random() > 0.8:
            state = not state
            callback(state)
        time.sleep(delay)
