import subprocess
import time
import requests

ACTUATOR_TOPIC = 'actuators/#'
POST_URL = 'http://localhost:5000/actuator/PI1/led'

# start mosquitto_sub in a subprocess (will exit after first message due to -C 1)
cmd = ['docker', 'compose', 'exec', 'mosquitto', 'mosquitto_sub', '-t', ACTUATOR_TOPIC, '-C', '1']
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

# Give subscriber a moment to start
time.sleep(0.5)

# Send POST to Flask actuator endpoint
resp = requests.post(POST_URL, json={'state': 'on'}, timeout=5)
print('POST status:', resp.status_code, resp.text)

try:
    out, err = proc.communicate(timeout=5)
    print('mosquitto_sub output:', out.strip())
    if out.strip():
        print('Actuator message received in mosquitto.')
    else:
        print('No message received by mosquitto subscriber.')
except subprocess.TimeoutExpired:
    proc.kill()
    print('Timeout waiting for mosquitto message.')
