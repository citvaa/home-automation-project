import json
p = 'docker/grafana/dashboards/home-automation.json'
s = open(p, 'r', encoding='utf-8').read()
try:
    json.loads(s)
    print('OK')
except json.JSONDecodeError as e:
    print('Err:', e)
    pos = e.pos
    print('pos:', pos)
    ctx = s[max(0, pos-60):pos+60]
    print('context repr:', repr(ctx))
    # Print nearby lines for human inspection
    lines = s.splitlines()
    for i in range(max(0, e.lineno-3), e.lineno+2):
        print(i+1, lines[i])
