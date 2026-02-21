import base64
import random
import time


def _random_color():
    return f"#{random.randint(0, 0xFFFFFF):06x}"


def _build_svg_frame(width=320, height=240):
    bg = _random_color()
    fg = _random_color()
    accent = _random_color()
    cx = random.randint(40, width - 40)
    cy = random.randint(40, height - 40)
    r = random.randint(20, 60)
    rect_w = random.randint(60, 140)
    rect_h = random.randint(40, 120)
    rect_x = random.randint(10, width - rect_w - 10)
    rect_y = random.randint(10, height - rect_h - 10)
    svg = (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}'>"
        f"<rect width='100%' height='100%' fill='{bg}'/>"
        f"<circle cx='{cx}' cy='{cy}' r='{r}' fill='{fg}' opacity='0.85'/>"
        f"<rect x='{rect_x}' y='{rect_y}' width='{rect_w}' height='{rect_h}' fill='{accent}' opacity='0.7'/>"
        f"</svg>"
    )
    return svg


def run_webc_simulator(delay, callback, stop_event, frame_bytes=512, mime="image/svg+xml"):
    while not stop_event.is_set():
        svg = _build_svg_frame()
        encoded = base64.b64encode(svg.encode("utf-8")).decode("ascii")
        payload = {
            "data": f"b64:{encoded}",
            "mime": mime,
            "bytes": len(encoded),
        }
        callback(payload)
        time.sleep(delay)
