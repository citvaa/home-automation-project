import json
import queue
import threading
import time
import re
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple, cast

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

try:
    from influxdb_client.client.influxdb_client import InfluxDBClient
    from influxdb_client.client.write.point import Point
    from influxdb_client.domain.write_precision import WritePrecision
except Exception:
    InfluxDBClient = None
    Point = None
    WritePrecision = None

import logging
from common.logging import get_logger
logger = get_logger(__name__)

from config.settings import Config, load_config
from server.utils import normalize_value, parse_timestamp


@dataclass
class SecurityState:
    mode: str = "DISARMED"  # DISARMED | ARMING | ARMED | ALARM
    occupancy: int = 0
    alarm_reason: Optional[str] = None
    ds1_active_since: Optional[datetime] = None
    dus1_history: Deque[Tuple[datetime, float]] = field(default_factory=deque)
    last_dms_state: Optional[bool] = None
    arming_timer: Optional[threading.Timer] = None
    led_timer: Optional[threading.Timer] = None
    ds1_entry_timer: Optional[threading.Timer] = None
    dms_digit_count: int = 0
    dms_digits: List[int] = field(default_factory=list)
    dms_digit_timer: Optional[threading.Timer] = None


class MqttInfluxService:
    """Service that subscribes to MQTT topics and writes incoming messages to InfluxDB.

    - MQTT callbacks put messages into internal queue
    - A writer worker batches messages and writes them to InfluxDB
    - Also exposes a .publish_actuator(...) helper to publish actuator messages
    """

    def __init__(self, cfg: Config, mqtt_client: Optional[Any] = None, influx_client: Optional[Any] = None):
        self.cfg = cfg
        self._mqtt_client = mqtt_client
        self._internal_mqtt = mqtt_client is None
        self._influx_client = influx_client
        self._internal_influx = influx_client is None

        # queue stores tuples of (topic, payload)
        self._queue: "queue.Queue[Tuple[str, Any]]" = queue.Queue()
        # actuator publish queue (topic, payload_json, qos)
        self._actuator_queue: "queue.Queue[Tuple[str, str, int]]" = queue.Queue(maxsize=1000)

        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        # publisher and monitoring threads
        self._publisher_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None

        # write API (set in start)
        self._write_api: Any = None

        # config for writer batching
        self._batch_size = cfg.batch.batch_size
        self._max_interval = cfg.batch.max_interval_seconds

        # monitoring / thresholds
        self._monitor_interval = 10  # seconds between health logs
        self._max_queue_size_warn = 500
        self._publish_timeout_warning = 2.0  # seconds
        self._write_timeout_warning = 2.0  # seconds (log if write takes longer)    

        # security state tracking (per device)
        self._security_lock = threading.Lock()
        self._security_states: Dict[str, SecurityState] = {}

    def start(self):
        # setup influx client
        if self._internal_influx:
            if InfluxDBClient is None:
                raise RuntimeError("influxdb-client not installed")
            self._influx_client = InfluxDBClient(url=self.cfg.server.influx.url, token=self.cfg.server.influx.token, org=self.cfg.server.influx.org)
        # At this point _influx_client should be set (either provided or created). Guard and cast for type-checkers.
        if self._influx_client is None:
            raise RuntimeError("InfluxDB client instance not available")
        # cast for static checkers that may not infer non-None above
        self._write_api = cast(Any, self._influx_client).write_api()
        # ensure actuator bucket exists (best-effort)
        self._ensure_buckets()

        # setup mqtt client
        if self._internal_mqtt:
            if mqtt is None:
                raise RuntimeError("paho-mqtt not installed")
            self._mqtt_client = mqtt.Client()
            if self.cfg.mqtt.username:
                self._mqtt_client.username_pw_set(self.cfg.mqtt.username, self.cfg.mqtt.password)

            self._mqtt_client.on_connect = self._on_connect
            self._mqtt_client.on_message = self._on_message

            try:
                print(f"[MQTT] Attempting to connect to {self.cfg.mqtt.broker}:{self.cfg.mqtt.port}...")
                self._mqtt_client.connect(self.cfg.mqtt.broker, self.cfg.mqtt.port)
                self._mqtt_client.loop_start()
                print(f"[MQTT] loop_start() called - waiting for connection callback...")
            except Exception as e:
                print(f"[MQTT] *** CONNECTION FAILED: {e} ***")
                logger.warning("MQTT connect failed: %s", e)
        else:
            # user provided client must have on_message attribute set or we set ours
            if self._mqtt_client is not None:
                try:
                    self._mqtt_client.on_message = self._on_message
                except Exception:
                    pass

        # start writer thread
        self._worker_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self._worker_thread.start()
        # start actuator publisher thread
        self._publisher_thread = threading.Thread(target=self._publisher_loop, daemon=True)
        self._publisher_thread.start()
        # start monitor thread
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("MqttInfluxService started")

    def stop(self):
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        # stop publisher
        if self._publisher_thread:
            self._publisher_thread.join(timeout=5)
        # stop monitor
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

        # drain actuator queue trying to publish remaining messages (best-effort)
        try:
            while not self._actuator_queue.empty():
                try:
                    topic, data, qos = self._actuator_queue.get_nowait()
                    if self._mqtt_client is not None:
                        try:
                            self._mqtt_client.publish(topic, data, qos=qos)
                        except Exception:
                            pass
                except queue.Empty:
                    break
        except Exception:
            pass

        if self._internal_mqtt and self._mqtt_client is not None:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass
        if self._internal_influx and self._influx_client is not None:
            try:
                self._influx_client.close()
            except Exception:
                pass
        logger.info("MqttInfluxService stopped")

    # MQTT callbacks
    def _on_connect(self, client, userdata, flags, rc):
        print(f"[MQTT] *** CONNECTED (rc={rc}) ***")
        logger.info("MQTT connected (rc=%s)", rc)
        for topic in self.cfg.server.subscribe_topics:
            client.subscribe(topic)
            print(f"[MQTT] Subscribed to {topic}")
            logger.info("subscribed to %s", topic)

    def _on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
        except Exception as e:
            logger.warning("Failed to decode message on %s: %s", getattr(msg, 'topic', '<unknown>'), e)
            return
        # log received message for debugging
        try:
            sensor_type = data.get("sensor_type", "?")
            print(f"[MQTT] Received {sensor_type} on {msg.topic}")
            logger.debug("received message on %s: %s", msg.topic, data)
        except Exception:
            pass
        # push tuple(topic, data)
        # type: ignore[call-arg] - queue typed to hold tuples
        self._queue.put((msg.topic, data))

    # Writer thread
    def _writer_loop(self):
        buffer: List[Dict] = []
        last_flush = time.time()
        while not self._stop_event.is_set():
            try:
                topic, data = self._queue.get(timeout=0.5)
                # Expand if envelope with 'readings'
                if isinstance(data, dict) and "readings" in data and isinstance(data["readings"], list):
                    for r in data["readings"]:
                        try:
                            if isinstance(r, dict):
                                self._process_security_reading(r)
                        except Exception:
                            logger.debug("security processing failed for reading", exc_info=True)
                        entry = {
                            "topic": topic,
                            "payload": r,
                        }
                        buffer.append(entry)
                else:
                    try:
                        if isinstance(data, dict):
                            self._process_security_reading(data)
                    except Exception:
                        logger.debug("security processing failed for message", exc_info=True)
                    buffer.append({"topic": topic, "payload": data})

                if len(buffer) >= self._batch_size:
                    self._flush_buffer(buffer)
                    buffer = []
                    last_flush = time.time()
            except queue.Empty:
                now = time.time()
                if buffer and (now - last_flush) >= self._max_interval:
                    self._flush_buffer(buffer)
                    buffer = []
                    last_flush = now
                continue
        # flush remaining
        if buffer:
            self._flush_buffer(buffer)

    def _flush_buffer(self, buffer: List[Dict]):
        # debug: show what's being flushed
        try:
            sample = [e["payload"] for e in buffer][:3]
            logger.debug("flushing %d messages; sample payloads: %s", len(buffer), sample)
        except Exception:
            pass
        points_by_bucket: Dict[str, List[Any]] = {}
        for e in buffer:
            topic = e["topic"]
            payload = e["payload"]
            bucket = self._get_bucket_for_topic(topic)
            # payload expected to have sensor fields or be a single reading
            try:
                sensor_type = payload.get("sensor_type") if isinstance(payload, dict) else None
                device_id = payload.get("device_id") if isinstance(payload, dict) else None
                device_name = payload.get("device_name") if isinstance(payload, dict) else None
                simulated = payload.get("simulated") if isinstance(payload, dict) else None
                value = payload.get("value") if isinstance(payload, dict) else None
                unit = payload.get("unit") if isinstance(payload, dict) else None
                timestamp = payload.get("timestamp") if isinstance(payload, dict) else None

                # Normalize value using shared utility (coerce strings to bool/number and detect unit)
                try:
                    value, unit = normalize_value(value, unit)
                except Exception as ex:
                    # never allow normalization to raise
                    logger.warning("normalization error: %s", ex)

                # construct Point
                if Point is None:
                    # fallback: construct structured dict with tags and normalized fields
                    fields: Dict[str, Any] = {}
                    # fields for common types
                    if isinstance(value, (int, float)):
                        fields["value"] = float(value)
                    elif isinstance(value, bool):
                        fields["value"] = int(value)
                    else:
                        fields["value_str"] = str(value)

                    tags = {"device_id": device_id or "unknown", "simulated": str(simulated)}
                    if unit:
                        tags["unit"] = str(unit)

                    lines = {
                        "measurement": sensor_type or "sensors",
                        "tags": tags,
                        "fields": fields,
                    }
                    points_by_bucket.setdefault(bucket, []).append(lines)
                else:
                    m = sensor_type or "sensors"
                    p = Point(m)
                    # tags
                    if device_id:
                        p.tag("device_id", device_id)
                    if device_name:
                        p.tag("device_name", device_name)
                    if sensor_type:
                        p.tag("sensor_type", sensor_type)
                    if simulated is not None:
                        p.tag("simulated", str(simulated))
                    if unit:
                        p.tag("unit", unit)
                    # fields (normalized)
                    if isinstance(value, (int, float)):
                        p.field("value", float(value))
                    elif isinstance(value, bool):
                        p.field("value", int(value))
                    else:
                        p.field("value_str", str(value))
                    # timestamp
                    if timestamp:
                        dt = parse_timestamp(timestamp)
                        if dt is not None and WritePrecision is not None and hasattr(WritePrecision, "NS"):
                            p.time(dt, WritePrecision.NS)
                    points_by_bucket.setdefault(bucket, []).append(p)
            except Exception as ex:
                logger.exception("error preparing point: %s", ex)
                continue

        # write to influx
        try:
            # Defensive: _write_api may be any object from different client versions
            if self._write_api is None:
                raise RuntimeError("InfluxDB write API not initialized")
            for bucket, points in points_by_bucket.items():
                if not points:
                    continue
                # time the write and warn if it takes too long
                start_write = time.time()
                if hasattr(self._write_api, "write"):
                    # influxdb-client modern API
                    # write expects bucket/org/record
                    self._write_api.write(bucket=bucket, org=self.cfg.server.influx.org, record=points)
                elif hasattr(self._write_api, "write_points"):
                    # fallback for other clients
                    self._write_api.write_points(points)
                else:
                    # last resort: try calling write directly
                    try:
                        self._write_api(points)
                    except Exception:
                        raise RuntimeError("No suitable write method found on InfluxDB client")
                write_dur = time.time() - start_write
                if write_dur > self._write_timeout_warning:
                    logger.warning("Influx write took %.2fs for %d points", write_dur, len(points))
                logger.info("wrote %d points to InfluxDB bucket='%s' in %.3fs", len(points), bucket, write_dur)
        except Exception as e:
            logger.exception("write error: %s", e)

    def _get_security_state(self, device_id: str) -> SecurityState:
        state = self._security_states.get(device_id)
        if state is None:
            state = SecurityState()
            self._security_states[device_id] = state
        return state

    def _security_snapshot(self, state: SecurityState) -> Dict[str, Any]:
        return {
            "mode": state.mode,
            "occupancy": state.occupancy,
            "alarm_reason": state.alarm_reason,
        }

    def get_security_snapshot(self, device_id: str) -> Dict[str, Any]:
        with self._security_lock:
            state = self._get_security_state(device_id)
            return self._security_snapshot(state)

    def arm_system(self, device_id: str, pin: str) -> Dict[str, Any]:
        if pin != self.cfg.security.pin:
            return {"ok": False, "error": "Invalid PIN"}
        with self._security_lock:
            state = self._get_security_state(device_id)
            if state.mode in ("ARMING", "ARMED"):
                return {"ok": True, "state": state.mode}
            self._start_arming(device_id, state, source="web")
            return {"ok": True, "state": state.mode}

    def disarm_system(self, device_id: str, pin: str) -> Dict[str, Any]:
        if pin != self.cfg.security.pin:
            return {"ok": False, "error": "Invalid PIN"}
        with self._security_lock:
            state = self._get_security_state(device_id)
            self._disarm(device_id, state, source="web")
            return {"ok": True, "state": state.mode}

    def _record_security_event(self, device_id: str, event_type: str, state: SecurityState):
        if self._write_api is None or Point is None:
            return
        try:
            p = Point("security_events")
            p.tag("device_id", device_id)
            p.field("event", event_type)
            p.field("state", state.mode)
            p.field("occupancy", int(state.occupancy))
            if state.alarm_reason:
                p.field("alarm_reason", state.alarm_reason)
            p.time(datetime.now(timezone.utc), WritePrecision.NS)
            self._write_api.write(bucket=self.cfg.server.influx.bucket, record=p)
        except Exception:
            logger.debug("security event write failed", exc_info=True)

    def _start_arming(self, device_id: str, state: SecurityState, source: str):
        state.mode = "ARMING"
        state.alarm_reason = None
        self._record_security_event(device_id, f"arming_started:{source}", state)

        if state.arming_timer:
            try:
                state.arming_timer.cancel()
            except Exception:
                pass

        def _finish():
            with self._security_lock:
                st = self._get_security_state(device_id)
                if st.mode == "ARMING":
                    st.mode = "ARMED"
                    self._record_security_event(device_id, "armed", st)
                    # If the door is already open, start entry-delay now and reset the active timestamp
                    if st.ds1_active_since is not None and st.ds1_entry_timer is None:
                        st.ds1_active_since = datetime.now(timezone.utc)
                        self._start_entry_delay(device_id, st)

        state.arming_timer = threading.Timer(self.cfg.security.arming_delay_seconds, _finish)
        state.arming_timer.daemon = True
        state.arming_timer.start()

    def _disarm(self, device_id: str, state: SecurityState, source: str):
        if state.arming_timer:
            try:
                state.arming_timer.cancel()
            except Exception:
                pass
            state.arming_timer = None
        if state.ds1_entry_timer:
            try:
                state.ds1_entry_timer.cancel()
            except Exception:
                pass
            state.ds1_entry_timer = None
        state.mode = "DISARMED"
        state.alarm_reason = None
        self._reset_dms_pin(state)
        self._publish_buzzer(device_id, False)
        self._record_security_event(device_id, f"disarmed:{source}", state)

    def _set_alarm(self, device_id: str, state: SecurityState, reason: str):
        print(f"[ALARM] _set_alarm called: device={device_id}, reason={reason}, current_mode={state.mode}")
        if state.mode == "ALARM":
            print(f"[ALARM] Already in ALARM mode, returning")
            return
        if state.arming_timer:
            try:
                state.arming_timer.cancel()
            except Exception:
                pass
            state.arming_timer = None
        if state.ds1_entry_timer:
            try:
                state.ds1_entry_timer.cancel()
            except Exception:
                pass
            state.ds1_entry_timer = None
        state.mode = "ALARM"
        state.alarm_reason = reason
        print(f"[ALARM] State updated: mode={state.mode}, reason={state.alarm_reason}")
        self._publish_buzzer(device_id, True)
        self._record_security_event(device_id, f"alarm:{reason}", state)
        print(f"[ALARM] Alarm event recorded and buzzer activated")

    def _reset_dms_pin(self, state: SecurityState):
        state.dms_digit_count = 0
        state.dms_digits = []
        if state.dms_digit_timer:
            try:
                state.dms_digit_timer.cancel()
            except Exception:
                pass
            state.dms_digit_timer = None

    def _commit_dms_digit(self, device_id: str, state: SecurityState):
        if state.dms_digit_count <= 0:
            return
        max_presses = self.cfg.security.dms_max_presses
        count = min(state.dms_digit_count, max_presses)
        digit = 0 if count == 10 else count
        state.dms_digits.append(digit)
        state.dms_digit_count = 0

        if len(state.dms_digits) >= self.cfg.security.dms_pin_length:
            pin_entered = "".join(str(d) for d in state.dms_digits[: self.cfg.security.dms_pin_length])
            if pin_entered == self.cfg.security.pin:
                self._record_security_event(device_id, "pin_ok:dms", state)
                self._disarm(device_id, state, source="dms")
            else:
                self._record_security_event(device_id, "pin_fail:dms", state)
            self._reset_dms_pin(state)

    def _handle_dms_press(self, device_id: str, state: SecurityState):
        # increment press count for current digit
        state.dms_digit_count += 1

        # restart digit timer
        if state.dms_digit_timer:
            try:
                state.dms_digit_timer.cancel()
            except Exception:
                pass

        def _timeout():
            with self._security_lock:
                st = self._get_security_state(device_id)
                self._commit_dms_digit(device_id, st)

        state.dms_digit_timer = threading.Timer(self.cfg.security.dms_digit_pause_seconds, _timeout)
        state.dms_digit_timer.daemon = True
        state.dms_digit_timer.start()

    def _publish_led(self, device_id: str, on: bool):
        payload = {
            "sensor_type": "DL",
            "state": "on" if on else "off",
            "value": bool(on),
            "device_id": device_id,
            "source": "server",
        }
        self.publish_actuator("led", payload, device_id=device_id)

    def _publish_buzzer(self, device_id: str, on: bool):
        payload = {
            "sensor_type": "DB",
            "state": "on" if on else "off",
            "value": bool(on),
            "device_id": device_id,
            "source": "server",
        }
        self.publish_actuator("buzzer", payload, device_id=device_id)

    def _handle_motion(self, device_id: str, state: SecurityState, timestamp: datetime):
        # Turn on LED for a short period
        self._publish_led(device_id, True)
        if state.led_timer:
            try:
                state.led_timer.cancel()
            except Exception:
                pass
        state.led_timer = threading.Timer(self.cfg.security.pir_led_seconds, self._publish_led, args=(device_id, False))
        state.led_timer.daemon = True
        state.led_timer.start()

        # Determine entry/exit from recent DUS1 data
        self._trim_dus1_history(state, timestamp)
        history = list(state.dus1_history)
        if len(history) >= 2:
            first_dist = history[0][1]
            last_dist = history[-1][1]
            delta = last_dist - first_dist
            if delta <= -self.cfg.security.dus1_trend_delta_cm:
                state.occupancy += 1
                self._record_security_event(device_id, "entry", state)
            elif delta >= self.cfg.security.dus1_trend_delta_cm:
                if state.occupancy > 0:
                    state.occupancy -= 1
                self._record_security_event(device_id, "exit", state)

        if state.mode in ("ARMED",) and state.occupancy == 0:
            self._set_alarm(device_id, state, "motion_empty")

    def _start_entry_delay(self, device_id: str, state: SecurityState):
        self._record_security_event(device_id, "entry_delay_started", state)

        def _timeout():
            print(f"[ENTRY_DELAY] *** TIMEOUT CALLBACK FIRED for {device_id} ***")
            with self._security_lock:
                st = self._get_security_state(device_id)
                print(f"[ENTRY_DELAY] Current state in callback: mode={st.mode}, alarm_reason={st.alarm_reason}")
                if st.mode == "ARMED":
                    print(f"[ENTRY_DELAY] Mode is ARMED, triggering entry_delay_timeout alarm")
                    self._set_alarm(device_id, st, "entry_delay_timeout")
                else:
                    print(f"[ENTRY_DELAY] Mode is {st.mode}, NOT triggering alarm")

        state.ds1_entry_timer = threading.Timer(self.cfg.security.entry_delay_seconds, _timeout)
        state.ds1_entry_timer.daemon = True
        state.ds1_entry_timer.start()
        print(f"[ENTRY_DELAY] Timer scheduled for {self.cfg.security.entry_delay_seconds}s (callback will fire in background)")

    def _trim_dus1_history(self, state: SecurityState, now: datetime):
        window = self.cfg.security.dus1_window_seconds
        while state.dus1_history and (now - state.dus1_history[0][0]).total_seconds() > window:
            state.dus1_history.popleft()

    def _process_security_reading(self, payload: Dict[str, Any]):
        sensor_type = payload.get("sensor_type")
        if not sensor_type:
            return
        device_id = payload.get("device_id") or self.cfg.device.id
        ts = parse_timestamp(payload.get("timestamp")) or datetime.now(timezone.utc)
        value, _ = normalize_value(payload.get("value"), payload.get("unit"))

        with self._security_lock:
            state = self._get_security_state(device_id)

            if sensor_type == "DUS1":
                if isinstance(value, (int, float)):
                    state.dus1_history.append((ts, float(value)))
                    self._trim_dus1_history(state, ts)
                return

            if sensor_type == "DS1":
                active = self._is_truthy(value)
                print(f"[DS1] Received value={value}, active={active}, current_mode={state.mode}, ds1_active_since={state.ds1_active_since}")
                if active:
                    if state.ds1_active_since is None:
                        state.ds1_active_since = ts
                        print(f"[DS1] *** DS1 became ACTIVE at {ts}, starting checks ***")
                        if state.mode == "ARMED" and state.ds1_entry_timer is None:
                            print(f"[DS1] Mode is ARMED and no timer running → STARTING ENTRY_DELAY TIMER")
                            self._start_entry_delay(device_id, state)
                        else:
                            print(f"[DS1] Not starting timer: mode={state.mode}, timer_is_None={state.ds1_entry_timer is None}")
                    else:
                        dur = (ts - state.ds1_active_since).total_seconds()
                        print(f"[DS1] DS1 still active for {dur:.1f}s (threshold={self.cfg.security.door_open_seconds}s)")
                        if (
                            state.mode == "ARMED"
                            and state.ds1_entry_timer is None
                            and dur >= self.cfg.security.door_open_seconds
                        ):
                            print(f"[DS1] *** DURATION THRESHOLD MET ({dur:.1f}s >= {self.cfg.security.door_open_seconds}s) → TRIGGERING DOOR_OPEN ***")
                            self._set_alarm(device_id, state, "door_open")
                else:
                    print(f"[DS1] *** DS1 became INACTIVE ***")
                    state.ds1_active_since = None
                    if state.ds1_entry_timer:
                        try:
                            state.ds1_entry_timer.cancel()
                        except Exception:
                            pass
                        state.ds1_entry_timer = None
                        self._record_security_event(device_id, "entry_delay_cancelled", state)
                return

            if sensor_type == "DPIR1":
                if self._is_truthy(value):
                    self._handle_motion(device_id, state, ts)
                return

            if sensor_type == "DMS":
                pressed = self._is_truthy(value)
                if pressed and state.last_dms_state is not True:
                    if state.mode == "DISARMED":
                        self._reset_dms_pin(state)
                        self._start_arming(device_id, state, source="dms")
                    else:
                        self._handle_dms_press(device_id, state)
                state.last_dms_state = pressed

    def _is_truthy(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "on", "pressed")
        return False

    def _get_bucket_for_topic(self, topic: str) -> str:
        if topic.startswith("actuators/"):
            return self.cfg.server.influx.actuator_bucket
        return self.cfg.server.influx.bucket

    def _ensure_buckets(self):
        try:
            if self._influx_client is None or not hasattr(self._influx_client, "buckets_api"):
                return
            api = self._influx_client.buckets_api()
            for name in {self.cfg.server.influx.bucket, self.cfg.server.influx.actuator_bucket}:
                if not name:
                    continue
                try:
                    if api.find_bucket_by_name(name) is None:
                        api.create_bucket(bucket_name=name, org=self.cfg.server.influx.org)
                        logger.info("created bucket '%s'", name)
                except Exception as ex:
                    logger.warning("bucket ensure failed for '%s': %s", name, ex)
        except Exception as ex:
            logger.warning("bucket ensure failed: %s", ex)

    def _query_latest_measurement(self, bucket: str, measurement: str, device_id: str) -> Optional[Dict[str, Any]]:
        if self._influx_client is None or not hasattr(self._influx_client, "query_api"):
            return None
        try:
            query_api = self._influx_client.query_api()
            flux = (
                f'from(bucket:"{bucket}") '
                f'|> range(start: -24h) '
                f'|> filter(fn: (r) => r._measurement == "{measurement}") '
                f'|> filter(fn: (r) => r["device_id"] == "{device_id}") '
                f'|> filter(fn: (r) => r._field == "value" or r._field == "value_str") '
                f'|> group() '
                f'|> sort(columns: ["_time"], desc: true) '
                f'|> limit(n: 1)'
            )
            tables = query_api.query(org=self.cfg.server.influx.org, query=flux)
            for table in tables:
                for record in table.records:
                    timestamp = record.get_time()
                    unit = record.values.get("unit") if hasattr(record, "values") else None
                    return {
                        "value": record.get_value(),
                        "timestamp": timestamp.isoformat() if timestamp is not None else None,
                        "unit": unit,
                        "bucket": bucket,
                    }
        except Exception as ex:
            logger.warning("failed querying latest measurement %s from bucket %s: %s", measurement, bucket, ex)
        return None

    def get_current_state(self, device_id: str) -> Dict[str, Any]:
        measurements: List[Tuple[str, str]] = [
            ("DS1", self.cfg.server.influx.bucket),
            ("DPIR1", self.cfg.server.influx.bucket),
            ("DUS1", self.cfg.server.influx.bucket),
            ("DMS", self.cfg.server.influx.bucket),
            ("WEBC", self.cfg.server.influx.bucket),
            ("DL", self.cfg.server.influx.actuator_bucket),
            ("DB", self.cfg.server.influx.actuator_bucket),
        ]

        elements: Dict[str, Any] = {}
        for measurement, bucket in measurements:
            latest = self._query_latest_measurement(bucket=bucket, measurement=measurement, device_id=device_id)
            elements[measurement] = latest

        return {
            "device_id": device_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "elements": elements,
            "security": self.get_security_snapshot(device_id),
        }

    # Helper to publish actuator messages
    def publish_actuator(self, actuator_type: str, payload: Dict[str, Any], qos: Optional[int] = None, device_id: Optional[str] = None):
        # allow overriding device_id for topic construction (useful for per-request device ids)
        if device_id is None:
            topic = self.cfg.get_actuator_topic(actuator_type)
        else:
            topic = self.cfg.mqtt.actuator_topic_pattern.format(device_id=device_id, actuator_type=actuator_type)
        if qos is None:
            qos = self.cfg.mqtt.qos
        # Ensure qos is an int (static checkers) and valid
        qos = int(qos)
        data = json.dumps(payload)
        # Enqueue non-blocking to avoid blocking callers (e.g., Flask request thread)
        try:
            self._actuator_queue.put_nowait((topic, data, qos))
            logger.info("enqueued actuator message to %s: %s", topic, data)
            return True
        except queue.Full:
            # drop and log
            logger.warning("actuator queue full, dropping message to %s", topic)
    def _publisher_loop(self):
        """Worker that pulls actuator messages off a queue and publishes them via MQTT.
        Keeps publishes off request threads to avoid blocking I/O in callbacks/handlers.
        """
        while not self._stop_event.is_set():
            try:
                topic, data, qos = self._actuator_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                start = time.time()
                if self._mqtt_client is None:
                    logger.info("No MQTT client; actuator publish skipped for %s", topic)
                else:
                    res = self._mqtt_client.publish(topic, data, qos=qos)
                    dur = time.time() - start
                    if dur > self._publish_timeout_warning:
                        logger.warning("actuator publish to %s took %.2fs", topic, dur)
                    logger.debug("actuator publish result for %s: %s", topic, res)
            except Exception as e:
                logger.exception("actuator publish error for %s: %s", topic, e)

    def _monitor_loop(self):
        """Periodic health checks and logging for queues to detect backpressure/deadlocks."""
        while not self._stop_event.wait(self._monitor_interval):
            try:
                in_q = self._queue.qsize()
                act_q = self._actuator_queue.qsize()
                logger.info("queue sizes - incoming:%d, actuator:%d", in_q, act_q)
                if in_q > self._max_queue_size_warn:
                    logger.warning("incoming queue size %d exceeds threshold %d", in_q, self._max_queue_size_warn)
                if act_q > self._max_queue_size_warn:
                    logger.warning("actuator queue size %d exceeds threshold %d", act_q, self._max_queue_size_warn)
            except Exception:
                pass


if __name__ == "__main__":
    cfg = load_config()
    svc = MqttInfluxService(cfg)
    try:
        svc.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        svc.stop()
