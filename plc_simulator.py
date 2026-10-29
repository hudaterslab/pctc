"""테스트용 PLC: 시작 시 0, 이후 15초마다 1/0을 번갈아 유지합니다."""

import logging
import math
import threading


class PLCSimulator:
    def __init__(self, hold_time=15.0, logger=None):
        self.hold_time = float(hold_time)
        if not math.isfinite(self.hold_time) or self.hold_time <= 0:
            raise ValueError("hold_time은 0보다 큰 유한한 값이어야 합니다.")
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        self._twist_lock = 0
        self._generation = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._twist_lock = 0
            self._generation += 1
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._generate_signal, daemon=True, name="plc-simulator")
            self._thread.start()

    def _generate_signal(self):
        self.logger.info("[PLC SIM] twist_lock=0 | 감시 OFF | 유지 %.1f초", self.hold_time)
        while not self._stop_event.wait(self.hold_time):
            with self._lock:
                if self._stop_event.is_set():
                    break
                self._twist_lock = 1 - self._twist_lock
                self._generation += 1
                signal = self._twist_lock
            self.logger.info("[PLC SIM] twist_lock=%d | 감시 %s | 유지 %.1f초",
                             signal, "ON" if signal else "OFF", self.hold_time)

    def read_twist_lock(self):
        return self.read_state()[0]

    def read_state(self):
        """값과 변경 번호를 함께 읽어, 긴 추론 중 OFF→ON 전환도 구분합니다."""
        with self._lock:
            return self._twist_lock, self._generation

    def stop(self):
        with self._lock:
            self._stop_event.set()
            self._twist_lock = 0
            self._generation += 1
            thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join()
