import os
import sys

# ==========================================
# [0] 레이더 MSE SDK 워커 (자식 프로세스 진입점)
# ==========================================
# UMRR-11 Type 132 SDK는 Python 종료 API가 없어 자식 프로세스가 소켓/스레드를 독점합니다.
# MseRadarHub는 이 파일을 RADAR_MSE_WORKER_FLAG와 레이더 client_id들로 다시 실행하고, 아래 분기에서 바로 워커로
# 들어가므로 OpenCV/DeepX 로드, 설정 파일 쓰기 같은 엔진 초기화를 거치지 않습니다(표준 라이브러리만 사용).
# 레이더가 여러 대면 SDK routing_table.json에 client로 하나씩 적고, 워커 하나가 모두 받습니다.
MSE_CLIENT_ID = 344113  # routing_table.json을 못 읽을 때 쓰는 기본 레이더
MSE_INTERFACE_NAME = 'umrr11_t132_mse'
MSE_PREFIX = 'HAI_MSE '
MSE_MAX_LINE = 262144
RADAR_MSE_WORKER_FLAG = '--radar-mse-worker'


def mse_client_ids(smart_access_config_path):
    """SDK 설정(smart_access_config.json)의 config_path에 있는 routing_table.json에서 MSE 레이더 client_id 목록을 읽습니다.
    못 읽거나 없으면 [MSE_CLIENT_ID]."""
    import json
    try:
        with open(smart_access_config_path, encoding='utf-8') as f:
            cfg_dir = json.load(f).get('config_path') or os.path.dirname(os.path.abspath(smart_access_config_path))
        with open(os.path.join(cfg_dir, 'routing_table.json'), encoding='utf-8') as f:
            clients = json.load(f).get('clients') or []
        ids = [int(c['client_id']) for c in clients
               if c.get('user_interface_name', MSE_INTERFACE_NAME) == MSE_INTERFACE_NAME and 'client_id' in c]
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        ids = []
    return list(dict.fromkeys(ids)) or [MSE_CLIENT_ID]


def mse_object_record(data, client_id, client_ids):
    header = data.GetGenericPortHeader()
    if (client_id not in client_ids or header.GetPortId() != 67
            or header.GetPortVersionMajor() != 4 or header.GetPortVersionMinor() != 0):
        raise ValueError('unexpected MSE object port version')
    objects = [dict(id=int(row.GetObjectID()), status=int(row.GetStatus()),
                    x_m=float(row.GetxPos()), y_m=float(row.GetyPos()), z_m=float(row.GetzPos()),
                    speed_mps=float(row.GetSpeedAbsolute()), heading_rad=float(row.GetHeading()),
                    quality=float(row.GetQuality())) for row in data.GetObjectList()]
    return {'kind': 'objects', 'client_id': int(client_id),
            'timestamp_us': int(header.GetTimestamp()), 'objects': objects}


def run_mse_worker(client_ids):
    import fcntl
    import json
    import signal
    import threading
    import time
    import smart_access_binding as smart
    import umrr11_t132_mse_v1_1_1_binding as mse

    # SDK has no Python shutdown API. This child exclusively owns its sockets/threads.
    signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    signal.signal(signal.SIGINT, lambda *_: os._exit(0))
    lock = open('/tmp/hai-radar-mse.lock', 'a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    output_lock = threading.Lock()

    def emit(record):
        with output_lock:
            print(MSE_PREFIX + json.dumps(record, allow_nan=False), flush=True)

    def receive(data, client_id):
        try:
            emit(mse_object_record(data, client_id, client_ids))
        except Exception as error:
            emit({'kind': 'error', 'message': str(error)})

    service = smart.CommunicationServicesIface.Get()
    if not service.Init():
        raise RuntimeError('Smart Access initialization failed')
    stream = mse.DataStreamServiceIface.Get()
    for client_id in client_ids:
        status = stream.RegisterComObjectListPortReceiveCallback(client_id, receive)
        if status != smart.ErrorCode.ERROR_CODE_OK:
            raise RuntimeError(f'MSE callback registration failed for client {client_id}: {status}')
    emit({'kind': 'ready', 'client_ids': client_ids})
    while True:
        time.sleep(1)


if __name__ == '__main__' and sys.argv[1:2] == [RADAR_MSE_WORKER_FLAG]:
    try:
        run_mse_worker([int(v) for v in sys.argv[2:]] or [MSE_CLIENT_ID])
    except Exception as error:
        import json
        print(MSE_PREFIX + json.dumps({'kind': 'error', 'message': str(error)}), flush=True)
        os._exit(1)

os.environ["GST_VAAPI_DISPLAY"] = "drm"
os.environ["GST_VAAPI_DRM_DEVICE"] = "/dev/dri/renderD128"
os.environ["LIBVA_DRIVER_NAME"] = "iHD"
os.environ["GST_VAAPI_ALL_DRIVERS"] = "1"
os.environ["GST_PLUGIN_FEATURE_RANK"] = "vah264dec:MAX,vah265dec:MAX"
import gc
import json
import csv
import copy
import shutil
import subprocess
import cv2
import math
import numpy as np
import time
import datetime
import traceback
import threading
import queue
import logging
import psutil
import atexit
from collections import Counter, deque, defaultdict
import concurrent.futures
import re
import requests
import pytz
from urllib.parse import urlsplit, quote
from logging.handlers import TimedRotatingFileHandler, QueueHandler, QueueListener
import argparse
import signal
from pathlib import Path
from dataclasses import dataclass
from statistics import median
try:
    import fcntl  # 리눅스 파이프 크기 조절(영상 수신기). 윈도우에는 없습니다.
except ImportError:
    fcntl = None

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

API_SEND_STATE = {"consecutive_failures": 0}
API_SEND_STATE_LOCK = threading.Lock()
EVENT_AUDIT_LOCK = threading.Lock()

# ==========================================
# [1] 시스템 기본 설정 및 상수
# ==========================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_COMMON_FILE = os.path.join(PROJECT_ROOT, "system_config.json")
CONFIG_CAMERAS_FILE = os.path.join(PROJECT_ROOT, "cameras.json")
CAMERA_LIST_FILE = os.path.join(PROJECT_ROOT, "cameras.csv")
EVENT_ROOT_DIR = os.path.join(PROJECT_ROOT, "CCTV_EVENT_ALERT")

SCREEN_WIDTH = 1280
SCREEN_HEIGHT = 720
WATCHDOG_TIMEOUT = 30.0
# 과속 측정용 PTS 평활 구간(프레임 수). 카메라 PTS의 미세 흔들림을 평균으로 상쇄합니다.
PTS_SMOOTH_WINDOW = 15

# 메인 모델(hanjin_cctv_v2, 8개 클래스: 0 Helmet, 1 Head, 2 Person, 3 NormalVehicle, 4 LowerBody, 5 Signalman,
# 6 TrunkLineTruck, 7 XBand) 중 과속 카메라가 쓰는 클래스 번호입니다. 나머지 클래스는 버립니다.
ID_G_PERSON = 2       # person
ID_G_CAR = 3          # normalvehicle
ID_PERSON_LOW = 4     # lowerbody
ID_G_TRUCK = 6        # trunklinetruck
TARGET_VEHICLES = [ID_G_CAR, ID_G_TRUCK]

# ------------------------------------------------------------
# 항만 이벤트: Backreach, 과속(speeding), 콘 미제거 출차(cone_ng), YT 헤드압착(yt_head_crush, yt_zone_person)
# ------------------------------------------------------------
BACKREACH_EVENT = "backreach"
SPEEDING_EVENT = "speeding"
CONE_NG_EVENT = "cone_ng"
# YT 헤드압착(협의안 v2 02): 헤드압착 알람과 양하 중 위험구역 사람 진입 알람을 이벤트 둘로 나눴습니다.
# 쿨다운이 이벤트 이름 단위라서, 하나로 묶으면 사람 알람 때문에 헤드압착 알람이 쿨다운에 막힙니다.
YT_HEAD_CRUSH_EVENT = "yt_head_crush"
YT_ZONE_PERSON_EVENT = "yt_zone_person"
YT_EVENTS = (YT_HEAD_CRUSH_EVENT, YT_ZONE_PERSON_EVENT)
# 해측 추락 방지: 크레인에서 비스듬히 내려다보는 카메라 묶음(보통 3대)으로 안벽 끝 위험 구역의 사람을 봅니다.
SEASIDE_PERSON_EVENT = "seaside_person"
# topdown 모델(같은 엔진·추적기)을 쓰는 이벤트입니다.
PERSON_TOPDOWN_EVENTS = (BACKREACH_EVENT, *YT_EVENTS, SEASIDE_PERSON_EVENT)
# topdown.dxnn 모델 키(system_config.json models 등의 키)입니다. 이 모델을 쓰는 이벤트(Backreach·YT)는 모두
# 엔진 하나와 추적기 하나(Camera.trk_topdown)를 같이 씁니다. 사람·차량도 9클래스 topdown이 찾습니다.
TOPDOWN_MODEL = "TOPDOWN"
# topdown.dxnn 클래스(최종 구성 9클래스, 1280x736). 트위스트락은 cone.dxnn이 찾습니다.
TOPDOWN_CLASS_NAMES = ("YT_HEADER", "CONTAINER_40FT", "CONTAINER_20FT", "YT_CHASSIS_EMPTY", "YT_CHASSIS_HALF",
                       "TANK_20FT", "SPREADER", "PERSON", "VEHICLE")
# Backreach·YT 이벤트 tid = 이 값 + topdown 추적 번호(과속 1,000,000+·콘 900,000+와 겹치지 않게).
TOPDOWN_TID_BASE = 3_000_000
# 개인정보 블러용으로 topdown 클래스 번호를 메인 모델 번호로 바꿉니다(사람=얼굴 블러, 차량·YT=번호판 블러).
TOPDOWN_PRIVACY_CLASS = {TOPDOWN_CLASS_NAMES.index(name): main_id for name, main_id in (
    ("YT_HEADER", ID_G_TRUCK), ("YT_CHASSIS_EMPTY", ID_G_TRUCK), ("YT_CHASSIS_HALF", ID_G_TRUCK),
    ("PERSON", ID_G_PERSON), ("VEHICLE", ID_G_CAR))}


def check_topdown_class_count(count):
    """올린 topdown.dxnn의 클래스 수가 이름표(TOPDOWN_CLASS_NAMES, 9클래스)와 같은지 봅니다. 다르면 클래스 번호가
    어긋나 YT·Backreach·사진 블러가 엉뚱한 물체를 보므로 경고합니다(이름표는 그대로)."""
    names = TOPDOWN_CLASS_NAMES
    if count != len(names):
        logger.warning(f"[MODEL] topdown.dxnn 클래스 수({count})가 이름표({len(names)}클래스)와 달라 클래스 번호가 "
                       f"맞지 않습니다. 9클래스 topdown을 쓰세요.")
    else:
        logger.info(f"[MODEL] topdown 클래스 {count}개: {', '.join(names)}")


# 메인 모델(hanjin_cctv_v2) 클래스 중 model_confidences에 이름으로 적을 수 있는 것(그 밖의 클래스는 번호로 적음).
MAIN_CLASS_IDS = {"person": ID_G_PERSON, "normalvehicle": ID_G_CAR, "lowerbody": ID_PERSON_LOW,
                  "trunklinetruck": ID_G_TRUCK}
# side.dxnn(콘 카메라 화면) 클래스. 콘 NG 컨테이너(0·1), NG 사진 사람(3)을 씁니다.
SIDE_CLASS_NAMES = ("container", "tank", "spreader", "person", "YT_HEADER")
# container_ocr_detector.dxnn(컨테이너 번호 OCR의 글자 영역) 클래스. 0 BIC 컨테이너 번호, 1 TYPE 크기·형식 코드
CONTAINER_OCR_DET_CLASS_NAMES = ("BIC", "TYPE")
# 메인 V2를 뺀 모델의 클래스 이름(model_confidences에 이름으로 적음)
MODEL_CLASS_NAMES = {TOPDOWN_MODEL: TOPDOWN_CLASS_NAMES, "FACE": ("face",), "PLATE": ("plate",), "SIDE": SIDE_CLASS_NAMES,
                     "CONE": ("cone",), "CONTAINER_OCR_DET": CONTAINER_OCR_DET_CLASS_NAMES}


def model_class_names(model_key):
    """모델의 {클래스 번호: 이름}. topdown은 TOPDOWN_CLASS_NAMES, 메인 V2는 MAIN_CLASS_IDS에 있는 클래스만입니다."""
    if model_key in MODEL_CLASS_NAMES:
        return dict(enumerate(MODEL_CLASS_NAMES[model_key]))
    if model_key == "MAIN_V2":
        return {index: name for name, index in MAIN_CLASS_IDS.items()}
    return {}


def model_class_ids(model_key):
    """model_confidences에 쓰는 클래스 이름(소문자) → 번호."""
    return {name.lower(): index for index, name in model_class_names(model_key).items()}


def class_confidences(model_key):
    """model_confidences.<모델>의 클래스별 값을 {클래스 번호: 기준}으로 바꿉니다. 적지 않은 클래스는 버립니다(infer).
    모르는 클래스 이름이나 숫자가 아닌 값은 경고하고 뺍니다."""
    entry = SYS_CFG["model_confidences"].get(model_key)
    if not isinstance(entry, dict):
        return {}
    ids = model_class_ids(model_key)
    table = {}
    for key, value in entry.items():
        name = str(key).strip()
        if name.isdigit():
            class_id = int(name)
        elif name.lower() in ids:
            class_id = ids[name.lower()]
        else:
            logger.warning(f"[MODEL] {model_key} 점수 기준: 모르는 클래스 '{key}'를 뺍니다"
                           f"(이름: {', '.join(ids) or '없음, 번호로 적기'})")
            continue
        try:
            table[class_id] = float(value)
        except (TypeError, ValueError):
            logger.warning(f"[MODEL] {model_key} 점수 기준: '{key}' 값 {value!r}을 숫자로 읽지 못해 뺍니다")
    return table


def apply_class_confidences(model, model_key):
    """모델에 클래스별 기준(YoLoDeepX.class_conf)을 붙이고, 클래스별 기준을 로그 한 줄로 남깁니다."""
    model.class_conf = class_confidences(model_key)
    if not model.class_conf:
        logger.warning(f"[MODEL] {model_key} 점수 기준: model_confidences.{model_key}에 적힌 클래스가 없어 "
                       f"이 모델은 아무것도 찾지 않습니다")
        return
    names = model_class_names(model_key)
    logger.info(f"[MODEL] {model_key} 점수 기준: " + ", ".join(
        f"{names.get(k, k)}({k})={v:g}" for k, v in sorted(model.class_conf.items())))

# 관제 receiver(새 API) 이벤트 번호 → 이름. 인천(10~13)과 평택(10, 11, 14, 15)을 합쳤습니다(번호 충돌 없음).
RECEIVER_EVENT_NAMES = {10: "QC_APRON_HDS", 11: "YT_HEAD_CRUSH", 12: "BACKREACH_OBSTACLE", 13: "SPEED_VIOLATION",
                        14: "SEASIDE_DETECT", 15: "CONTAINER_CONE_DETECT"}


def deep_merge_dict(base, override):
    """딕셔너리를 깊은 병합(Deep Merge)하는 유틸리티 함수"""
    result = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = deep_merge_dict(result[k], v)
        else:
            result[k] = copy.deepcopy(v)
    return result


def load_system_config():
    default_config = {
        # 새 관제 receiver API. receiver.enabled를 켜고 crane_id, cctv_ids를 넣어야 전송합니다.
        "crane_id": "",
        "cctv_server_id": "1",
        "receiver": {"enabled": False, "cctv_ids": [], "base_url": "https://dev.hjit.hudaters.net/hjit/receiver/api/v1/cctv"},
        "health_check": {"interval_sec": 60, "verify_tls": True},
        "event_api": {"verify_tls": True},
        "video_api": {"verify_tls": True, "timeout_sec": 60},
        "roi_snapshot": {"interval_sec": 3600, "verify_tls": True},
        "system_performance": {
            "target_fps": 10.0,
            "dynamic_cpu_adjust_enabled": False,
            # OpenCV 내부 스레드 수(cv2.setNumThreads). 기본(코어 수)이면 카메라 스레드마다 연산 하나를 8개로 쪼개
            # 기다리는 동안 CPU를 써서, 실제 시간의 4~7배를 CPU로 씁니다.
            "opencv_threads": 1
        },
        "logging": {
            "dir": "./CCTV_EVENT_ALERT/logs",
            "retention_days": 14,
            "event_audit_enabled": True,
            "disk_free_warn_gb": 5.0
        },
        "event_config": {
            # 과속: 레이더 트랙 기준 판정. 트랙당 1회라 카메라 cooldown은 0입니다.
            "speeding": {
                "enabled": True, "cooldown_sec": 0, "api_event_type": 13, "blur_plate": True,
                "speed_limit_kmh": 30.0, "margin_kmh": 5.0, "margin_ratio": 0.0,
                "min_span_sec": 1.0, "min_samples": 5, "min_quality": 0.0,
                # 레이더 가짜 물체(분리대 반사 등, 11/7): 판정 뒤 차 박스가 한 번도 안 붙고 레이더 트랙이 ghost_radar_post_sec
                # 안에 사라졌고 번호판도 못 읽었으면 가짜로 보고 관제로 보내지 않으며, 사진·원본·영상을 events/<ip>/ghost/로
                # 옮깁니다(radar_ghost_check). 레이더마다 radars.<id>.speeding.ghost_filter로 켭니다.
                "ghost_filter": False, "ghost_radar_post_sec": 1.0,
                "track_timeout_sec": 1.0, "same_vehicle_guard_sec": 3.0, "same_vehicle_guard_m": 8.0,
                # 레이더가 차 한 대를 둘로 잡을 때(운전석 좌우, 긴 트레일러 앞뒤) 두 번째 과속을 빼는 범위:
                # same_vehicle_guard_sec 안에 과속을 낸 차의 진행 방향 뒤 trail_m(앞으로는 3m) 안, 옆 trail_lat_m 안,
                # 속도 차 trail_kmh 안이면 같은 차로 봅니다.
                # 옆 3m: 트럭 앞(운전석)과 트레일러·컨테이너 뒤를 따로 잡으면 옆으로 2.3~3.0m 벌어집니다(11/5 09:55).
                "same_vehicle_trail_m": 20.0, "same_vehicle_trail_lat_m": 3.0, "same_vehicle_trail_kmh": 5.0,
                # 단속 방향(카메라마다 cameras.json event_config.speeding에서 바꿈, 마법사가 물음):
                # forward = 카메라(레이더)로 다가오는 차(앞번호판), backward = 멀어지는 차(뒷번호판). 둘 다 true면 양쪽 모두.
                "forward": True, "backward": False,
                # 속도를 재는 구간(레이더 거리 m, [가까운 끝, 먼 끝]): 이 구간 안 속도 중앙값으로 판정합니다(min_samples·
                # min_span_sec 이상). 다가오는 차는 가까운 끝을, 멀어지는 차는 먼 끝을 지날 때 판정합니다.
                "forward_measure_m": [30.0, 80.0], "backward_measure_m": [0.0, 40.0],
                # 번호판 사진 구간(레이더 거리 m). 화면에 안 보이는 구간이면 화면 안에서 가장 가까운 1초로 당깁니다.
                "forward_photo_m": [0.0, 5.0], "backward_photo_m": [0.0, 10.0],
                # ROI(단속 차로)는 차로 고르기만 합니다: ROI를 레이더 좌표로 바꾼 다각형(roi_radar_polygon) 안에 이만큼 이상
                # 찍힌 트랙만 그 카메라 판정 대상입니다. 카메라에 안 보이는 먼 곳에서 잰 속도도 ROI를 지나가면 씁니다.
                "lane_min_samples": 3,
                # 레이더 트랙 샘플을 들고 있는 시간(초): 먼 곳에서 잰 속도를 ROI에 들어온 뒤 판정할 때까지 둡니다.
                "track_keep_sec": 20.0,
                # 관제(receiver) 과속 사진: 번호판을 읽은 뒤 detectedObjects에 label "speedviolation"·regNo(차량번호, 여러 장
                # 다수결)·speed(레이더 속도 km/h)를 넣어 보냅니다(관제 요청, 관제가 TOS로 넘김). 상태(확정·불확실 등)와
                # 상관없이 다수결 번호를 보내고 상태는 plateStatus로 같이 보냅니다. 차량번호가 없으면(번호판을 못 찾음,
                # api_wait_sec 안에 못 읽음) 관제로 보내지 않고 로그만 남깁니다(사진·영상).
                "api_wait_sec": 60.0,
                # 레이더가 보는 범위. ROI 다각형이 없는 카메라(보정값 없음)는 이 범위(와 polygon)로 차로를 정합니다.
                "zone": {"min_range_m": 0.0, "max_range_m": 200.0, "max_abs_lateral_m": 50.0}
            },
            # 콘 미제거 출차: 양하 후 착지한 컨테이너에 콘이 달린 채 YT가 출발하면 NG.
            # 원본 1920폭 프레임으로 받습니다(ConeNgDetector.stream_spec, 공통 수신기 FrameReader). 판정은 세션당 1회라 cooldown 0.
            "cone_ng": {
                "enabled": True, "cooldown_sec": 0, "api_event_type": 15, "blur_face": True, "blur_plate": False,
                # 같은 크레인 차선을 보는 카메라 IP 묶음 {"그룹 이름": ["ip", ...]}. 묶이지 않은 카메라는 혼자 판정합니다.
                "groups": {},
                # 판정 기준값(CONE_NG_DEFAULTS) 중 바꿀 것만. 모델(side·cone)과 점수 기준은 models·model_confidences의 SIDE·CONE
                "logic": {},
                # 콘을 찾을 컨테이너 수: 판정 대상이 될 수 있는 것(점수·크기 조건 통과) 중 면적 큰 순.
                # 판정은 주 컨테이너 하나만 따라가므로 1. 카메라당 NPU 호출 = 1 + 2 x 이 값
                "max_containers": 1,
                # 컨테이너가 없는(IDLE) 동안 관측 횟수 상한(Hz). 0이면 제한 없음. 모서리 콘 크롭은 착지·출발 단계에서만 돌립니다.
                "idle_observe_hz": 3.0,
                # 콘 카메라의 디코더 출력(원본 프레임) 가로 상한(Backreach 1280, 그 밖 720). 720폭에서는 콘이 작아 잡히지 않습니다.
                "source_max_width": 1920,
                # 콘 카메라가 초당 받는 장수 상한(0 = 제한 없음). 1080p 30fps를 모두 내려받으면 실시간을 못 따라갑니다.
                # 과속이 함께 있는 카메라는 레이더 연결에 모든 프레임이 필요해 줄이지 않습니다.
                "source_max_fps": 10.0,
                # 컨테이너 번호 인식: 착지해 있는 동안 컨테이너가 가장 크게 보인 원본 프레임을 카메라마다 N장 모아 읽습니다.
                # "LANDED": 착지(정차) 뒤 ocr_landed_delay_sec가 지나면 NG를 기다리지 않고 세션마다 한 번 읽습니다.
                # "NG": NG 이벤트 때 읽습니다(LANDED로 이미 읽었으면 그 결과를 씁니다).
                "ocr_on": ["NG"],
                "ocr_landed_delay_sec": 5.0,
                "ocr_frames_per_camera": 5,
                "ocr_min_gap_sec": 1.0,
                "ocr_container_margin": 0.05,
                # 묶음(groups) 4분할 영상: YT가 출발하면 그 순간 앞 REC_PRE_SEC·뒤 REC_POST_SEC를 묶인 카메라마다 녹화해
                # 2x2 한 영상으로 합칩니다(관제로 보내지 않고 단말에만 저장). 카메라가 1대인 그룹은 만들지 않습니다.
                "group_video": True
            },
            # Backreach: 감시 ON일 때 topdown 객체 bbox가 다각형 ROI와 겹치면 이벤트(다른 이벤트와 같은 쿨다운·중복 제한).
            # PLC 신호를 받을 수 없어 ON·OFF는 화면의 스프레더로 정합니다(BackreachSpreaderGate): topdown이 스프레더를
            # spreader_on_sec(초) 넘게 보면 ON, spreader_miss_frames(추론 프레임)보다 오래 못 보면 OFF.
            # idle_infer_fps: 감시 OFF인 동안(이벤트를 낼 수 없음) topdown 추론 횟수(초당, 0 = 줄이지 않음).
            "backreach": {"enabled": True, "cooldown_sec": 600, "api_event_type": 12, "blur_face": True, "blur_plate": True,
                          "idle_infer_fps": 3.0, "spreader_on_sec": 3.0, "spreader_miss_frames": 3},
            # YT 헤드압착(관제 11)과 양하 중 위험구역 사람 진입(관제 10 QC_APRON_HDS). Backreach와 같은 topdown을 씁니다.
            # 판단 기준값(정지 10초, 이탈 5초 등)은 YT_DEFAULT_CONFIG이고, 바꿀 값만 yt_head_crush에 적습니다(두 이벤트가 같이 씀).
            # 사람 진입만 쓰는 값(person_hold_sec, 끊김 허용 프레임 person_miss_frames, 위험구역 가로 비율
            # danger_zone_width_ratio)은 yt_zone_person에 적습니다.
            # 헤드압착은 목숨과 연결되어 쿨다운이 50초이고, 위험이 이어지면 같은 컨테이너에도 다시 알람합니다
            # (YTHeadCrushDetector.alarm_once_per_object).
            YT_HEAD_CRUSH_EVENT: {"enabled": True, "cooldown_sec": 50, "api_event_type": 11,
                                  "blur_face": True, "blur_plate": True},
            YT_ZONE_PERSON_EVENT: {"enabled": True, "cooldown_sec": 600, "api_event_type": 10,
                                   "blur_face": True, "blur_plate": True},
            # 해측 추락 방지(관제 14 SEASIDE_DETECT). 판정 기준값은 SEASIDE_DEFAULT_CONFIG이고 바꿀 값만 여기에 적습니다.
            # groups: 같은 크레인 카메라 묶음 {"묶음 이름": ["ip", ...]}(마법사가 정함). 카메라별 끝선·위험 구역·색과
            # 제외 구역은 cameras.json 카메라의 event_config.seaside_person에 마법사가 적습니다.
            SEASIDE_PERSON_EVENT: {"enabled": True, "cooldown_sec": 60, "api_event_type": 14,
                                   "blur_face": True, "blur_plate": False, "groups": {}}
        },
        # NPU 모델 7개. 키는 model_confidences·model_output_formats·model_engine_pool_sizes와 같습니다.
        # - MAIN_V2(hanjin_cctv_v2): 과속. FACE(face): 사진 얼굴 가리기. PLATE(plate): 사진 번호판 가리기·과속 번호판 인식
        # - TOPDOWN(topdown 1280x736, 9클래스): YT 헤드압착·사람 진입·Backreach
        # - SIDE(side 640x384, 5클래스: 0 container, 1 tank, 2 spreader, 3 person, 4 YT_HEADER): 콘 NG의 컨테이너(0·1),
        #   NG 사진 얼굴 가리기용 사람(3). 5클래스 모두 콘 카메라 화면에 그립니다.
        # - CONE(cone 640x384, 1클래스 콘): 콘 NG 모서리 크롭(1280x736을 반으로 줄여 넣음)
        # - CONTAINER_OCR_DET(container_ocr_detector 1280x736, 2클래스: 0 BIC, 1 TYPE): 컨테이너 번호 OCR의 글자 영역.
        #   콘 NG가 처음 난 뒤에 올립니다(container_ocr).
        # side·cone은 raw 출력이어야 합니다(PPU는 장치에서 0.25 미만을 버려 콘 0.01 기준을 못 씀).
        "models": {
            "MAIN_V2": "hanjin_cctv_v2.dxnn",
            "FACE": "face.dxnn",
            "PLATE": "plate.dxnn",
            "TOPDOWN": "topdown.dxnn",
            "SIDE": "side.dxnn",
            "CONE": "cone.dxnn",
            "CONTAINER_OCR_DET": "container_ocr_detector.dxnn"
        },
        # 모델별·클래스별 점수 기준. 모델마다 {"클래스": 기준}이고, 적지 않은 클래스는 버립니다(모델이 찾아도 쓰지 않음).
        # 클래스는 이름(대소문자 무관, model_class_names) 또는 번호("7")로 적습니다.
        # - 메인 V2: 과속이 쓰는 사람·하반신 0.5, 승용차·트럭 0.6
        # - topdown: 9클래스 모두 0.25(YT 헤드압착·사람 진입·Backreach가 같이 씀)
        # - side: 컨테이너·탱크 0.25, NG 사진 얼굴 가리기용 사람 0.5, 스프레더·YT_HEADER 0.25(화면 표시만, 판정에는 안 씀)
        # - cone: 콘 0.01
        # - container_ocr_detector: 컨테이너 번호 영역 BIC·TYPE 0.25
        # - PPU 출력 모델(메인·얼굴)은 장치가 약 0.25 아래를 먼저 버려서 그보다 낮은 값은 효과가 없습니다.
        # 검출한 뒤의 판정 기준은 이벤트 설정에 따로 있습니다(콘 판정 cone_ng.logic container_conf 0.7·cone_conf 0.05,
        # 번호판 인식 plr.det_conf).
        "model_confidences": {
            "MAIN_V2": {"person": 0.5, "lowerbody": 0.5, "normalvehicle": 0.6, "trunklinetruck": 0.6},
            "FACE": {"face": 0.35},
            "PLATE": {"plate": 0.1},
            "TOPDOWN": {name: 0.25 for name in TOPDOWN_CLASS_NAMES},
            "SIDE": {"container": 0.25, "tank": 0.25, "spreader": 0.25, "person": 0.5, "YT_HEADER": 0.25},
            "CONE": {"cone": 0.01},
            "CONTAINER_OCR_DET": {"BIC": 0.25, "TYPE": 0.25}
        },
        "model_output_formats": {
            "MAIN_V2": "ppu",
            "FACE": "ppu",
            "PLATE": "yolo",
            "TOPDOWN": "yolo",
            "SIDE": "yolo",
            "CONE": "yolo",
            "CONTAINER_OCR_DET": "yolo"
        },
        # NPU 엔진 수. 엔진 하나는 한 번에 한 장만 처리해(코어 하나), 여러 카메라가 같은 모델을 쓰면 엔진이 많아야 코어
        # 3개를 나눠 씁니다. 엔진마다 NPU 메모리를 따로 씁니다. 메인 V2(과속)·topdown(YT·Backreach 카메라 여러 대)·
        # side·cone(콘 카메라 여러 대, 모서리 크롭 두 장)은 2개입니다. 엔진이 많을수록 NPU 메모리가 늘고 온도가 빨리 오릅니다.
        # container_ocr_detector는 NG 때 번호 OCR 스레드 하나만 써서 1개입니다.
        "model_engine_pool_sizes": {
            "MAIN_V2": 2,
            "FACE": 1,
            "PLATE": 1,
            "TOPDOWN": 2,
            "SIDE": 2,
            "CONE": 2,
            "CONTAINER_OCR_DET": 1
        },
        # topdown(1280x736 NHWC 입력)은 DXRT ORT 후처리를 씁니다.
        "model_use_ort": {"TOPDOWN": True},
        "video_decode": {
            "backend": "gstreamer",
            "hw_acceleration": "auto",
            "hw_device": "/dev/dri/renderD128",
            "fallback_to_cpu": True,
            "fps_limit": 10.0,
            "gstreamer_latency_ms": 50,
            "gstreamer_protocols": "tcp",
            "stats_log_interval_sec": 60.0,
            "max_pts_lag_sec": 5.0
        },
        # 이벤트 시 원본 해상도 프레임 확보. ffmpeg 디코딩 백엔드에서만 동작합니다.
        "fullres_capture": {
            "enabled": True,
            "ring_dir": "/dev/shm/hai_accel_ring",
            # 추출은 카메라마다 한 건씩(약 10~13초)이라, 이벤트가 몰려도 대기 중인 구간이 덮어써지지 않게 넉넉히 둡니다
            # (HEVC 2880x1620 약 4.5Mbps 기준 카메라당 약 34MB, /dev/shm 메모리).
            "ring_sec": 60,
            "pre_sec": 3.0,
            "post_sec": 2.0,
            "every_n": 3,
            "events": ["speeding"],
            "jpeg_quality": 95,
            "hwaccel": True
        },
        # 번호판 인식(PLR). fullres_capture가 뽑은 원본 프레임에서 번호판을 찾고(사진 번호판 가리기와 같은
        # models.PLATE plate.dxnn 엔진), 글자는 CPU(ONNX Runtime)로 읽습니다. dxnn으로 바꾼 PP-OCR은 ONNX보다
        # 정확도가 10% 넘게 낮아 OCR은 CPU만 씁니다.
        "plr": {
            "enabled": True,
            # 상대 경로는 프로젝트 폴더 기준입니다(resolve_model_path).
            "ocr_cpu_model": "plate_ocr.onnx",
            "ocr_dict": "plate_dict.txt",
            "ocr_cpu_threads": 2,
            # 번호판 인식에 쓸 번호판의 최소 점수. 모델 점수 기준(model_confidences.PLATE, 사진 가리기)보다 높게 씁니다.
            "det_conf": 0.25,
            "max_dt_sec": 0.35,
            "vehicle_margin": 0.3,
            "plate_padding": 0.5,
            "compare_csv": "plr_compare.csv"
        },
        # 컨테이너 번호 OCR(cone_ng NG 때). 번호 영역은 models.CONTAINER_OCR_DET(container_ocr_detector.dxnn, 0 BIC,
        # 1 TYPE, 점수 기준 model_confidences.CONTAINER_OCR_DET)으로 찾고, 글자는 PP-OCR 48x320으로 읽습니다.
        # 모델 구성은 CPU(ONNX, container_ocr.onnx)이고, ONNX 파일이 없는 동안은 NPU용 container_rec_48x320.dxnn으로 읽습니다.
        "container_ocr": {
            "enabled": True,
            "ocr_npu_model": "container_rec_48x320.dxnn",
            "ocr_cpu_model": "container_ocr.onnx",
            "ocr_dict": "container_dict.txt",
            "ocr_cpu_threads": 2,
            "min_votes": 3,
            "result_csv": "container_ocr.csv",
            # 콘 NG 사진은 번호를 읽은 뒤 관제로 보냅니다(detectedObjects에 containerNo 등). 이 시간 안에
            # 번호를 못 읽으면 기다리지 않고 번호 없이 보냅니다(첫 NG는 OCR 모델을 올리느라 몇 초 걸릴 수 있음).
            "api_wait_sec": 3.0
        },
        # 레이더↔카메라 연결. homography는 레이더 지면 (x, y) → 추론 프레임 픽셀 (u, v), image_size는 그때의 [w, h].
        # latency_offset_sec = 카메라 지연 - 레이더 지연.
        "radar_camera": {
            "latency_offset_sec": 0.0,
            "homography": None,
            "image_size": None,
            "assoc_gate_px": 80,
            "display_speed": True,
            "calibration_collect": False,
            "calibration_dir": "./CCTV_EVENT_ALERT/logs/radar_calib"
        },
        # 레이더 여러 대(카메라·레이더 한 쌍씩). 레이더는 SDK routing_table.json에 client로 적고, 카메라는 cameras.json의
        # radar_client_id로 자기 레이더를 고릅니다(없으면 routing_table의 첫 레이더).
        # 키는 client_id, 값은 그 레이더에만 덮어쓸 설정: radar_camera(보정값), speeding(event_config.speeding의
        # zone·forward·backward·speed_limit_kmh 등). 예: {"344114": {"radar_camera": {"colocated": {...}}, "speeding": {"zone": {...}}}}
        "radars": {},
        "BATCH_SIZE": 9,
        "CURRENT_FPS_LOG_INTERVAL_SEC": 60.0,
        # 관제 이벤트 영상: 이벤트 순간 앞·뒤 초(API 담당자 요청 총 1분)
        "REC_PRE_SEC": 30,
        "REC_POST_SEC": 30,
        # 이벤트 영상 H.264 변환(transcode_to_h264). 녹화는 mp4v 임시 파일로 받고, 구간이 끝나면 ffmpeg libx264(스레드 1,
        # 낮은 우선순위)로 바꿉니다. codec이 h264가 아니거나 ffmpeg가 실패하면 mp4v 그대로 보냅니다.
        # ultrafast는 veryfast의 약 절반 CPU, 파일은 약 2배입니다.
        # max_parallel 1: 여러 대가 같이 이벤트를 내도 변환은 한 번에 하나.
        # max_kbps: 비트레이트 상한(0 = 없음). 관제 서버는 21MB 영상을 413(너무 큼)으로 거절합니다(17MB는 통과).
        # 2000이면 60초 영상이 약 15MB를 넘지 않고, 단순한 화면은 상한에 닿지 않아 그대로입니다.
        "video_record": {"codec": "h264", "preset": "ultrafast", "crf": 26, "max_parallel": 1, "max_kbps": 2000},
        "OUTPUT_RETENTION_DAYS": 14,
        "OUTPUT_CLEANUP_INTERVAL_SEC": 86400,
        "INTERACTIVE_INPUT_GUARD_SEC": 0.35,
        "VISUAL_ALARM_DURATION": 5.0
    }

    try:
        loaded_config = {}
        if os.path.exists(CONFIG_COMMON_FILE):
            with open(CONFIG_COMMON_FILE, 'r', encoding='utf-8') as f:
                loaded_config = json.load(f)
        merged_config = deep_merge_dict(default_config, loaded_config)
        # 점수 기준은 모델마다 파일에 적힌 클래스만 씁니다(기본값의 클래스를 다시 채우지 않아, 지운 클래스는 버림).
        for key, table in (loaded_config.get("model_confidences") or {}).items():
            if isinstance(table, dict):
                merged_config["model_confidences"][key] = copy.deepcopy(table)
    except Exception as e:
        print(f"[Warning] 설정 파일 로드 실패. 기본값을 사용합니다: {e}")
        return default_config

    # 기본값을 합친 설정을 파일에 저장합니다(파일이 없으면 새로 만들고, 새 항목도 파일에 보이게).
    try:
        with open(CONFIG_COMMON_FILE, 'w', encoding='utf-8') as f:
            json.dump(merged_config, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"[Warning] 설정 파일 쓰기 실패: {e}")
    return merged_config

SYS_CFG = load_system_config()
cv2.setNumThreads(int(SYS_CFG["system_performance"]["opencv_threads"]))
BATCH_SIZE = SYS_CFG["BATCH_SIZE"]
IMAGE_SAVER_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=1)
# 관제 전송(이벤트 사진·영상, ROI 사진) 전용. 관제가 느려도 사진 저장(IMAGE_SAVER_POOL)을 막지 않고, 줄에는 파일 경로만
# 쌓입니다(사진은 저장한 파일을 읽어 보냄). 일꾼이 1개라 보내는 순서(사진 → 영상)는 예전과 같습니다.
EVENT_SEND_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="event-send")
EVENT_SEND_MAX_PENDING = 1000
# 녹화가 끝난 영상의 H.264 변환·meta 저장·전송 예약. 카메라 녹화 스레드가 변환(1~2초)을 기다리며 화면을 쌓지 않게 따로 둡니다.
VIDEO_FINISH_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="video-finish")

def polygon_contour(points):
    """면적이 있는 다각형만 허용합니다. 빈 ROI는 전체 화면을 뜻하지 않습니다."""
    try:
        contour = np.asarray(points, dtype=np.float32)
    except (TypeError, ValueError):
        return None
    if contour.ndim != 2 or contour.shape[1] != 2 or len(contour) < 3:
        return None
    if not np.isfinite(contour).all() or cv2.contourArea(contour) <= 0:
        return None
    return contour


def _edge_intersects_box(start, end, box):
    # 선분의 x/y 구간을 박스 범위로 잘라 교차 여부를 구합니다.
    # 정수 픽셀로 반올림하지 않아 아주 작은 겹침도 유지됩니다.
    enter, leave = 0.0, 1.0
    for axis in (0, 1):
        origin = float(start[axis])
        delta = float(end[axis]) - origin
        lower, upper = box[axis], box[axis + 2]
        if delta == 0:
            if origin < lower or origin > upper:
                return False
            continue
        first, last = sorted(((lower - origin) / delta, (upper - origin) / delta))
        enter, leave = max(enter, first), min(leave, last)
        if enter > leave:
            return False
    return True


def box_overlaps_polygon(box, contour):
    """포함/부분 겹침/경계 접촉을 허용하며 오목한 ROI도 그대로 판정합니다."""
    x1, y1, x2, y2 = map(float, box)
    if not np.isfinite([x1, y1, x2, y2]).all() or x2 <= x1 or y2 <= y1:
        return False
    # 박스가 ROI 안에 완전히 포함된 경우.
    if cv2.pointPolygonTest(contour, (x1, y1), False) >= 0:
        return True
    # 나머지 겹침은 ROI의 꼭짓점 또는 변이 박스 안에 들어옵니다.
    return any(
        _edge_intersects_box(contour[index - 1], point, (x1, y1, x2, y2))
        for index, point in enumerate(contour)
    )


# 과속 카메라 메인 검출에서 화면 넓이의 이 비율을 넘는 박스는 비·적외선 노이즈로 보고 버립니다.
# 11/6 줌 뒤 가까이 온 트럭이 화면 절반을 넘어(번호판이 가장 커질 때 박스가 사라짐) 0.5에서 0.9로 올렸습니다.
SPEEDING_MAX_BOX_AREA_FRAC = 0.9


def filter_speed_main_detections(raw_dets, max_area_threshold):
    """과속 카메라 메인 모델 검출에서 사람·하반신과 승용차·트럭만 남깁니다. 점수 기준(model_confidences.MAIN_V2
    클래스별)은 infer가 이미 걸렀습니다."""
    rows = []
    for d in raw_dets:
        # BBox 면적이 max_area_threshold(화면의 SPEEDING_MAX_BOX_AREA_FRAC)를 넘는 거대 객체(비/IR 노이즈 등) 차단
        if (float(d[2]) - float(d[0])) * (float(d[3]) - float(d[1])) > max_area_threshold:
            continue
        if int(d[5]) in (ID_G_PERSON, ID_PERSON_LOW, *TARGET_VEHICLES):
            rows.append(d)
    return detection_array(rows)


def resolve_model_path(model_path):
    """설정 파일의 모델 경로가 상대 경로면 프로젝트 폴더 기준 절대 경로로 바꿉니다."""
    if not model_path:
        return model_path
    if os.path.isabs(model_path):
        return model_path
    return os.path.join(PROJECT_ROOT, model_path)

def get_model_output_format(model_key):
    return SYS_CFG["model_output_formats"][model_key]

def get_model_engine_pool_size(model_key):
    """모델 엔진 수(model_engine_pool_sizes). 엔진마다 NPU 메모리를 따로 씁니다."""
    return max(1, int(SYS_CFG["model_engine_pool_sizes"][model_key]))

def detection_array(rows):
    """추론 결과 리스트를 트래커/로그가 기대하는 Nx6 numpy 배열 형태로 맞춥니다."""
    if rows is None or len(rows) == 0:
        return np.empty((0, 6))
    return np.array(rows, dtype=float)


def create_roi_snapshot(cam, frame):
    """현재 카메라 프레임에 ROI와 설정된 이벤트 명만 그립니다."""
    img = frame.copy()

    # 1. ROI Polygon 그리기
    if len(cam.roi_poly) > 2:
        cv2.polylines(img, [np.array(cam.roi_poly, np.int32)], True, (0, 255, 255), 2)

    # 2. ROI Line 그리기(두 점이 선 하나)
    for p1, p2 in zip(cam.roi_lines[0::2], cam.roi_lines[1::2]):
        cv2.line(img, tuple(p1), tuple(p2), (0, 0, 255), 2)

    # 3. 설정된 이벤트 명 좌측 상단에 표시
    y_pos = 30
    for evt in cam.events:
        display_name = EVENT_REGISTRY[evt].gui_name if evt in EVENT_REGISTRY else evt.upper()
        cv2.putText(img, f"Event: {display_name}", (20, y_pos), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 1)
        y_pos += 30

    return img

def _send_roi_snapshot_task(cam_id, crane_id, img, roi_info_str, w, h, send_type="hourly"):
    """관제 서버로 ROI 스냅샷을 백그라운드에서 전송합니다."""
    if not receiver_send_enabled(cam_id):
        return False
    url = receiver_url("roi_snapshot")
    if not url:
        return False
    send_label = {
        "roi_refresh": "ROI 설정반영 스냅샷",
        "startup": "시작 ROI 스냅샷",
        "hourly": "1시간 정기 ROI 스냅샷",
    }.get(str(send_type), f"ROI 스냅샷({send_type})")
    try:
        # 이미지를 메모리 상에서 JPEG 바이너리로 인코딩
        ok, img_encoded = cv2.imencode('.jpg', img)
        if not ok:
            raise ValueError('ROI JPEG encoding failed')

        data = {
            "craneId": str(crane_id),
            "cctvId": int(cam_id),
            "imageWidth": int(w),
            "imageHeight": int(h),
            "cctvServerId": str(SYS_CFG["cctv_server_id"]),
            "isReqRoiSetup": False,
            "roiInfo": roi_info_str
        }

        files = {
            "image": (f"snapshot_cam{cam_id}.jpg", img_encoded.tobytes(), "image/jpeg")
        }

        resp = requests.post(url, data=data, files=files, verify=SYS_CFG["roi_snapshot"]["verify_tls"], timeout=15)

        if receiver_response_ok(resp):
            logger.info(f" [ROI Snapshot][{send_label}] CAM:{cam_id} 전송 성공")
            return True
        else:
            logger.error(
                f" [ROI Snapshot][{send_label}] CAM:{cam_id} API 에러 "
                f"({resp.status_code}): {resp.text}"
            )
    except Exception as e:
        logger.error(f" [ROI Snapshot][{send_label}] CAM:{cam_id} 전송 실패: {e}")
    return False

# ==========================================
# [2] 로깅 시스템 초기화
# ==========================================
LOG_DIR = SYS_CFG["logging"]["dir"]
os.makedirs(LOG_DIR, exist_ok=True)

# 파일 로그는 늘 DEBUG까지, 콘솔은 INFO까지 남깁니다(main에서 디버그 출력을 켜면 콘솔도 DEBUG).
logger = logging.getLogger("CCTV_SYSTEM")
logger.setLevel(logging.DEBUG)


_SHOWINFO_FILTER = None


def showinfo_filter():
    """PTS를 얻는 showinfo 필터. 기본값은 프레임마다 모든 픽셀의 체크섬·평균·표준편차를 계산해
    1080p 10fps에서 ffmpeg 하나에 CPU 17%를 더 씁니다(2026-10-20 146: 39% → 23%). checksum 옵션(FFmpeg 4.4+)이 있으면 끕니다."""
    global _SHOWINFO_FILTER
    if _SHOWINFO_FILTER is None:
        try:
            out = subprocess.run(["ffmpeg", "-hide_banner", "-h", "filter=showinfo"],
                                 capture_output=True, text=True, timeout=10).stdout
            _SHOWINFO_FILTER = "showinfo=checksum=0" if re.search(r"^\s*checksum\b", out, re.M) else "showinfo"
        except Exception:
            _SHOWINFO_FILTER = "showinfo"
        logger.info(f"[DECODE] PTS 필터: {_SHOWINFO_FILTER}")
    return _SHOWINFO_FILTER
formatter = logging.Formatter('%(asctime)s | %(levelname)-7s | [%(funcName)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

log_filename = datetime.datetime.now().strftime("cctv_%Y%m%d.log")
log_filepath = os.path.join(LOG_DIR, log_filename)

log_retention_days = max(1, int(SYS_CFG["logging"]["retention_days"]))
file_handler = TimedRotatingFileHandler(log_filepath, when="H", interval=1, backupCount=24 * log_retention_days, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

# 비동기 로깅을 위한 큐(Queue) 설정
log_queue = queue.Queue(-1)
queue_handler = QueueHandler(log_queue)
queue_handler.setLevel(logging.DEBUG)
logger.addHandler(queue_handler)

LOG_LISTENER = QueueListener(log_queue, file_handler, stream_handler, respect_handler_level=True)
LOG_LISTENER.start()

def graceful_shutdown():
    """시스템 종료 시 스레드 풀과 로거를 안전하게 정리합니다."""
    logger.info("[SYSTEM] Waiting for background I/O tasks to finish.")
    # 영상 마무리·사진 저장이 전송을 예약하므로, 전송 풀은 마지막에 닫습니다.
    for pool in (VIDEO_FINISH_POOL, IMAGE_SAVER_POOL, EVENT_SEND_POOL):
        try:
            pool.shutdown(wait=True)
        except Exception:
            pass
    try:
        LOG_LISTENER.stop()
    except Exception:
        pass

atexit.register(graceful_shutdown)

def cleanup_old_files(root_dir, retention_days, label):
    """root_dir 안에서 마지막 수정이 retention_days보다 오래된 파일을 지우고 빈 폴더를 정리합니다."""
    if retention_days <= 0:
        logger.info(f"[Retention] {label} 보관 정리가 비활성화되어 있습니다.")
        return

    root_dir = os.path.abspath(root_dir)
    if not os.path.isdir(root_dir):
        logger.debug(f"[Retention] {label} 폴더가 아직 없습니다: {root_dir}")
        return

    cutoff_ts = time.time() - (float(retention_days) * 86400.0)
    removed_files = 0
    removed_dirs = 0

    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=False):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            try:
                if os.path.getmtime(file_path) < cutoff_ts:
                    os.remove(file_path)
                    removed_files += 1
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.warning(f"[Retention] 오래된 {label} 파일 삭제 실패: {file_path} | {e}")

        if dirpath == root_dir:
            continue

        try:
            if not os.listdir(dirpath):
                os.rmdir(dirpath)
                removed_dirs += 1
        except OSError:
            pass

    if removed_files or removed_dirs:
        logger.info(f"[Retention] {label} {retention_days}일 보관 정리 완료: 파일 {removed_files}개, 빈 폴더 {removed_dirs}개 삭제")

def run_output_retention_cleanup(retention_days):
    """이벤트 산출물(사진·영상·기록)과 실행 로그를 보관 기간만큼만 남깁니다."""
    cleanup_old_files(EVENT_ROOT_DIR, retention_days, "이벤트 산출물")
    cleanup_old_files(LOG_DIR, retention_days, "실행 로그")

# ==========================================
# [3] 딥엑스 NPU 엔진 및 환경변수 설정
# ==========================================
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
os.environ["QT_QPA_PLATFORM"] = "xcb"
os.environ["OPENCV_LOG_LEVEL"] = "FATAL"
os.environ["OPENCV_FFMPEG_DEBUG"] = "0"
# 카메라 메타데이터(Data) 트랙은 받지 않습니다.
os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp|stimeout;3000000|max_delay;500000"

# dx_engine이 없으면(NPU 없는 PC) HAS_DX_ENGINE을 False로 두고 계속 실행합니다.
HAS_DX_ENGINE = False
try:
    from dx_engine import InferenceEngine, InferenceOption
    HAS_DX_ENGINE = True
except ImportError:
    logger.warning(" [환경 알림] dx_engine 모듈을 찾을 수 없습니다. 서버(GPU/CPU) 환경으로 간주합니다.")

# ==========================================
# [4] 공통 유틸리티 함수
# ==========================================
def sanitize_camera_url(url: str) -> str:
    """RTSP URL에서 공백과 ASCII가 아닌 글자를 지웁니다."""
    return re.sub(r'\s+', '', str(url or "").encode('ascii', 'ignore').decode('ascii'))

def apply_camera_password(url: str) -> str:
    """CAM_PASSWORD 환경변수가 있으면 비밀번호가 비어 있는 RTSP URL에 URL 인코딩하여 넣습니다."""
    password = os.environ.get("CAM_PASSWORD", "")
    if not password or not url or "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    netloc, sep, path = rest.partition("/")
    if "@" not in netloc:
        return url
    userinfo, host = netloc.rsplit("@", 1)
    user, _, current = userinfo.partition(":")
    if current and current not in ("<PASSWORD>", "%3CPASSWORD%3E", "${CAM_PASSWORD}", "$CAM_PASSWORD"):
        return url
    return f"{scheme}://{user}:{quote(password, safe='')}@{host}{sep}{path}"

def extract_ip(rtsp_url: str) -> str:
    """RTSP URL에서 식별용 고유 ID(IP+Channel)를 추출합니다."""
    try:
        clean_url = sanitize_camera_url(rtsp_url)
        if "://" not in clean_url:
            clean_url = f"rtsp://{clean_url}"

        parsed = urlsplit(clean_url)
        host = parsed.netloc.rsplit("@", 1)[-1].strip("[]").split(":")[0].split(".")[-1]

        # Path와 Query(채널 정보 등)를 포함하여 고유한 키 생성
        path = re.sub(r'[^a-zA-Z0-9]', '_', parsed.path)
        query = re.sub(r'[^a-zA-Z0-9]', '_', parsed.query)

        uid = f"{host}{path}_{query}".strip('_')
        return uid if uid else "unknown_cam"
    except Exception as e:
        logger.warning(f"고유 식별자 추출 실패: {e}")
        return "unknown_cam"

def load_rtsp_list_from_csv(csv_path):
    """CSV 파일에서 카메라 RTSP URL 목록을 로드합니다."""
    if not os.path.exists(csv_path):
        logger.error(f"카메라 목록 CSV를 찾을 수 없습니다: {csv_path}")
        return []

    rtsp_list = []
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                first_col = line.split(',')[0].strip()
                if first_col.lower() in ['url', 'rtsp', 'rtsp_url', 'camera_url']:
                    continue
                url = sanitize_camera_url(first_col)
                if url:
                    rtsp_list.append(url)
    except Exception as e:
        logger.error(f"카메라 리스트 로드 중 예외 발생: {e}")

    unique_list = list(dict.fromkeys(rtsp_list))
    logger.info(f"카메라 CSV 로드 완료: {len(unique_list)}대")
    return unique_list

def calculate_iou(box1, box2):
    """두 BBox 간의 IoU(Intersection over Union)를 계산합니다."""
    x1 = max(box1[0], box2[0])
    y1 = max(box1[1], box2[1])
    x2 = min(box1[2], box2[2])
    y2 = min(box1[3], box2[3])

    inter_area = max(0, x2 - x1) * max(0, y2 - y1)
    if inter_area == 0:
        return 0

    box1_area = (box1[2] - box1[0]) * (box1[3] - box1[1])
    box2_area = (box2[2] - box2[0]) * (box2[3] - box2[1])

    return inter_area / (box1_area + box2_area - inter_area)

def to_json_safe(value):
    """numpy 값, tuple, deque 등을 JSON 저장 가능한 기본 타입으로 바꿉니다."""
    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, deque)):
        return [to_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return to_json_safe(value.tolist())
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    return value

def int_box(box):
    return [int(round(float(v))) for v in box]

def now_kst():
    return datetime.datetime.now(pytz.timezone('Asia/Seoul'))

def safe_id_part(value):
    text = str(value if value is not None else "-")
    text = re.sub(r"[^0-9A-Za-z_.-]+", "_", text).strip("_")
    return text or "-"

def make_event_id(cam_id, ip, event_name, tid, fid, ts=None):
    ts = ts or now_kst()
    stamp = ts.strftime('%Y%m%dT%H%M%S%f')[:-3]
    return (
        f"{stamp}_cam{safe_id_part(cam_id)}_{safe_id_part(ip)}_"
        f"{safe_id_part(event_name)}_tid{safe_id_part(tid)}_fid{safe_id_part(fid)}"
    )

def append_event_audit_record(record, stage="event", status="ok", extra=None):
    if not SYS_CFG["logging"]["event_audit_enabled"]:
        return
    try:
        audit_record = dict(record or {})
        if extra:
            audit_record.update(extra)
        audit_record.setdefault("ts", now_kst().isoformat())
        audit_record["audit_stage"] = stage
        audit_record["artifact_status"] = status
        ts_text = str(audit_record.get("ts", ""))
        day = ts_text[:10].replace("-", "") if len(ts_text) >= 10 else now_kst().strftime('%Y%m%d')
        audit_path = os.path.join(EVENT_ROOT_DIR, "logs", f"event_{day}.jsonl")
        with EVENT_AUDIT_LOCK:
            _write_jsonl_records(audit_path, [audit_record])
    except Exception as e:
        logger.error(f"[EVENT AUDIT] write failed | event_id={(record or {}).get('event_id', '-')} | {e}")

def log_disk_health(paths):
    """디스크 남은 용량이 logging.disk_free_warn_gb보다 적을 때만 경고합니다."""
    threshold_gb = float(SYS_CFG["logging"]["disk_free_warn_gb"])
    seen_roots = set()
    for label, path in paths:
        target = os.path.abspath(path or PROJECT_ROOT)
        while not os.path.exists(target):
            parent = os.path.dirname(target)
            if parent == target:
                target = PROJECT_ROOT
                break
            target = parent
        if target in seen_roots:
            continue
        seen_roots.add(target)
        try:
            usage = shutil.disk_usage(target)
            free_gb = usage.free / (1024 ** 3)
            total_gb = usage.total / (1024 ** 3)
            if free_gb < threshold_gb:
                logger.warning(
                    f"⚠️ [DISK HEALTH] label={label} path={target} free_gb={free_gb:.2f} "
                    f"total_gb={total_gb:.2f} threshold_gb={threshold_gb:.2f}"
                )
        except Exception as e:
            logger.warning(f"[DISK HEALTH] check failed | label={label} path={path} | {e}")

def record_api_send_state(ok, event_id="-"):
    with API_SEND_STATE_LOCK:
        failures = API_SEND_STATE["consecutive_failures"]
        if ok:
            if failures > 0:
                logger.info(f"[API SEND RECOVERED] event_id={event_id or '-'} previous_failures={failures}")
            API_SEND_STATE["consecutive_failures"] = 0
            return

        failures += 1
        API_SEND_STATE["consecutive_failures"] = failures
        if failures in (1, 3, 10) or failures % 10 == 0:
            logger.warning(f"[API SEND DEGRADED] event_id={event_id or '-'} consecutive_failures={failures}")

def normalize_roi_points(points, width, height):
    if not points or width <= 0 or height <= 0:
        return []
    return [[round(float(x) / width, 6), round(float(y) / height, 6)] for x, y in points]

def denormalize_roi_points(points, width, height):
    if not points or width <= 0 or height <= 0:
        return []
    return [[int(round(float(x) * width)), int(round(float(y) * height))] for x, y in points]

def create_mosaic_image(images, screen_w=SCREEN_WIDTH, screen_h=SCREEN_HEIGHT):
    """여러 카메라의 영상을 하나의 모자이크 화면으로 합성합니다."""
    if not images:
        return None

    count = len(images)
    cols = max(1, math.ceil(math.sqrt(count)))
    rows = max(1, math.ceil(count / cols))

    cell_w = screen_w // cols
    cell_h = screen_h // rows

    mosaic = np.zeros((rows * cell_h, cols * cell_w, 3), dtype=np.uint8)

    for i, img in enumerate(images):
        r, c = divmod(i, cols)
        x, y = c * cell_w, r * cell_h

        if img is None:
            cell_img = np.zeros((cell_h, cell_w, 3), dtype=np.uint8)
            cv2.putText(cell_img, "No Signal", (10, 50), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 1)
        else:
            cell_img = cv2.resize(img, (cell_w, cell_h))

        mosaic[y:y+cell_h, x:x+cell_w] = cell_img
        cv2.rectangle(mosaic, (x, y), (x+cell_w, y+cell_h), (100, 100, 100), 1)

    return mosaic

# ==========================================
# [5] API 통신 및 이미지 저장
# ==========================================
def receiver_url(config_key):
    suffixes = {"event_api": "/img", "video_api": "/video", "roi_snapshot": "/roi/img", "health_check": "/health"}
    base = str(SYS_CFG["receiver"]["base_url"] or "").strip().rstrip("/")
    return base + suffixes[config_key] if base else ""


def receiver_camera_allowed(cctv_id):
    ids = SYS_CFG["receiver"]["cctv_ids"]
    return isinstance(ids, list) and str(cctv_id) in {str(value) for value in ids}


def receiver_send_enabled(cctv_id=None):
    config = SYS_CFG["receiver"]
    if config["enabled"] is not True or not str(SYS_CFG["crane_id"] or "").strip():
        return False
    if cctv_id is None:
        return isinstance(config["cctv_ids"], list) and bool(config["cctv_ids"])
    return receiver_camera_allowed(cctv_id)


def receiver_collected_at(event_ts):
    """이미지와 영상에 같은 검출 시각을 KST ISO 문자열로 전달합니다."""
    value = datetime.datetime.fromisoformat(str(event_ts).replace("Z", "+00:00"))
    kst = pytz.timezone("Asia/Seoul")
    value = kst.localize(value) if value.tzinfo is None else value.astimezone(kst)
    return value.replace(tzinfo=None).isoformat(timespec="milliseconds")


def receiver_event_type(event_name, configured=None):
    """관제 이벤트 번호(event_config.<이벤트>.api_event_type). 카메라별 설정 값(configured)이 있으면 그것입니다."""
    if configured is None:
        configured = SYS_CFG["event_config"].get(event_name, {}).get("api_event_type")
    return int(configured) if configured is not None else None


def receiver_response_ok(response):
    if not 200 <= response.status_code < 300:
        return False
    try:
        payload = response.json()
        return isinstance(payload, dict) and payload.get("success") is True
    except (ValueError, TypeError):
        return False


# 관제(헬스체크 activeSettings)가 끈 이벤트: 판정·녹화·번호판 읽기는 그대로 하고 관제 전송(사진·영상)만 막습니다.
# {str(cctv_id): {이벤트 이름: bool}} — Camera가 상태를 읽을 때마다 채웁니다(2026-11-11).
RECEIVER_EVENT_STATUS_BY_CAM = {}


def receiver_event_send_allowed(cctv_id, event_name):
    flags = RECEIVER_EVENT_STATUS_BY_CAM.get(str(cctv_id))
    return not (isinstance(flags, dict) and flags.get(event_name) is False)


def _send_receiver_file(file_path, file_field, config_key, event_meta, extra_fields=None):
    """녹화/저장이 완료된 파일만 보내고 HTTP 및 업무 처리 결과를 함께 확인합니다."""
    config = SYS_CFG[config_key]
    event_id = event_meta.get("event_id", "-")
    audit = dict(event_meta, **{file_field + "_path": str(file_path)})
    stage = "api_send" if file_field == "image" else "video_api_send"
    reason = None
    if not receiver_send_enabled(event_meta.get("cctv_id")):
        reason = "receiver_disabled_or_camera_excluded"
    elif not receiver_event_send_allowed(event_meta.get("cctv_id"), event_meta.get("event_name")):
        reason = "event_disabled_by_receiver"  # 관제가 끈 이벤트: 전송만 막음(판정·저장은 했음)
    elif not receiver_url(config_key):
        reason = "missing_url"
    elif not event_meta.get("crane_id") or not event_meta.get("collected_at"):
        reason = "missing_event_identity"
    elif event_meta.get("api_event_type") is None:
        reason = "unknown_event_type"
    if reason:
        logger.debug("[API SKIP] event_id=%s kind=%s reason=%s", event_id, file_field, reason)
        append_event_audit_record(audit, stage=stage, status="skipped", extra={"reason": reason})
        return False
    try:
        data = {"craneId": str(event_meta["crane_id"]), "cctvId": int(event_meta["cctv_id"]),
                "cctvServerId": str(SYS_CFG["cctv_server_id"]),
                "eventType": int(event_meta["api_event_type"]), "collectedAt": event_meta["collected_at"]}
        if data["cctvId"] < 1 or data["eventType"] < 1:
            raise ValueError("cctvId and eventType must be positive integers")
        data.update(extra_fields or {})
        if not os.path.isfile(file_path) or os.path.getsize(file_path) == 0:
            raise ValueError(f"{file_field} file is missing or empty")
        started = time.monotonic()
        mime = "image/jpeg" if file_field == "image" else "video/mp4"
        with open(file_path, "rb") as handle:
            response = requests.post(
                receiver_url(config_key), data=data, files={file_field: (os.path.basename(file_path), handle, mime)},
                verify=config["verify_tls"], timeout=config.get("timeout_sec", 10),
            )
        success = receiver_response_ok(response)
        try:
            payload = response.json()
        except ValueError:
            payload = {"message": response.text[:1000]}
        details = {"status_code": response.status_code, "elapsed_ms": round((time.monotonic() - started) * 1000),
                   "response": payload}
        append_event_audit_record(audit, stage=stage, status="ok" if success else "failed", extra=details)
        record_api_send_state(success, event_id)
        log = logger.info if success else logger.error
        log("[API SEND %s] kind=%s event_id=%s crane=%s cam=%s eventType=%s collectedAt=%s HTTP=%s response=%s",
            "OK" if success else "FAIL", file_field, event_id, data["craneId"], data["cctvId"],
            data["eventType"], data["collectedAt"], response.status_code, str(payload)[:1000])
        return success
    except Exception as error:
        logger.error("[API SEND FAIL] kind=%s event_id=%s error=%s", file_field, event_id, error)
        append_event_audit_record(audit, stage=stage, status="failed", extra={"reason": str(error)})
        record_api_send_state(False, event_id)
        return False


def send_event_image_to_receiver(image_path, event_name, crane_id, cctv_id, bboxes,
                                 img_width=None, img_height=None, event_id=None, api_event_type=None,
                                 event_ts=None):
    meta = {"event_id": event_id or "-", "event_name": event_name, "crane_id": crane_id, "cctv_id": cctv_id,
            "api_event_type": receiver_event_type(event_name, api_event_type),
            "collected_at": receiver_collected_at(event_ts) if event_ts else None}
    extra = {"detectedObjects": json.dumps(to_json_safe(bboxes or []), ensure_ascii=False)}
    if img_width is not None:
        extra["imageWidth"] = int(img_width)
    if img_height is not None:
        extra["imageHeight"] = int(img_height)
    return _send_receiver_file(image_path, "image", "event_api", meta, extra)


# 이벤트 영상 동시 변환 수(video_record.max_parallel). 카메라 여러 대가 같은 순간 이벤트를 내도
# 변환이 한꺼번에 몰려 판정·추론과 CPU를 다투지 않게 줄을 세웁니다.
_ENCODE_SLOTS = threading.BoundedSemaphore(max(1, int(SYS_CFG["video_record"]["max_parallel"])))


def transcode_to_h264(src, dst, fps=None):
    """관제 영상은 H.264로 보냅니다(API 담당자 요청). OpenCV는 H.264로 바로 못 써서(avc1 실패)
    mp4v로 쓴 녹화 파일을 FFmpeg libx264로 바꿉니다. 브라우저에서 바로 재생되게 yuv420p·앞쪽 moov(+faststart),
    홀수 크기는 짝수로 채웁니다. fps를 주면 저장된 시각 대신 초당 fps장 간격으로 다시 매겨(입력 -r)
    재생 시간을 실제 녹화 시간에 맞춥니다. 실패하면 False(호출한 쪽이 mp4v 그대로 보냄).
    설정은 video_record: preset·crf, 변환은 max_parallel개씩만(스레드 1, nice 10), 복잡한 화면은 max_kbps로 비트레이트를
    막아 관제 업로드 한도를 넘지 않게 합니다."""
    cfg = SYS_CFG["video_record"]
    cmd = (["nice", "-n", "10"] if shutil.which("nice") else []) + [
        "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin", "-y"]
    if fps:
        cmd += ["-r", f"{float(fps):.3f}"]
    max_kbps = int(float(cfg["max_kbps"] or 0))
    rate_cap = ["-maxrate", f"{max_kbps}k", "-bufsize", f"{2 * max_kbps}k"] if max_kbps > 0 else []
    cmd += ["-i", src, "-vf", "pad=ceil(iw/2)*2:ceil(ih/2)*2", "-c:v", "libx264",
            "-preset", str(cfg["preset"]), "-crf", str(cfg["crf"]), *rate_cap,
            "-pix_fmt", "yuv420p", "-movflags", "+faststart", "-an", "-threads", "1", dst]
    try:
        with _ENCODE_SLOTS:
            result = subprocess.run(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, timeout=120)
    except Exception as e:
        logger.warning(f"[VIDEO] H.264 변환 실패({os.path.basename(src)}): {e}")
        return False
    if result.returncode != 0 or not os.path.isfile(dst) or os.path.getsize(dst) == 0:
        err = (result.stderr or "").strip()[-300:]
        logger.warning(f"[VIDEO] H.264 변환 실패({os.path.basename(src)}) rc={result.returncode}: {err}")
        return False
    return True


def send_event_video_to_receiver(video_path, event_meta, width, height, frame_count, fps, codec="h264"):
    # derationMs는 Receiver OpenAPI의 실제 필드명입니다. codec은 실제 파일 코덱(h264, 변환 실패 때만 mp4v)입니다.
    extra = {"videoWidth": width, "videoHeight": height,
             "derationMs": round(frame_count / max(float(fps), 0.1) * 1000), "codec": codec}
    return _send_receiver_file(video_path, "video", "video_api", event_meta, extra)


def _draw_event_api_image(frame, event_type, bbox, tid, objects_meta=None):
    """관제로 보낼 사진: 객체 상자를 빨간색으로 그리고 라벨은 이벤트 이름만 적습니다(고객사 요청).
    이벤트 대상(tid)은 가는 선, 같이 찍힌 객체는 굵은 선입니다."""
    api_img = frame.copy()
    for obj in objects_meta or [{'box': bbox, 'tid': tid}]:
        x1, y1, x2, y2 = int_box(obj.get('box', bbox))
        is_target = tid is not None and obj.get('tid') is not None and int(obj['tid']) == int(tid)
        cv2.rectangle(api_img, (x1, y1), (x2, y2), (0, 0, 255), 1 if is_target else 2)
        cv2.putText(api_img, str(event_type), (x1, max(20, y1 - 8)), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (0, 0, 255), 2,
                    cv2.LINE_AA)
    return api_img


def _write_image_file(path, image, label="image", event_id="-"):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        ok = cv2.imwrite(path, image)
        if not ok:
            logger.error(f"[EVIDENCE SAVE FAIL] event_id={event_id or '-'} label={label} path={path} reason=cv2.imwrite_false")
            return False
        logger.debug(f"[EVIDENCE SAVE OK] event_id={event_id or '-'} label={label} path={path}")
        return True
    except Exception as e:
        logger.error(f"[EVIDENCE SAVE FAIL] event_id={event_id or '-'} label={label} path={path} | {e}")
        return False

def submit_event_send(fn, *args):
    """관제 전송을 EVENT_SEND_POOL에 예약합니다. 관제가 오래 막혀 EVENT_SEND_MAX_PENDING건 넘게 밀리면 보내지 않고
    경고만 남깁니다(사진·영상 파일은 단말에 저장되어 있음)."""
    if EVENT_SEND_POOL._work_queue.qsize() > EVENT_SEND_MAX_PENDING:
        logger.warning(f"[SEND] 관제 전송이 {EVENT_SEND_MAX_PENDING}건 넘게 밀려 이번 전송을 건너뜁니다: {getattr(fn, '__name__', fn)}")
        return None
    return EVENT_SEND_POOL.submit(fn, *args)


def _save_and_send_task(img, img_path, api_img, api_img_path, api_params):
    """사진 저장 일꾼(IMAGE_SAVER_POOL): 사진 두 장을 파일로 쓰고, 관제 전송은 파일 경로만 넘겨 EVENT_SEND_POOL에 맡깁니다
    (관제가 느려도 저장이 밀리지 않고, 전송 줄이 화면을 붙잡지 않음)."""
    event_id = api_params['event_id']
    audit_base = {
        "event_id": event_id,
        "ts": api_params['event_ts'],
        "event_name": api_params['event_name'],
        "crane_id": api_params['crane_id'],
        "cctv_id": api_params['cctv_id'],
        "ip": api_params['ip'],
        "image_path": img_path,
        "api_img_path": api_img_path
    }
    for path, image, label in ((img_path, img, "event_image"), (api_img_path, api_img, "api_image")):
        ok = _write_image_file(path, image, label=label, event_id=event_id)
        append_event_audit_record(audit_base, stage=f"{label}_saved", status="ok" if ok else "failed",
                                  extra={"saved_path" if ok else "failed_path": path})
        if not ok:
            return
    plate_future = api_params.get('plate_future')
    if api_params['event_name'] == SPEEDING_EVENT and plate_future is None:
        # 번호판을 읽지 않는 과속 이벤트(원본 추출 꺼짐): 차량번호가 없어 관제로 보내지 않음
        RECEIVER_SKIPPED_EVENTS.append(str(event_id))
        logger.info(f"[SPEEDING API] 차량번호가 없어(번호판 읽기 안 함) 관제로 보내지 않습니다(사진·영상). event_id={event_id}")
        append_event_audit_record(audit_base, stage="api_send", status="skipped", extra={"reason": "plate_not_read"})
        return
    if plate_future is not None:
        # 과속: 번호판(원본 추출·PLR, 보통 이벤트 5~15초 뒤)을 읽은 뒤 보냅니다. 전송 일꾼을 붙잡지 않게 결과가 나오면
        # 예약하고, 차량번호가 없으면(못 찾음, api_wait_sec 안에 못 읽음) 보내지 않습니다.
        wait = float(SYS_CFG["event_config"][SPEEDING_EVENT].get("api_wait_sec", 60.0))
        timer = threading.Timer(wait, resolve_plate_future, args=(event_id, None))
        timer.daemon = True
        timer.start()

        def send(done):
            timer.cancel()
            final = done.result()
            reason = plate_number_missing_reason(final)
            if reason:
                # 차량번호 없음(번호판 못 찾음·읽기 시간초과): 관제로 보내지 않고 로그만(영상도 _queue_video_upload에서 뺌)
                RECEIVER_SKIPPED_EVENTS.append(str(event_id))
                logger.info(f"[SPEEDING API] 차량번호가 없어({reason}) 관제로 보내지 않습니다(사진·영상). event_id={event_id}")
                append_event_audit_record(audit_base, stage="api_send", status="skipped",
                                          extra={"reason": "no_plate_number", "plate_status": reason})
                return
            params = dict(api_params, plate_final=final)
            scene = final.get("api_scene")
            if scene:
                # 관제 사진을 번호판이 읽힌 원본 장면(write_api_scene)으로 바꿉니다. 판정 순간 화면은 차가 이미 지나가
                # 번호판이 안 보일 때가 많습니다. 상자 좌표·크기도 그 사진 기준으로 보냅니다.
                try:
                    shutil.copyfile(scene["path"], api_img_path)
                    params.update(bboxes=[dict(b, box=list(scene["box"])) for b in api_params['bboxes']],
                                  img_width=scene["size"][0], img_height=scene["size"][1])
                except Exception as e:
                    logger.warning(f"[SPEEDING API] 관제 사진을 원본 장면으로 못 바꿔 판정 순간 사진을 보냅니다. event_id={event_id}: {e}")
            submit_event_send(_send_event_image_task, api_img_path, params)
        plate_future.add_done_callback(send)
        return
    submit_event_send(_send_event_image_task, api_img_path, api_params)


def _send_event_image_task(api_img_path, api_params):
    """관제 전송 일꾼(EVENT_SEND_POOL): 저장한 관제용 사진을 보냅니다."""
    event_id = api_params['event_id']
    bboxes = api_params['bboxes']
    ocr_future = api_params.get('ocr_future')
    if ocr_future is not None:
        # 콘 NG: 로컬 저장은 저장 일꾼이 이미 했고, 관제 전송만 컨테이너 번호를 읽을 때까지 기다립니다.
        fields = container_ocr_api_fields(ocr_future, SYS_CFG["container_ocr"]["api_wait_sec"])
        bboxes = [dict(b, **fields) for b in bboxes]
        logger.info(f"[CONTAINER_OCR] 관제 사진에 번호 포함 event_id={event_id} containerNo='{fields['containerNo']}' "
                    f"sizeType='{fields['containerSizeType']}' status={fields['ocrStatus']}")
    if api_params['event_name'] == SPEEDING_EVENT:
        # 과속: 관제 요청 형식(label speedviolation, regNo 번호판)
        bboxes = speeding_api_objects(bboxes, api_params.get('plate_final'))
        logger.info(f"[SPEEDING API] 관제 사진에 번호판 포함 event_id={event_id} "
                    f"regNo='{bboxes[0]['regNo'] if bboxes else ''}' status={bboxes[0]['plateStatus'] if bboxes else '-'} "
                    f"speed={bboxes[0]['speed'] if bboxes else '-'}")

    try:
        send_event_image_to_receiver(
            image_path=api_img_path,
            event_name=api_params['event_name'],
            crane_id=api_params['crane_id'],
            cctv_id=api_params['cctv_id'],
            bboxes=bboxes,
            img_width=api_params['img_width'],
            img_height=api_params['img_height'],
            event_id=event_id,
            api_event_type=api_params.get("api_event_type"),
            event_ts=api_params["event_ts"],
        )
    except Exception as e:
        logger.error(f"[Task 내부 API 호출 에러] {e}")

def _write_jsonl_records(path, records):
    """기록을 JSONL(한 줄에 기록 하나) 파일 끝에 덧붙입니다."""
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'a', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(to_json_safe(record), ensure_ascii=False) + "\n")
    except Exception as e:
        logger.error(f"[InferLog] JSONL write failed: {path} | {e}")

def save_event_image_with_mark(frame, ip, event_type, bbox, tid, crane_id, cctv_id, objects_meta, event_id, event_ts,
                               api_event_type, ocr_future=None, plate_future=None):
    """원본 프레임 이미지를 로컬에 저장하고 탐지 메타데이터를 API 큐에 등록합니다.
    ocr_future(콘 NG)가 있으면 컨테이너 번호를 읽은 뒤 detectedObjects에 넣어 보냅니다(container_ocr_api_fields).
    plate_future(과속)가 있으면 번호판을 읽은 뒤 regNo를 넣어 보냅니다(speeding_api_objects)."""
    if IMAGE_SAVER_POOL._work_queue.qsize() > 50:
        logger.warning("이미지 저장 큐가 포화 상태입니다. 저장을 스킵합니다.")
        return

    try:
        img = frame.copy()
        x1, y1, x2, y2 = map(int, bbox)

        dpath = os.path.join(EVENT_ROOT_DIR, "events", ip, "images", str(event_type))
        api_dpath = os.path.join(EVENT_ROOT_DIR, "events", ip, "images_api", str(event_type))
        os.makedirs(dpath, exist_ok=True)
        os.makedirs(api_dpath, exist_ok=True)

        fname = f"{safe_id_part(event_id)}.jpg"
        img_path = os.path.join(dpath, fname)
        api_img_path = os.path.join(api_dpath, fname)
        evidence_paths = {
            "event_id": event_id,
            "ts": event_ts,
            "image_path": img_path,
            "api_img_path": api_img_path,
            "image_basename": fname
        }

        api_img = _draw_event_api_image(img, event_type, [x1, y1, x2, y2], tid, objects_meta)

        h, w = frame.shape[:2]

        ai_detected_bboxes = []
        for o in objects_meta:
            item = {"box": [int(b) for b in o['box']], "label": o['label'], "score": round(float(o['score']), 2)}
            if o.get('tid') is not None:
                item["tid"] = int(o['tid'])
            if o.get('speed_kmh') is not None:   # 과속: 레이더 판정 속도(관제 detectedObjects speed)
                item["speed_kmh"] = round(float(o['speed_kmh']), 1)
            ai_detected_bboxes.append(item)

        api_params = {
            'api_event_type': api_event_type,
            'ip': ip,
            'event_id': event_id,
            'event_ts': event_ts,
            'event_name': event_type,
            'crane_id': str(crane_id),
            'cctv_id': int(cctv_id),
            'bboxes': ai_detected_bboxes,
            'img_width': w,
            'img_height': h,
            'ocr_future': ocr_future,
            'plate_future': plate_future
        }

        IMAGE_SAVER_POOL.submit(_save_and_send_task, img, img_path, api_img, api_img_path, api_params)
        append_event_audit_record(
            {
                "event_id": event_id,
                "ts": event_ts,
                "event_name": event_type,
                "crane_id": str(crane_id),
                "cctv_id": int(cctv_id),
                "ip": ip,
                "tid": int(tid),
                "bbox": [x1, y1, x2, y2],
                "image_path": img_path,
                "api_img_path": api_img_path,
                "image_basename": fname
            },
            stage="evidence_queued",
            status="queued"
        )
        return evidence_paths

    except Exception as e:
        logger.error(f"[EventLogic Error] 이미지 마킹 중 예외 발생: {e}")
        return None

# ==========================================
# [6] DeepX NPU 모델 추론
# ==========================================
class YoLoDeepX:
    # 클래스별 점수 기준 {클래스 번호: 기준}(apply_class_confidences). 적지 않은 클래스는 버립니다.
    # None(기준을 붙이지 않은 모델)이면 infer의 conf_override 하나를 모든 클래스에 씁니다.
    class_conf = None

    def __init__(self, engine_path, output_format="yolo", pool_size=1,
                 use_ort=None, match_input_shape=False, max_det=None):
        if not HAS_DX_ENGINE:
            raise RuntimeError("dx_engine is not installed; YoLoDeepX can only run on a DeepX/NPU runtime.")

        self.engine_path = engine_path
        self.output_format = self._output_format(output_format)
        self.pool_size = max(1, int(pool_size or 1))
        self.engine_pool = queue.Queue(maxsize=self.pool_size)
        self.engines_ref = []
        self.match_input_shape = match_input_shape
        self.max_det = max_det

        try:
            io = InferenceOption()
            if use_ort is not None:
                io.use_ort = bool(use_ort)
            for _ in range(self.pool_size):
                engine = InferenceEngine(self.engine_path, io)
                self.engine_pool.put(engine)
                self.engines_ref.append(engine)

            self._load_input_shape(self.engines_ref[0])
            logger.info(
                f"[DeepX] 모델 로드 성공: {os.path.basename(self.engine_path)} "
                f"(output={self.output_format}, pool={self.pool_size}, "
                f"input={self.input_width}x{self.input_height})"
            )
        except Exception as e:
            logger.error(f"[DeepX Load Fail] 엔진 초기화 실패 ({engine_path}): {e}")
            raise e

    def __del__(self):
        self.release()

    def release(self):
        while hasattr(self, "engine_pool") and not self.engine_pool.empty():
            try:
                self.engine_pool.get_nowait()
            except Exception:
                break
        for engine in getattr(self, "engines_ref", []):
            try:
                dispose = getattr(engine, "dispose", None)
                if callable(dispose):
                    dispose()
            except Exception:
                pass
        self.engines_ref = []

    @staticmethod
    def _output_format(output_format):
        """설정의 출력 형식: "ppu"(장치가 거른 상자 목록) 또는 "yolo"(raw YOLO, 앞 4칸 = 중심 x·y·폭·높이)."""
        fmt = str(output_format or "").strip().lower()
        if fmt not in ("ppu", "yolo"):
            raise ValueError(f"모델 출력 형식은 ppu 또는 yolo여야 합니다: {output_format!r}")
        return fmt

    def prepare(self, img):
        """추론 입력 준비: letter_box(비율 유지, 회색 114 여백) → BGR→RGB 텐서. (텐서, 배율, 여백, 원래 크기)를 돌려줍니다."""
        npu_input, scale, offset = self.letter_box(img)
        return self._prepare_input_tensor(npu_input), scale, offset, img.shape[:2]

    def infer(self, img, conf_override=None, prepared=None):
        """prepared(같은 그림을 같은 입력 모양으로 준비한 결과)를 주면 그림 준비를 건너뜁니다."""
        if prepared is None:
            if img is None:
                return np.empty((0,6))
            prepared = self.prepare(img)
        input_tensor, scale, offset, (h_orig, w_orig) = prepared
        engine = self.engine_pool.get()

        try:
            # 클래스별 기준(model_confidences)이 있으면 그중 가장 낮은 값까지 받은 뒤 클래스마다 거르고, 적지 않은 클래스는
            # 버립니다. NMS는 클래스마다 따로라, 낮은 점수 상자가 늘어도 다른 클래스·더 높은 점수 상자는 그대로입니다.
            class_conf = self.class_conf
            if class_conf is not None and not class_conf:
                return np.empty((0, 6))
            run_thres = (min(class_conf.values()) if class_conf
                         else conf_override if conf_override is not None else 0.40)
            output_tensor = engine.run([input_tensor])

            if self.output_format == "ppu":
                raw_dets = self.postprocess_ppu(output_tensor, conf_thres=run_thres)
            else:
                raw_dets = self.postprocess(output_tensor, conf_thres=run_thres)

            if not raw_dets:
                return np.empty((0,6))

            res = []
            dw, dh = offset

            for box, score, cls_id in raw_dets:
                if class_conf and float(score) < class_conf.get(int(cls_id), float("inf")):
                    continue
                x1 = np.clip((box[0] - dw) / scale, 0, w_orig)
                y1 = np.clip((box[1] - dh) / scale, 0, h_orig)
                x2 = np.clip((box[2] - dw) / scale, 0, w_orig)
                y2 = np.clip((box[3] - dh) / scale, 0, h_orig)
                res.append([x1, y1, x2, y2, score, cls_id])

            # 클래스별 기준으로 다 걸러져도 (0, 6) 모양으로 돌려줍니다.
            return np.array(res, dtype=float).reshape(-1, 6)
        except Exception as e:
            logger.error(f"NPU Inference Error: {e}")
            return np.empty((0,6))
        finally:
            self.engine_pool.put(engine)

    def raw_yolo_num_classes(self):
        """raw YOLO 출력([1, 4+클래스 수, 후보 수])의 클래스 수입니다. topdown·콘 모델이 맞는 모델인지 볼 때 씁니다
        (check_topdown_class_count, ConeObserver). 회색 빈 그림 한 장을 넣어 실제 출력 모양으로 셉니다.
        raw YOLO가 아니거나 셀 수 없으면 None."""
        if self.output_format != "yolo" or not self.engines_ref:
            return None
        engine = self.engine_pool.get()
        try:
            blank = np.full((self.input_height, self.input_width, 3), 114, dtype=np.uint8)
            outputs = engine.run([self._prepare_input_tensor(blank)])
            counts = [count for count in (self._raw_yolo_class_count(np.shape(o)) for o in outputs) if count]
        except Exception as e:
            logger.warning(f"[DeepX] 클래스 수 확인 실패({os.path.basename(self.engine_path)}): {e}")
            return None
        finally:
            self.engine_pool.put(engine)
        return counts[0] if counts else None

    @staticmethod
    def _raw_yolo_class_count(shape):
        dims = [int(x) for x in shape if int(x) > 1]
        if len(dims) == 2 and max(dims) >= 1000 and min(dims) > 4:
            return min(dims) - 4
        return None

    def _load_input_shape(self, engine):
        """입력 크기를 읽습니다. 모델은 모두 NHWC [1, 높이, 폭, 3] 입력입니다."""
        shape = [int(v) for v in engine.get_input_tensors_info()[0]["shape"]]
        if len(shape) != 4 or shape[-1] != 3:
            raise ValueError(f"NHWC [1, 높이, 폭, 3] 입력 모델이 아닙니다: {shape}")
        self.input_height, self.input_width = shape[1], shape[2]
        # 1280x736은 1280x720 그림을 줄이지 않고 위아래 8px만 채워 넣는 변환본입니다(9클래스 topdown).
        if self.match_input_shape and shape not in ([1, 1280, 1280, 3], [1, 736, 1280, 3]):
            raise ValueError(f"topdown 모델은 [1,1280,1280,3] 또는 [1,736,1280,3] NHWC 입력이 필요합니다: {shape}")

    def letter_box(self, img, new_shape=None):
        if new_shape is None:
            new_shape = (self.input_height, self.input_width)
        h, w = img.shape[:2]
        scale = min(new_shape[0]/h, new_shape[1]/w)
        if self.match_input_shape:
            # topdown 모델의 학습/변환 입력과 같은 반올림·중앙 여백입니다.
            nw, nh = int(round(w * scale)), int(round(h * scale))
        else:
            nw, nh = int(w*scale), int(h*scale)

        # 크기가 이미 맞으면(1280x720 그림 → 가로 1280 모델) 크기 바꾸기를 건너뜁니다.
        resized = img if (nw, nh) == (w, h) else cv2.resize(img, (nw, nh))
        canvas = np.full((new_shape[0], new_shape[1], 3), 114, dtype=np.uint8)

        dw, dh = (new_shape[1] - nw) // 2, (new_shape[0] - nh) // 2
        canvas[dh:dh+nh, dw:dw+nw] = resized

        return canvas, scale, (dw, dh)

    def _prepare_input_tensor(self, npu_input):
        input_tensor = cv2.cvtColor(npu_input, cv2.COLOR_BGR2RGB)
        # PPU 모델과 topdown은 SDK가 알려 준 입력 모양 [1, 높이, 폭, 3]에 맞춰 배치 차원을 붙입니다.
        # 그 밖의 raw YOLO 모델(번호판·side·cone)은 HWC 그대로 넣습니다.
        if self.output_format == "ppu" or self.match_input_shape:
            input_tensor = input_tensor[np.newaxis]
        return np.ascontiguousarray(input_tensor, dtype=np.uint8)

    def postprocess(self, output_tensor, conf_thres=0.40, iou_thres=0.45):
        try:
            pred = np.array(output_tensor[0])

            # YOLOv8 배열 형태 보정
            if pred.ndim == 3 and pred.shape[1] < pred.shape[2]:
                pred = pred.transpose((0, 2, 1))
            if pred.ndim == 3:
                pred = pred[0]

            class_scores = pred[:, 4:]
            if class_scores.shape[1] == 1:
                # Single-class YOLO heads expose one score column after xywh.
                scores = class_scores[:, 0]
                class_ids = np.zeros(scores.shape, dtype=np.int32)
            else:
                scores = np.max(class_scores, axis=1)
                class_ids = np.argmax(class_scores, axis=1)

            # Confidence 필터링
            mask = scores > conf_thres
            pred = pred[mask]
            scores = scores[mask]
            class_ids = class_ids[mask]

            if len(pred) == 0:
                return []

            # NMSBoxes 포맷 맞춤 (x_min, y_min, width, height)
            boxes_xywh = pred[:, :4].copy()
            boxes_xywh[:, 0] = boxes_xywh[:, 0] - boxes_xywh[:, 2] / 2  # 중심 X -> 최소 X
            boxes_xywh[:, 1] = boxes_xywh[:, 1] - boxes_xywh[:, 3] / 2  # 중심 Y -> 최소 Y

            # Class-Aware NMS
            max_wh = 7680
            class_offset = class_ids * max_wh
            boxes_shifted = boxes_xywh.copy()
            boxes_shifted[:, 0] += class_offset
            boxes_shifted[:, 1] += class_offset

            indices = cv2.dnn.NMSBoxes(boxes_shifted.tolist(), scores.tolist(), conf_thres, iou_thres)

            results = []
            if len(indices) > 0:
                for i in indices.flatten()[:self.max_det]:
                    x_min, y_min, w, h = boxes_xywh[i]
                    results.append([[x_min, y_min, x_min + w, y_min + h], scores[i], class_ids[i]])

            return results
        except Exception as e:
            logger.error(f"NPU Postprocess Error ({os.path.basename(self.engine_path)}): {e}")
            return []

    def postprocess_ppu(self, output_tensor, conf_thres=0.40, iou_thres=0.45):
        try:
            raw_data = output_tensor[0]
            if isinstance(raw_data, bytes):
                flat = np.frombuffer(raw_data, dtype=np.uint8).copy()
            else:
                flat = np.ascontiguousarray(raw_data).view(np.uint8).ravel().copy()

            if len(flat) == 0:
                return []

            stride = 32
            if len(flat) % stride != 0:
                logger.error(f"[DeepX PPU] 출력 버퍼 길이({len(flat)})가 {stride}의 배수가 아닙니다.")
                return []

            flat_stride = flat.reshape(len(flat) // stride, stride)
            boxes_raw = np.ascontiguousarray(flat_stride[:, :16]).view(np.float32).reshape(-1, 4)
            scores = np.ascontiguousarray(flat_stride[:, 20:24]).view(np.float32).flatten()
            labels = np.ascontiguousarray(flat_stride[:, 24:28]).view(np.uint32).flatten()

            mask = scores >= conf_thres
            if not np.any(mask):
                return []

            boxes_raw = boxes_raw[mask]
            scores = scores[mask]
            labels = labels[mask]

            cx = boxes_raw[:, 0]
            cy = boxes_raw[:, 1]
            bw = boxes_raw[:, 2]
            bh = boxes_raw[:, 3]

            x1 = cx - bw * 0.5
            y1 = cy - bh * 0.5
            x2 = cx + bw * 0.5
            y2 = cy + bh * 0.5

            max_wh = 7680
            class_offset = labels.astype(np.float32) * max_wh
            boxes_shifted = np.column_stack([x1 + class_offset, y1 + class_offset, bw, bh])

            indices = cv2.dnn.NMSBoxes(boxes_shifted.tolist(), scores.tolist(), conf_thres, iou_thres)
            if indices is None or len(indices) == 0:
                return []

            results = []
            for i in np.array(indices).reshape(-1):
                results.append([[x1[i], y1[i], x2[i], y2[i]], scores[i], labels[i]])

            return results
        except Exception as e:
            logger.error(f"NPU PPU Postprocess Error ({os.path.basename(self.engine_path)}): {e}")
            return []

# ==========================================
# [7] 객체 트래커 및 영상 녹화기
# ==========================================
class SimpleTracker:
    def __init__(self, max_lost=30, history_len=60): # 15FPS 기준 약 4초 분량의 궤적 저장
        self.next_id = 1
        self.tracks = {}
        self.max_lost = max_lost
        self.history_len = history_len

    def update(self, detections):
        used_dets = set()

        for tid, trk in self.tracks.items():
            best_iou = 0
            best_idx = -1

            for i, det in enumerate(detections):
                if i in used_dets:
                    continue
                if int(det[5]) != trk['cls']:
                    continue

                iou = calculate_iou(trk['bbox'], det[:4])
                if iou > best_iou:
                    best_iou = iou
                    best_idx = i

            if best_iou > 0.2:
                # 중심점 좌표 계산 및 히스토리에 누적
                cx = int((detections[best_idx][0] + detections[best_idx][2]) / 2)
                cy = int((detections[best_idx][1] + detections[best_idx][3]) / 2)

                self.tracks[tid].update({
                    'bbox': detections[best_idx][:4],
                    'lost': 0,
                    'conf': detections[best_idx][4]
                })
                self.tracks[tid]['history'].append((cx, cy))
                used_dets.add(best_idx)
            else:
                self.tracks[tid]['lost'] += 1

        self.tracks = {tid: t for tid, t in self.tracks.items() if t['lost'] <= self.max_lost}

        res_tracks = []
        for i, det in enumerate(detections):
            if i not in used_dets:
                cx = int((det[0] + det[2]) / 2)
                cy = int((det[1] + det[3]) / 2)
                self.tracks[self.next_id] = {
                    'bbox': det[:4], 'lost': 0, 'cls': int(det[5]), 'conf': det[4],
                    'history': deque([(cx, cy)], maxlen=self.history_len) # 신규 객체 궤적 초기화
                }
                self.next_id += 1

        for tid, trk in self.tracks.items():
            if trk['lost'] == 0:
                res_tracks.append([*trk['bbox'], tid, trk['conf'], trk['cls']])

        return np.array(res_tracks)


class MotionTracker:
    """SimpleTracker의 ID 바뀜을 줄인 추적기(출력 형식 동일: [x1, y1, x2, y2, tid, conf, cls]).

    SimpleTracker와 다른 점
    - 움직임 예측: 박스 이동 속도를 기억해 다음 위치를 예측하고, 예측 박스와 검출을 비교
    - 전체 짝짓기: 오래된 추적부터 먼저 가져가지 않고, 모든 (추적, 검출) 쌍을 겹침 큰 순서로 1:1. 방금까지 보이던 추적이 놓친 추적보다 우선
    - 크기 급변 거부: 박스 넓이가 한 번에 AREA_RATIO(2.2)배 이상 바뀌면 같은 차로 보지 않음
      (주차 트럭 앞을 지나가는 차의 박스를 트럭 ID가 가져가는 경우 방지)
    - 박스 가로세로 비율이 ASPECT_RATIO(1.5)배 이상 다르면 같은 차로 보지 않음
      (주차 트럭 앞을 지나가는 승용차가 잠깐 가려졌다 다시 보일 때 트럭과 ID가 맞바뀌는 것 방지)
    - 승용/트럭 분류가 프레임마다 바뀌어도 같은 차로 이어 붙이고, 종류는 다수결
    - 놓친 추적은 MAX_LOST(30프레임)까지 유지"""
    IOU_THR = 0.2        # 예측 박스와 검출의 최소 겹침
    AREA_RATIO = 2.2
    ASPECT_RATIO = 1.5
    MAX_LOST = 30

    def __init__(self, history_len=60):
        self.history_len = history_len
        self.next_id = 1
        self.tracks = {}

    def _pred(self, trk):
        k = trk['lost'] + 1
        return [b + v * k for b, v in zip(trk['bbox'], trk['vel'])]

    def update(self, detections):
        dets = [list(map(float, d[:6])) for d in detections] if len(detections) else []
        preds = {tid: self._pred(trk) for tid, trk in self.tracks.items()}
        pairs = []
        for tid, pb in preds.items():
            pa = max(1.0, (pb[2] - pb[0]) * (pb[3] - pb[1]))
            pasp = max(1.0, pb[2] - pb[0]) / max(1.0, pb[3] - pb[1])
            for i, d in enumerate(dets):
                ov = calculate_iou(pb, d[:4])
                if ov < self.IOU_THR:
                    continue
                ratio = max(1.0, (d[2] - d[0]) * (d[3] - d[1])) / pa
                if ratio > self.AREA_RATIO or ratio < 1.0 / self.AREA_RATIO:
                    continue
                asp = max(1.0, d[2] - d[0]) / max(1.0, d[3] - d[1]) / pasp
                if asp > self.ASPECT_RATIO or asp < 1.0 / self.ASPECT_RATIO:
                    continue  # 가로로 긴 승용차 박스와 정사각형에 가까운 트럭 정면 박스가 서로 ID를 바꾸는 것 방지
                # 방금까지 보이던 추적 우선, 그다음 겹침 큰 순
                pairs.append((self.tracks[tid]['lost'] > 0, -ov, tid, i))
        used_t, used_d = set(), set()
        for _, _, tid, i in sorted(pairs):
            if tid in used_t or i in used_d:
                continue
            used_t.add(tid)
            used_d.add(i)
            trk, d = self.tracks[tid], dets[i]
            k = trk['lost'] + 1
            step = [(n - o) / k for n, o in zip(d[:4], trk['bbox'])]
            trk['vel'] = [0.6 * v + 0.4 * s for v, s in zip(trk['vel'], step)]
            trk['bbox'], trk['lost'], trk['conf'] = d[:4], 0, d[4]
            trk['cls_votes'][int(d[5])] += 1
            trk['history'].append((int((d[0] + d[2]) / 2), int((d[1] + d[3]) / 2)))
        for tid, trk in list(self.tracks.items()):
            if tid in used_t:
                continue
            trk['lost'] += 1
            trk['vel'] = [v * 0.8 for v in trk['vel']]  # 놓친 동안 예측 이동을 점점 줄임
            if trk['lost'] > self.MAX_LOST:
                del self.tracks[tid]
        for i, d in enumerate(dets):
            if i in used_d:
                continue
            self.tracks[self.next_id] = {
                'bbox': d[:4], 'vel': [0.0, 0.0, 0.0, 0.0], 'lost': 0, 'conf': d[4],
                'cls_votes': Counter({int(d[5]): 1}),
                'history': deque([(int((d[0] + d[2]) / 2), int((d[1] + d[3]) / 2))], maxlen=self.history_len)}
            self.next_id += 1
        res = [[*trk['bbox'], tid, trk['conf'], trk['cls_votes'].most_common(1)[0][0]]
               for tid, trk in self.tracks.items() if trk['lost'] == 0]
        return np.array(res)


def moving_tracks(hist_store, tracks, now):
    """움직이는 차량 박스만 골라냅니다(레이더는 움직이는 차량만 내보내므로 연결 후보).

    - 최근 1초(window) 동안 박스 하단 중심이 박스 폭의 15%(ratio) 이상
      움직였거나, 박스 크기가 6% 이상 바뀌면(정면으로 천천히 오가는 차) 움직이는 차
    - 단, 3초(long_sec) 넘게 보인 박스가 최근 3초 동안 거의 제자리(이동 < 폭 30%, 크기 변화 < 15%)면 서 있는 차.
      앞을 지나가는 차에 가렸다 보이면서 주차 차량 박스가 1초 사이 출렁이는 것을 움직임으로 오인하지 않게 함
    hist_store: {tid: deque[(시각, 하단 중심 x, 하단 y, 폭)]} 호출하는 쪽이 보관(프레임마다 같은 것을 넘김)."""
    window, ratio, long_sec = 1.0, 0.15, 3.0
    live, moving = set(), []
    for t in tracks:
        tid = int(t[4])
        live.add(tid)
        hist = hist_store.setdefault(tid, deque(maxlen=128))
        hist.append((now, (float(t[0]) + float(t[2])) / 2.0, float(t[3]), float(t[2]) - float(t[0])))
        while hist and now - hist[0][0] > long_sec + 0.5:
            hist.popleft()
        old = next((h for h in hist if now - h[0] <= window), None)
        if old is None or now - old[0] < window * 0.5:
            continue  # 이력이 짧으면 아직 판단하지 않음(연결 후보 아님)
        cur = hist[-1]
        grow = abs(cur[3] - old[3]) / max(8.0, old[3])  # 정면으로 천천히 오가는 차는 위치보다 크기가 변함
        if not (math.hypot(cur[1] - old[1], cur[2] - old[2]) >= ratio * max(8.0, cur[3]) or grow >= 0.06):
            continue
        first = hist[0]
        if now - first[0] >= long_sec:
            net = math.hypot(cur[1] - first[1], cur[2] - first[2])
            grow_long = abs(cur[3] - first[3]) / max(8.0, first[3])
            if net < 0.3 * max(8.0, cur[3]) and grow_long < 0.15:
                continue
        moving.append(t)
    for tid in [k for k in hist_store if k not in live]:
        del hist_store[tid]
    return np.array(moving) if moving else np.empty((0, 7))


def record_frame_from_display(display_fr, size):
    """녹화용 그림(Camera.draw(for_record=True) 결과)을 녹화 크기(size = (가로, 세로), 추론 프레임 크기)로 맞춥니다.
    과속(고해상도 원본)·콘(가로 1280) 카메라는 화면이 추론 프레임보다 커서 줄입니다(녹화 메모리는 예전과 같음).
    관제 요청으로 시각(Event Time)은 적지 않습니다."""
    w, h = int(size[0]), int(size[1])
    if display_fr.shape[1] != w or display_fr.shape[0] != h:
        return cv2.resize(display_fr, (w, h), interpolation=cv2.INTER_AREA)
    return display_fr


class VideoRecorder:
    """이벤트 영상 녹화기(카메라마다 하나).

    관제 영상은 이벤트마다 이벤트 순간 앞 REC_PRE_SEC(30초)·뒤 REC_POST_SEC(30초), 총 1분입니다(API 담당자 요청).
    이벤트가 겹쳐도 이벤트마다 따로 1분 영상을 만듭니다(겹친 시간의 화면은 두 영상에 같이 들어감).
    녹화되는 화면은 메인 루프가 처리한 장수라 초당 10장보다 적고 고르지 않아, 재생 시간은 화면 시각으로 맞춥니다(_finish_closed_clip).
    """
    # 녹화 스레드에 쌓아 둘 화면 수 상한(초당 10장 30초 분량). 정상일 때는 바로 비워져 닿지 않고, 저장이 밀릴 때 메모리를 지킵니다.
    MAX_QUEUED_FRAMES = 300

    def __init__(self, ip):
        self.ip = ip
        self.pre_sec = float(SYS_CFG["REC_PRE_SEC"])
        self.post_sec = float(SYS_CFG["REC_POST_SEC"])
        # 최근 화면은 앞 pre_sec(+2초 여유)만큼만 시각 기준으로 남깁니다. 초당 15장으로 쳐도 넘지 않게 개수 상한도 둡니다.
        self.buffer = deque(maxlen=int(15 * (self.pre_sec + 2.0)))
        self.write_queue = queue.Queue()
        self.dropped = 0   # 녹화 대기열이 MAX_QUEUED_FRAMES를 넘어 버린 화면 수(풀리면 로그 남기고 0)
        self.lock = threading.Lock()
        # 녹화 중인 이벤트 구간(이벤트마다 하나): event, meta, time(이벤트 순간), start, end, fps(시각을 모를 때 쓸 값)
        self.clips = []
        self.running = True

        self.thread = threading.Thread(target=self._writer_loop, daemon=True)
        self.thread.start()

    def update(self, frame, infer_meta=None, timestamp=None):
        if frame is None: return
        ts = timestamp or time.time()
        item = (frame.copy(), infer_meta, ts)
        with self.lock:
            self.buffer.append(item)
            while self.buffer and self.buffer[0][2] < ts - self.pre_sec - 2.0:
                self.buffer.popleft()
            if not self.clips:
                return
            # 이벤트 뒤 post_sec가 지난 구간은 닫고(이 화면은 넣지 않음), 아직 열린 구간이 있으면 이 화면을 녹화합니다.
            for clip in [clip for clip in self.clips if ts > clip["end"]]:
                self.clips.remove(clip)
                self.write_queue.put(("__clip_end__", clip))
            if not self.clips:
                return
            if self.write_queue.qsize() >= self.MAX_QUEUED_FRAMES:
                # 저장이 밀려 화면이 쌓이면 이 화면은 버립니다(영상에서 그만큼 빠짐). 구간 시작·끝 표시는 버리지 않습니다.
                if self.dropped == 0:
                    logger.warning(f"[VIDEO] {self.ip} 녹화 대기열이 {self.MAX_QUEUED_FRAMES}장을 넘어 화면을 버립니다(저장 지연).")
                self.dropped += 1
                return
            if self.dropped:
                logger.warning(f"[VIDEO] {self.ip} 녹화 대기열이 풀렸습니다. 버린 화면 {self.dropped}장.")
                self.dropped = 0
            self.write_queue.put(item)

    def trigger(self, event_name, event_meta=None, now=None, on_raw_done=None):
        """이벤트 순간(now)을 가운데 둔 구간을 엽니다. 앞 pre_sec는 최근 화면에서 넣고, 뒤 post_sec는 update가 채웁니다.
        on_raw_done을 주면 H.264 변환·관제 전송 없이 mp4v 임시 파일만 남기고, 끝나면 on_raw_done(파일 상태 또는 None)을
        부릅니다(콘 묶음 4분할 영상이 카메라별 녹화를 모아 합칠 때)."""
        now = time.time() if now is None else now
        clip = {"event": event_name, "meta": event_meta, "time": now, "start": now - self.pre_sec,
                "end": now + self.post_sec, "fps": 3.0, "on_raw_done": on_raw_done}
        with self.lock:
            pre_frames = [item for item in self.buffer if item[2] >= clip["start"]]
            self.clips.append(clip)
            self.write_queue.put(("__clip_start__", clip, pre_frames))
        logger.info(f" [녹화시작] {self.ip} - {event_name} (이벤트 앞 {self.pre_sec:g}초·뒤 {self.post_sec:g}초, "
                    f"앞 화면 {len(pre_frames)}장)")

    @staticmethod
    def _finish_clip(raw_path, fpath, fps=None):
        """녹화가 끝난 mp4v 임시 파일(raw_path)을 H.264(fpath)로 바꿉니다. fps를 주면 재생 속도를 실제 녹화 속도에 맞춥니다.
        실패하거나 video_record.codec이 h264가 아니면 mp4v 파일을 그대로 fpath로 옮깁니다. 돌려주는 값은 실제 코덱
        (h264 또는 mp4v)이고, 관제 영상 전송의 codec에 씁니다."""
        want_h264 = str(SYS_CFG["video_record"]["codec"]).lower() == "h264"
        if want_h264 and transcode_to_h264(raw_path, fpath, fps):
            try:
                os.remove(raw_path)
            except OSError:
                pass
            return "h264"
        try:
            os.replace(raw_path, fpath)
        except OSError as e:
            logger.error(f"[VIDEO] 녹화 파일 정리 실패({raw_path}): {e}")
        return "mp4v"

    def _queue_video_upload(self, fpath, frame_count, size, meta, codec, fps):
        """새 receiver API: 녹화가 끝난 영상을 그 이벤트의 사진과 같은 collectedAt으로 보냅니다.
        과속: 아직 번호판을 읽는 중이면 결과를 기다렸다가 보내고, 차량번호가 없는 이벤트는 보내지 않습니다."""
        if not isinstance(meta, dict) or not meta.get("event_id"):
            return
        event_id = str(meta["event_id"])
        with PLATE_FUTURES_LOCK:
            pending = PLATE_FUTURES.get(event_id)
        if pending is not None:
            pending.add_done_callback(lambda _: self._queue_video_upload(fpath, frame_count, size, meta, codec, fps))
            return
        if event_id in GHOST_EVENTS:
            gdir = ghost_event_dir(self.ip, event_id)
            stem = fpath[:-4] if fpath.endswith(".mp4") else fpath
            try:
                os.makedirs(gdir, exist_ok=True)
                for path in (fpath, stem + ".meta.json", stem + ".infer.jsonl"):
                    if os.path.exists(path):
                        shutil.move(path, os.path.join(gdir, os.path.basename(path)))
            except Exception as e:
                logger.error(f"[GHOST] 영상 옮기기 실패 event_id={event_id}: {e}")
            logger.info(f"[GHOST] 레이더 가짜 물체 이벤트라 영상을 {gdir}로 옮기고 보내지 않습니다. event_id={event_id}")
            return
        if event_id in RECEIVER_SKIPPED_EVENTS:
            logger.info(f"[VIDEO API] 차량번호가 없는 과속 이벤트라 영상도 보내지 않습니다. event_id={event_id}")
            return
        try:
            submit_event_send(send_event_video_to_receiver, fpath, copy.deepcopy(meta),
                              size[0], size[1], frame_count, fps, codec)
        except Exception as e:
            logger.error(f"[VIDEO API] 전송 예약 실패 event_id={meta.get('event_id')} | {e}")

    def _open_clip(self, clip, frame, fps):
        """구간 하나의 녹화 파일을 엽니다. OpenCV는 H.264로 바로 못 써서 mp4v 임시 파일(.mp4v.mp4, 확장자가 .mp4여야
        mp4로 씀)에 쓰고, 구간이 끝나면 H.264로 바꿔 .mp4에 둡니다(_close_clip)."""
        dpath = os.path.join(EVENT_ROOT_DIR, "events", self.ip, "videos", clip["event"])
        os.makedirs(dpath, exist_ok=True)
        time_str = datetime.datetime.fromtimestamp(clip["time"]).strftime('%Y%m%d_%H%M%S')
        base = stem = os.path.join(dpath, f"{time_str}_{self.ip}_{clip['event']}")
        n = 1
        while os.path.exists(stem + ".mp4") or os.path.exists(stem + ".mp4v.mp4"):
            n += 1
            stem = f"{base}_{n}"
        h, w = frame.shape[:2]
        state = {"clip": clip, "fpath": stem + ".mp4", "raw": stem + ".mp4v.mp4", "size": (w, h),
                 "infer_log_path": stem + ".infer.jsonl", "meta_path": stem + ".meta.json",
                 "frames": 0, "first_ts": None, "last_ts": None, "infer_log": None, "ts": [],
                 "writer": cv2.VideoWriter(stem + ".mp4v.mp4", cv2.VideoWriter_fourcc(*'mp4v'), fps, (w, h))}
        try: state["infer_log"] = open(state["infer_log_path"], 'w', encoding='utf-8')
        except Exception: pass
        return state

    def _write_entry(self, entry, item):
        """구간에 화면 한 장을 씁니다. 파일은 첫 화면이 올 때 엽니다."""
        frame, infer_meta, ts = item
        if entry["state"] is None:
            entry["state"] = self._open_clip(entry["clip"], frame, entry["fps"])
        state = entry["state"]
        if state["infer_log"] and infer_meta is not None:
            try:
                log_record = dict(infer_meta)
                log_record["video_frame_index"] = state["frames"]
                state["infer_log"].write(json.dumps(to_json_safe(log_record), ensure_ascii=False) + "\n")
            except Exception: pass
        state["writer"].write(frame)
        state["frames"] += 1
        state["ts"].append(ts)
        if state["first_ts"] is None:
            state["first_ts"] = ts
        state["last_ts"] = ts

    def _close_clip(self, state):
        """파일을 닫고, H.264 변환·meta 저장·전송 예약은 VIDEO_FINISH_POOL에 맡깁니다(변환하는 동안 녹화 스레드가 밀리지 않게)."""
        state["writer"].release()
        if state["infer_log"]:
            state["infer_log"].close()
        if state["clip"].get("on_raw_done"):
            # 묶음 4분할 영상용 녹화(ConeGroupVideo): 변환·전송 없이 mp4v 임시 파일 상태만 넘깁니다.
            state["clip"]["on_raw_done"](state)
            return
        VIDEO_FINISH_POOL.submit(self._finish_closed_clip, state)

    def _finish_closed_clip(self, state):
        try:
            clip, frames = state["clip"], state["frames"]
            span = state["last_ts"] - state["first_ts"]
            # 재생 속도 = 실제로 녹화한 장수 ÷ 걸린 시간. 1분 구간이면 영상도 약 1분입니다.
            fps = (frames - 1) / span if frames > 1 and span > 0 else clip["fps"]
            codec = self._finish_clip(state["raw"], state["fpath"], fps)
            meta = clip["meta"]
            if isinstance(meta, dict):
                meta.update({"video_path": state["fpath"], "infer_log_path": state["infer_log_path"],
                             "recorded_fps": round(fps, 3), "video_frames": frames, "video_codec": codec})
                try:
                    with open(state["meta_path"], 'w', encoding='utf-8') as f_meta:
                        json.dump(to_json_safe(meta), f_meta, indent=4, ensure_ascii=False)
                except Exception as e:
                    logger.error(f"메타데이터 저장 실패: {e}")
            logger.info(f"[VIDEO] {os.path.basename(state['fpath'])} {frames}장 {frames / fps:.1f}초({fps:.1f}fps, {codec}) "
                        f"이벤트 앞 {clip['time'] - state['first_ts']:.1f}초·뒤 {state['last_ts'] - clip['time']:.1f}초")
            self._queue_video_upload(state["fpath"], frames, state["size"], meta, codec, fps)
        except Exception as e:
            logger.error(f"[VIDEO] {self.ip} 영상 마무리 실패: {e}")

    def _handle_item(self, entries, item):
        """녹화 스레드가 받은 것 하나(구간 시작·끝 또는 화면)를 처리합니다."""
        kind = item[0] if isinstance(item[0], str) else None
        if kind == "__clip_start__":
            _, clip, pre_frames = item
            times = [frame_item[2] for frame_item in pre_frames]
            span = times[-1] - times[0] if len(times) > 1 else 0.0
            # 파일은 앞 화면의 실제 속도로 엽니다(H.264 변환이 실패해 mp4v를 그대로 보내도 재생 시간이 크게 틀리지 않게).
            entry = {"clip": clip, "state": None, "fps": (len(times) - 1) / span if span > 0 else clip["fps"]}
            entries.append(entry)
            for frame_item in pre_frames:
                self._write_entry(entry, frame_item)
        elif kind == "__clip_end__":
            entry = next((e for e in entries if e["clip"] is item[1]), None)
            if entry is None:
                return
            entries.remove(entry)
            if entry["state"] is not None:
                self._close_clip(entry["state"])
            elif entry["clip"].get("on_raw_done"):
                entry["clip"]["on_raw_done"](None)
            else:
                logger.warning(f"[VIDEO] {self.ip} - {item[1]['event']}: 녹화된 화면이 없어 영상을 만들지 않았습니다.")
        else:
            # 이 화면 시각이 구간(이벤트 앞 pre_sec ~ 뒤 post_sec) 안인 구간마다 씁니다.
            for entry in entries:
                if entry["clip"]["start"] <= item[2] <= entry["clip"]["end"]:
                    self._write_entry(entry, item)

    def _writer_loop(self):
        entries = []   # 녹화 중인 구간: {"clip": 구간, "state": 파일 상태(첫 화면에서 엶), "fps": 파일을 열 때 쓸 값}
        while self.running:
            try: item = self.write_queue.get(timeout=1.0)
            except queue.Empty: continue
            try:
                self._handle_item(entries, item)
            except Exception as e:
                logger.error(f"[VIDEO] {self.ip} 녹화 처리 실패: {e}")
            # 처리한 것을 다음 것이 올 때까지 붙잡지 않습니다(구간 시작에 딸린 앞 화면 목록은 녹화 1건에 수백 MB).
            item = None


# ==========================================
# [8] 이벤트 감지
# ==========================================
def fps_cap(value, default):
    """설정의 초당 장수 상한. 0 이하면 제한 없음(None), 읽을 수 없으면 default."""
    try:
        value = float(default if value is None else value)
    except (TypeError, ValueError):
        value = float(default)
    return value if value > 0 else None


class BaseEventDetector:
    gui_name = "BASE"
    # ---- 영상 수신 이름표: 카메라가 자기 이벤트들의 이름표를 모아 수신기를 하나 만듭니다(Camera._reader_spec). ----
    # 새 이벤트는 아무것도 적지 않으면 공통 수신기(FrameReader)로 초당 video_decode.fps_limit(기본 10)장, 가로 720으로 받습니다.
    # 다르게 받아야 하는 이벤트만 아래 값(또는 stream_spec)을 바꿉니다.
    stream_reader = "FrameReader"    # 수신기 클래스 이름. 자기 수신 방식이 있는 이벤트만 바꿉니다(과속: "SpeedFrameReader").
    stream_max_width = 720           # 원본 그림 가로 상한(더 큰 그림이 필요한 이벤트만 키움)
    stream_all_frames = False        # True면 프레임을 줄이지 않고 모두 받습니다(과속: 레이더 시각 맞춤).
    # 같은 객체(추적 번호)에 같은 이벤트는 한 번만 냅니다. 위험이 이어지는 동안 쿨다운마다 다시 내야 하는 이벤트만 False.
    alarm_once_per_object = True

    @classmethod
    def stream_spec(cls):
        """이 이벤트가 바라는 영상 수신 (수신기 이름, 초당 장수 상한(None = 전부), 원본 가로 상한)."""
        max_fps = None if cls.stream_all_frames else fps_cap(SYS_CFG["video_decode"]["fps_limit"], 10.0)
        return cls.stream_reader, max_fps, int(cls.stream_max_width)

    def __init__(self, config, roi_poly=None, roi_lines=None):
        self.config = config
        self.roi_poly = np.array(roi_poly, dtype=np.int32) if roi_poly and len(roi_poly) >= 3 else np.empty((0, 2), dtype=np.int32)
        self.roi_lines = roi_lines or []

    def process(self, tracks, track_map, frame, fid, **kwargs):
        return []


# ==========================================
# 실제 레이더 입력: UMRR-11 Type 132 MSE (SDK 워커 프로세스 수신)
# ==========================================
# Object SpeedAbsolute는 이미 m/s 단위 절대 속도이며 raw Doppler가 아닙니다.
MSE_LOGGER = logging.getLogger("CCTV_SYSTEM.radar_mse")


def radar_unit_cfg(client_id):
    """system_config radars.<client_id>: 이 레이더에만 덮어쓸 설정(없으면 {})."""
    if client_id is None:
        return {}
    return SYS_CFG["radars"].get(str(client_id)) or {}


def radar_camera_cfg(client_id=None):
    """레이더↔카메라 보정값: radar_camera 위에 radars.<client_id>.radar_camera를 덮어씁니다."""
    base = SYS_CFG["radar_camera"]
    over = radar_unit_cfg(client_id).get("radar_camera")
    return deep_merge_dict(base, over) if over else base


def radar_speeding_cfg(client_id=None):
    """과속 판정 설정: event_config.speeding 위에 radars.<client_id>.speeding을 덮어씁니다."""
    base = SYS_CFG["event_config"]["speeding"]
    over = radar_unit_cfg(client_id).get("speeding")
    return deep_merge_dict(base, over) if over else base


class MseRadarHub:
    """MSE SDK 워커 프로세스 1개로 routing_table.json의 레이더 전부를 받아 client_id별 MseRadarInput에 나눠 줍니다.
    SDK가 센서 UDP 포트를 독점하므로 레이더가 여러 대여도 워커는 하나입니다."""
    _shared = None
    _shared_lock = threading.Lock()

    @classmethod
    def shared(cls):
        with cls._shared_lock:
            if cls._shared is None or cls._shared._closed:
                cls._shared = cls()
            return cls._shared

    @classmethod
    def close_shared(cls):
        with cls._shared_lock:
            return True if cls._shared is None else cls._shared.close()

    def __init__(self, config_path=None):
        self.config_path = Path(config_path) if config_path else (
            Path(PROJECT_ROOT) / 'hardware_check/config/smart_access_config.json')
        self.client_ids = mse_client_ids(self.config_path)
        self._lock = threading.RLock()
        self._process = None
        self._thread = None
        self._closed = False
        self._error = None
        self._retry_at = 0.0
        self._last_log = -math.inf
        self._inputs = {}

    def input(self, client_id=None):
        """client_id 레이더의 입력(없으면 routing_table의 첫 레이더). routing_table에 없는 id면 경고하고 데이터 없이 둡니다."""
        if client_id in (None, ''):
            if len(self.client_ids) > 1:
                MSE_LOGGER.warning('[Radar] radar_client_id가 없는 과속 카메라는 첫 레이더 %s를 씁니다(레이더 %s대). '
                                   'cameras.json에 radar_client_id를 적어 주세요.', self.client_ids[0], len(self.client_ids))
            client_id = self.client_ids[0]
        client_id = int(client_id)
        with self._lock:
            radar = self._inputs.get(client_id)
            if radar is None:
                if client_id not in self.client_ids:
                    MSE_LOGGER.warning('[Radar] client %s is not in routing_table.json %s', client_id, self.client_ids)
                radar = self._inputs[client_id] = MseRadarInput(self, client_id)
            return radar

    def start(self):
        with self._lock:
            if self._closed:
                return False
            if self._process is not None and self._process.poll() is None:
                return True
            if self._thread is not None and self._thread.is_alive():
                return False
            if time.monotonic() < self._retry_at:
                return False
            process = None
            try:
                cfg = json.loads(self.config_path.read_text(encoding='utf-8'))
                library = str(Path(cfg['shared_lib_path']).resolve())
                env = os.environ.copy()
                for name in ('PYTHONPATH', 'LD_LIBRARY_PATH'):
                    env[name] = library + (os.pathsep + env[name] if env.get(name) else '')
                env['SMART_ACCESS_CFG_FILE_PATH'] = str(self.config_path.resolve())
                for radar in self._inputs.values():
                    radar.reset()
                self._error = None
                process = subprocess.Popen(
                    ['/usr/bin/python3', '-u', os.path.abspath(__file__), RADAR_MSE_WORKER_FLAG,
                     *[str(c) for c in self.client_ids]],
                    env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace',
                    start_new_session=True)
                self._process = process
                self._thread = threading.Thread(target=self._read, args=(process,),
                                                name='radar-mse-reader', daemon=True)
                self._thread.start()
                MSE_LOGGER.info('[Radar] real MSE input started; sensors=%s', self.client_ids)
                return True
            except Exception as error:
                if process is not None:
                    self._stop_process(process)
                    process.stdout.close()
                self._fail(str(error))
                self._retry_at = time.monotonic() + 5.0
                return False

    def _fail(self, message):
        with self._lock:
            for radar in self._inputs.values():
                radar.reset()
            self._error = message
            now = time.monotonic()
            if now - self._last_log >= 30.0:
                MSE_LOGGER.warning('[Radar] real MSE input: %s', message)
                self._last_log = now

    def error(self):
        with self._lock:
            return 'closed' if self._closed else self._error

    def _accept(self, record):
        if record.get('kind') == 'error':
            raise ValueError(str(record.get('message', 'SDK error')))
        if record.get('kind') == 'ready':
            return
        client_id = record.get('client_id')
        if record.get('kind') != 'objects' or client_id not in self.client_ids:
            raise ValueError('unexpected MSE message or sensor')
        with self._lock:
            radar = self._inputs.get(client_id)
            self._error = None
        if radar is not None:  # 이 레이더를 쓰는 카메라가 없으면 버립니다.
            radar.accept(record)

    def _read(self, process):
        try:
            while True:
                line = process.stdout.readline(MSE_MAX_LINE + 1)
                if not line:
                    break
                if len(line) > MSE_MAX_LINE:
                    raise ValueError('MSE worker message exceeds size limit')
                if line.startswith(MSE_PREFIX):
                    self._accept(json.loads(line[len(MSE_PREFIX):]))
            if not self._closed:
                self._fail('MSE worker stopped')
        except Exception as error:
            if not self._closed:
                self._fail(str(error))
        finally:
            self._stop_process(process)
            process.stdout.close()
            with self._lock:
                for radar in self._inputs.values():
                    radar.reset()
                self._retry_at = time.monotonic() + 5.0

    @staticmethod
    def _stop_process(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def close(self):
        with self._lock:
            self._closed = True
            process, thread = self._process, self._thread
            for radar in self._inputs.values():
                radar.reset()
        if process is not None:
            self._stop_process(process)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=7)
        return ((process is None or process.poll() is not None)
                and (thread is None or not thread.is_alive()))


class MseRadarInput:
    """레이더 1대(client_id)의 최근 프레임. 수신·워커 관리는 MseRadarHub가 합니다."""

    def __init__(self, hub, client_id):
        self.hub = hub
        self.client_id = int(client_id)
        self._lock = threading.RLock()
        self._latest = None
        self._last_timestamp = None
        self._frame_listeners = []
        self._history = deque(maxlen=64)  # 최근 레이더 프레임(약 8초). 화면 두 곳이 같은 프레임을 그리도록 시각으로 찾습니다.

    def add_frame_listener(self, listener):
        """레이더 프레임마다 listener({'timestamp_us','arrival','objects','client_id'})를 수신 스레드에서 호출합니다."""
        with self._lock:
            if listener not in self._frame_listeners:
                self._frame_listeners.append(listener)

    def start(self):
        return self.hub.start()

    def reset(self):
        """워커 재시작·오류 때 지난 프레임을 버립니다(센서 시각 기준도 새로 잡음)."""
        with self._lock:
            self._latest = None
            self._last_timestamp = None

    def accept(self, record):
        stamp = record.get('timestamp_us')
        rows = record.get('objects')
        if type(stamp) is not int or stamp <= 0 or not isinstance(rows, list) or len(rows) > 1024:
            raise ValueError('invalid MSE frame')
        objects, ids = [], set()
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('invalid MSE object')
            oid, status = row.get('id'), row.get('status')
            if (type(oid) is not int or not 0 <= oid <= 65535 or oid in ids
                    or type(status) is not int or not 0 <= status <= 255):
                raise ValueError('invalid or duplicate MSE object identity')
            ids.add(oid)
            for key in ('x_m', 'y_m', 'z_m', 'speed_mps', 'heading_rad', 'quality'):
                value = row.get(key)
                if type(value) not in (int, float) or not math.isfinite(value):
                    raise ValueError('non-finite MSE object field')
            if not 0 <= row['speed_mps'] <= 300.0/3.6:
                raise ValueError('MSE absolute speed outside 0..300 km/h')
            if status != 0:  # Vendor definition: 0 = object, 2 = guardrail.
                continue
            obj = dict(row)
            obj['speed_kmh'] = row['speed_mps'] * 3.6
            objects.append(obj)
        with self._lock:
            # A repeated timestamp does not refresh an old frame. A backwards timestamp
            # invalidates the old observation and becomes the baseline for a sensor reboot.
            if self._last_timestamp is not None and stamp <= self._last_timestamp:
                if stamp < self._last_timestamp:
                    self._latest = None
                    self._history.clear()
                    self._last_timestamp = stamp
                return
            self._last_timestamp = stamp
            self._latest = {'timestamp_us': stamp, 'arrival': time.monotonic(), 'objects': objects,
                            'client_id': self.client_id}
            self._history.append(self._latest)
            latest, listeners = self._latest, list(self._frame_listeners)
        for listener in listeners:
            try:
                listener(latest)
            except Exception as error:
                MSE_LOGGER.error('[Radar] frame listener failed: %s', error)

    def frame_at(self, t_arrival, max_gap=0.3, max_age=1.0):
        """도착 시각 t_arrival에 가장 가까운 레이더 프레임 {'timestamp_us','arrival','objects'} (없으면 None).
        t_arrival이 최신 프레임보다 미래면(지연값이 음수라 맞는 레이더 값이 아직 안 온 경우) 최신 프레임을 돌려줍니다.
        t_arrival=None이면 최신 프레임."""
        if self.hub.error():
            return None
        with self._lock:
            latest = self._latest
            if latest is None or time.monotonic() - latest['arrival'] > max_age:
                return None
            if t_arrival is None or t_arrival >= latest['arrival']:
                return latest
            best = min(self._history, key=lambda f: abs(f['arrival'] - t_arrival))
            return best if abs(best['arrival'] - t_arrival) <= max_gap else None

    def get_status_lines(self):
        """GUI 레이더 상태 줄: 상태(live·stale·waiting·error·closed), 최근 프레임 물체 수와 앞 3개의 속도."""
        error = self.hub.error()
        with self._lock:
            latest = self._latest
            age = time.monotonic() - latest['arrival'] if latest else None
            if error == 'closed':
                state = 'closed'
            elif error:
                state = 'error'
            elif latest and 0 <= age <= 1.0:
                state = 'live'
            elif latest:
                state = 'stale'
            else:
                state = 'waiting'
            rows = [dict(row) for row in latest['objects']] if state == 'live' else []
        name = f" {self.client_id}" if len(self.hub.client_ids) > 1 else ""
        lines = [f"RADAR{name} {state.upper()} | objects: {len(rows)}"]
        if rows:
            lines.append('Sensor: ' + ' / '.join(
                f"R{row['id']} {row['speed_kmh']:.1f} km/h" for row in rows[:3]))
        return lines


RADAR_EVENT_TID_BASE = 1_000_000  # 레이더 트랙 이벤트 tid 공간(RGB 트랙 ID와 겹치지 않게)


def speeding_lane_settings(cfg):
    """과속 차로(카메라)별 판정 설정: 방향(forward 다가오는 차, backward 멀어지는 차)과 방향별 측정·사진 구간(레이더 거리 m)."""
    def span(key, default):
        lo, hi = (float(v) for v in (cfg.get(key) or default))
        return min(lo, hi), max(lo, hi)
    return {"forward": bool(cfg.get("forward", True)), "backward": bool(cfg.get("backward", False)),
            "forward_measure": span("forward_measure_m", (30.0, 80.0)),
            "backward_measure": span("backward_measure_m", (0.0, 40.0)),
            "forward_photo": span("forward_photo_m", (0.0, 5.0)),
            "backward_photo": span("backward_photo_m", (0.0, 10.0))}


class RadarSpeedJudge:
    """레이더 트랙 기준 과속 판정기. 레이더(client_id)마다 하나씩 둡니다.

    - ROI(단속 차로): ROI를 레이더 좌표로 바꾼 다각형 안에 lane_min_samples 이상 찍힌 트랙만 그 카메라의 판정 대상입니다.
      판정 순간(레이더가 놓쳤으면 마지막으로 본 위치)에도 차로 안이어야 하고, 속도는 차로 안에서 잰 샘플만 씁니다
      (차로를 잠깐 스치고 옆으로 빠지는 레이더 허상을 거름).
    - 방향: 카메라마다 forward(센서로 다가오는 차, 앞번호판)·backward(멀어지는 차, 뒷번호판)를 켭니다(둘 다면 양쪽).
    - 속도: 방향별 측정 구간(레이더 거리, forward_measure_m·backward_measure_m) 안 샘플의 중앙값.
    - 판정 시점: 다가오는 차는 측정 구간 가까운 끝을, 멀어지는 차는 먼 끝을 지날 때입니다. 그때 아직 ROI를 안 지났으면
      ROI에 들어올 때, 그 전에 레이더가 놓치면 놓쳤을 때 판정합니다. 측정 샘플이 모자라면 판정하지 않습니다.
    - 트랙당 이벤트 1회. 카메라 박스 연결은 증거 표시에만 씁니다.
    """
    _by_client = {}
    _shared_lock = threading.Lock()

    @classmethod
    def for_radar(cls, radar):
        with cls._shared_lock:
            judge = cls._by_client.get(radar.client_id)
            if judge is None or judge.radar is not radar:
                judge = cls._by_client[radar.client_id] = cls(radar)
                radar.add_frame_listener(judge.on_frame)
            return judge

    def __init__(self, radar=None, cfg=None):
        self.radar = radar
        self._cfg_override = cfg
        self._lock = threading.Lock()
        self._tracks = {}            # 레이더 id -> 트랙 상태
        self._fired_lost = deque(maxlen=64)
        self._seq = 0
        self._consumers = {}         # camera_key -> 이벤트 deque
        self._frame_hooks = []
        self._camera_zones = {}      # camera_key -> 차로 {polygon, 방향·측정·사진 구간}(set_camera_zone)
        # 레이더가 여러 대면 로그에 어느 레이더인지 붙입니다.
        hub = getattr(radar, 'hub', None)
        self.client_label = f"{radar.client_id}:" if hub is not None and len(hub.client_ids) > 1 else ""

    def _cfg(self):
        return self._cfg_override if self._cfg_override is not None else radar_speeding_cfg(getattr(self.radar, 'client_id', None))

    def params(self):
        cfg = self._cfg()
        zone = cfg.get("zone", {}) or {}
        limit = float(cfg.get("speed_limit_kmh", 30.0))
        margin = max(float(cfg.get("margin_kmh", 5.0)), limit * float(cfg.get("margin_ratio", 0.0)))
        return {
            "limit": limit, "margin": margin, "threshold": limit + margin,
            "min_span": float(cfg.get("min_span_sec", 1.0)),
            "min_samples": int(cfg.get("min_samples", 5)), "min_quality": float(cfg.get("min_quality", 0.0)),
            "timeout": float(cfg.get("track_timeout_sec", 1.0)),
            "guard_sec": float(cfg.get("same_vehicle_guard_sec", 3.0)),
            "guard_m": float(cfg.get("same_vehicle_guard_m", 8.0)),
            "trail_m": float(cfg.get("same_vehicle_trail_m", 20.0)),
            "trail_lat_m": float(cfg.get("same_vehicle_trail_lat_m", 3.0)),
            "trail_kmh": float(cfg.get("same_vehicle_trail_kmh", 5.0)),
            "min_range": float(zone.get("min_range_m", 0.0)), "max_range": float(zone.get("max_range_m", 200.0)),
            "max_lat": float(zone.get("max_abs_lateral_m", 50.0)),
            # 레이더 좌표 다각형 [[x, y], ...](선택). ROI 다각형이 없는 차로(보정값 없음)는 이것으로 고릅니다.
            "polygon": zone.get("polygon") or None,
            "lane_min_samples": int(cfg.get("lane_min_samples", 3)),
            "keep_sec": float(cfg.get("track_keep_sec", 20.0)),
            # 판정 차로: 카메라마다 {polygon, forward, backward, *_measure, *_photo}. 카메라 차로가 없으면 설정만으로 하나(None).
            "lanes": self._camera_zones or {None: dict(speeding_lane_settings(cfg), polygon=None)},
        }

    def register(self, camera_key):
        with self._lock:
            self._consumers.setdefault(camera_key, deque(maxlen=64))

    def pop_events(self, camera_key):
        with self._lock:
            q = self._consumers.setdefault(camera_key, deque(maxlen=64))
            events = list(q)
            q.clear()
            return events

    def add_frame_hook(self, hook):
        with self._lock:
            if hook not in self._frame_hooks:
                self._frame_hooks.append(hook)

    def set_camera_zone(self, camera_key, polygon, settings=None):
        """카메라 차로: ROI(단속 차로)를 레이더 좌표로 바꾼 다각형과 그 카메라의 방향·측정·사진 구간(speeding_lane_settings,
        없으면 이 레이더 설정). polygon이 None이면(보정값 없음·망가짐) 설정 구역(zone)으로 차로를 고릅니다.
        그 차로에서 난 이벤트는 그 카메라로만 갑니다."""
        if settings is None:
            settings = speeding_lane_settings(self._cfg())
        zones = dict(self._camera_zones)        # 통째로 바꿔 끼웁니다(params()가 잠금 없이 읽음)
        zones[camera_key] = dict(settings, polygon=polygon or None)
        self._camera_zones = zones

    @staticmethod
    def _in_bounds(obj, p):
        """설정 구역(zone)의 거리·좌우 범위 안인지(판정·보정 수집 공통)."""
        x, y = float(obj['x_m']), float(obj['y_m'])
        return p["min_range"] <= math.hypot(x, y) <= p["max_range"] and abs(y) <= p["max_lat"]

    @staticmethod
    def _in_lane(lane, x, y, p):
        """차로 다각형(없으면 설정 polygon, 그것도 없으면 범위 전체) 안인지."""
        poly = lane.get("polygon") or p.get("polygon")
        return point_in_polygon(poly, x, y) if poly else True

    def on_frame(self, frame):
        p = self.params()
        arrival = float(frame['arrival'])
        events, same_vehicle, near = [], [], []
        with self._lock:
            for obj in frame.get('objects', []):
                if float(obj.get('quality', 0.0)) < p["min_quality"] or not self._in_bounds(obj, p):
                    continue
                x, y = float(obj['x_m']), float(obj['y_m'])
                st = self._track_for(obj, arrival, p)
                st['last_arrival'] = arrival
                st['samples'].append((arrival, frame.get('timestamp_us'), float(obj['speed_kmh']), x, y))
                while st['samples'] and st['samples'][0][0] < arrival - p["keep_sec"]:
                    st['samples'].popleft()
                for key, lane in p["lanes"].items():
                    if self._in_lane(lane, x, y, p):
                        st['lane_hits'][key] = st['lane_hits'].get(key, 0) + 1
                self._judge(st, p, events, same_vehicle, lost=False, near=near)
            for oid in [k for k, st in self._tracks.items() if arrival - st['last_arrival'] > p["timeout"]]:
                st = self._tracks.pop(oid)
                self._judge(st, p, events, same_vehicle, lost=True, near=near)
                if st['fired'] and st['samples']:
                    last = st['samples'][-1]
                    self._fired_lost.append((st['last_arrival'], last[3], last[4], st.get('continues') or st['seq']))
            for ev in events:
                # 그 차로(카메라)로만 보냅니다. 카메라 차로가 없으면(설정 구역) 모든 카메라로.
                keys = [ev['camera_key']] if ev.get('camera_key') is not None else list(self._consumers)
                for key in keys:
                    self._consumers.setdefault(key, deque(maxlen=64)).append(ev)
            hooks = list(self._frame_hooks)
        for ev in events:
            logger.warning(
                f"[RADAR SPEEDING] {self.client_label}R{ev['radar_id']} seq={ev['seq']} {ev['direction']} "
                f"median={ev['median_kmh']:.1f}km/h (min {ev['min_kmh']:.1f} / max {ev['max_kmh']:.1f}, n={ev['samples']}, "
                f"{ev['window_range_m'][0]:.0f}->{ev['window_range_m'][1]:.0f}m) > "
                f"{ev['threshold_kmh']:.1f} (limit {ev['limit_kmh']:.1f} + margin {ev['margin_kmh']:.1f}) "
                f"at x={ev['x_m']:.1f} y={ev['y_m']:.1f} {ev['decided_by']} first={ev['track_first_range_m']:.0f}m"
            )
        for ev in near:
            # 분석용: 측정은 끝났고 기준(제한+여유) 아래지만 제한속도는 넘은 차(기준값이 맞는지 볼 때)
            logger.info(
                f"[RADAR SPEEDING] {self.client_label}R{ev['radar_id']} seq={ev['seq']} {ev['direction']} 과속 아님"
                f"(제한 {ev['limit_kmh']:.0f} 넘음, 기준 {ev['threshold_kmh']:.0f} 이하) median={ev['median_kmh']:.1f}km/h "
                f"(min {ev['min_kmh']:.1f} / max {ev['max_kmh']:.1f}, n={ev['samples']}, "
                f"{ev['window_range_m'][0]:.0f}->{ev['window_range_m'][1]:.0f}m) cam={ev['camera_key']} "
                f"at x={ev['x_m']:.1f} y={ev['y_m']:.1f} {ev['decided_by']} first={ev['track_first_range_m']:.0f}m"
            )
        for ev, ahead in same_vehicle:
            logger.info(
                f"[RADAR SPEEDING] {self.client_label}R{ev['radar_id']} seq={ev['seq']} {ev['median_kmh']:.1f}km/h는 방금 과속을 낸 "
                f"R{ahead['radar_id']} seq={ahead['seq']}와 같은 차(뒤 {ahead['behind_m']:.1f}m, 옆 {ahead['lateral_m']:.1f}m, "
                f"속도 차 {ahead['dkmh']:.1f}km/h)로 보고 이벤트를 내지 않습니다.")
        if hooks:
            # 보정 기록용: ROI 차로는 지금 보정값으로 만든 것이라 빼고, 설정 구역·품질 기준만 씁니다.
            calib_objs = [obj for obj in frame.get('objects', [])
                          if float(obj.get('quality', 0.0)) >= p["min_quality"] and self._in_bounds(obj, p)
                          and self._in_lane({}, float(obj['x_m']), float(obj['y_m']), p)]
        for hook in hooks:
            try:
                hook(frame, calib_objs)
            except Exception as e:
                logger.error(f"[RADAR] frame hook failed: {e}")

    def _track_for(self, obj, arrival, p):
        """ID 재사용(timeout 초과 후 재등장)은 새 트랙. 이벤트를 낸 차량 근처에서 새로 잡힌 트랙은 이벤트 표시를 물려받고
        (같은 차 재검출), 따라가기(position_at)가 이 트랙으로 이어집니다(continues = 처음 이벤트를 낸 트랙 seq)."""
        oid = int(obj['id'])
        st = self._tracks.get(oid)
        if st is not None and arrival - st['last_arrival'] <= p["timeout"]:
            return st
        self._seq += 1
        st = {'seq': self._seq, 'radar_id': oid, 'first_arrival': arrival, 'last_arrival': arrival,
              'samples': deque(maxlen=1024), 'fired': False, 'lane_hits': {}, 'done': set()}
        x, y = float(obj['x_m']), float(obj['y_m'])
        candidates = list(self._fired_lost) + [
            (other['last_arrival'], other['samples'][-1][3], other['samples'][-1][4], other.get('continues') or other['seq'])
            for other in self._tracks.values() if other['fired'] and other['samples'] and other['last_arrival'] < arrival]
        for lost_at, lx, ly, root in candidates:
            if arrival - lost_at <= p["guard_sec"] and math.hypot(x - lx, y - ly) <= p["guard_m"]:
                st['fired'], st['continues'] = True, root
                logger.info(f"[RADAR SPEEDING] {self.client_label}R{oid} seq={self._seq}는 직전 이벤트 차량의 재검출로 보고 이벤트 대상에서 뺍니다.")
                break
        self._tracks[oid] = st
        return st

    @staticmethod
    def _direction(st):
        """트랙 처음과 지금 레이더 거리로 방향: 2m 넘게 가까워졌으면 forward(다가옴), 멀어졌으면 backward, 아니면 None."""
        first, last = st['samples'][0], st['samples'][-1]
        d = math.hypot(last[3], last[4]) - math.hypot(first[3], first[4])
        return "forward" if d < -2.0 else ("backward" if d > 2.0 else None)

    def _judge(self, st, p, events, same_vehicle, lost, near=None):
        """트랙 하나를 차로(카메라)마다 판정합니다(판정 시점은 클래스 설명). lost: 레이더가 이 트랙을 놓침.
        판정한 차로는 측정이 모자라거나 과속이 아니어도 다시 보지 않습니다(done).
        near: 과속은 아니지만 제한속도를 넘은 차를 모을 목록(분석 로그)."""
        if st['fired'] or len(st['samples']) < 2:
            return
        direction = self._direction(st)
        if direction is None:
            return
        last = st['samples'][-1]
        rng = math.hypot(last[3], last[4])
        for key, lane in p["lanes"].items():
            hits = st['lane_hits'].get(key, 0)
            if key in st['done'] or not lane.get(direction) or hits < p["lane_min_samples"]:
                continue
            lo, hi = lane[direction + "_measure"]
            if not (lost or (rng < lo if direction == "forward" else rng > hi)):
                continue
            # 판정 위치가 차로 밖이면 이 차로의 차가 아닙니다(차로를 스친 레이더 허상). 다시 차로에 들어오면 그때 판정합니다.
            if not self._in_lane(lane, last[3], last[4], p):
                if key not in st.setdefault('out_lane', set()):
                    st['out_lane'].add(key)
                    logger.info(f"[RADAR SPEEDING] {self.client_label}R{st['radar_id']} seq={st['seq']} 판정 위치가 차로 밖"
                                f"(x={last[3]:.1f} y={last[4]:.1f}, {last[2]:.1f}km/h, {'놓침' if lost else '측정 구간 끝'})"
                                f"이라 판정하지 않습니다.")
                continue
            st['done'].add(key)
            ev = self._evaluate(st, p, direction, lo, hi, lane)
            if ev is None:
                continue
            # 판정한 까닭: 레이더가 놓침 / 측정 구간 끝을 지나서 / (이미 지났는데) 이제 ROI 차로에 들어와서
            ev.update(camera_key=key, photo_m=list(lane[direction + "_photo"]), lane_hits=hits,
                      decided_by="radar_lost" if lost else "lane_entered" if hits == p["lane_min_samples"]
                      else "passed_near_end" if direction == "forward" else "passed_far_end")
            if ev['median_kmh'] <= p["threshold"]:
                if near is not None and ev['median_kmh'] > p["limit"]:
                    near.append(ev)
                continue
            self._fire(st, ev, last[0], p, events, same_vehicle)
            return

    def _fire(self, st, ev, now, p, events, same_vehicle):
        """과속 판정: 같은 차(레이더가 둘로 잡은 차)가 아니면 이벤트로 냅니다. 어느 쪽이든 이 트랙은 더 판정하지 않습니다."""
        st['fired'], st['fired_at'], st['fired_kmh'] = True, now, float(ev['median_kmh'])
        ahead = self._same_vehicle_as_fired(st, ev, now, p)
        if ahead is None:
            events.append(ev)
        else:
            same_vehicle.append((ev, ahead))

    def _same_vehicle_as_fired(self, st, ev, now, p):
        """레이더가 차 한 대를 둘로 잡는 경우(운전석 좌우, 긴 트레일러 앞뒤): 과속을 내려는 트랙이 guard_sec 안에 과속을 낸
        다른 트랙의 진행 방향 뒤 trail_m(앞으로는 3m까지) 안, 옆 trail_lat_m 안이고 속도 차가 trail_kmh 안이면 같은 차입니다.
        반환: 그 차 정보(dict) 또는 None."""
        x, y = float(ev['x_m']), float(ev['y_m'])
        t_ev = float(ev.get('decision_arrival', now))
        for other in self._tracks.values():
            if other is st or other.get('fired_at') is None or now - other['fired_at'] > p["guard_sec"] or not other['samples']:
                continue
            # 같은 순간 위치끼리 비교: 이 트랙 판정 위치(레이더가 놓친 뒤 판정하면 1초 전 위치)와 그때의 그 차 위치
            last = min(other['samples'], key=lambda q: abs(q[0] - t_ev))
            ref = next((q for q in other['samples'] if q[0] >= last[0] - 1.0), other['samples'][0])
            dx, dy = last[3] - ref[3], last[4] - ref[4]
            norm = math.hypot(dx, dy)
            if norm < 0.3:                      # 거의 안 움직였으면 진행 방향을 모름
                continue
            ux, uy = dx / norm, dy / norm
            rx, ry = x - last[3], y - last[4]
            behind, lateral = -(rx * ux + ry * uy), abs(rx * uy - ry * ux)
            dkmh = abs(float(ev['median_kmh']) - float(other['fired_kmh']))
            if -3.0 <= behind <= p["trail_m"] and lateral <= p["trail_lat_m"] and dkmh <= p["trail_kmh"]:
                return {'radar_id': other['radar_id'], 'seq': other['seq'], 'behind_m': behind, 'lateral_m': lateral,
                        'dkmh': dkmh}
        return None

    def _evaluate(self, st, p, direction, lo, hi, lane):
        """측정 구간(레이더 거리 lo~hi m) 안이면서 차로(lane) 안인 샘플로 잰 결과(이벤트 dict 모양, 속도 중앙값 median_kmh).
        과속인지는 부르는 쪽이 정합니다. 샘플이 모자라면(min_samples개·min_span초 미만) None."""
        win = [q for q in st['samples'] if lo <= math.hypot(q[3], q[4]) <= hi and self._in_lane(lane, q[3], q[4], p)]
        if len(win) < p["min_samples"] or win[-1][0] - win[0][0] < p["min_span"]:
            return None
        speeds = [q[2] for q in win]
        median = float(np.median(speeds))
        last, first = st['samples'][-1], st['samples'][0]
        return {
            'seq': st['seq'], 'radar_id': st['radar_id'], 'tid': RADAR_EVENT_TID_BASE + st['seq'],
            'median_kmh': median, 'min_kmh': float(min(speeds)), 'max_kmh': float(max(speeds)),
            'samples': len(win), 'window_arrival': [win[0][0], win[-1][0]], 'decision_arrival': last[0],
            'measure_m': [lo, hi],
            'window_range_m': [round(math.hypot(win[0][3], win[0][4]), 1), round(math.hypot(win[-1][3], win[-1][4]), 1)],
            'sensor_ts': last[1], 'x_m': last[3], 'y_m': last[4],
            'limit_kmh': p["limit"], 'margin_kmh': p["margin"], 'threshold_kmh': p["threshold"],
            'direction': direction,
            # 이 차가 지나온 레이더 경로 (도착 시각, x, y, 속도): 번호판 사진 구간 고르기(멀어지는 차는 판정 전에 지나감)
            'path': [(q[0], q[3], q[4], q[2]) for q in st['samples']],
            # 분석용: 측정 구간 레이더 값 하나하나 [판정 시각 기준 초, 거리 m, x, y, 속도 km/h]와 이 트랙을 처음 본 거리·시각
            'window_samples': [[round(q[0] - last[0], 3), round(math.hypot(q[3], q[4]), 2), round(q[3], 2), round(q[4], 2),
                                round(q[2], 2)] for q in win],
            'track_first_range_m': round(math.hypot(first[3], first[4]), 1),
            'track_first_sec': round(first[0] - last[0], 2),
        }

    def position_at(self, seq, t_arrival, max_dt=None, max_ahead=1.0):
        """seq 트랙에서 t_arrival(레이더 도착 시각 기준)에 가장 가까운 샘플 (x, y, speed_kmh).
        max_dt를 주면 그보다 시간이 먼 샘플뿐일 때(구역을 벗어났거나 트랙이 끊김) None.
        t_arrival이 최신 샘플보다 미래면(지연값이 음수라 이 영상 시각의 레이더 값이 아직 안 옴) max_ahead초까지는
        최근 0.5초 이동으로 앞으로 밀어 계산합니다."""
        with self._lock:
            # 그 트랙, 없으면 그 트랙을 이어받은 트랙(레이더 번호가 바뀐 같은 차, continues) 중 가장 최근 것
            cands = [st for st in self._tracks.values() if st['samples'] and (st['seq'] == seq or st.get('continues') == seq)]
            if not cands:
                return None
            st = max(cands, key=lambda c: c['last_arrival'])
            last = st['samples'][-1]
            ahead = t_arrival - last[0]
            if 0.0 < ahead <= max_ahead:
                ref = next((q for q in st['samples'] if q[0] >= last[0] - 0.5), st['samples'][0])
                span = last[0] - ref[0]
                if span < 0.05:
                    return last[3], last[4], last[2]
                return (last[3] + (last[3] - ref[3]) / span * ahead, last[4] + (last[4] - ref[4]) / span * ahead,
                        last[2])
            best = min(st['samples'], key=lambda q: abs(q[0] - t_arrival))
            if max_dt is not None and abs(best[0] - t_arrival) > max_dt:
                return None
            return best[3], best[4], best[2]

    def rows_for_frame(self, frame):
        """레이더 프레임 한 장의 모든 객체를 화면 표시 행으로(어느 카메라 차로에도 없는 객체는 in_zone=False).
        속도는 그 프레임 도착 시각 ±0.25초 트랙 속도 중앙값(구역 밖은 측정값 그대로)."""
        if frame is None:
            return []
        p = self.params()
        arrival = float(frame['arrival'])
        rows = []
        with self._lock:
            for obj in frame.get('objects', []):
                x, y = float(obj['x_m']), float(obj['y_m'])
                in_zone = (float(obj.get('quality', 0.0)) >= p["min_quality"] and self._in_bounds(obj, p)
                           and any(self._in_lane(lane, x, y, p) for lane in p["lanes"].values()))
                st = self._tracks.get(int(obj['id'])) if in_zone else None
                around = [s[2] for s in st['samples'] if abs(s[0] - arrival) <= 0.25] if st else []
                speed = float(np.median(around)) if around else float(obj['speed_kmh'])
                rows.append({'radar_id': int(obj['id']), 'x_m': float(obj['x_m']), 'y_m': float(obj['y_m']),
                             'speed_kmh': speed, 'over': in_zone and speed > p["threshold"],
                             'fired': bool(st and st['fired']), 'in_zone': in_zone})
        return rows


def point_in_polygon(poly, x, y):
    inside, n = False, len(poly)
    for i in range(n):
        (x1, y1), (x2, y2) = poly[i], poly[(i + 1) % n]
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-12) + x1:
            inside = not inside
    return inside


def roi_radar_polygon(roi_norm, rc):
    """카메라 ROI(정규화 화면 좌표)를 레이더 지면 좌표 다각형 [[x, y], ...]으로 바꿉니다(호모그래피의 역변환).
    호모그래피가 없거나 보정이 망가졌으면(거의 특이 행렬, 정상 보정은 조건수 1e4 안팎) None.
    ROI 꼭짓점이 레이더 뒤쪽이나 수평선 너머로 가도(화면 전체 ROI 등) None입니다. rc: 그 레이더 보정값(radar_camera_cfg)."""
    H, size = rc.get("homography"), rc.get("image_size")
    if not H or not size or not roi_norm or len(roi_norm) < 3:
        return None
    H = np.asarray(H, dtype=np.float64).reshape(3, 3)
    if not np.all(np.isfinite(H)) or np.linalg.cond(H) > 1e7:
        return None
    Hi = np.linalg.inv(H)
    pts = [Hi @ np.array([float(px) * float(size[0]), float(py) * float(size[1]), 1.0]) for px, py in roi_norm]
    if any(abs(p[2]) < 1e-12 for p in pts) or len({bool(p[2] > 0) for p in pts}) != 1:
        return None
    poly = [[round(float(p[0] / p[2]), 2), round(float(p[1] / p[2]), 2)] for p in pts]
    return poly if all(x > 0 for x, _ in poly) else None


def colocated_projector(col, shape, reflector_z=0.5):
    """카메라·레이더가 위아래로 붙어 같은 정면을 볼 때의 기하 투영."""
    size = col.get("image_size") or [shape[1], shape[0]]
    s = shape[1] / float(size[0])
    f = float(col["f_px"]) * s
    cx, cy = shape[1] / 2.0, shape[0] / 2.0
    yaw, pitch = math.radians(float(col.get("yaw_deg", 0.0))), math.radians(float(col.get("pitch_deg", 0.0)))
    cam_h = float(col.get("cam_height_m", 1.7))

    def project(x, y):
        x, y = float(x), float(y)
        if x <= 0.5:
            return None
        az = math.atan2(y, x) - yaw
        el = math.atan2(cam_h - reflector_z, x) - pitch  # pitch: 카메라 광축 하향각(+)
        if abs(az) >= math.radians(85) or abs(el) >= math.radians(85):
            return None
        return cx - f * math.tan(az), cy + f * math.tan(el)
    return project


def radar_camera_projector(shape, rc=None):
    """레이더 지면 (x, y) → 지금 추론 프레임 픽셀 (u, v) 함수.
    호모그래피가 있으면 그것을, 없으면 같은 정면 기하 모델(radar_camera.colocated)을 씁니다. 둘 다 없으면 None.
    rc: 이 카메라 레이더의 보정값(radar_camera_cfg). 없으면 radar_camera."""
    rc = SYS_CFG["radar_camera"] if rc is None else rc
    H = rc.get("homography")
    if not H or shape is None:
        col = rc.get("colocated") or {}
        if shape is not None and col.get("enabled") and col.get("f_px"):
            return colocated_projector(col, shape)
        return None
    H = np.asarray(H, dtype=np.float64).reshape(3, 3)
    size = rc.get("image_size") or [shape[1], shape[0]]
    sx, sy = shape[1] / float(size[0]), shape[0] / float(size[1])

    def project(x, y):
        v = H @ np.array([float(x), float(y), 1.0])
        if abs(v[2]) < 1e-9:
            return None
        return float(v[0] / v[2] * sx), float(v[1] / v[2] * sy)
    return project


def box_point_distance(t, u, v):
    """레이더 점(u, v)과 차량 박스 '하단 띠'의 거리(px). 점이 띠 안이면 0.
    띠 = 가로는 박스 폭, 세로는 박스 높이의 하단 35% ~ 박스 아래 30%까지.
    레이더 대표점은 가까운 차일수록(특히 큰 트럭) 박스 하단보다 아래로 찍히고 좌우로도 흔들리므로,
    하단 중앙 한 점까지의 거리 대신 이 띠까지의 거리로 비교합니다."""
    x1, y1, x2, y2 = (float(c) for c in t[:4])
    h = max(1.0, y2 - y1)
    top, bottom = y2 - 0.35 * h, y2 + 0.30 * h
    dx = 0.0 if x1 <= u <= x2 else min(abs(u - x1), abs(u - x2))
    dy = 0.0 if top <= v <= bottom else min(abs(v - top), abs(v - bottom))
    return math.hypot(dx, dy)


def projection_focal_px(frame_shape, rc=None):
    """지금 투영의 초점거리(추론 프레임 px). 같은 정면 기하 모델일 때만 알 수 있고, 호모그래피면 None."""
    rc = SYS_CFG["radar_camera"] if rc is None else rc
    col = rc.get("colocated") or {}
    if rc.get("homography") or not col.get("enabled") or not col.get("f_px") or frame_shape is None:
        return None
    size = col.get("image_size") or [frame_shape[1], frame_shape[0]]
    return float(col["f_px"]) * frame_shape[1] / float(size[0])


def size_consistent(t, row, focal_px):
    """박스 폭이 레이더 거리에서 차 한 대(폭 1.8m의 0.4~4배)로 보일 만한 크기인지(focal_px가 None이면 검사 안 함)."""
    if focal_px is None:
        return True
    rng = max(1.0, math.hypot(float(row['x_m']), float(row['y_m'])))
    ratio = (float(t[2]) - float(t[0])) / (focal_px * 1.8 / rng)
    return 0.4 <= ratio <= 4.0


def box_band_in_roi(t, roi_poly):
    """차량 박스 하단 띠(박스 높이 아래 35%)가 ROI(단속 차로, 추론 프레임 px 다각형)에 조금이라도 걸치는지.
    ROI가 없으면(점 3개 미만) True. 가까이 와서 박스 아래가 ROI 밖으로 나간 차도 띠 위쪽이 걸치면 인정합니다."""
    if roi_poly is None or len(roi_poly) < 3:
        return True
    poly = [(float(q[0]), float(q[1])) for q in roi_poly]
    x1, y1, x2, y2 = (float(c) for c in t[:4])
    h = max(1.0, y2 - y1)
    return any(point_in_polygon(poly, x1 + fx * (x2 - x1), y2 - fy * h)
               for fy in (0.0, 0.175, 0.35) for fx in (0.0, 0.25, 0.5, 0.75, 1.0))


def associate_radar_tracks(tracks, track_map, radar_rows, project, gate_px, extra_tracks=None, frame_shape=None,
                           focal_px=None, roi_poly=None):
    """투영한 레이더 점과 차량 박스(하단 띠, box_point_distance)의 거리가 gate_px 이내인 쌍을 가까운 순으로 1:1 매칭합니다.
    extra_tracks: tracks(움직이는 차량)와 짝이 안 된 점은, 정지로 보였던 박스라도 점이 하단 띠 안에 있으면 연결
    (멀리서 천천히 다가오는 큰 차는 박스가 거의 안 움직여 정지로 오판되기 때문).
    frame_shape: 주면 화면 밖으로 투영된 점(센서 옆을 지나가는 차 등)은 연결하지 않습니다.
    focal_px: 주면 박스 크기가 레이더 거리와 맞지 않는 쌍은 연결하지 않습니다(size_consistent).
    roi_poly: 주면(추론 프레임 px 다각형, 단속 차로) 하단 띠가 ROI에 걸친 박스만 연결합니다(box_band_in_roi).
    반환: {rgb tid: (row, (u, v))}, 그리고 모든 행의 투영점 [(row, (u, v))]."""
    projected = []
    for row in radar_rows:
        uv = project(row['x_m'], row['y_m'])
        if uv is not None:
            projected.append((row, uv))
    if frame_shape is not None:
        fh, fw = frame_shape[:2]
        usable = [i for i, (_, uv) in enumerate(projected) if -0.02 * fw <= uv[0] <= 1.02 * fw and uv[1] <= 1.15 * fh]
    else:
        usable = list(range(len(projected)))

    def pairs(cands, gate):
        out = []
        for t in cands:
            tid = int(t[4])
            if track_map.get(tid) not in TARGET_VEHICLES:
                continue
            if roi_poly is not None and not box_band_in_roi(t, roi_poly):
                continue
            bx, by = (float(t[0]) + float(t[2])) / 2.0, float(t[3])
            for i in usable:
                uv = projected[i][1]
                d = box_point_distance(t, uv[0], uv[1])
                if d <= gate and size_consistent(t, projected[i][0], focal_px):
                    # 띠가 겹치는 박스끼리는 하단 중앙에 더 가까운 쪽 우선
                    out.append((d, math.hypot(uv[0] - bx, uv[1] - by), tid, i))
        return out
    matched, used_tids, used_rows = {}, set(), set()

    def take(cands):
        for d, _, tid, i in sorted(cands):
            if tid in used_tids or i in used_rows:
                continue
            used_tids.add(tid)
            used_rows.add(i)
            matched[tid] = projected[i]
    take(pairs(tracks, gate_px))
    if extra_tracks is not None and len(extra_tracks):
        moving_ids = {int(t[4]) for t in tracks}
        take(pairs([t for t in extra_tracks if int(t[4]) not in moving_ids], 0.0))
    return matched, projected


class RadarCameraCalibrationRecorder:
    """보정용 쌍 수집: 움직이는 차량 박스가 1개인 카메라 프레임과, 설정 구역 안 객체가 1개인 레이더 프레임을 각각 jsonl로
    남깁니다(ROI 판정 구역은 지금 보정값으로 만든 것이라 레이더 쪽에 적용하지 않음).
    시간 맞추기와 호모그래피 계산은 오프라인 보정 도구가 합니다."""
    _instance = None
    _lock = threading.Lock()

    @classmethod
    def get(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def __init__(self):
        root = Path(SYS_CFG["radar_camera"]["calibration_dir"])
        if not root.is_absolute():
            root = Path(PROJECT_ROOT) / root
        root.mkdir(parents=True, exist_ok=True)
        self.path = root / f"calib_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}.jsonl"
        self._file = self.path.open('a', encoding='utf-8', buffering=1)
        self._write_lock = threading.Lock()
        logger.info(f"[RADAR CALIB] 보정 데이터 수집 시작: {self.path}")

    def _write(self, record):
        with self._write_lock:
            self._file.write(json.dumps(record, ensure_ascii=False) + "\n")

    def radar_hook(self, frame, objs):
        """objs: 설정 구역 안 레이더 객체(ROI 판정 구역 제한 없음). 한 개뿐인 프레임만 기록합니다."""
        if len(objs) == 1:
            obj = objs[0]
            self._write({"type": "radar", "client_id": frame.get('client_id'), "t": frame['arrival'], "id": int(obj['id']),
                         "x": float(obj['x_m']),
                         "y": float(obj['y_m']), "speed_kmh": float(obj['speed_kmh'])})

    def camera_obs(self, camera_key, t_cam, pts, track, shape):
        x1, y1, x2, y2 = (float(v) for v in track[:4])
        self._write({"type": "camera", "cam": str(camera_key), "t": t_cam, "pts": pts, "tid": int(track[4]),
                     "box": [x1, y1, x2, y2], "uv": [(x1 + x2) / 2.0, y2], "shape": [int(shape[0]), int(shape[1])]})


def radar_log_points(rows, project, matched):
    """분석용(.infer.jsonl "radar") 레이더 점: 차로 안이거나 움직이는(3km/h 이상) 점만
    [레이더 id, 거리 m, x, y, 속도 km/h, 차로 안, 화면 u, v, 붙은 박스 tid]. matched: associate_radar_tracks 결과."""
    tid_of = {int(row['radar_id']): int(tid) for tid, (row, _) in matched.items()}
    points = []
    for r in rows:
        if not r['in_zone'] and abs(float(r['speed_kmh'])) < 3.0:
            continue
        x, y = float(r['x_m']), float(r['y_m'])
        uv = project(x, y) if project is not None else None
        points.append([int(r['radar_id']), round(math.hypot(x, y), 1), round(x, 2), round(y, 2),
                       round(float(r['speed_kmh']), 1), bool(r['in_zone'])]
                      + ([round(float(uv[0]), 1), round(float(uv[1]), 1)] if uv is not None else [None, None])
                      + [tid_of.get(int(r['radar_id']))])
    return points


def vote_event_track(ev, votes, offset):
    """판정 구간(약 1.5초) 동안 이 레이더 점이 붙었던 박스 tid 표 → (다수 tid 또는 None, {tid: 표}).
    1등이 3표 이상이고 전체의 60% 이상이어야 다수 tid입니다."""
    if not votes:
        return None, {}
    t0 = float(ev['window_arrival'][0]) + offset - 0.5
    t1 = float(ev['decision_arrival']) + offset + 0.2
    counts = {}
    for ct, rid, tid in list(votes):
        if rid == ev['radar_id'] and t0 <= ct <= t1:
            counts[tid] = counts.get(tid, 0) + 1
    if not counts:
        return None, {}
    tid, n = max(counts.items(), key=lambda kv: kv[1])
    if n >= 3 and n / float(sum(counts.values())) >= 0.6:
        return tid, counts
    return None, counts


class SpeedingDetector(BaseEventDetector):
    gui_name = "SPEEDING"
    # 과속은 자기 수신기(SpeedFrameReader: 프레임별 PTS·원본 링버퍼)로 모든 프레임을 받습니다(레이더 시각 맞춤).
    stream_reader = "SpeedFrameReader"
    stream_all_frames = True

    def process(self, tracks, track_map, frame, fid, **kwargs):
        """판정은 레이더 판정기(RadarSpeedJudge)가 센서 속도로 합니다. 실제 MSE 레이더가 없으면 이벤트도 없습니다."""
        if kwargs.get('radar_judge') is None:
            return []
        return self._process_radar_judge(tracks, track_map, frame, fid, **kwargs)

    def _process_radar_judge(self, tracks, track_map, frame, fid, **kwargs):
        """레이더 판정기가 낸 이벤트를 기존 dispatch 형식으로 바꿉니다. 판정 구역은 레이더 쪽(ROI를 바꾼 다각형)이고,
        차량 박스 연결은 ROI(단속 차로)에 걸친 박스만 씁니다."""
        judge = kwargs['radar_judge']
        camera_key = kwargs.get('camera_key')
        project = kwargs.get('radar_projector')
        offset = float(kwargs.get('radar_latency_offset', 0.0))
        camera_time = kwargs.get('camera_time')
        pts_at = kwargs.get('pts_at')
        gate = float(kwargs.get('radar_assoc_gate_px', 80))
        privacy_tracks = kwargs.get('privacy_tracks', [])
        h, w = (frame.shape[:2] if frame is not None else (0, 0))
        triggered = []
        moving = kwargs.get('radar_moving_tracks')
        moving = moving if moving is not None else tracks
        votes = kwargs.get('radar_votes')
        for ev in judge.pop_events(camera_key):
            # 1) 판정 구간 동안 이 레이더 점이 붙었던 박스의 다수결, 2) 없으면 판정 시점 한 프레임 연결
            #    (박스 하단 띠 + 레이더 거리-박스 크기 검사). 표가 갈리고 한 프레임 연결이 1등과 다르면 연결 보류.
            assoc_tid, assoc_track, method, single = None, None, None, None
            if project is not None and camera_time is not None:
                # 이 카메라 프레임과 같은 순간의 레이더 위치(카메라 시각 - 지연 차)로 박스를 찾습니다.
                pos = judge.position_at(ev['seq'], camera_time - offset)
                if pos is not None:
                    row = {'radar_id': ev['radar_id'], 'x_m': pos[0], 'y_m': pos[1], 'speed_kmh': pos[2]}
                    matched, _ = associate_radar_tracks(moving, track_map, [row], project, gate, frame_shape=frame.shape
                                                        if frame is not None else None,
                                                        focal_px=projection_focal_px(frame.shape if frame is not None else None,
                                                                                     kwargs.get('radar_camera_cfg')),
                                                        roi_poly=self.roi_poly if len(self.roi_poly) >= 3 else None)
                    if matched:
                        single = next(iter(matched))
            voted, counts = vote_event_track(ev, votes, offset)
            live = {int(t[4]): t for t in tracks}
            # 판정 순간 한 프레임 연결(single)이 없으면 표가 6개 이상일 때만 다수결을 믿습니다.
            if voted is not None and voted in live and (single is not None or counts.get(voted, 0) >= 6):
                assoc_tid, method = voted, "vote"
            elif single is not None and (not counts or single == max(counts, key=counts.get)):
                assoc_tid, method = single, "frame"
            elif single is not None:
                method = "conflict"
            if assoc_tid is not None:
                # 같은 차 박스: same_vehicle_guard_sec 안에 이 카메라에서 같은 차 박스에 붙어 과속을 낸 레이더 점이 있으면
                # 속도·간격이 달라도 같은 차(레이더가 트럭 앞쪽·컨테이너를 둘로 잡음). 차 검출 박스 하나가 차 한 대라는 전제.
                now_m = time.monotonic()
                guard = float(self.config.get("same_vehicle_guard_sec", 3.0))
                recent = self.__dict__.setdefault("_recent_box_events", deque(maxlen=32))
                dup = next((r for r in recent if r[1] == assoc_tid and now_m - r[0] <= guard), None)
                if dup is not None:
                    logger.info(f"[RADAR SPEEDING] R{ev['radar_id']} seq={ev['seq']} {ev['median_kmh']:.1f}km/h는 "
                                f"{now_m - dup[0]:.1f}초 전 과속을 낸 R{dup[2]}({dup[3]:.1f}km/h)와 같은 차 박스(tid {assoc_tid})에 "
                                f"붙어 같은 차로 보고 이벤트를 내지 않습니다.")
                    continue
                recent.append((now_m, assoc_tid, ev['radar_id'], float(ev['median_kmh'])))
                assoc_track = live[assoc_tid]
            if assoc_track is not None:
                bbox = assoc_track[:4]
                class_id = track_map.get(assoc_tid)
                objects = [{'label': 'car' if class_id == ID_G_CAR else 'truck', 'box': [int(x) for x in bbox],
                            'score': float(assoc_track[5]), 'tid': assoc_tid, 'class_id': int(class_id),
                            'speed_kmh': round(ev['median_kmh'], 1), 'radar_id': ev['radar_id']}]
            else:
                bbox = [0, 0, max(0, w - 1), max(0, h - 1)]
                objects = [{'label': 'radar_vehicle', 'box': [int(x) for x in bbox], 'score': 1.0,
                            'tid': ev['tid'], 'class_id': -1,
                            'speed_kmh': round(ev['median_kmh'], 1), 'radar_id': ev['radar_id']}]
            fullres_window, radar_path = None, []
            if callable(pts_at):
                event_pts, ring_dir = pts_at(ev['decision_arrival'] + offset)
                # 측정 구간이 길어(먼 곳 수십 m) 기본 원본 구간은 판정 순간 앞 1.5초까지만(따라가며 고른 구간이 없을 때)
                start_pts, start_ring = pts_at(max(ev['window_arrival'][0], ev['decision_arrival'] - 1.5) + offset)
                if event_pts is not None and ring_dir and start_ring == ring_dir:
                    fullres_window = {'event_pts': event_pts, 'start_pts': start_pts, 'ring_dir': ring_dir}
                # 이 차가 판정 전에 지나온 레이더 경로 [pts, 거리, 속도, (화면 u, v)]. 멀어지는 차는 번호판 사진 구간(카메라 앞)을
                # 판정보다 먼저 지나가므로, 원본 링버퍼에서 그때를 꺼냅니다(radar_path_window).
                for t_arr, x, y, kmh in ev.get('path') or []:
                    q_pts, _ = pts_at(t_arr + offset)
                    if q_pts is None:
                        continue
                    uv = project(x, y) if project is not None else None
                    radar_path.append([round(float(q_pts), 4), round(math.hypot(x, y), 2), round(float(kmh), 1)]
                                      + ([round(float(uv[0]), 1), round(float(uv[1]), 1)] if uv is not None else []))
            triggered.append({
                'tid': ev['tid'],
                'bbox': bbox,
                'frame': frame.copy() if frame is not None else None,
                'fid': fid,
                'privacy_tracks': privacy_tracks,
                'privacy_fid': fid,
                'objects': objects,
                'fullres_window': fullres_window,
                'radar_path': radar_path,
                'photo_m': ev.get('photo_m'),
                'direction': ev.get('direction'),
                'decision_trace': {
                    'detector': 'SpeedingDetector',
                    'reason': 'radar_track_median_speed_exceeded',
                    'radar_client_id': getattr(judge.radar, 'client_id', None),
                    'radar_id': ev['radar_id'], 'radar_track_seq': ev['seq'],
                    'speed_kmh': round(ev['median_kmh'], 2),
                    'speed_min_kmh': round(ev['min_kmh'], 2), 'speed_max_kmh': round(ev['max_kmh'], 2),
                    'samples': ev['samples'],
                    'window_sec': round(ev['window_arrival'][1] - ev['window_arrival'][0], 2),
                    'measure_m': ev.get('measure_m'), 'window_range_m': ev.get('window_range_m'),
                    # 분석용: 판정한 까닭, ROI 차로 안 샘플 수, 이 트랙을 처음 본 거리·시각(판정 기준 초),
                    # 측정 구간 레이더 값 하나하나 [판정 기준 초, 거리 m, x, y, 속도 km/h]
                    'decided_by': ev.get('decided_by'), 'lane_hits': ev.get('lane_hits'),
                    'track_first_range_m': ev.get('track_first_range_m'), 'track_first_sec': ev.get('track_first_sec'),
                    'window_samples': ev.get('window_samples'),
                    'speed_limit_kmh': ev['limit_kmh'], 'margin_kmh': ev['margin_kmh'],
                    'threshold_kmh': ev['threshold_kmh'],
                    'radar_sensor_ts': ev['sensor_ts'], 'radar_x_m': round(ev['x_m'], 2), 'radar_y_m': round(ev['y_m'], 2),
                    'camera_association': 'matched' if assoc_track is not None else (
                        'no_calibration' if project is None else ('conflict' if method == 'conflict' else 'no_match')),
                    'camera_tid': assoc_tid,
                    'assoc_method': method, 'assoc_votes': {str(k): v for k, v in counts.items()},
                    'assoc_single_frame': single, 'direction': ev.get('direction'),
                    'latency_offset_sec': offset,
                    'radar_source': 'umrr11_t132_mse_live',
                }
            })
        return triggered


# ==========================================
# 콘 미제거 출차(cone_ng) 판정 + NG 때 컨테이너 번호 OCR
# ==========================================
# 크레인 한 차선을 보는 카메라들(event_config.cone_ng.groups)이 판정기(ConeNgFusion) 하나를 함께 씁니다.
# 카메라마다: 원본 프레임 → 컨테이너 검출(side.dxnn) → 컨테이너 아래 두 모서리 크롭 → 콘 검출(cone.dxnn) → ConeCameraObs.
# NG가 나면 착지해 있는 동안 모아 둔 원본 컨테이너 크롭으로 번호를 읽습니다(별도 스레드, 이벤트 폴더 container_ocr.json).
# 아래 판정 로직(CONE_NG_DEFAULTS ~ ConeNgFusion)은 콘 판정 담당자 코드(cone_ng.py v4)를 옮긴 것입니다.
# 고칠 때는 그쪽 단위 테스트(test_cone_ng.py)를 이 코드에 대해 다시 돌립니다.
CONE_NG_DEFAULTS = dict(
    container_conf=0.7,    # container/tank box threshold (white reefers score 0.75-0.85; the false boxes
                           # that a higher value used to hide are now removed by the size gates and the
                           # landed-container lock below)
    min_w_frac=0.3,        # ignore far background containers narrower than this (fraction of frame width)
    full_frame_frac=(0.97, 0.7),  # ignore boxes this wide AND this tall: the model's whole-road false box
    bottom_edge_frac=0.98, # ignore boxes reaching the bottom of the frame: trucks in the near lane, and a
                           # camera whose view swung away (PTZ / shake) for a few seconds
    cone_conf=0.05,        # cone (cone.dxnn) threshold inside the corner crops
    smooth_n=5,            # median over the last N samples (box jitter) ...
    gap_reset_sec=0.5,     # ... restarted after a detection gap this long, so old positions don't linger
    desc_min_frac=0.05,    # bottom edge must come down >= 5% of frame height before a landing counts ...
    top_edge_frac=0.03,    # ... or the container first shows up touching the top of the frame (lowered in from
                           #     above; with a tight framing only its last few cm of descent are visible) and
                           #     has not travelled sideways (a truck driving in with a container has)
    still_frac=0.01,       # max cx / bottom range (fraction of frame w / h) during landed_sec
    still_max_gap=0.6,     # ... with no detection gap longer than this inside the window (a terminal judging
                           # every 2-3rd frame of 7.5 fps leaves 0.40 s gaps)
    landed_sec=1.5,
    lift_frac=0.03,        # bottom edge rising this much after landing (while not moving sideways) = lifted again
    height_tol_frac=0.06,  # while moving sideways the bottom may drift this much (perspective as the truck pulls
                           # away: 20-37 px seen); beyond it the container left the chassis height (spreader)
    move_frac=0.02,        # horizontal shift from the landed position = truck starts moving (38 px at 1920)
    frame_edge_px=10,      # a box edge this close to the frame edge is cut off there: its corner is not used for the
                           # shift (NPU boxes stop 4-9 px short of the edge)
    rest_frac=0.005,       # last time the unsmoothed corners were this close to the landed position = it was
                           # still standing (the "cone before the move" window ends there)
    depart_sec=2.0,        # it is a departure once it has kept moving this long, or ...
    line_up_max_frac=0.08, # ... has travelled this far (154 px at 1920; lining-up moves seen: 30-90 px).
    stop_sec=0.5,          # A stop (shift changing < stop_frac of W over stop_sec) before either is the driver
    stop_frac=0.003,       # lining up under the crane: re-anchor there, not a departure
    cone_window_sec=2.0,   # ARMED banner: cones seen this recently ...
    cone_min_hits=3,       # ... on at least this many camera samples
    cone_min_strong=2,     # ... of which at least this many at cone_strong_conf or more
    cone_strong_conf=0.3,  # real cones on a standing container score 0.4-0.8; stray boxes rarely reach 0.3 twice
    cone_corner_dx=0.08,   # a cone sits at a bottom corner: centre within 8% of box width of x1 or x2 ...
    cone_dy=(-0.2, 0.085), # ... and near the bottom edge (fraction of box height, + = below the edge).
                           # The chassis rear bracket under the corner sits at +0.10..+0.15.
    cone_hold_sec=2.0,     # NG, per camera: dense cones (as above) in the 2 s before that camera saw the move ...
    carry_sec=3.0,         # ... and the cone seen again on that camera's corner within 3 s after it moved
    carry_min_hits=1,      #     (an attached cone travels with the container; one held by a lasher does not)
    carry_min_conf=0.3,    #     at this score at least (chassis brackets on a moving truck score 0.03-0.55)
    end_sec=3.0,           # no container on any camera this long = session over
)


@dataclass
class ConeCameraObs:
    cam: str
    t: float                      # common time base (seconds) shared by all cameras of the group
    frame_wh: tuple
    containers: list              # [{'xyxy': [x1,y1,x2,y2], 'conf': float}]
    cones: list                   # [{'xyxy': ..., 'conf': float, 'object_index': index into containers}]


class ConeCameraState:
    """Follows the main container in one camera and derives descended / landed / lifted / departed."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.reset()

    def reset(self):
        self.box = None
        self.raw = deque(maxlen=self.cfg['smooth_n'])
        self.hist = deque()           # (t, cx, bottom) smoothed
        self.last_seen = None
        self.min_bottom = None
        self.descended = False
        self.anchor = None            # (t, cx, bottom, x1, x2) where it landed (or last lined up)
        self.moving_since = None
        self.departed_t = None
        self.rest_t = None            # last time it stood at the landed position (raw corners)
        self.shift_hist = deque()     # (t, shift) while landed, for stop detection
        self.repositions = 0
        self.cone_ts = deque()        # (t, best corner-cone score) per sample
        self.entry = None             # first sighting: {'top': touched the top edge, 'cx': [min, max] since}

    @property
    def landed(self):
        return self.anchor is not None

    def _pick(self, containers, w, h):
        fw, fh = self.cfg['full_frame_frac']
        conf = self.cfg['container_conf']

        def ok(c):
            bw, bh = c['xyxy'][2] - c['xyxy'][0], c['xyxy'][3] - c['xyxy'][1]
            return (c['conf'] >= conf and bw >= self.cfg['min_w_frac'] * w
                    and not (bw >= fw * w and bh >= fh * h)
                    and c['xyxy'][3] < self.cfg['bottom_edge_frac'] * h)
        good = [(i, c) for i, c in enumerate(containers) if ok(c)]
        if not good:
            return None, None
        if self.box is not None:
            i, c = max(good, key=lambda ic: calculate_iou(ic[1]['xyxy'], self.box))
            if calculate_iou(c['xyxy'], self.box) > 0.3:
                return i, c
            if self.anchor is not None:
                # Landed: stay on that container. Another big box (a truck passing in the near lane)
                # while ours is hidden is an occlusion, not our container moving.
                return None, None
        area = lambda c: (c['xyxy'][2] - c['xyxy'][0]) * (c['xyxy'][3] - c['xyxy'][1])
        return max(good, key=lambda ic: area(ic[1]))

    def update(self, o):
        cfg, (w, h), t = self.cfg, o.frame_wh, o.t
        idx, c = self._pick(o.containers, w, h)
        if c is None:
            if self.last_seen is not None and t - self.last_seen >= cfg['end_sec']:
                self.reset()
            return
        if self.anchor is None and self.box is not None and calculate_iou(c['xyxy'], self.box) <= 0.3:
            # Not landed yet and the tracked box became another object (e.g. a truck passing in the
            # near lane, then ours): start this object's descent evidence afresh.
            last_seen = self.last_seen
            self.reset(); self.last_seen = last_seen
        if self.last_seen is not None and t - self.last_seen > cfg['gap_reset_sec']:
            self.raw.clear()  # after a gap the median must not be held at the old position
        cx_now = (c['xyxy'][0] + c['xyxy'][2]) / 2
        if self.entry is None:
            self.entry = dict(top=c['xyxy'][1] <= cfg['top_edge_frac'] * h, cx=[cx_now, cx_now])
        else:
            self.entry['cx'] = [min(self.entry['cx'][0], cx_now), max(self.entry['cx'][1], cx_now)]
        self.box, self.last_seen = c['xyxy'], t
        x1, _, x2, y2 = c['xyxy']
        self.raw.append(((x1 + x2) / 2, y2, x1, x2))
        cx, bottom = median(p[0] for p in self.raw), median(p[1] for p in self.raw)
        sx1, sx2 = median(p[2] for p in self.raw), median(p[3] for p in self.raw)
        self.hist.append((t, cx, bottom))
        while self.hist and self.hist[0][0] < t - 10.0:
            self.hist.popleft()

        best = max((k['conf'] for k in o.cones if k.get('object_index') == idx
                    and k['conf'] >= cfg['cone_conf'] and self._at_corner(c['xyxy'], k['xyxy'])), default=None)
        if best is not None:
            self.cone_ts.append((t, best))
        while self.cone_ts and self.cone_ts[0][0] < t - cfg['cone_hold_sec'] - 30.0:
            self.cone_ts.popleft()

        if self.anchor is None:
            self.min_bottom = bottom if self.min_bottom is None else min(self.min_bottom, bottom)
            # Lowered by the spreader: it came down (or appeared at the top edge) without travelling
            # sideways. A truck driving in with its container travels sideways.
            e = self.entry
            still_sideways = e is not None and e['cx'][1] - e['cx'][0] < cfg['move_frac'] * w
            came_down = bottom - self.min_bottom >= cfg['desc_min_frac'] * h or (e is not None and e['top'])
            if came_down and still_sideways:
                self.descended = True
            if self.descended and self._still(t, w, h):
                self.anchor = (t, cx, bottom, sx1, sx2)
                self.rest_t = t
            return

        at, acx, abottom, ax1, ax2 = self.anchor
        if (self.moving_since is None and self.departed_t is None
                and bottom - abottom >= 0.5 * cfg['lift_frac'] * h and self._still(t, w, h)):
            # Landing was confirmed while it was still settling: re-anchor where it came to rest.
            self.anchor = (at, cx, bottom, sx1, sx2)
            at, acx, abottom, ax1, ax2 = self.anchor
        shift = self._shift(cx - acx, sx1, sx2, ax1, ax2, w, cfg['frame_edge_px'])
        if abottom - bottom >= cfg['lift_frac'] * h and abs(shift) < cfg['move_frac'] * w:
            # Spreader picked it up again: back to "hanging", needs a new descent to land.
            self.anchor, self.descended, self.min_bottom = None, False, bottom
            self.moving_since = self.departed_t = None
            self.shift_hist.clear()
            return
        if self.departed_t is not None:
            return
        if abs(self._shift(cx_now - acx, x1, x2, ax1, ax2, w, cfg['frame_edge_px'])) < cfg['rest_frac'] * w:
            self.rest_t = t
        self.shift_hist.append((t, shift))
        while self.shift_hist and self.shift_hist[0][0] < t - 2 * cfg['stop_sec']:
            self.shift_hist.popleft()
        # The truck carries the container at chassis height; a sideways move of a container that
        # left the chassis height is the spreader / trolley, not a departure.
        at_height = abs(bottom - abottom) < cfg['height_tol_frac'] * h
        if abs(shift) < cfg['move_frac'] * w or not at_height:
            self.moving_since = None
            return
        if self.moving_since is None:
            self.moving_since = t
            return
        if abs(shift) >= cfg['line_up_max_frac'] * w or t - self.moving_since >= cfg['depart_sec']:
            self.departed_t = t  # a stall after this no longer cancels it
        elif t - self.moving_since >= cfg['stop_sec'] and self._stopped(t, shift, w):
            # Moved a little and stopped again: lining up under the crane, not leaving.
            self.anchor = (at, cx, bottom, sx1, sx2)
            self.moving_since = None
            self.shift_hist.clear()
            self.repositions += 1
            self.rest_t = t

    def _stopped(self, t, shift, w):
        past = [p for p in self.shift_hist if p[0] <= t - self.cfg['stop_sec']]
        return bool(past) and abs(shift - past[-1][1]) < self.cfg['stop_frac'] * w

    @staticmethod
    def _shift(dcx, x1, x2, ax1, ax2, w, edge=3):
        """Horizontal shift of the container from where it landed, from the corners inside the frame.
        A box cut by the frame edge changes width as the truck moves, so its centre is unreliable."""
        s = [x1 - ax1] if x1 > edge and ax1 > edge else []
        s += [x2 - ax2] if x2 < w - edge and ax2 < w - edge else []
        if len(s) == 2:
            return min(s, key=abs) if s[0] * s[1] > 0 else 0.0  # both corners must move the same way
        return s[0] if s else dcx

    def _at_corner(self, box, cone):
        x1, _, x2, y2 = box
        bw, bh = x2 - x1, box[3] - box[1]
        cx, cy = (cone[0] + cone[2]) / 2, (cone[1] + cone[3]) / 2
        lo, hi = self.cfg['cone_dy']
        return min(abs(cx - x1), abs(cx - x2)) <= self.cfg['cone_corner_dx'] * bw and lo <= (cy - y2) / bh <= hi

    def _still(self, t, w, h):
        span = self.cfg['landed_sec']
        win = [p for p in self.hist if p[0] >= t - span]
        if not win or win[0][0] > t - 0.9 * span:
            return False
        if any(b[0] - a[0] > self.cfg['still_max_gap'] for a, b in zip(win, win[1:])):
            return False
        cxs, bs = [p[1] for p in win], [p[2] for p in win]
        return max(cxs) - min(cxs) <= self.cfg['still_frac'] * w and max(bs) - min(bs) <= self.cfg['still_frac'] * h

    def cones_dense(self, t, since):
        """Dense corner cones in [since, t] on this camera."""
        cs = [c for ts, c in self.cone_ts if since <= ts <= t]
        return (len(cs) >= self.cfg['cone_min_hits']
                and sum(c >= self.cfg['cone_strong_conf'] for c in cs) >= self.cfg['cone_min_strong'])

    def cone_departed_with_it(self, t):
        """This camera saw its container leave with a cone: dense cones just before it moved and the
        cone again on the moving corner afterwards."""
        ms, cfg = self.moving_since, self.cfg
        if self.departed_t is None or ms is None:
            return False
        rest = self.rest_t if self.rest_t is not None and self.rest_t <= ms else ms
        if not self.cones_dense(rest, rest - cfg['cone_hold_sec']):  # while it was still standing
            return False
        carried = [c for ts, c in self.cone_ts if ms <= ts <= min(t, ms + cfg['carry_sec'])
                   and c >= cfg['carry_min_conf']]
        return len(carried) >= cfg['carry_min_hits']

    def cone_evidence(self, t):
        """Corner-cone numbers behind the departure check, for the NG / DEPARTED_OK records (same windows as
        cone_departed_with_it): standing = hits, hits at cone_strong_conf+, best score in the cone_hold_sec before
        the move (needs cone_min_hits / cone_min_strong); carried = hits at carry_min_conf+ and best score in the
        carry_sec after it (needs carry_min_hits)."""
        ms, cfg = self.moving_since, self.cfg
        if ms is None:
            return None
        rest = self.rest_t if self.rest_t is not None and self.rest_t <= ms else ms
        standing = [c for ts, c in self.cone_ts if rest - cfg['cone_hold_sec'] <= ts <= rest]
        carried = [c for ts, c in self.cone_ts if ms <= ts <= min(t, ms + cfg['carry_sec'])]
        return {"standing_hits": len(standing),
                "standing_strong": sum(c >= cfg['cone_strong_conf'] for c in standing),
                "standing_best": round(max(standing, default=0.0), 3),
                "carried_hits": sum(c >= cfg['carry_min_conf'] for c in carried),
                "carried_best": round(max(carried, default=0.0), 3)}

    @property
    def stage(self):
        if self.box is None:
            return 'IDLE'
        if self.departed_t is not None:
            return 'DEPARTING'
        if self.landed:
            return 'LANDED'
        return 'DESCENDING' if self.descended else 'CONTAINER'


class ConeNgFusion:
    """Group-level session over all cameras of one lane. update() returns new events."""

    def __init__(self, cfg=None):
        self.cfg = {**CONE_NG_DEFAULTS, **(cfg or {})}
        self.cams = {}
        self.session = 0
        self.active = False
        self.last_present = None
        self._new_session_flags()

    def _new_session_flags(self):
        self.armed_sent = self.ng_sent = False
        self.departed = False
        self.depart_cams = []
        self.move_start = None
        self.ng_event = None
        self.cone_evidence = {}   # 출발한 카메라마다 판단에 쓴 콘 숫자(ConeCameraState.cone_evidence)

    def update(self, o):
        cfg = self.cfg
        cam = self.cams.setdefault(o.cam, ConeCameraState(cfg))
        cam.update(o)
        t, events = o.t, []

        seen = [s.last_seen for s in self.cams.values() if s.box is not None and s.last_seen is not None]
        if seen and t - max(seen) < cfg['end_sec']:
            if not self.active:
                self.active, self.session = True, self.session + 1
                self._new_session_flags()
            self.last_present = max(seen)
        elif self.active and t - self.last_present >= cfg['end_sec']:
            if self.departed and not self.ng_sent:
                events.append(dict(self._event('DEPARTED_OK', t, move_cams=self.depart_cams),
                                   cone_evidence=self.cone_evidence))
            self.active = False
            return events
        if not self.active:
            return events

        cone_cams = self._cone_cams(t, t - cfg['cone_window_sec'])
        landed = any(s.landed for s in self.cams.values())
        if landed and cone_cams and not self.armed_sent:
            self.armed_sent = True
            events.append(self._event('ARMED', t, cone_cams=cone_cams))
        move_cams = sorted(k for k, s in self.cams.items() if s.departed_t is not None)
        if move_cams:
            self.departed, self.depart_cams = True, move_cams
            first = min(self.cams[k].moving_since for k in move_cams)
            self.move_start = first if self.move_start is None else min(self.move_start, first)
            self.cone_evidence = {k: self.cams[k].cone_evidence(t) for k in move_cams}
            # Evidence is never mixed across cameras: one camera must see the cone, the move and the
            # cone travelling with the container.
            held = [k for k in move_cams if self.cams[k].cone_departed_with_it(t)]
            if held and not self.ng_sent:
                self.ng_sent = True
                self.ng_event = dict(self._event('NG', t, cone_cams=held, move_cams=move_cams),
                                     cone_evidence=self.cone_evidence)
                events.append(self.ng_event)
        return events

    def _cone_cams(self, t, since):
        """Cameras with dense corner cones in [since, t] (ARMED banner / OCR trigger)."""
        return sorted(k for k, s in self.cams.items() if s.cones_dense(t, since))

    def _event(self, kind, t, cone_cams=(), move_cams=()):
        return dict(type=kind, t=round(t, 3), session=self.session, cone_cams=list(cone_cams),
                    move_cams=list(move_cams), move_start=self.move_start and round(self.move_start, 3),
                    boxes={k: s.box for k, s in self.cams.items() if s.box is not None})

    @property
    def state(self):
        """One word for the overlay banner."""
        if not self.active:
            return 'IDLE'
        if self.ng_sent:
            return 'NG'
        stages = {s.stage for s in self.cams.values()}
        if 'DEPARTING' in stages:
            return 'DEPARTING'
        if 'LANDED' in stages:
            t = self.last_present
            return 'ARMED' if self._cone_cams(t, t - self.cfg['cone_window_sec']) else 'LANDED'
        if 'DESCENDING' in stages:
            return 'DESCENDING'
        return 'CONTAINER'


# ---------- 컨테이너 표기 형식 검사 (ISO 6346) ----------
_ISO6346_LETTER_VALUES = {}
_v = 10
for _c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    if _v % 11 == 0:
        _v += 1
    _ISO6346_LETTER_VALUES[_c] = _v
    _v += 1
del _v, _c
_ISO6346_LENGTH_CODES = set("12345ABCDEFGHKMNP") | {"L"}  # L(45ft)은 1995년판 코드지만 아직 쓰입니다
_ISO6346_HEIGHT_CODES = set("02456789") | set("CDEFLMNPRSTU")
_ISO6346_TYPE_BASE = (["G0", "G1", "G2", "G3", "V0", "V2", "V4", "R0", "R1", "R2", "R3",
                       "H0", "H1", "H2", "H5", "H6", "U0", "U1", "U2", "U3", "U4", "U5",
                       "B0", "B1", "B3", "B4", "B5", "B6", "P0", "P1", "P2", "P3", "P4", "P5",
                       "S0", "S1", "S2"] + ["T%d" % i for i in range(10)])
_ISO6346_ALT = dict(zip("0123456789", "ABDGJMVWXY"))  # 강도 낮춘 컨테이너의 4번째 글자
_ISO6346_TYPE_CODES = set(_ISO6346_TYPE_BASE) | {t[0] + _ISO6346_ALT[t[1]] for t in _ISO6346_TYPE_BASE}
# 확장 코드: 공개 코드표에는 없지만 탱크 컨테이너에 쓰여 인정하는 K 계열(결과에 표시)
CONTAINER_EXTENSION_TYPE_CODES = frozenset("K%d" % i for i in range(10))


def container_number_ok(s):
    """컨테이너 번호 형식 검사: 영문 4자(4번째 U/J/Z) + 숫자 7자, 체크디지트 일치."""
    if len(s) != 11 or not container_owner_ok(s[:4]) or not (s[4:].isascii() and s[4:].isdigit()):
        return False
    total = sum((_ISO6346_LETTER_VALUES[ch] if ch.isalpha() else int(ch)) * (2 ** i) for i, ch in enumerate(s[:10]))
    return total % 11 % 10 == int(s[10])


def container_owner_ok(s):
    """두 줄 표기 윗줄(소유자 코드) 모양."""
    return len(s) == 4 and s.isascii() and s.isalpha() and s.isupper() and s[3] in "UJZ"


def container_serial_ok(s):
    """두 줄 표기 아랫줄(일련번호 + 체크디지트) 모양."""
    return len(s) == 7 and s.isascii() and s.isdigit()


def container_size_type_ok(s):
    """크기·형식 코드 형식 검사: ISO 6346 코드표 또는 확장 코드."""
    return (len(s) == 4 and s[0] in _ISO6346_LENGTH_CODES and s[1] in _ISO6346_HEIGHT_CODES
            and (s[2:] in _ISO6346_TYPE_CODES or s[2:] in CONTAINER_EXTENSION_TYPE_CODES))


def container_size_type_is_extension(s):
    return container_size_type_ok(s) and s[2:] in CONTAINER_EXTENSION_TYPE_CODES


# ---------- 콘 검출: 원본 프레임 → 컨테이너 → 아래 모서리 크롭 → 콘 ----------
def _cone_corner_roi(box, side, frame_w, frame_h):
    """컨테이너 아래 모서리 중심의 16:9 크롭 창(1080p에서 640x360 ~ 1280x720)."""
    x1, y1, x2, y2 = box
    unit = max(1, math.ceil(min(frame_h * 2 / 3, max(frame_h / 3, 0.8 * (y2 - y1))) / 9))
    rw, rh = 16 * unit, 9 * unit
    cx = x1 if side == "left" else x2
    rx, ry = math.floor(cx - rw / 2), math.floor(y2 - rh / 2)
    clipped = [max(0, rx), max(0, ry), min(frame_w, rx + rw), min(frame_h, ry + rh)]
    return {"requested": [rx, ry, rx + rw, ry + rh], "xyxy": clipped, "wh": (rw, rh), "scale": 1280.0 / rw}


def _cone_crop_canvas(image, roi):
    """크롭 창을 1280x720으로 키우고 위아래 8px을 채운 1280x736 입력(창이 프레임 밖이면 114로 채움, 늘이지 않음)."""
    rx, ry = roi["requested"][:2]
    x1, y1, x2, y2 = roi["xyxy"]
    rw, rh = roi["wh"]
    crop = np.full((rh, rw, 3), 114, np.uint8)
    crop[y1 - ry:y2 - ry, x1 - rx:x2 - rx] = image[y1:y2, x1:x2]
    content = cv2.resize(crop, (1280, 720), interpolation=cv2.INTER_LINEAR)
    return cv2.copyMakeBorder(content, 8, 8, 0, 0, cv2.BORDER_CONSTANT, value=(114, 114, 114))


class ConeObserver:
    """콘 카메라 화면 검출(side.dxnn: 0 container, 1 tank, 2 spreader, 3 person, 4 YT_HEADER)과 모서리 크롭의
    콘 검출(cone.dxnn: 0 콘). 판정에는 컨테이너·탱크만, NG 사진 얼굴 가리기에는 사람만 쓰고, 5클래스 모두 화면에 그립니다.

    두 모델 모두 NPU(YoLoDeepX) raw 출력이어야 합니다. 콘은 0.05 아래 점수까지 판정에 쓰므로
    장치에서 0.25 미만을 버리는 PPU 출력은 쓸 수 없습니다.
    NPU는 한 장씩 차례로 넣으면 코어 하나만 씁니다(컨테이너 58ms, 콘 39ms). 엔진을 여러 개 두고 모서리 크롭을
    스레드로 동시에 넣어, 카메라 여러 대와 크롭 두 장이 코어 3개를 나눠 쓰게 합니다."""
    CONTAINER_CLASSES = (0, 1)
    PERSON_CLASS = 3  # side.dxnn 사람(NG 사진 얼굴 가리기용, people)
    # 콘 모델은 콘 전용 cone.dxnn(1클래스, 0번 콘)입니다. 클래스 수가 1이 아니면(다른 모델을 적었으면) 콘을 찾지 않고 경고합니다.
    CONE_CLASS = 0

    def __init__(self, cfg):
        self.cfg = cfg
        self.logic = {**CONE_NG_DEFAULTS, **(cfg.get("logic") or {})}
        # 모델·출력 형식·엔진 수·점수 기준은 models·model_output_formats·model_engine_pool_sizes·model_confidences의 SIDE·CONE
        models = SYS_CFG["models"]
        cone_pool = get_model_engine_pool_size("CONE")
        self.det_container = YoLoDeepX(resolve_model_path(models["SIDE"]),
                                       output_format=get_model_output_format("SIDE"),
                                       pool_size=get_model_engine_pool_size("SIDE"))
        self.det_cone = YoLoDeepX(resolve_model_path(models["CONE"]),
                                  output_format=get_model_output_format("CONE"), pool_size=cone_pool)
        apply_class_confidences(self.det_container, "SIDE")
        apply_class_confidences(self.det_cone, "CONE")
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(2, cone_pool), thread_name_prefix="cone-npu")
        cone_name = os.path.basename(str(self.det_cone.engine_path))
        cone_count = self.det_cone.raw_yolo_num_classes()
        self.cone_class = self.CONE_CLASS if cone_count == 1 else None
        logger.info(f"[CONE_NG] 모델 로드: side={os.path.basename(str(self.det_container.engine_path))} "
                    f"cone={cone_name} (엔진 {self.det_container.pool_size}/{cone_pool}, 콘 클래스 {self.cone_class})")
        if self.cone_class is None:
            logger.warning(f"[CONE_NG] 콘 모델 {cone_name}(클래스 {cone_count}개)이 콘 전용 1클래스 모델이 아니라 "
                           f"콘을 찾지 않습니다. models.CONE을 cone.dxnn으로 바꾸세요.")

    def people(self, image):
        """NG 사진 얼굴 가리기용 사람(side.dxnn 3번). privacy_tracks 모양 [x1, y1, x2, y2, tid, 점수, 클래스]이고,
        클래스는 얼굴 블러가 보는 메인 모델 사람 번호(ID_G_PERSON)로 적습니다. NG 때 한 번만 부릅니다."""
        dets = self.det_container.infer(image)  # 점수 기준 model_confidences.SIDE person
        return [[float(d[0]), float(d[1]), float(d[2]), float(d[3]), -1, float(d[4]), ID_G_PERSON]
                for d in dets if int(d[5]) == self.PERSON_CLASS]

    def _target(self, c, w, h):
        """판정이 주 컨테이너로 고를 수 있는 상자(ConeCameraState._pick과 같은 점수·크기·화면 조건)."""
        x1, y1, x2, y2 = c["xyxy"]
        fw, fh = self.logic["full_frame_frac"]
        return (c["conf"] >= self.logic["container_conf"] and x2 - x1 >= self.logic["min_w_frac"] * w
                and not (x2 - x1 >= fw * w and y2 - y1 >= fh * h) and y2 < self.logic["bottom_edge_frac"] * h)

    def _corner_cones(self, image, box, side, idx):
        if self.cone_class is None:
            return []
        h, w = image.shape[:2]
        roi = _cone_corner_roi(box, side, w, h)
        vx1, vy1, vx2, vy2 = roi["xyxy"]
        if vx2 <= vx1 or vy2 <= vy1:
            return []
        rx, ry = roi["requested"][:2]
        scale = roi["scale"]
        out = []
        for d in self.det_cone.infer(_cone_crop_canvas(image, roi)):  # 점수 기준 model_confidences.CONE
            if int(d[5]) != self.cone_class:
                continue
            a, b, cc, dd = (float(v) for v in d[:4])
            m = [a / scale + rx, (b - 8) / scale + ry, cc / scale + rx, (dd - 8) / scale + ry]
            mx, my = (m[0] + m[2]) / 2, (m[1] + m[3]) / 2
            if not (vx1 <= mx < vx2 and vy1 <= my < vy2):
                continue  # 채운 자리(프레임 밖)에 중심이 있는 검출은 버립니다
            m = [max(vx1, m[0]), max(vy1, m[1]), min(vx2, m[2]), min(vy2, m[3])]
            if m[2] > m[0] and m[3] > m[1]:
                out.append({"xyxy": m, "conf": float(d[4]), "object_index": idx})
        return out

    def observe(self, image, keep_container=None, with_cones=True):
        """image(원본 BGR)에서 컨테이너 박스와 그 아래 모서리의 콘을 찾습니다.
        반환 (containers, cones, others): containers는 면적 큰 순 [{'xyxy', 'conf', 'cls'}](0 컨테이너, 1 탱크),
        cones는 [{'xyxy', 'conf', 'object_index'}] (object_index = containers 안 위치),
        others는 컨테이너·탱크 밖 클래스(2 스프레더, 3 사람, 4 YT_HEADER) [{'xyxy', 'conf', 'cls'}](화면 표시용).
        with_cones=False면 모서리 크롭 추론을 건너뜁니다(콘을 판정에 쓰지 않는 단계)."""
        h, w = image.shape[:2]
        dets = self.det_container.infer(image)  # 점수 기준 model_confidences.SIDE
        boxes = [{"xyxy": [float(v) for v in d[:4]], "conf": float(d[4]), "cls": int(d[5])} for d in dets]
        containers = [b for b in boxes if b["cls"] in self.CONTAINER_CLASSES]
        others = [b for b in boxes if b["cls"] not in self.CONTAINER_CLASSES]
        if keep_container is not None:
            containers = [c for c in containers if keep_container(c["xyxy"])]
        containers.sort(key=lambda c: -(c["xyxy"][2] - c["xyxy"][0]) * (c["xyxy"][3] - c["xyxy"][1]))
        targets = [i for i, c in enumerate(containers) if self._target(c, w, h)]
        max_containers = self.cfg.get("max_containers")
        if not with_cones:
            targets = []
        jobs = [self._pool.submit(self._corner_cones, image, containers[i]["xyxy"], side, i)
                for i in (targets[:max_containers] if max_containers else targets) for side in ("left", "right")]
        cones = [k for j in jobs for k in j.result()]
        if len(cones) > 1:
            # 이웃 크롭(같은 컨테이너의 왼쪽·오른쪽, 겹친 컨테이너)에서 같은 콘을 두 번 찾은 것을 합칩니다.
            keep = cv2.dnn.NMSBoxes([[c["xyxy"][0], c["xyxy"][1], c["xyxy"][2] - c["xyxy"][0], c["xyxy"][3] - c["xyxy"][1]]
                                     for c in cones], [c["conf"] for c in cones], 0.0, 0.5)
            cones = [cones[int(i)] for i in np.array(keep).reshape(-1)]
        return containers, cones, others


CONE_OBSERVER = None
CONE_OBSERVER_ERROR = None
_CONE_OBSERVER_LOCK = threading.Lock()


def get_cone_observer():
    global CONE_OBSERVER, CONE_OBSERVER_ERROR
    with _CONE_OBSERVER_LOCK:
        if CONE_OBSERVER is None and CONE_OBSERVER_ERROR is None:
            try:
                CONE_OBSERVER = ConeObserver(SYS_CFG["event_config"][CONE_NG_EVENT])
            except Exception as e:
                CONE_OBSERVER_ERROR = str(e)
                logger.error(f"[CONE_NG] 모델 초기화 실패 → 콘 미제거 출차 판정을 끕니다: {e}")
    return CONE_OBSERVER


# 콘 카메라 화면에 그리는 side.dxnn 나머지 클래스: 번호 -> (표시 이름, BGR 색)
SIDE_OTHER_DRAW = {2: ("SPREADER", (255, 128, 0)), 3: ("PERSON", (203, 102, 255)), 4: ("YT_HEADER", (255, 255, 0))}

CONE_GROUP_RECORDERS = {}   # 콘 카메라 키 -> (VideoRecorder, cam_id): 묶음 4분할 영상이 카메라별 녹화를 씁니다
CONE_GROUP_VIDEO_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="cone-group-video")


class ConeGroupVideo:
    """묶음 4분할 영상 하나: 묶인 카메라(앞 4대)마다 같은 순간 앞 REC_PRE_SEC·뒤 REC_POST_SEC를 녹화(mp4v 임시)하고,
    모두 끝나면 화면 시각을 맞춰 2x2(초당 10장)로 합쳐 H.264로 남깁니다. 칸 순서는 groups에 적은 순서(왼위·오른위·왼아래·오른아래).
    콘을 본 카메라는 빨간 테두리(CONE), 번호를 읽은 카메라는 초록 테두리(OCR)입니다. 단말에만 저장합니다
    (events/cone_group/<그룹>/videos). 해측 묶음(SeaSideGroupVideo)은 아래 묶음 종류 값만 바꿔 같이 씁니다."""
    KIND = "cone_group"              # 저장 폴더 events/<KIND>/<그룹>/videos, 녹화 이름
    RECORDERS = CONE_GROUP_RECORDERS # 묶음 카메라 키 -> (VideoRecorder, cam_id)
    HIT_TAG = "CONE"                 # 이벤트를 본 카메라 표시(빨간 테두리)
    LOG = "[CONE_GROUP_VIDEO]"
    FPS = 10.0
    MAX_TILE_W = 960

    def __init__(self, group, session, members, cone_cams, ocr_request):
        self.group, self.session, self.members = group, session, list(members)[:4]
        self.cone_cams, self.ocr_request = set(cone_cams), ocr_request
        self.event_ids = []   # 이 세션에 난 묶음 이벤트(save_group_companions가 더함, meta.json에 남김)
        self.time = time.time()
        self.parts = {}
        self.pending = set()
        self.lock = threading.Lock()

    def start(self):
        recorders = {k: self.RECORDERS[k] for k in self.members if k in self.RECORDERS}
        if not recorders:
            logger.warning(f"{self.LOG} group={self.group} 녹화기가 있는 카메라가 없어 4분할 영상을 만들지 않습니다.")
            return
        self.pending = set(recorders)
        logger.info(f"{self.LOG} group={self.group} session={self.session} 4분할 녹화 시작 "
                    f"(카메라 {self.members}, {self.HIT_TAG} {sorted(self.cone_cams)})")
        for key, (recorder, _) in recorders.items():
            recorder.trigger(self.KIND, now=self.time, on_raw_done=lambda state, key=key: self._part_done(key, state))

    def _part_done(self, key, state):
        with self.lock:
            self.parts[key] = state
            self.pending.discard(key)
            if self.pending:
                return
        CONE_GROUP_VIDEO_POOL.submit(self._compose_safe)

    def _compose_safe(self):
        try:
            self._compose()
        except Exception as e:
            logger.error(f"{self.LOG} group={self.group} 합치기 실패: {e}\n{traceback.format_exc()}")
        finally:
            for state in self.parts.values():
                for path in ((state["raw"], state["infer_log_path"]) if state else ()):
                    try:
                        os.remove(path)
                    except OSError:
                        pass

    def _ocr_cam(self):
        """번호를 읽은 카메라(착지 OCR 결과). 아직 읽는 중이면 최대 60초 기다립니다."""
        future = self.ocr_request[1] if self.ocr_request else None
        if future is None:
            return ""
        try:
            result = future.result(timeout=60)
        except Exception:
            return ""
        return str(((result or {}).get("final") or {}).get("ocr_cam") or "")

    def _compose(self):
        ocr_cam = self._ocr_cam()
        sizes = [s["size"] for s in self.parts.values() if s]
        if not sizes:
            logger.warning(f"{self.LOG} group={self.group} 녹화된 화면이 없어 4분할 영상을 만들지 않았습니다.")
            return
        w, h = sizes[0]
        scale = min(1.0, self.MAX_TILE_W / float(w))
        tw, th = int(w * scale) // 2 * 2, int(h * scale) // 2 * 2
        slots = []
        for key in self.members + [None] * (4 - len(self.members)):
            state = self.parts.get(key) if key else None
            cam_id = self.RECORDERS.get(key, (None, "-"))[1] if key else "-"
            tags = [t for t, on in ((self.HIT_TAG, key in self.cone_cams), ("OCR", key == ocr_cam)) if on and key]
            slots.append({"key": key, "label": f"CAM{cam_id} {key} {' '.join(tags)}".strip() if key else "",
                          "tags": tags, "ts": state["ts"] if state else [],
                          "cap": cv2.VideoCapture(state["raw"]) if state else None, "idx": -1, "tile": None})

        dpath = os.path.join(EVENT_ROOT_DIR, "events", self.KIND, safe_id_part(self.group), "videos")
        os.makedirs(dpath, exist_ok=True)
        stem = os.path.join(dpath, f"{datetime.datetime.fromtimestamp(self.time).strftime('%Y%m%d_%H%M%S')}_"
                                   f"{safe_id_part(self.group)}_s{self.session}")
        raw, fpath = stem + ".mp4v.mp4", stem + ".mp4"
        writer = cv2.VideoWriter(raw, cv2.VideoWriter_fourcc(*'mp4v'), self.FPS, (2 * tw, 2 * th))
        t0 = self.time - float(SYS_CFG["REC_PRE_SEC"])
        n_out = int((float(SYS_CFG["REC_PRE_SEC"]) + float(SYS_CFG["REC_POST_SEC"])) * self.FPS)
        blank = np.zeros((th, tw, 3), np.uint8)
        try:
            for n in range(n_out):
                tau = t0 + n / self.FPS
                tiles = []
                for s in slots:
                    # 이 시각까지 녹화된 마지막 화면(없으면 검은 화면)
                    while s["cap"] is not None and s["idx"] + 1 < len(s["ts"]) and s["ts"][s["idx"] + 1] <= tau:
                        ok, frame = s["cap"].read()
                        s["idx"] += 1
                        if ok:
                            s["tile"] = cv2.resize(frame, (tw, th), interpolation=cv2.INTER_AREA)
                    tile = (s["tile"] if s["tile"] is not None else blank).copy()
                    if self.HIT_TAG in s["tags"] or "OCR" in s["tags"]:
                        color = (0, 0, 255) if self.HIT_TAG in s["tags"] else (0, 200, 0)
                        cv2.rectangle(tile, (2, 2), (tw - 3, th - 3), color, 4)
                    if s["label"]:
                        # 칸 이름은 위 가운데: 녹화 화면(GUI 그림)의 왼쪽 위 CAM·FPS, 오른쪽 위 이벤트 목록,
                        # 왼쪽 아래 콘·해측 상태 글자와 겹치지 않게 합니다.
                        lw = cv2.getTextSize(s["label"], cv2.FONT_HERSHEY_SIMPLEX, 0.7, 4)[0][0]
                        org = (max(10, (tw - lw) // 2), 28)
                        cv2.putText(tile, s["label"], org, cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 4, cv2.LINE_AA)
                        cv2.putText(tile, s["label"], org, cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255, 255, 255), 2, cv2.LINE_AA)
                    tiles.append(tile)
                writer.write(np.vstack([np.hstack(tiles[:2]), np.hstack(tiles[2:])]))
        finally:
            writer.release()
            for s in slots:
                if s["cap"] is not None:
                    s["cap"].release()
        codec = VideoRecorder._finish_clip(raw, fpath, self.FPS)
        meta = {"group": self.group, "session": self.session, "time": self.time, "members": self.members,
                "event_ids": list(self.event_ids), f"{self.HIT_TAG.lower()}_cams": sorted(self.cone_cams), "ocr_cam": ocr_cam,
                "container_ocr_dir": self.ocr_request[0] if self.ocr_request else None,
                "video_path": fpath, "video_codec": codec, "fps": self.FPS, "frames": n_out,
                "camera_frames": {k: (len(s["ts"]) if s else 0) for k, s in self.parts.items()}}
        with open(stem + ".meta.json", "w", encoding="utf-8") as f:
            json.dump(to_json_safe(meta), f, indent=4, ensure_ascii=False)
        logger.info(f"{self.LOG} {os.path.basename(fpath)} {n_out / self.FPS:.0f}초 2x2({2 * tw}x{2 * th}, {codec}) "
                    f"{self.HIT_TAG}={sorted(self.cone_cams)} OCR={ocr_cam or '-'} 카메라별 장수={meta['camera_frames']}")


def save_group_companions(group, source_key, event_id):
    """묶음 카메라 한 대에서 이벤트가 나면 묶인 나머지 카메라도 같은 이벤트로 함께 남깁니다(같은 객체인지는 따지지 않음).
    group은 묶음 판정기(ConeNgGroup·SeaSideGroup: KIND, RECORDERS, members, video). 카메라마다 그 순간 녹화 화면
    (VideoRecorder 최근 화면) 1장을 events/<KIND>/<그룹>/companions/<event_id>/에 저장하고, 묶음 4분할 영상 meta에
    event_id를 남깁니다. 반환 {카메라 키: 사진 경로}(이벤트 meta의 group_companions)."""
    if group is None:
        return {}
    out = {}
    for key in group.members:
        if key == source_key or key not in group.RECORDERS:
            continue
        recorder = group.RECORDERS[key][0]
        with recorder.lock:
            item = recorder.buffer[-1] if recorder.buffer else None
        if item is None:
            continue
        path = os.path.join(EVENT_ROOT_DIR, "events", group.KIND, safe_id_part(group.name), "companions",
                            str(event_id), f"{safe_id_part(key)}.jpg")
        IMAGE_SAVER_POOL.submit(_write_image_file, path, item[0], "group_companion", event_id)
        out[key] = path
    if group.video is not None:
        group.video.event_ids.append(event_id)
    logger.info(f"[{group.KIND.upper()}] group={group.name} event_id={event_id} 이벤트 카메라={source_key} "
                f"함께 저장={sorted(out)}")
    return out


class ConeNgGroup:
    """크레인 한 차선의 카메라들이 함께 쓰는 판정기와 NG 때 OCR에 넘길 원본 크롭.

    카메라 워커 스레드마다 update()를 부르므로 잠금 안에서 처리합니다. NG는 콘·출발·콘 이동을 모두 본
    카메라(cone_cams[0])의 다음 프레임에서 그 카메라 이벤트로 냅니다(증거 사진이 그 카메라 화면이 되도록)."""
    KIND = "cone_group"              # 묶음 결과 폴더 events/cone_group/<그룹>/ (save_group_companions)
    RECORDERS = CONE_GROUP_RECORDERS
    _groups = {}
    _registry_lock = threading.Lock()

    @property
    def members(self):
        return (self.cfg.get("groups") or {}).get(self.name) or []

    @classmethod
    def for_camera(cls, camera_key, cfg):
        name = next((g for g, members in (cfg.get("groups") or {}).items() if camera_key in members), camera_key)
        with cls._registry_lock:
            if name not in cls._groups:
                cls._groups[name] = cls(name, cfg)
                logger.info(f"[CONE_NG] 판정 그룹 '{name}' 시작 "
                            f"(카메라 {(cfg.get('groups') or {}).get(name, [camera_key])})")
            return cls._groups[name]

    def __init__(self, name, cfg):
        self.name = name
        self.cfg = cfg
        self.fusion = ConeNgFusion(cfg.get("logic") or {})
        self.lock = threading.Lock()
        self.pending = defaultdict(list)     # 카메라 -> 그 카메라가 낼 이벤트
        self.ocr_frames = defaultdict(list)  # 카메라 -> [{'t', 'area', 'box', 'crop', 'offset'}] (이번 세션)
        self._ocr_session = None
        # 세션마다: 착지 OCR 예약 (결과 폴더, Future), 콘을 본 카메라, 4분할 영상을 만들었는지
        self._session = None
        self.landed_ocr = None
        self.cone_cams = set()
        self.video_started = False
        self.video = None   # 이번 세션 4분할 영상(ConeGroupVideo)

    def update(self, camera_key, obs, image):
        with self.lock:
            events = self.fusion.update(obs)
            if self._session != self.fusion.session:
                self._session = self.fusion.session
                self.landed_ocr, self.cone_cams, self.video_started, self.video = None, set(), False, None
            self._collect_ocr_frame(camera_key, obs.t, image)
            for e in events:
                self.cone_cams.update(e["cone_cams"])
                if e["type"] == "NG":
                    target = e["cone_cams"][0] if e["cone_cams"] else camera_key
                    if self.landed_ocr and self.landed_ocr[1] is not None:
                        e["ocr_request"] = self.landed_ocr   # 착지 때 읽은 결과를 NG 사진에 씁니다
                    elif "NG" in (self.cfg.get("ocr_on") or []):
                        e["ocr_frames"] = [dict(f, cam=k) for k, fs in self.ocr_frames.items() for f in fs]
                    self.pending[target].append(e)
                else:
                    evidence = f" cone_evidence={e['cone_evidence']}" if e.get("cone_evidence") else ""
                    logger.info(f"[CONE_NG] group={self.name} {e['type']} session={e['session']} t={e['t']} "
                                f"cone_cams={e['cone_cams']} move_cams={e['move_cams']}{evidence}")
            self._landed_ocr(obs.t)
            self._group_video(obs.t)
            out = self.pending.pop(camera_key, [])
            return out, self.fusion.state

    def _landed_ocr(self, t):
        """ocr_on "LANDED": 어느 카메라든 컨테이너가 착지(정차)한 지 ocr_landed_delay_sec가 지나면 그때까지 모은 크롭으로
        세션마다 한 번 번호를 읽습니다(NG를 기다리지 않음)."""
        if "LANDED" not in (self.cfg.get("ocr_on") or []) or self.landed_ocr is not None or not self.fusion.active:
            return
        landed = [s.anchor[0] for s in self.fusion.cams.values() if s.landed and s.departed_t is None]
        if not landed or t - min(landed) < float(self.cfg.get("ocr_landed_delay_sec", 5.0)):
            return
        frames = [dict(f, cam=k) for k, fs in self.ocr_frames.items() for f in fs]
        if not frames:
            return
        self.landed_ocr = request_group_container_ocr(self.name, self.fusion.session, frames)
        logger.info(f"[CONE_NG] group={self.name} 착지 OCR 요청 session={self.fusion.session} "
                    f"크롭 {len(frames)}장(카메라 {sorted(self.ocr_frames)})")

    def _group_video(self, t):
        """묶인 카메라가 2대 이상이면 YT가 출발한 순간(세션당 한 번) 앞뒤를 4분할 영상으로 남깁니다(ConeGroupVideo)."""
        if self.video_started or not self.fusion.departed or not self.cfg.get("group_video", True):
            return
        members = (self.cfg.get("groups") or {}).get(self.name) or []
        if len(members) < 2:
            return
        self.video_started = True
        cone_cams = self.cone_cams | set(self.fusion._cone_cams(t, t - 30.0))
        self.video = ConeGroupVideo(self.name, self.fusion.session, members, cone_cams, self.landed_ocr)
        self.video.start()

    def _collect_ocr_frame(self, camera_key, t, image):
        """착지해 있는(출발 전) 컨테이너가 가장 크게 보인 원본 프레임을 카메라마다 N장(min_gap 간격) 남깁니다.
        메모리를 줄이려고 컨테이너 영역만 잘라 둡니다."""
        if self._ocr_session != self.fusion.session:
            self._ocr_session = self.fusion.session
            self.ocr_frames.clear()
        state = self.fusion.cams.get(camera_key)
        if not self.fusion.active or state is None or not state.landed or state.departed_t is not None or state.box is None:
            return
        n_keep = int(self.cfg.get("ocr_frames_per_camera", 5))
        min_gap = float(self.cfg.get("ocr_min_gap_sec", 1.0))
        x1, y1, x2, y2 = state.box
        area = (x2 - x1) * (y2 - y1)
        frames = self.ocr_frames[camera_key]
        near = [f for f in frames if abs(f["t"] - t) < min_gap]
        if near and all(f["area"] >= area for f in near):
            return
        if not near and len(frames) >= n_keep and min(f["area"] for f in frames) >= area:
            return
        h, w = image.shape[:2]
        margin = float(self.cfg.get("ocr_container_margin", 0.05))
        mx, my = margin * (x2 - x1), margin * (y2 - y1)
        X1, Y1 = int(max(0, x1 - mx)), int(max(0, y1 - my))
        X2, Y2 = int(min(w, x2 + mx)), int(min(h, y2 + my))
        if X2 <= X1 or Y2 <= Y1:
            return
        for f in near:
            frames.remove(f)
        frames.append({"t": round(float(t), 3), "area": float(area), "box": [float(v) for v in state.box],
                       "crop": image[Y1:Y2, X1:X2].copy(), "offset": [X1, Y1]})
        if len(frames) > n_keep:
            frames.remove(min(frames, key=lambda f: f["area"]))
        frames.sort(key=lambda f: f["t"])


class ConeNgDetector(BaseEventDetector):
    """콘 미제거 출차(NG): 양하 후 착지한 컨테이너에 콘이 달린 채 YT가 출발하면 NG입니다(판정은 ConeNgFusion).

    run_logic이 넘기는 값: camera_key, source_frame(수신기 원본 프레임, 가로 1920), camera_time(받은 시각, 과속 수신기면 PTS 기준).
    원본 프레임이 없으면 추론 프레임으로 대신하지만 콘·번호가 작아 권장하지 않습니다."""
    gui_name = "CONE_NG"
    TID_BASE = 900000  # 이벤트 tid = TID_BASE + 그룹 세션 번호(차량 추적 ID와 겹치지 않게)

    @classmethod
    def stream_spec(cls):
        """콘은 작은 콘을 찾으려고 원본 가로 cone_ng.source_max_width(기본 1920)로, 초당 cone_ng.source_max_fps(기본 10)장 받습니다.
        720폭에서는 콘이 너무 작아 잡히지 않습니다(최고 점수 0.02, 1920폭 0.77)."""
        cfg = SYS_CFG["event_config"][CONE_NG_EVENT]
        return cls.stream_reader, fps_cap(cfg["source_max_fps"], 10.0), int(cfg["source_max_width"])

    def __init__(self, config, roi_poly=None, roi_lines=None):
        super().__init__(config, roi_poly, roi_lines)
        self.group = None
        self._warned_no_source = False
        self._stats = None
        self._last_idle_t = None
        self.overlay = None  # GUI용 마지막 관측(원본 좌표): 컨테이너·모서리 크롭·콘·판정 단계

    def _log_stats(self, camera_key, t, observe_ms, cones, state):
        """10초마다 판정 횟수(Hz)·관측 시간·모서리 콘 점수를 남깁니다(ARMED 조건: 2초 안 3회, 그중 2회 0.3 이상).
        컨테이너가 없던(전부 IDLE) 10초는 숫자가 모두 0이라 남기지 않습니다."""
        s = self._stats
        if s is None or t < s["t0"]:
            s = self._stats = {"t0": t, "n": 0, "ms": [], "cone": [], "stages": Counter()}
        s["n"] += 1
        s["ms"].append(observe_ms)
        cam = state.cams.get(camera_key) if state is not None else None
        if cam is not None and cam.cone_ts and cam.cone_ts[-1][0] == t:
            s["cone"].append(cam.cone_ts[-1][1])
        s["stages"][cam.stage if cam is not None else "-"] += 1
        if t - s["t0"] >= 10.0:
            span = t - s["t0"]
            if set(s["stages"]) != {"IDLE"}:
                logger.info(f"[CONE_NG] stats cam={camera_key} rate={s['n'] / span:.1f}Hz "
                            f"observe_ms={median(s['ms']):.0f}/{max(s['ms']):.0f} "
                            f"corner_cones={len(s['cone'])} strong={sum(c >= 0.3 for c in s['cone'])} "
                            f"best={max(s['cone'], default=0):.2f} stages={dict(s['stages'])} raw_cones={len(cones)}")
            self._stats = None

    def _make_overlay(self, observer, camera_key, wh, containers, cones, others, with_cones=True, t=None):
        """화면 표시용 관측 요약. 모서리 크롭은 observe()와 같은 기준(판정 대상 컨테이너, max_containers)으로 다시 구합니다."""
        w, h = wh
        fusion = self.group.fusion
        cam = fusion.cams.get(camera_key)
        targets = [i for i, c in enumerate(containers) if observer._target(c, w, h)] if with_cones else []
        max_containers = self.config.get("max_containers")
        rois = [_cone_corner_roi(containers[i]["xyxy"], side, w, h)["xyxy"]
                for i in (targets[:max_containers] if max_containers else targets) for side in ("left", "right")]
        picked = cam.box if cam is not None else None
        return {
            "wh": wh, "containers": containers, "others": others, "rois": rois, "picked": picked,
            "cones": [dict(k, corner=picked is not None and cam._at_corner(picked, k["xyxy"])) for k in cones],
            "strong": fusion.cfg["cone_strong_conf"],
            "state": fusion.state, "stage": cam.stage if cam is not None else "-",
            "session": fusion.session, "t": t,
        }

    def process(self, tracks, track_map, frame, fid, **kwargs):
        if frame is None:
            return []
        observer = get_cone_observer()
        if observer is None:
            return []
        camera_key = kwargs.get("camera_key")
        if self.group is None:
            self.group = ConeNgGroup.for_camera(camera_key, self.config)
        image = kwargs.get("source_frame")
        if image is None:
            if not self._warned_no_source:
                logger.warning(f"[CONE_NG] {camera_key}: 원본 프레임이 없어 추론 프레임으로 판정합니다(정확도 낮음).")
                self._warned_no_source = True
            image = frame
        ih, iw = image.shape[:2]
        fh, fw = frame.shape[:2]
        sx, sy = fw / float(iw), fh / float(ih)

        # 차선 ROI(추론 프레임 좌표) 밖에 바닥 중심이 있는 컨테이너는 배경으로 보고 뺍니다.
        roi = self.roi_poly
        keep = ((lambda b: cv2.pointPolygonTest(roi, ((b[0] + b[2]) / 2 * sx, b[3] * sy), False) >= 0)
                if roi.size else None)
        t = kwargs.get("camera_time")
        t = float(t) if t is not None else time.monotonic()
        cam_state = self.group.fusion.cams.get(camera_key)
        stage = cam_state.stage if cam_state is not None else "IDLE"
        if stage == "IDLE":
            # 컨테이너가 없는 동안은 idle_observe_hz로만 봅니다(카메라 여러 대가 NPU·CPU를 나눠 쓰도록).
            idle_hz = float(self.config.get("idle_observe_hz", 3.0))
            if idle_hz > 0 and self._last_idle_t is not None and 0 <= t - self._last_idle_t < 1.0 / idle_hz:
                return []
            self._last_idle_t = t
        # 콘은 착지 후(ARMED, 출발 전 콘)와 출발 중(콘이 따라가는지)에만 판정에 씁니다. 그 외 단계는 모서리 크롭을 건너뜁니다.
        with_cones = stage in ("LANDED", "DEPARTING")
        t_obs = time.perf_counter()
        containers, cones, others = observer.observe(image, keep_container=keep, with_cones=with_cones)
        observe_ms = (time.perf_counter() - t_obs) * 1000
        events, _ = self.group.update(camera_key, ConeCameraObs(camera_key, t, (iw, ih), containers, cones), image)
        self._log_stats(camera_key, t, observe_ms, cones, self.group.fusion)
        self.overlay = self._make_overlay(observer, camera_key, (iw, ih), containers, cones, others, with_cones,
                                          t=round(t, 3))

        triggered = []
        for e in events:
            box = e["boxes"].get(camera_key) or (containers[0]["xyxy"] if containers else [0, 0, iw, ih])
            bbox = [box[0] * sx, box[1] * sy, box[2] * sx, box[3] * sy]
            tid = self.TID_BASE + int(e["session"])
            privacy = self._event_people(observer, image, sx, sy) or kwargs.get("privacy_tracks", [])
            triggered.append({
                "tid": tid,
                "bbox": bbox,
                "frame": frame.copy(),
                "fid": fid,
                "privacy_tracks": privacy,
                "privacy_fid": fid,
                "objects": [{"label": "cone_ng", "box": [int(v) for v in bbox], "score": 1.0, "tid": tid}],
                "decision_trace": {
                    "detector": "ConeNgDetector", "reason": "cone_on_departing_container", "group": self.group.name,
                    "session": e["session"], "t": e["t"], "move_start": e["move_start"],
                    "cone_cams": e["cone_cams"], "move_cams": e["move_cams"],
                    "cone_evidence": e.get("cone_evidence"),
                    "boxes_source": {k: [round(float(v), 1) for v in b] for k, b in e["boxes"].items()},
                    "ocr_frames": len(e.get("ocr_frames") or []),
                },
                "container_ocr_frames": e.get("ocr_frames") or [],
                "container_ocr_request": e.get("ocr_request"),
            })
        return triggered

    @staticmethod
    def _event_people(observer, image, sx, sy):
        """cone_ng만 켠 카메라는 매 프레임 사람을 찾지 않아 사람 트랙이 없고, 얼굴 블러는 사람 박스 안의 얼굴만 가립니다.
        그래서 NG 사진 한 장에 대해서만 원본 프레임(image)에서 side.dxnn 사람(ConeObserver.people)을 찾아
        추론 프레임 좌표(sx, sy배)의 privacy_tracks 모양([x1,y1,x2,y2,tid,score,cls])으로 돌려줍니다."""
        try:
            people = observer.people(image)
        except Exception as e:
            logger.warning(f"[CONE_NG] 블러용 사람 검출 실패: {e}")
            return []
        return [[p[0] * sx, p[1] * sy, p[2] * sx, p[3] * sy, *p[4:]] for p in people]


# ---------- 컨테이너 번호 OCR: 컨테이너 크롭 → 번호 상자 검출 → PP-OCR 48x320 CTC → 형식 검사 → 투표 ----------
class ContainerOcrReader:
    """번호·크기형식 코드 영역 검출(container_ocr_detector.dxnn 0 BIC 컨테이너 번호, 1 TYPE 크기·형식 코드)과
    인식(PP-OCR SVTR_LCNet, CTC). 글자 인식은 CPU(ONNX)가 기본이고, ONNX 파일이 없으면 NPU(dxnn)로 읽습니다.

    규칙:
    1) 상자 크롭 = 학습 크롭과 같은 AABB 내림/올림 + 2px (여백 10%는 세로 표기를 크게 떨어뜨림)
    2) 크롭 높이 >= 1.5 x 폭이면 세로 표기로 보고 90도 반시계 회전
    3) 높이 48 비율 유지, 폭 <= 320, 오른쪽 128 채운 uint8 BGR 48x320. NPU는 그대로, CPU(ONNX)는 /255, (x-0.5)/0.5, CHW
    4) CTC 빔 상위 3개 중 형식 검사를 처음 통과한 것(통과 없으면 인식 실패)"""
    OCR_H, OCR_W = 48, 320
    NUMBER_CLASS, SIZE_TYPE_CLASS = 0, 1  # container_ocr_detector.dxnn: 0 BIC(컨테이너 번호), 1 TYPE(크기·형식 코드)

    def __init__(self, cfg):
        self.cfg = cfg
        # 번호 영역 모델·출력 형식·엔진 수·점수 기준은 models·model_output_formats·model_engine_pool_sizes·
        # model_confidences의 CONTAINER_OCR_DET
        self.det = YoLoDeepX(resolve_model_path(SYS_CFG["models"]["CONTAINER_OCR_DET"]),
                             output_format=get_model_output_format("CONTAINER_OCR_DET"),
                             pool_size=get_model_engine_pool_size("CONTAINER_OCR_DET"))
        apply_class_confidences(self.det, "CONTAINER_OCR_DET")
        count = self.det.raw_yolo_num_classes()
        if count is not None and count != len(CONTAINER_OCR_DET_CLASS_NAMES):
            raise RuntimeError(f"번호 영역 모델 {os.path.basename(str(self.det.engine_path))}(클래스 {count}개)이 "
                               f"BIC·TYPE 2클래스 모델이 아닙니다. models.CONTAINER_OCR_DET을 "
                               f"container_ocr_detector.dxnn으로 바꾸세요.")
        with open(resolve_model_path(cfg["ocr_dict"]), encoding="utf-8") as f:
            self.chars = ["blank"] + [line.rstrip("\n") for line in f]
        self.ocr_npu = self.ocr_cpu = None
        npu_path = resolve_model_path(cfg.get("ocr_npu_model"))
        cpu_path = resolve_model_path(cfg.get("ocr_cpu_model"))
        if cpu_path and os.path.exists(cpu_path):
            import onnxruntime as ort
            options = ort.SessionOptions()
            options.intra_op_num_threads = max(1, int(cfg.get("ocr_cpu_threads", 2)))
            options.inter_op_num_threads = 1
            options.add_session_config_entry("session.set_denormal_as_zero", "1")
            self.ocr_cpu = ort.InferenceSession(cpu_path, options, providers=["CPUExecutionProvider"])
            self._cpu_input = self.ocr_cpu.get_inputs()[0].name
        elif npu_path and os.path.exists(npu_path):
            self.ocr_npu = InferenceEngine(npu_path)
        else:
            raise RuntimeError("컨테이너 OCR 모델(cpu/npu)을 찾지 못했습니다.")
        self.ocr_name = ("cpu " + os.path.basename(cpu_path)) if self.ocr_cpu else ("npu " + os.path.basename(npu_path))
        logger.info(f"[CONTAINER_OCR] 모델 로드: det={os.path.basename(str(self.det.engine_path))} ocr={self.ocr_name}")

    def ocr_input(self, piece):
        if piece.shape[0] >= 1.5 * piece.shape[1]:
            piece = cv2.rotate(piece, cv2.ROTATE_90_COUNTERCLOCKWISE)
        h, w = piece.shape[:2]
        resized_w = min(self.OCR_W, int(math.ceil(self.OCR_H * w / float(h))))
        canvas = np.full((self.OCR_H, self.OCR_W, 3), 128, np.uint8)
        canvas[:, :resized_w] = cv2.resize(piece, (resized_w, self.OCR_H))
        return canvas

    def recognize(self, piece):
        x = np.ascontiguousarray(self.ocr_input(piece))
        if self.ocr_npu is not None:
            prob = self.ocr_npu.run([x])[0]
        else:
            xf = ((x.astype(np.float32) / 255.0 - 0.5) / 0.5).transpose(2, 0, 1)[None]
            prob = self.ocr_cpu.run(None, {self._cpu_input: xf})[0]
        return self._beam_topk(np.asarray(prob, np.float64).reshape(-1, len(self.chars)))

    def _beam_topk(self, prob, k=3, beam=10, topc=5, min_p=1e-4):
        """CTC prefix beam search 상위 k개 [(문자열, 글자당 기하평균 확률)]."""
        beams = {"": (1.0, 0.0)}  # 접두어 -> (공백으로 끝난 확률, 글자로 끝난 확률)
        for row in prob:
            cand = [int(c) for c in np.argsort(row)[::-1][:topc] if c != 0 and row[c] >= min_p]
            nxt = {}

            def add(key, pb, pn):
                ob, on = nxt.get(key, (0.0, 0.0))
                nxt[key] = (ob + pb, on + pn)
            for prefix, (pb, pn) in beams.items():
                add(prefix, (pb + pn) * row[0], 0.0)
                last = prefix[-1] if prefix else None
                for c in cand:
                    ch, p = self.chars[c], row[c]
                    if ch == last:
                        add(prefix, 0.0, pn * p)
                        add(prefix + ch, 0.0, pb * p)
                    else:
                        add(prefix + ch, 0.0, (pb + pn) * p)
            beams = dict(sorted(nxt.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:beam])
        out = sorted(((t, v[0] + v[1]) for t, v in beams.items() if t), key=lambda kv: -kv[1])[:k]
        return [(t, float(p ** (1.0 / max(1, len(t))))) for t, p in out]

    def read_image(self, image):
        """컨테이너 크롭 한 장 → {'numbers': [...], 'size_types': [...], 'boxes': [...]} (형식 통과한 것만)."""
        h, w = image.shape[:2]
        dets = self.det.infer(image)  # 점수 기준 model_confidences.CONTAINER_OCR_DET의 BIC·TYPE
        boxes = []
        for d in dets:
            x1, y1, x2, y2 = (float(v) for v in d[:4])
            cx1, cy1 = max(0, math.floor(x1) - 2), max(0, math.floor(y1) - 2)
            cx2, cy2 = min(w, math.ceil(x2) + 2), min(h, math.ceil(y2) + 2)
            if cx2 - cx1 < 4 or cy2 - cy1 < 4:
                continue
            boxes.append({"cls": int(d[5]), "box": [x1, y1, x2, y2], "conf": round(float(d[4]), 4),
                          "cands": self.recognize(image[cy1:cy2, cx1:cx2])})
        numbers, number_conf, used = [], {}, set()
        bic = sorted([i for i, b in enumerate(boxes) if b["cls"] == self.NUMBER_CLASS], key=lambda i: -boxes[i]["conf"])
        # 두 줄 표기: 소유자 코드 4자로 읽힌 상자 바로 아래의 7자 상자를 이어 붙인 뒤 체크디지트
        for i in bic:
            u = boxes[i]
            if i in used or not u["cands"] or not container_owner_ok(u["cands"][0][0]):
                continue
            below = [j for j in bic if j != i and j not in used and self._is_below(u["box"], boxes[j]["box"])]
            for j in sorted(below, key=lambda j: boxes[j]["box"][1] - u["box"][3]):
                combos = sorted(((a + b, pa * pb) for a, pa in u["cands"] for b, pb in boxes[j]["cands"]
                                 if container_owner_ok(a) and container_serial_ok(b)), key=lambda c: -c[1])[:3]
                hit = next((c for c in combos if container_number_ok(c[0])), None)
                if hit:
                    numbers.append(hit[0])
                    number_conf[hit[0]] = max(number_conf.get(hit[0], 0.0), hit[1] ** 0.5)
                    used.update((i, j))
                    break
        for i in bic:
            if i not in used:
                hit = next(((t, p) for t, p in boxes[i]["cands"] if container_number_ok(t)), None)
                if hit:
                    numbers.append(hit[0])
                    number_conf[hit[0]] = max(number_conf.get(hit[0], 0.0), hit[1])
        size_types = [hit for hit in (next((t for t, _ in b["cands"] if container_size_type_ok(t)), None)
                                      for b in boxes if b["cls"] == self.SIZE_TYPE_CLASS) if hit]
        return {"numbers": numbers, "number_conf": {k: round(v, 4) for k, v in number_conf.items()},
                "size_types": size_types,
                "boxes": [{"cls": b["cls"], "box": [round(v, 1) for v in b["box"]], "conf": b["conf"],
                           "cands": [[t, round(p, 4)] for t, p in b["cands"]]} for b in boxes]}

    @staticmethod
    def _is_below(u, l):
        """l 상자가 u 상자 바로 아래(두 줄 표기 짝): 윗변이 u 높이 절반보다 아래, 간격 1.5배 높이 이내, 가로로 겹침."""
        uh = u[3] - u[1]
        return l[1] >= u[1] + 0.5 * uh and (l[1] - u[3]) <= 1.5 * uh and min(u[2], l[2]) - max(u[0], l[0]) > 0


def finalize_container_number(frame_reads, min_votes=3):
    """프레임마다 형식 통과한 번호를 모아 투표합니다.
    1위가 min_votes표 이상이고 2위의 2배 이상이면 '확정', 표가 있으면 '불확실', 없으면 '인식 실패'.
    표가 같으면 인식 확률(글자당 기하평균) 합이 큰 번호를 앞에 둡니다(불확실이어도 대표 번호가 그럴듯하도록)."""
    counts, conf = Counter(), Counter()
    for r in frame_reads:
        for n in set(r["numbers"]):
            counts[n] += 1
            conf[n] += float((r.get("number_conf") or {}).get(n, 0.0))
    votes = sorted(counts.items(), key=lambda kv: (-kv[1], -conf[kv[0]]))
    if votes and votes[0][1] >= min_votes and (len(votes) == 1 or votes[0][1] >= 2 * votes[1][1]):
        status = "확정"
    elif votes:
        status = "불확실"
    else:
        status = "인식 실패"
    codes = Counter(c for r in frame_reads for c in set(r["size_types"])).most_common()
    code = codes[0][0] if codes else ""
    return {"number": votes[0][0] if votes else "", "status": status, "votes": votes[:5],
            "size_type": code, "size_type_extension": bool(code) and container_size_type_is_extension(code),
            "size_type_votes": codes[:3]}


CONTAINER_OCR_READER = None
CONTAINER_OCR_ERROR = None
CONTAINER_OCR_POOL = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="container-ocr")


def get_container_ocr_reader():
    global CONTAINER_OCR_READER, CONTAINER_OCR_ERROR
    if CONTAINER_OCR_READER is None and CONTAINER_OCR_ERROR is None:
        try:
            CONTAINER_OCR_READER = ContainerOcrReader(SYS_CFG["container_ocr"])
        except Exception as e:
            CONTAINER_OCR_ERROR = str(e)
            logger.error(f"[CONTAINER_OCR] 초기화 실패 → 컨테이너 번호 인식을 끕니다: {e}")
    return CONTAINER_OCR_READER


def request_container_ocr(cam, event_id, event_name, frames):
    """NG 이벤트의 컨테이너 크롭들로 번호 인식을 예약합니다(카메라 스레드를 막지 않음).
    반환 (결과 폴더, Future). Future는 끝나면 run_container_ocr 결과(실패하면 None)이고, 관제 사진 전송이 기다립니다.
    OCR이 꺼져 있거나 크롭이 없으면 (None, None)."""
    if not SYS_CFG["container_ocr"]["enabled"] or not frames:
        return None, None
    out_dir = os.path.join(EVENT_ROOT_DIR, "events", cam.ip, "container_ocr", str(event_name), str(event_id))
    meta = {"event_id": event_id, "event_name": event_name, "cam_id": cam.cam_id, "ip": cam.ip}
    return out_dir, CONTAINER_OCR_POOL.submit(_run_container_ocr_safe, out_dir, meta, frames)


def request_group_container_ocr(group_name, session, frames):
    """착지(정차) OCR(ocr_on "LANDED"): 이벤트 없이 묶음 세션마다 한 번 읽습니다(관제로 보내지 않음).
    결과는 events/cone_group/<그룹>/container_ocr/LANDED/<세션 id>/container_ocr.json. 반환은 request_container_ocr와 같습니다."""
    if not SYS_CFG["container_ocr"]["enabled"] or not frames:
        return None, None
    sid = f"{now_kst().strftime('%Y%m%dT%H%M%S')}_{safe_id_part(group_name)}_s{session}"
    out_dir = os.path.join(EVENT_ROOT_DIR, "events", "cone_group", safe_id_part(group_name), "container_ocr", "LANDED", sid)
    meta = {"event_id": sid, "event_name": "LANDED", "cam_id": "-", "ip": str(group_name), "session": session}
    return out_dir, CONTAINER_OCR_POOL.submit(_run_container_ocr_safe, out_dir, meta, frames)


def _run_container_ocr_safe(out_dir, meta, frames):
    try:
        return run_container_ocr(out_dir, meta, frames)
    except Exception as e:
        logger.error(f"[CONTAINER_OCR] 처리 실패 event_id={meta.get('event_id')} | {e}\n{traceback.format_exc()}")
        return None


def container_ocr_api_fields(ocr_future, wait_sec):
    """콘 NG 사진의 detectedObjects(JSON)에 더할 컨테이너 번호. API는 바꾸지 않고 이 JSON에만 넣습니다(관제 요청).
    containerNo: 번호(못 읽으면 ""), containerSizeType: 크기·형식 코드(예: 45G1),
    ocrStatus: 확정 / 불확실 / 인식 실패(finalize_container_number), 시간초과(wait_sec 안에 못 읽음), 처리 실패."""
    try:
        result = ocr_future.result(timeout=max(0.0, float(wait_sec)))
    except concurrent.futures.TimeoutError:
        return {"containerNo": "", "containerSizeType": "", "ocrStatus": "시간초과"}
    except Exception:
        result = None
    final = (result or {}).get("final")
    if not final:
        return {"containerNo": "", "containerSizeType": "", "ocrStatus": "처리 실패"}
    return {"containerNo": str(final.get("number") or ""), "containerSizeType": str(final.get("size_type") or ""),
            "ocrStatus": str(final.get("status") or "")}


def run_container_ocr(out_dir, meta, frames):
    cfg = SYS_CFG["container_ocr"]
    reader = get_container_ocr_reader()
    if reader is None:
        return None
    started = time.monotonic()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for i, f in enumerate(frames):
        name = f"crop_{i:02d}_{f['cam']}.jpg".replace(":", "_")
        cv2.imwrite(str(out_dir / name), f["crop"], [cv2.IMWRITE_JPEG_QUALITY, 95])
        r = reader.read_image(f["crop"])
        r.update({"file": name, "cam": f["cam"], "t": f["t"], "container_box": f["box"], "crop_offset": f["offset"]})
        rows.append(r)
    final = finalize_container_number(rows, int(cfg["min_votes"]))
    # 번호를 읽은 카메라(증거): 대표 번호를 읽은 크롭이 가장 많은 카메라와 그중 확률이 가장 높은 크롭
    hits = [r for r in rows if final["number"] and final["number"] in r["numbers"]]
    by_cam = Counter(r["cam"] for r in hits).most_common()
    final["ocr_cam"] = by_cam[0][0] if by_cam else ""
    best = max((r for r in hits if r["cam"] == final["ocr_cam"]),
               key=lambda r: float((r.get("number_conf") or {}).get(final["number"], 0.0)), default=None)
    final["ocr_file"] = best["file"] if best else ""
    result = dict(meta, created_at=now_kst().isoformat(), final=final, frames=rows,
                  models={"det_model": os.path.basename(reader.det.engine_path), "ocr_model": reader.ocr_name},
                  elapsed_sec=round(time.monotonic() - started, 3))
    tmp = out_dir / ".container_ocr.tmp"
    tmp.write_text(json.dumps(to_json_safe(result), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(out_dir / "container_ocr.json")

    # 이벤트 단위 누적표 (엑셀에서 바로 열도록 UTF-8 BOM)
    csv_path = Path(LOG_DIR) / str(cfg["result_csv"])
    new_file = not csv_path.exists()
    with open(csv_path, "a", encoding="utf-8-sig" if new_file else "utf-8", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["created_at", "event_id", "event_name", "cam_id", "ip", "number", "status", "votes",
                             "size_type", "size_type_extension", "frames", "elapsed_sec", "dir"])
        writer.writerow([result["created_at"], meta["event_id"], meta["event_name"], meta["cam_id"], meta["ip"],
                         final["number"], final["status"], " ".join(f"{n}:{c}" for n, c in final["votes"]),
                         final["size_type"], final["size_type_extension"], len(rows), result["elapsed_sec"], str(out_dir)])
    logger.info(f"[CONTAINER_OCR] event_id={meta['event_id']} 번호='{final['number']}'({final['status']}, "
                f"표 {final['votes'][:3]}) 크기·형식='{final['size_type']}' 읽은 카메라={final['ocr_cam'] or '-'} frames={len(rows)} "
                f"elapsed={result['elapsed_sec']}s dir={out_dir}")
    return result


def topdown_privacy_tracks(t_topdown):
    """Backreach·YT topdown 트랙을 개인정보 블러용 메인 모델 번호(사람·차량)로 바꿉니다(트랙이 없으면 None)."""
    rows = []
    for t in (t_topdown if t_topdown is not None else []):
        class_id = TOPDOWN_PRIVACY_CLASS.get(int(t[6]))
        if class_id is not None:
            rows.append([float(t[0]), float(t[1]), float(t[2]), float(t[3]), int(t[4]), float(t[5]), class_id])
    return np.asarray(rows, dtype=float).reshape(-1, 7)


def event_model_keys(events):
    """설정된 이벤트가 쓰는 메인·Backreach 모델 키입니다(NPU에 올릴 모델).
    과속: 메인 V2(사람·하반신·승용차·트럭). Backreach·YT: topdown(같은 엔진을 같이 씀).
    콘 NG의 side·cone 모델은 ConeObserver가 올립니다(NG 사진 블러용 사람도 side). 매 프레임 돌리는 것은
    main의 run_camera_inference입니다."""
    keys = []
    if SPEEDING_EVENT in events:
        keys.append("MAIN_V2")
    if any(name in events for name in PERSON_TOPDOWN_EVENTS):
        keys.append(TOPDOWN_MODEL)
    return keys


def format_ai_fps_log(cam):
    """카메라 AI FPS 로그 한 줄. 메인 루프가 CURRENT_FPS_LOG_INTERVAL_SEC(기본 60초)마다 카메라별로 남깁니다.
    ai_fps는 초당 추론·이벤트 판정 횟수(Camera.current_fps, 화면의 AI FPS와 같음)입니다."""
    receive_max = cam.reader.max_fps
    target = SYS_CFG["system_performance"]["target_fps"]
    line = (f"[AI FPS] CAM:{cam.cam_id}({cam.ip}) events={','.join(cam.events) or '-'} "
            f"ai_fps={cam.current_fps:.1f} target={target:g}"
            + (f" receive_max={receive_max:g}" if receive_max else ""))
    if any(name in cam.events for name in PERSON_TOPDOWN_EVENTS):
        # topdown 실제 추론 횟수입니다. 감시 대기 중(Backreach는 감시 OFF, YT는 양하 감시 전)에는 덜 돌리므로 따로 적습니다.
        line += f" infer_fps={cam.backreach_infer_fps():.1f}(topdown)"
    if BACKREACH_EVENT in cam.events:
        # Backreach 감시 ON·OFF(스프레더로 정함)
        line += f" backreach={'ON' if cam._read_backreach_state()[0] == 1 else 'OFF'}"
    return line


class BackreachSpreaderGate:
    """Backreach 감시 ON·OFF(카메라마다). PLC 신호를 받을 수 없어(2026-10-30) 화면의 스프레더로 정합니다.

    topdown이 스프레더를 spreader_on_sec(3초) 넘게 보면 ON, spreader_miss_frames(3) 추론 프레임보다 오래 못 보면 OFF입니다.
    그 이하로 끊긴 것은 검출 깜빡임으로 보고 ON 상태와 ON까지의 3초 세기를 이어 갑니다.
    read_state()는 PLC 때와 같은 (1·0, 변경 번호)입니다. 변경 번호는 ON·OFF가 바뀔 때마다 늘어, 추론·블러 중에
    구간이 바뀐 이벤트를 버리는 데 씁니다."""

    def __init__(self, config, name=""):
        self.on_sec = float(config.get("spreader_on_sec", 3.0))
        self.miss_frames = int(config.get("spreader_miss_frames", 3))
        self.spreader_ids = {TOPDOWN_CLASS_NAMES.index(n) for n in config.get("spreader_classes", ["SPREADER"])
                             if n in TOPDOWN_CLASS_NAMES}
        self.name = name
        self.on = False
        self.generation = 0
        self.seen_since = None   # 스프레더가 (끊김 허용 안에서) 계속 보이기 시작한 시각
        self.missed = 0          # 스프레더를 못 본 연속 추론 프레임 수

    def update(self, detections, now=None):
        """새로 추론한 topdown 검출(행: x1, y1, x2, y2, 점수, 클래스)로 상태를 갱신합니다.
        직전 검출을 다시 쓰는 프레임에는 부르지 않습니다(프레임 = topdown 추론 한 번)."""
        now = time.monotonic() if now is None else now
        seen = detections is not None and any(int(d[5]) in self.spreader_ids for d in detections)
        if seen:
            self.missed = 0
            if self.seen_since is None:
                self.seen_since = now
            if not self.on and now - self.seen_since >= self.on_sec:
                self._set(True, f"스프레더 {now - self.seen_since:.1f}초")
        else:
            self.missed += 1
            if self.missed > self.miss_frames:
                self.seen_since = None
                if self.on:
                    self._set(False, f"스프레더 {self.missed}프레임 안 보임")

    def _set(self, on, reason):
        self.on = on
        self.generation += 1
        logger.info(f"[BACKREACH] {self.name} {reason} → 감시 {'ON' if on else 'OFF'}")

    def read_state(self):
        return (1 if self.on else 0), self.generation


class BackreachDetector(BaseEventDetector):
    """Backreach 감시. 감시 ON(스프레더, BackreachSpreaderGate)이면 topdown의 모든 클래스(사람·차량 포함) 중
    bbox가 다각형 ROI와 조금이라도 겹치거나 경계에 닿는 객체를 이벤트로 냅니다.
    중심점·하단점 기준이 아니고, 오목한 ROI도 그대로 판정합니다. 빈 ROI에서는 이벤트가 없습니다.
    쿨다운(기본 600초)과 같은 객체 중복 제한은 Camera 공통 처리가 다른 이벤트와 똑같이 적용합니다."""
    gui_name = "BACKREACH"
    # topdown(1280 입력)은 1920 원본을 1280x720으로 줄여 넣는 기준으로 만들어졌습니다.
    # 가로 720으로 받으면 GPU가 줄인 그림을 CPU가 모델에 넣을 때 다시 1280으로 키워 작은 물체를 놓칠 수 있어,
    # 가로 1280으로 받습니다(GPU가 1920 → 1280으로 바로 줄이고, CPU는 위아래 빈칸만 채움).
    stream_max_width = 1280

    def process(self, tracks, track_map, frame, fid, **kwargs):
        if kwargs.get('backreach_on') != 1:
            return []
        contour = polygon_contour(self.roi_poly)
        if contour is None:
            return []
        triggered = []
        frame_copy = frame.copy() if frame is not None else None
        privacy_tracks = kwargs.get('privacy_tracks', [])
        tracks = kwargs.get('topdown_tracks')
        for t in (tracks if tracks is not None else []):
            box = [float(v) for v in t[:4]]
            if not box_overlaps_polygon(box, contour):
                continue
            raw_tid, score, class_id = int(t[4]), float(t[5]), int(t[6])
            tid = TOPDOWN_TID_BASE + raw_tid
            label = TOPDOWN_CLASS_NAMES[class_id] if 0 <= class_id < len(TOPDOWN_CLASS_NAMES) else f"class_{class_id}"
            triggered.append({
                'tid': tid,
                'bbox': box,
                'frame': frame_copy,
                'fid': fid,
                'privacy_tracks': privacy_tracks,
                'privacy_fid': fid,
                # raw_tid: 이벤트 궤적을 topdown 추적기(Camera.trk_topdown)에서 찾는 번호
                'objects': [{'label': label, 'box': [int(v) for v in box], 'score': score, 'tid': tid,
                             'class_id': class_id, 'raw_tid': raw_tid}],
                'decision_trace': {
                    'detector': 'BackreachDetector',
                    'reason': 'spreader_on_and_bbox_overlaps_roi',
                    'backreach_on': 1,
                    'signal_generation': kwargs.get('signal_generation'),
                    'model': TOPDOWN_MODEL,
                    'raw_tid': raw_tid,
                    'class_id': class_id,
                    'score': score,
                    'roi_match': 'bbox_overlap',
                },
            })
        return triggered


# ============================================================================
# [YT 헤드압착 모듈 시작] 항만 AI 이벤트 02 - YT 헤드압착 위험 감지
# ----------------------------------------------------------------------------
# 판단 순서 (협의안 v2 슬라이드 4·5):
#   YT·샤시 인식 → YT 정지 + 빈 샤시 10초 → 양하 감시 시작 →
#   스프레더 결착 컨테이너 인식·규격(20ft/40ft) → 적재영역(빨강) 설정 →
#   하락 예측 지점이 적재영역을 벗어난 상태가 5초 이상 → yt_head_crush (4번, 관제 11)
#   감시 중 빈 샤시로 만든 위험구역(노랑)에 작업자 → yt_zone_person (5번, 관제 10)
# ROI: 4번은 작업 차선(초록, 카메라 roi_poly_norm, 선택 = 그 안의 YT만 감시).
#      5번 위험구역(노랑)은 그리지 않습니다. 감시가 시작되면 빈 샤시 박스로 자동으로 만듭니다(yt_danger_zone).
# 통합본 연결:
#   - 모델·추적: Backreach와 같은 topdown 엔진과 추적기(Camera.trk_topdown)를 같이 씁니다(NPU 메모리 추가 없음).
#     4번은 YT·컨테이너·스프레더, 5번은 작업자(PERSON)를 같은 topdown 검출에서 봅니다. 사진 얼굴·번호판 블러도
#     topdown PERSON·VEHICLE·YT 상자로 합니다.
#   - 감시 상태: 카메라마다 YTDischargeMonitor 하나(Camera.yt_monitor)를 프레임마다 한 번 갱신하고,
#     두 이벤트는 그 결과(yt_snapshot)를 받아 판단만 합니다.
#   - PLC 양하 신호는 없다고 보고 영상으로만 감시를 시작합니다.
# ============================================================================
# 클래스 이름은 Backreach와 같은 topdown 표(TOPDOWN_CLASS_NAMES)입니다.
YT_DEFAULT_CONFIG = {
    # 양하 감시 시작: YT가 멈춘 상태에서 빈 샤시가 이 시간 동안 보이면 감시합니다.
    "arm_stop_sec": 10.0,
    # 샤시 길이 대비 이 비율보다 많이 움직이면 정지 시간을 처음부터 다시 잽니다.
    "stop_move_ratio": 0.05,
    # 감시 중에는 컨테이너가 헤드를 일부 가려 박스가 흔들리므로, 이 비율 넘게 움직여야 출발로 봅니다.
    "depart_move_ratio": 0.25,
    "head_classes": ["YT_HEADER"],
    "empty_chassis_classes": ["YT_CHASSIS_EMPTY", "YT_CHASSIS_HALF"],
    # 반 샤시(20ft 하나가 실림)는 상자가 비어 있는 절반(20ft 길이)만 감쌉니다. 그래서 샤시 전체 길이(40ft)를
    # 상자 길이의 2배로 보고 바닥면 길이·여유·움직임 기준을 정하고, 헤드와 묶을 때 실린 20ft만큼 떨어져 있어도 묶습니다.
    "half_chassis_classes": ["YT_CHASSIS_HALF"],
    "container_classes": {"CONTAINER_40FT": "40ft", "CONTAINER_20FT": "20ft", "TANK_20FT": "20ft"},
    # 스프레더와 겹친 컨테이너만 하역 중으로 봅니다.
    "spreader_classes": ["SPREADER"],
    # 사람 진입(5번)이 보는 topdown 클래스
    "person_classes": ["PERSON"],
    # 적재영역(빨강) = 샤시 박스를 차선 방향으로 샤시 길이 × 이 비율만큼 넓힌 영역입니다.
    # 정상 하역도 컨테이너가 샤시보다 2~4% 넘쳐서 0.05면 오알람이 나고, 0.15는 헤드 위험 영상만 알람합니다.
    "load_margin_ratio": 0.15,
    # 차선 폭 방향으로 컨테이너 중심이 샤시 중심에서 샤시 폭 × 이 비율 넘게 어긋나면 이탈입니다.
    "cross_offset_ratio": 0.5,
    # 하락 예측 지점이 적재영역을 벗어난 상태가 이 시간 이상 이어지면 알람합니다(협의안 5초).
    "deviation_hold_sec": 5.0,
    # 검출이 이 시간 이하로 끊기면 상태를 유지합니다.
    "lost_grace_sec": 1.0,
    # YT가 이 시간 동안 보이지 않으면 감시를 끝냅니다. 감시 전에는 헤드, 감시 중에는 그 자리의 헤드·빈 샤시·컨테이너 중
    # 하나라도 보이면 YT가 있는 것으로 봅니다(감시 중 헤드만 안 보이는 건 괜찮음, _YTUnit.keep_alive).
    "release_sec": 3.0,
    # 하역 중 컨테이너를 찾는 범위 = YT 박스를 샤시 폭 × 이 비율만큼 사방으로 넓힌 영역입니다.
    # 사람 진입(5번)의 위험구역(노랑)과는 관계없습니다(그 가로는 danger_zone_width_ratio).
    "danger_margin_ratio": 0.5,
    # 위험구역(노랑) 안에 사람이 이 시간 이상 있으면 알람합니다(사람마다 따로, 구역을 나가면 다시 0초). 0이면 바로 알람합니다.
    # 사람 진입(yt_zone_person) 설정에 적습니다.
    "person_hold_sec": 3.0,
    # 사람이 구역 안으로 안 잡힌 프레임(검출이 끊기거나 박스가 경계 밖으로 잠깐 흔들림)이 연속 이 수 이하면
    # 머문 시간을 이어 셉니다. 이보다 길면 구역을 나간 것으로 보고 다시 0초부터입니다. 사람 진입(yt_zone_person) 설정에 적습니다.
    "person_miss_frames": 3,
    # 위험구역(노랑) 가로 = 빈 샤시 높이(차선 방향 길이) × 이 값, 샤시 가운데 기준입니다. 높이는 빈 샤시 그대로입니다.
    # 예: 1.5면 가로가 샤시 높이의 1.5배. 사람 진입(yt_zone_person) 설정에 적습니다.
    "danger_zone_width_ratio": 1.0,
    # 작업 차선 ROI(초록, 카메라 roi_poly_norm)가 있으면 그 안에 중심이 있는 YT만 감시합니다. 없으면 화면 전체입니다.
    # Backreach와 같은 카메라면 Backreach 다각형 ROI를 같이 씁니다.
    "use_roi": True,
}


def _yt_box_area(box):
    return max(0.0, box[2] - box[0]) * max(0.0, box[3] - box[1])


def _yt_intersection(a, b):
    return _yt_box_area([max(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), min(a[3], b[3])])


def _yt_center(box):
    return ((box[0] + box[2]) / 2.0, (box[1] + box[3]) / 2.0)


def _yt_expand(box, dx, dy):
    return [box[0] - dx, box[1] - dy, box[2] + dx, box[3] + dy]


def _yt_union(*boxes):
    return [min(b[0] for b in boxes), min(b[1] for b in boxes), max(b[2] for b in boxes), max(b[3] for b in boxes)]


def _yt_point_in_box(point, box):
    return box[0] <= point[0] <= box[2] and box[1] <= point[1] <= box[3]


def _yt_boxes_touch(a, b):
    """두 박스가 겹치거나 경계가 닿으면 True."""
    return a[0] <= b[2] and b[0] <= a[2] and a[1] <= b[3] and b[1] <= a[3]


def yt_danger_zone(chassis_box, axis, width_ratio):
    """빈 샤시 박스로 만든 위험구역(노랑) [x1, y1, x2, y2].

    높이(차선 방향)는 샤시 박스 그대로이고, 가로(차선을 가로지르는 방향)는 샤시 높이 × width_ratio를
    샤시 가운데에 둡니다. axis는 차선 방향("y" 세로 차선, "x" 가로 차선)입니다."""
    x1, y1, x2, y2 = chassis_box
    if axis == "y":
        half, center = (y2 - y1) * width_ratio / 2.0, (x1 + x2) / 2.0
        return [center - half, y1, center + half, y2]
    half, center = (x2 - x1) * width_ratio / 2.0, (y1 + y2) / 2.0
    return [x1, center - half, x2, center + half]


@dataclass
class _YTObj:
    box: list
    tid: int
    score: float
    name: str
    cls: int = -1


def _yt_event_object(label, box, score, raw_tid, class_id):
    """공통 처리(사진·궤적·관제 detectedObjects)에 넘기는 객체입니다. 번호는 Backreach와 같은 규칙(TOPDOWN_TID_BASE +
    추적 번호)이고, 궤적을 찾을 수 있게 추적기 번호(raw_tid)를 함께 넣습니다."""
    return {"label": label, "box": [int(v) for v in box], "score": float(score),
            "tid": TOPDOWN_TID_BASE + int(raw_tid), "class_id": int(class_id), "raw_tid": int(raw_tid)}


def _yt_parse_tracks(tracks):
    """topdown 트랙 배열 → _YTObj 목록(클래스 이름은 TOPDOWN_CLASS_NAMES). 트랙이 없으면(None) 빈 목록."""
    if tracks is None:
        return []
    objects = []
    for row in np.asarray(tracks, dtype=float).reshape(-1, 7):
        class_id = int(row[6])
        name = TOPDOWN_CLASS_NAMES[class_id] if 0 <= class_id < len(TOPDOWN_CLASS_NAMES) else f"class_{class_id}"
        objects.append(_YTObj([float(v) for v in row[:4]], int(row[4]), float(row[5]), name, class_id))
    return objects


def _yt_pair_heads(heads, chassis, half_classes=()):
    """헤드마다 차선 방향으로 붙어 있는 샤시를 짝짓습니다. 샤시가 안 보이면 None.
    반 샤시(half_classes)는 비어 있는 절반만 잡히므로, 헤드와 그 사이에 실린 20ft(상자 길이)만큼 떨어져 있어도 짝짓습니다."""
    pairs, used = [], set()
    for head in sorted(heads, key=lambda obj: -obj.score):
        best, best_gap = None, None
        for index, ch in enumerate(chassis):
            if index in used:
                continue
            axis = 1 if (ch.box[3] - ch.box[1]) >= (ch.box[2] - ch.box[0]) else 0
            cross = 1 - axis
            cross_len = ch.box[cross + 2] - ch.box[cross]
            overlap = min(head.box[cross + 2], ch.box[cross + 2]) - max(head.box[cross], ch.box[cross])
            if overlap < 0.5 * min(cross_len, head.box[cross + 2] - head.box[cross]):
                continue
            gap = max(0.0, max(head.box[axis], ch.box[axis]) - min(head.box[axis + 2], ch.box[axis + 2]))
            max_gap = cross_len + (ch.box[axis + 2] - ch.box[axis] if ch.name in half_classes else 0.0)
            if gap > max_gap:
                continue
            if best_gap is None or gap < best_gap:
                best, best_gap = index, gap
        if best is not None:
            used.add(best)
        pairs.append((head, chassis[best] if best is not None else None))
    return pairs


class _YTUnit:
    """한 자리에 멈춘 YT 한 대(헤드 + 샤시)의 양하 감시 상태."""

    def __init__(self, uid, head, chassis, t, half=False, name=""):
        self.uid = uid
        self.name = name   # 로그의 카메라 이름(CAM:번호(ip))
        self.waited = False   # 이번 정차에서 "10초가 찼는데 감시를 못 켬"을 남겼는지
        self.half = half   # 반 샤시(상자 = 비어 있는 절반)
        self.cycle = 0
        self.armed = False
        self.armed_at = None
        self.stop_since = t
        self.empty_seen = None
        self.head_seen = t
        self.last_seen = t
        self.head = head
        self.anchor = _yt_center(head.box)
        self.container = None
        self.container_seen = None
        self.evaluation = None
        self.dev_since = None
        self.set_geometry(head.box, chassis.box)

    def set_geometry(self, head_box, chassis_box, half=None):
        if half is not None:
            self.half = bool(half)
        self.head_box = list(head_box)
        self.chassis_box = list(chassis_box)
        width, height = chassis_box[2] - chassis_box[0], chassis_box[3] - chassis_box[1]
        self.axis = 1 if height >= width else 0
        # length = 샤시 전체(40ft) 길이. 반 샤시 상자는 그 절반이라 2배로 봅니다.
        self.length = max(1.0, max(width, height) * (2.0 if self.half else 1.0))
        self.width = max(1.0, min(width, height))
        self.head_side = 1 if _yt_center(head_box)[self.axis] > _yt_center(chassis_box)[self.axis] else -1

    def disarm(self):
        self.armed = False
        self.armed_at = None
        self.container = None
        self.container_seen = None
        self.evaluation = None
        self.dev_since = None

    def observe(self, head, chassis, t, cfg):
        center = _yt_center(head.box)
        ratio = cfg["depart_move_ratio"] if self.armed else cfg["stop_move_ratio"]
        moved = math.dist(center, self.anchor) > ratio * self.length
        if moved:
            # YT가 움직였으면 새 정차로 보고 처음부터 다시 판단합니다.
            self.log_restart(t, cfg, f"헤드 {math.dist(center, self.anchor):.0f}px 움직임(기준 {ratio * self.length:.0f}px)")
            if chassis is None:
                dx, dy = center[0] - self.anchor[0], center[1] - self.anchor[1]
                chassis_box = [self.chassis_box[0] + dx, self.chassis_box[1] + dy,
                               self.chassis_box[2] + dx, self.chassis_box[3] + dy]
            else:
                chassis_box = chassis.box
            self.anchor = center
            self.stop_since = t
            self.waited = False
            self.empty_seen = None
            self.disarm()
            self.set_geometry(head.box, chassis_box,
                              None if chassis is None else chassis.name in cfg["half_chassis_classes"])
        elif not self.armed and chassis is not None:
            # 감시 전에는 최신 박스를 따라가고, 감시 중에는 정차 위치(와 빈·반 샤시)를 고정합니다.
            self.set_geometry(head.box, chassis.box, chassis.name in cfg["half_chassis_classes"])
        self.head = head
        self.head_seen = t
        self.last_seen = t
        if chassis is not None and chassis.name in cfg["empty_chassis_classes"]:
            self.empty_seen = t

    def note_empty_chassis(self, chassis, t):
        """헤드가 안 보인 프레임에도 정차 자리(샤시 박스 안)에 빈 샤시가 보이면 빈 샤시를 본 것으로 둡니다."""
        if any(_yt_point_in_box(_yt_center(c.box), self.chassis_box) for c in chassis):
            self.empty_seen = t

    def log_restart(self, t, cfg, reason):
        """감시가 끝나거나, 정차 시간(arm_stop_sec)의 절반 넘게 센 정차가 처음부터 다시 시작될 때 이유를 한 줄 남깁니다
        (거의 다 됐다 실패한 순간, 정탐·오탐 분석용)."""
        stopped = t - self.stop_since
        if self.armed:
            logger.info(f"[YT] {self.name} YT{self.uid} 감시 끝(감시 {t - self.armed_at:.1f}초): {reason}")
        elif stopped >= cfg["arm_stop_sec"] / 2:
            logger.info(f"[YT] {self.name} YT{self.uid} 정차 {stopped:.1f}초에서 처음부터 다시 셈: {reason}")

    def _log_deviation_end(self, t, cfg, reason):
        """알람 기준(deviation_hold_sec)의 절반 넘게 이어진 이탈이 끝날 때 몇 초였는지 남깁니다."""
        hold = cfg["deviation_hold_sec"]
        if self.dev_since is None or t - self.dev_since < hold / 2:
            return
        result = "알람 기준 채움" if t - self.dev_since >= hold else f"알람 기준 {hold:g}초 못 채움"
        logger.info(f"[YT] {self.name} YT{self.uid} 이탈 {t - self.dev_since:.1f}초 만에 끝남({result}): {reason}")

    def update_arming(self, t, cfg):
        if self.armed:
            return
        recent_empty = (self.empty_seen is not None and self.empty_seen >= self.stop_since
                        and t - self.empty_seen <= cfg["lost_grace_sec"])
        # 10초가 찬 순간 헤드가 안 보여도 release_sec(3초) 안에 봤으면 켭니다(10초를 세는 동안의 헤드 끊김 허용과 같은 기준).
        head_recent = t - self.head_seen <= cfg["release_sec"]
        if head_recent and recent_empty and t - self.stop_since >= cfg["arm_stop_sec"]:
            self.armed = True
            self.armed_at = t
            self.cycle += 1
            logger.info(f"[YT] {self.name} YT{self.uid} 감시 시작: 정차 {t - self.stop_since:.1f}초, "
                        f"헤드 {t - self.head_seen:.1f}초 전·빈 샤시 {t - self.empty_seen:.1f}초 전에 봄")
        elif t - self.stop_since >= cfg["arm_stop_sec"] and not self.waited:
            # 정차 시간이 찼는데 못 켠 이유를 이번 정차에 한 번만 남깁니다(조건이 채워지면 그때 켜짐).
            self.waited = True
            if not head_recent:
                why = f"헤드를 {t - self.head_seen:.1f}초 동안 못 봄(기준 {cfg['release_sec']:g}초)"
            elif self.empty_seen is None or self.empty_seen < self.stop_since:
                why = "이번 정차에서 빈 샤시를 못 봄"
            else:
                why = f"빈 샤시를 {t - self.empty_seen:.1f}초 동안 못 봄(기준 {cfg['lost_grace_sec']:g}초)"
            logger.info(f"[YT] {self.name} YT{self.uid} 정차 {t - self.stop_since:.1f}초가 찼는데 감시를 못 켬: {why}")

    def load_area(self, cfg):
        margin = cfg["load_margin_ratio"] * self.length
        return _yt_expand(self.chassis_box, margin if self.axis == 0 else 0.0, margin if self.axis == 1 else 0.0)

    def container_search_zone(self, cfg):
        """하역 중 컨테이너를 찾는 범위: YT(헤드+샤시) 박스를 샤시 폭 × danger_margin_ratio만큼 넓힌 영역."""
        margin = cfg["danger_margin_ratio"] * self.width
        return _yt_expand(_yt_union(self.head_box, self.chassis_box), margin, margin)

    def keep_alive(self, objects, t, cfg):
        """감시 중에는 헤드가 안 보여도 그 자리(container_search_zone)에 YT 헤드·빈 샤시·컨테이너가 하나라도 보이면
        YT가 있는 것으로 봅니다. YT가 출발하거나(observe) 그 자리에 release_sec 동안 아무것도 없으면 감시가 끝납니다."""
        zone = self.container_search_zone(cfg)
        if any(_yt_point_in_box(_yt_center(o.box), zone) for o in objects):
            self.last_seen = t

    def track_container(self, containers, spreaders, t, cfg):
        zone = self.container_search_zone(cfg)
        chassis_center = _yt_center(self.chassis_box)
        candidates = []
        for obj in containers:
            if not _yt_point_in_box(_yt_center(obj.box), zone):
                continue
            spreader = next((s for s in spreaders if _yt_intersection(obj.box, s.box)
                             >= 0.3 * min(_yt_box_area(obj.box), _yt_box_area(s.box))), None)
            if spreader is None:
                continue
            candidates.append((math.dist(_yt_center(obj.box), chassis_center), obj, spreader))
        if candidates:
            _, obj, spreader = min(candidates, key=lambda item: item[0])
            self.container = {"obj": obj, "size": cfg["container_classes"][obj.name], "spreader": spreader}
            self.container_seen = t
            self.last_seen = t
            self.evaluation = self.evaluate(obj, self.container["size"], cfg)
            if self.evaluation["deviated"]:
                self.dev_since = self.dev_since if self.dev_since is not None else t
            else:
                self._log_deviation_end(t, cfg, "적재영역 안으로 돌아옴")
                self.dev_since = None
        elif self.container is not None and t - self.container_seen > cfg["lost_grace_sec"]:
            self._log_deviation_end(t, cfg, f"하역 중 컨테이너를 {t - self.container_seen:.1f}초 동안 못 봄")
            self.container = None
            self.container_seen = None
            self.evaluation = None
            self.dev_since = None

    def evaluate(self, obj, size, cfg):
        """컨테이너 중심을 하락 예측 지점으로 보고, 규격 길이만큼의 바닥면이 적재영역 안인지 봅니다.

        매달린 컨테이너는 카메라에 가까워 박스가 실제보다 크게 보이므로 박스 크기 대신
        샤시 길이에 대한 규격 비율(40ft=샤시 전체, 20ft=절반)로 바닥면을 추정합니다.
        반 샤시는 상자(비어 있는 절반)의 2배를 샤시 길이로 보므로, 20ft 바닥면이 그 빈 절반만 하고
        적재영역도 빈 절반 + 앞뒤 여유(샤시 전체 길이의 load_margin_ratio)입니다.
        """
        axis, cross = self.axis, 1 - self.axis
        start, end = self.chassis_box[axis], self.chassis_box[axis + 2]
        margin = cfg["load_margin_ratio"] * self.length
        half = self.length / 2.0 if size == "40ft" else self.length / 4.0
        center = _yt_center(obj.box)
        low, high = center[axis] - half, center[axis] + half
        over_low = max(0.0, (start - margin) - low)
        over_high = max(0.0, high - (end + margin))
        cross_offset = center[cross] - _yt_center(self.chassis_box)[cross]
        cross_out = abs(cross_offset) > cfg["cross_offset_ratio"] * self.width
        head_over = over_high if self.head_side > 0 else over_low
        tail_over = over_low if self.head_side > 0 else over_high
        if head_over > 0:
            direction = "head"
        elif tail_over > 0:
            direction = "tail"
        elif cross_out:
            direction = "cross"
        else:
            direction = None
        footprint = [0.0, 0.0, 0.0, 0.0]
        footprint[axis], footprint[axis + 2] = low, high
        footprint[cross] = center[cross] - self.width / 2.0
        footprint[cross + 2] = center[cross] + self.width / 2.0
        return {
            "deviated": direction is not None,
            "direction": direction,
            "head_over_px": round(head_over, 1),
            "tail_over_px": round(tail_over, 1),
            "cross_offset_px": round(cross_offset, 1),
            "over_ratio": round(max(head_over, tail_over) / self.length, 3),
            "predicted_footprint": [round(v, 1) for v in footprint],
        }

    def view(self, t, cfg):
        deviation_sec = t - self.dev_since if self.dev_since is not None else 0.0
        container = None
        if self.container is not None:
            obj, spreader = self.container["obj"], self.container["spreader"]
            container = {"label": obj.name, "size": self.container["size"], "box": list(obj.box),
                         "tid": obj.tid, "score": obj.score, "class_id": obj.cls, "visible": self.container_seen == t,
                         "spreader_box": list(spreader.box) if spreader is not None else None}
        return {
            "uid": self.uid, "cycle": self.cycle, "armed": self.armed,
            "head_box": list(self.head_box), "chassis_box": list(self.chassis_box),
            "chassis_half": self.half, "chassis_length": round(self.length, 1),
            "head_tid": self.head.tid, "head_score": self.head.score,
            "head_label": self.head.name, "head_class_id": self.head.cls,
            "axis": "y" if self.axis == 1 else "x", "head_side": self.head_side,
            "stopped_sec": round(t - self.stop_since, 2),
            "load_area": self.load_area(cfg),
            "container": container, "evaluation": copy.deepcopy(self.evaluation),
            "deviation_sec": round(deviation_sec, 2), "deviation_hold_sec": cfg["deviation_hold_sec"],
        }


class YTDischargeMonitor:
    """카메라 한 대의 YT 양하 감시 상태(Camera.yt_monitor). 같은 프레임은 한 번만 계산합니다."""

    def __init__(self, config=None, name=""):
        self.lock = threading.RLock()
        self.name = name   # 로그의 카메라 이름
        self.config = deep_merge_dict(YT_DEFAULT_CONFIG, config)
        self.units = []
        self.next_uid = 1
        self.frame_key = None
        self.snapshot = {"timestamp": None, "units": []}

    def update(self, fid, t, tracks, roi_poly=None):
        """프레임 하나를 반영합니다. tracks는 topdown 트랙 [x1, y1, x2, y2, 추적 번호, 점수, 클래스] 배열(t_topdown),
        roi_poly는 카메라 작업 차선 ROI(초록, 픽셀)입니다."""
        with self.lock:
            key = (fid, float(t))
            if key == self.frame_key:
                return self.snapshot
            self.frame_key = key
            t = float(t)
            cfg = self.config
            objects = _yt_parse_tracks(tracks)
            heads = [o for o in objects if o.name in cfg["head_classes"]]
            chassis = [o for o in objects if o.name in cfg["empty_chassis_classes"]]
            containers = [o for o in objects if o.name in cfg["container_classes"]]
            spreaders = [o for o in objects if o.name in cfg["spreader_classes"]]
            roi = polygon_contour(roi_poly if roi_poly is not None else []) if cfg["use_roi"] else None
            if roi is not None:
                roi = roi.tolist()
                heads = [o for o in heads if point_in_polygon(roi, *_yt_center(o.box))]
                chassis = [o for o in chassis if point_in_polygon(roi, *_yt_center(o.box))]

            matched = set()
            half_classes = cfg["half_chassis_classes"]
            for head, ch in _yt_pair_heads(heads, chassis, half_classes):
                unit = self._match_unit(head.box, matched)
                if unit is None:
                    if ch is None:
                        continue
                    unit = _YTUnit(self.next_uid, head, ch, t, ch.name in half_classes, self.name)
                    self.next_uid += 1
                    self.units.append(unit)
                matched.add(unit.uid)
                unit.observe(head, ch, t, cfg)
            for unit in self.units:
                if unit.uid not in matched:
                    unit.note_empty_chassis(chassis, t)
                unit.update_arming(t, cfg)
                if unit.armed:
                    unit.track_container(containers, spreaders, t, cfg)
                    unit.keep_alive(heads + chassis + containers, t, cfg)
            for unit in self.units:
                gone = t - unit.last_seen
                if gone > cfg["release_sec"]:
                    what = "그 자리에 헤드·빈 샤시·컨테이너가" if unit.armed else "헤드가"
                    unit.log_restart(t, cfg, f"{what} {gone:.1f}초 동안 안 보임(기준 {cfg['release_sec']:g}초)")
            self.units = [unit for unit in self.units if t - unit.last_seen <= cfg["release_sec"]]
            self.snapshot = {"timestamp": t, "units": [unit.view(t, cfg) for unit in self.units]}
            return self.snapshot

    def _match_unit(self, head_box, exclude):
        best, best_dist = None, None
        center = _yt_center(head_box)
        limit = 0.5 * math.hypot(head_box[2] - head_box[0], head_box[3] - head_box[1])
        for unit in self.units:
            if unit.uid in exclude:
                continue
            # 감시 중 고정한 박스가 아니라 마지막으로 본 헤드 위치와 비교해야 출발하는 YT를 따라갑니다.
            dist = math.dist(center, _yt_center(unit.head.box))
            if dist <= max(limit, unit.width) and (best_dist is None or dist < best_dist):
                best, best_dist = unit, dist
        return best

    def armed(self):
        """양하 감시 중인 YT가 있는지. 있으면 topdown을 매 프레임 추론합니다(Camera.backreach_inference_due)."""
        return any(unit["armed"] for unit in self.snapshot["units"])


class _YTDetectorBase(BaseEventDetector):
    """YT 이벤트 공통. 카메라의 양하 감시 상태(kwargs['yt_snapshot'])를 받아 판단만 합니다."""
    gui_name = "YT"
    # topdown(1280 입력)을 Backreach와 같이 쓰므로 Backreach처럼 가로 1280으로 받습니다.
    stream_max_width = 1280

    def __init__(self, config, roi_poly=None, roi_lines=None):
        super().__init__(deep_merge_dict(YT_DEFAULT_CONFIG, config), roi_poly, roi_lines)


class YTHeadCrushDetector(_YTDetectorBase):
    """하락 예측 지점이 적재영역을 벗어난 상태가 deviation_hold_sec 이상이면 알람합니다."""
    gui_name = "YT HEAD CRUSH"
    alarm_once_per_object = False   # 목숨과 연결: 같은 컨테이너라도 위험이 이어지면 쿨다운(50초)마다 다시 알람

    def process(self, tracks, track_map, frame, fid, **kwargs):
        snapshot = kwargs.get('yt_snapshot') or {}
        hold = float(self.config["deviation_hold_sec"])
        triggered = []
        for unit in snapshot.get("units", []):
            container, evaluation = unit["container"], unit["evaluation"]
            if not unit["armed"] or container is None or not evaluation or not evaluation["deviated"]:
                continue
            if unit["deviation_sec"] < hold:
                continue
            alarm = _yt_event_object(container["label"], container["box"], container["score"], container["tid"],
                                     container["class_id"])
            head = _yt_event_object(unit["head_label"], unit["head_box"], unit["head_score"], unit["head_tid"],
                                    unit["head_class_id"])
            triggered.append({
                'tid': alarm['tid'],
                'bbox': [float(v) for v in container["box"]],
                'frame': frame.copy() if frame is not None else None,
                'fid': fid,
                'privacy_tracks': kwargs.get('privacy_tracks', []),
                'privacy_fid': fid,
                'objects': [alarm, head],
                'decision_trace': {
                    'detector': 'YTHeadCrushDetector',
                    'reason': f"drop_point_outside_load_area_{evaluation['direction']}",
                    'unit': unit["uid"], 'cycle': unit["cycle"],
                    'container_size': container["size"],
                    'deviation_sec': unit["deviation_sec"], 'deviation_hold_sec': hold,
                    'evaluation': evaluation, 'load_area': unit["load_area"],
                    'chassis_box': unit["chassis_box"], 'head_box': unit["head_box"],
                    'spreader_box': container["spreader_box"],
                },
            })
        return triggered


class YTZonePersonDetector(_YTDetectorBase):
    """YT 양하 감시 중(YT 헤드+빈 샤시가 10초 정지)에 위험구역(노랑) 안에 작업자가 있으면 알람합니다.
    위험구역은 감시 중인 YT마다 빈 샤시 박스로 만듭니다(yt_danger_zone, 가로 비율 danger_zone_width_ratio).
    사람 박스가 구역에 조금이라도 겹치거나 닿으면 안으로 봅니다. 안으로 안 잡힌 프레임이 연속 person_miss_frames
    이하면 머문 시간을 이어 셉니다(검출 깜빡임)."""
    gui_name = "YT ZONE PERSON"

    def __init__(self, config, roi_poly=None, roi_lines=None):
        super().__init__(config, roi_poly, roi_lines)
        self.inside_since = {}   # 추적 번호 -> 구역 안으로 처음 잡힌 시각
        self.missed = {}         # 추적 번호 -> 구역 안으로 안 잡힌 연속 프레임 수
        self.last_inside = {}    # 추적 번호 -> 구역 안으로 마지막으로 잡힌 시각(기준 전에 나간 사람 로그용)

    def danger_zones(self, snapshot):
        """감시 중인 YT마다 (YT 상태, 위험구역 [x1, y1, x2, y2]). 화면(Camera.draw)도 이 구역을 그립니다."""
        ratio = float(self.config["danger_zone_width_ratio"])
        return [(unit, yt_danger_zone(unit["chassis_box"], unit["axis"], ratio))
                for unit in snapshot.get("units", []) if unit["armed"]]

    def process(self, tracks, track_map, frame, fid, **kwargs):
        snapshot = kwargs.get('yt_snapshot') or {}
        t = snapshot.get("timestamp")
        zones = self.danger_zones(snapshot)
        if t is None or not zones:
            self._log_left(kwargs.get('camera_key'), list(self.inside_since), "감시가 끝남")
            self.inside_since, self.missed, self.last_inside = {}, {}, {}
            return []
        persons = [obj for obj in _yt_parse_tracks(kwargs.get('topdown_tracks'))
                   if obj.name in self.config["person_classes"]]
        hold = float(self.config["person_hold_sec"])
        triggered, inside = [], set()
        frame_copy = None
        for person in persons:
            hits = [(unit, zone) for unit, zone in zones if _yt_boxes_touch(person.box, zone)]
            if not hits:
                continue
            # 구역이 겹친 YT가 여럿이면 사람과 가장 가까운 YT로 기록합니다.
            unit, zone = min(hits, key=lambda hit: math.dist(_yt_center(person.box), _yt_center(hit[0]["chassis_box"])))
            inside.add(person.tid)
            since = self.inside_since.setdefault(person.tid, t)
            self.last_inside[person.tid] = t
            if t - since < hold:
                continue
            if frame_copy is None and frame is not None:
                frame_copy = frame.copy()
            obj = _yt_event_object(person.name, person.box, person.score, person.tid, person.cls)
            triggered.append({
                'tid': obj['tid'],
                'bbox': [float(v) for v in person.box],
                'frame': frame_copy,
                'fid': fid,
                'privacy_tracks': kwargs.get('privacy_tracks', []),
                'privacy_fid': fid,
                'objects': [obj],
                'decision_trace': {
                    'detector': 'YTZonePersonDetector',
                    'reason': 'person_in_discharge_danger_zone',
                    'unit': unit["uid"], 'cycle': unit["cycle"],
                    'danger_zone': [[float(zone[0]), float(zone[1])], [float(zone[2]), float(zone[1])],
                                    [float(zone[2]), float(zone[3])], [float(zone[0]), float(zone[3])]],
                    'danger_zone_width_ratio': float(self.config["danger_zone_width_ratio"]),
                    'inside_sec': round(t - since, 2), 'person_hold_sec': hold,
                },
            })
        # 이번 프레임에 구역 안으로 안 잡힌 사람은 끊긴 프레임을 세고, person_miss_frames를 넘으면 지웁니다(다시 0초).
        limit = int(self.config["person_miss_frames"])
        missed = {tid: 0 if tid in inside else self.missed.get(tid, 0) + 1 for tid in self.inside_since}
        self.missed = {tid: n for tid, n in missed.items() if n <= limit}
        self._log_left(kwargs.get('camera_key'), [tid for tid in self.inside_since if tid not in self.missed], "나감")
        self.inside_since = {tid: since for tid, since in self.inside_since.items() if tid in self.missed}
        self.last_inside = {tid: last for tid, last in self.last_inside.items() if tid in self.inside_since}
        return triggered

    def _log_left(self, camera_key, tids, reason):
        """위험구역에 알람 기준(person_hold_sec)의 절반 넘게 머물다 기준 전에 사라진 사람을 한 줄 남깁니다(정탐·오탐 분석용)."""
        hold = float(self.config["person_hold_sec"])
        for tid in tids:
            since = self.inside_since[tid]
            stayed = self.last_inside.get(tid, since) - since
            if hold / 2 <= stayed < hold:
                logger.info(f"[YT ZONE] cam={camera_key} 사람 tid={tid} 위험구역에 {stayed:.1f}초 머물다 {reason}"
                            f"(기준 {hold:g}초)")


def yt_draw_overlay(image, snapshot, scale=(1.0, 1.0), ui=1.0):
    """감시 중인 YT의 적재영역(빨강), 하락 예측 바닥면(초록/자홍), 이탈 시간을 BGR 이미지에 그립니다.
    작업 차선 ROI(초록)는 사용자가 그린 ROI라 Camera.draw가 항상 그리고, 위험구역(노랑)은 Camera.draw가
    감시 중에만 사람 진입(5번) 설정으로 그립니다(YTZonePersonDetector.danger_zones).
    scale은 추론 좌표 → 그리는 화면 배율, ui는 글자·선 굵기 배율입니다(Camera.draw와 같음)."""
    sx, sy = float(scale[0]), float(scale[1])

    def pt(x, y):
        return int(round(x * sx)), int(round(y * sy))

    def th(v):
        return max(1, int(round(v * ui)))

    def rect(box, color, thickness):
        cv2.rectangle(image, pt(box[0], box[1]), pt(box[2], box[3]), color, th(thickness))

    def label(text, box, color, size):
        x, y = pt(box[0], box[1])
        cv2.putText(image, text, (max(0, x), max(th(12), y - th(6))), cv2.FONT_HERSHEY_SIMPLEX, size * ui, color, th(1))

    for unit in snapshot.get("units", []):
        if not unit["armed"]:
            label(f"YT {unit['uid']} WAIT {unit['stopped_sec']:.0f}s", unit["chassis_box"], (200, 200, 200), 0.4)
            continue
        rect(unit["load_area"], (0, 0, 255), 2)
        evaluation = unit["evaluation"]
        status = f"YT {unit['uid']} DISCHARGE"
        if evaluation:
            deviated = evaluation["deviated"]
            # 빨간 적재영역과 구분되도록 예측 바닥면은 이탈 시 자홍, 정상 시 초록으로 그립니다.
            rect(evaluation["predicted_footprint"], (255, 0, 255) if deviated else (0, 255, 0), 2)
            if deviated:
                status += f" | OFF {evaluation['direction']} {unit['deviation_sec']:.1f}/{unit['deviation_hold_sec']:.0f}s"
            else:
                status += " | ALIGNED"
        label(status, unit["load_area"], (0, 0, 255), 0.45)
    return image
# ============================================================================
# [YT 헤드압착 모듈 끝]
# ============================================================================


# ============================================================================
# [해측 추락 방지 모듈 시작] seaside_person (관제 14 SEASIDE_DETECT)
# ----------------------------------------------------------------------------
# 크레인에 달려 비스듬히 내려다보는 카메라 묶음(보통 3대)으로 안벽 끝 위험 구역에 사람이 person_hold_sec 이상
# 머물면 알람합니다.
# 가변 ROI(거리 보정 없이 화면 좌표만):
#   - 설치 때(마법사) 카메라마다 스냅샷에서 안벽 끝선 두 점과 바다 쪽 한 점을 클릭하고, 위험 구역 다각형을 그립니다.
#     끝선 양옆의 육지 색·물 색을 함께 남깁니다(cameras.json event_config.seaside_person).
#   - 카메라가 크레인과 함께 움직이므로 프레임마다 엣지 검출로 끝선 후보를 찾고, "바다 쪽 옆은 물, 육지 쪽 옆은 물이
#     아니고 그 안쪽은 육지"인 후보 중 가장 바다 쪽을 끝선으로 고릅니다(SeaSideEdgeTracker).
#   - 설치 때 끝선 → 지금 끝선으로 옮기는 이동·회전을 그린 위험 구역에 그대로 적용합니다(끝선에 묶인 ROI).
#   - 판정: 사람 발 위치(박스 아래 가운데)가 옮겨진 위험 구역 안에 있으면 머무름을 셉니다.
#   - 끝선을 못 찾으면 마지막 위치를 roi_hold_sec 동안 쓰고, 그 뒤에는 판정을 멈춥니다(틀린 구역으로 판정하지 않음).
#   - 크레인 PLC는 없다고 봅니다.
# 묶음: 한 카메라에서 이벤트가 나면 그 카메라 이벤트 1건으로 내고, 묶인 나머지 카메라의 그 순간 사진과 4분할 영상을
#   같은 이벤트로 남깁니다(콘 NG 묶음과 같은 방식, 카메라 간 같은 사람인지는 따지지 않음). 묶음 쿨다운
#   group_cooldown_sec 동안 묶음 안에서 새 이벤트를 내지 않습니다(겹치는 화면의 같은 사람이 2~3건으로 나가지 않게).
# TODO(현장): 실제 카메라·사진이 없어 기준값(머무름·쿨다운·끝선 검출 띠·색 기준)은 합성 장면으로만 정한 임시값입니다.
# TODO(현장): 끝선이 멀어지면(화면 위로) 같은 거리가 작게 보이는데, 지금은 구역을 이동·회전만 하고 크기는 그대로 둡니다.
# ============================================================================
SEASIDE_DEFAULT_CONFIG = {
    "person_classes": ["PERSON"],     # topdown 9클래스의 PERSON
    "person_hold_sec": 1.0,           # 위험 구역 안에 이만큼 머물면 알람 TODO(현장)
    "gap_tolerance_sec": 0.5,         # 검출이 잠깐 끊겨도 머무름을 이어 셈
    "group_cooldown_sec": 60.0,       # 묶음 쿨다운 TODO(현장)
    "idle_infer_fps": 3.0,            # 구역 근처에 사람이 없을 때 topdown 추론 횟수(초당, 0 = 줄이지 않음)
    "near_ratio": 0.15,               # 위험 구역에서 화면 높이의 이 비율 안에 사람이 보이면 그 카메라는 매 프레임 추론
    "near_hold_sec": 3.0,             # 근처 사람이 사라진 뒤에도 이만큼 매 프레임 추론
    "edge": {
        "mode": "vision",             # "vision" = 끝선을 따라 구역을 옮김(크레인에 단 카메라), "fixed" = 그린 그대로
        "check_every_sec": 5.0,       # fixed: 끝선으로 카메라 틀어짐 점검 주기
        "drift_px": 20.0,             # fixed: 설치 끝선과 찾은 끝선이 이만큼(가로 640 기준 px) 벌어진 채
        "drift_sec": 30.0,            #        이만큼 이어지면 틀어짐 경고(판정은 계속)
        "update_every_sec": 0.5,      # vision: 화면이 멈춰 있을 때 끝선 검출 주기(움직이는 동안은 매 프레임)
        "motion_px": 3.0,             # vision: 마지막으로 끝선을 맞춘 뒤 화면 전체가 이만큼(가로 640 기준 px) 밀리면 이동 중
        "band_px": 40,                # 끝선을 찾는 띠 폭(지금 끝선 위치에서 양쪽, 가로 640 기준 px)
        "min_confidence": 0.6,        # 끝선 신뢰도(찾은 직선 길이 / 기대 길이의 절반) 하한
        "color_tol": 40.0,            # 설치 때 육지·물 색과 이만큼(BGR 거리) 안이면 같은 것으로 봄
        "sea_px": [2, 12],            # 끝선에서 바다 쪽으로 이 범위(px, 가로 640 기준)는 물이어야 함
        "land_near_px": [2, 10],      # 끝선에서 육지 쪽 바로 옆 이 범위는 물이 아니어야 함
        "land_px": [8, 30],           # 끝선에서 육지 쪽 이 범위는 육지 색이어야 함
        "smooth_alpha": 0.3,          # 멈춰 있을 때 끝선 갱신 비율(흔들림 줄이기, 차이가 smooth_within_px 안일 때만)
        "smooth_within_px": 4.0,
        "roi_hold_sec": 5.0,          # 끝선을 못 찾으면 마지막 위치를 이만큼만 씀, 그 뒤 판정 중지
        "max_jump_px": 60.0           # 한 번에 이보다 크게 튀면 잘못 찾은 끝선으로 봄(가로 640 기준 px)
    },
    # 카메라별(마법사, cameras.json): edge_norm 끝선 두 점, sea_norm 바다 쪽 한 점, zone_norm 위험 구역,
    # land_ref·water_ref 육지·물 색(BGR), exclude_polys_norm 크레인 구조물 제외 구역(화면에 고정)
    "edge_norm": None, "sea_norm": None, "zone_norm": None, "land_ref": None, "water_ref": None,
    "exclude_polys_norm": [],
    "group_video": True               # 묶음 4분할 영상(단말 저장)
}
SEASIDE_WORK_W = 640   # 끝선 검출·색 비교 작업 폭(px 기준값도 이 폭 기준)


class EdgeLine:
    """화면 직선(작업 폭 px 좌표): 지나는 점 p0, 방향 u(단위), 육지 쪽 법선 n(단위)."""

    def __init__(self, p1, p2, land_n=None):
        p1, p2 = np.asarray(p1, np.float64), np.asarray(p2, np.float64)
        u = p2 - p1
        u /= (np.linalg.norm(u) or 1.0)
        n = np.array([-u[1], u[0]])
        if land_n is not None and np.dot(n, land_n) < 0:
            n = -n
        self.p1, self.p2, self.u, self.n = p1, p2, u, n

    def offset(self, point):
        """점이 선에서 육지 쪽으로 떨어진 거리(px, 바다 쪽은 -)."""
        return float(np.dot(np.asarray(point, np.float64) - self.p1, self.n))

    def foot(self, point):
        """점에서 선에 내린 수선의 발."""
        p = np.asarray(point, np.float64)
        return self.p1 + np.dot(p - self.p1, self.u) * self.u


def seaside_edge_candidates(gray, expected, band_px, max_angle_deg=10.0):
    """화면 속 안벽 끝선 후보: 지금 끝선(expected EdgeLine) 둘레 band_px 띠에서 Canny + HoughLinesP로 찾은,
    끝선과 나란한 직선들. 반환 [(p1, p2, 신뢰도 0~1 = 길이 / 화면 폭의 절반), ...].
    어느 것이 끝선인지는 SeaSideEdgeTracker._pick가 색으로 고릅니다(가장 긴 직선은 선박 난간·안전선일 수 있음).
    TODO(현장): 야간·우천·그림자·선박 접안 시험. 부족하면 물·안벽 분할 모델이나 안벽 반사 마커로 바꿉니다."""
    h, w = gray.shape[:2]
    a, b = expected.p1 - expected.u * 2 * w, expected.p1 + expected.u * 2 * w
    ok, q1, q2 = cv2.clipLine((0, 0, w, h), (int(a[0]), int(a[1])), (int(b[0]), int(b[1])))
    if not ok:
        return []
    mask = np.zeros_like(gray)
    cv2.line(mask, q1, q2, 255, max(2, int(2 * band_px)))
    edges = cv2.bitwise_and(cv2.Canny(gray, 50, 150), mask)
    lines = cv2.HoughLinesP(edges, 1, np.pi / 180, threshold=30, minLineLength=max(10, int(0.1 * w)), maxLineGap=10)
    if lines is None:
        return []
    exp_angle = math.atan2(expected.u[1], expected.u[0])
    out = []
    for x1, y1, x2, y2 in np.asarray(lines).reshape(-1, 4):   # OpenCV 판마다 (N, 1, 4) 또는 (N, 4)
        diff = abs((math.atan2(y2 - y1, x2 - x1) - exp_angle + math.pi / 2) % math.pi - math.pi / 2)
        if math.degrees(diff) <= max_angle_deg:
            out.append(((x1, y1), (x2, y2), min(1.0, math.hypot(x2 - x1, y2 - y1) / (0.5 * w))))
    return out


def seaside_strip(image, line, d0, d1, nu=12, nd=3):
    """선을 따라(10~90%) 육지 쪽 법선 방향 d0~d1 px 띠의 색 표본(N x 3). 화면 밖 점은 뺍니다."""
    h, w = image.shape[:2]
    out = []
    for s in np.linspace(0.1, 0.9, nu):
        base = line.p1 + (line.p2 - line.p1) * s
        for d in np.linspace(d0, d1, nd):
            x, y = base + line.n * d
            if 0 <= x < w and 0 <= y < h:
                out.append(image[int(y), int(x)])
    return np.asarray(out, np.float64).reshape(-1, 3)


def seaside_color_refs(image, line):
    """설치 화면에서 끝선 양옆 색: 육지(육지 쪽 10~40px), 물(바다 쪽 4~16px)의 중앙값 BGR."""
    land, water = seaside_strip(image, line, 10, 40), seaside_strip(image, line, -16, -4)
    if not len(land) or not len(water):
        return None, None
    return np.median(land, axis=0), np.median(water, axis=0)


class SeaSideEdgeTracker:
    """카메라 한 대의 끝선 추적. 설치 끝선(ref)과 지금 끝선(cur)으로 위험 구역을 옮깁니다(작업 폭 px 좌표).
    - vision: 화면 밀림(위상 상관)으로 움직임을 알고, 움직이는 동안 매 프레임·멈추면 update_every_sec마다 끝선을 찾습니다.
      끝선을 못 찾은 채 roi_hold_sec가 지나면 UNKNOWN(판정 중지)입니다.
    - fixed: 구역은 그린 그대로. 몇 초마다 끝선을 찾아 설치 끝선과 오래 어긋나면 틀어짐 경고만 남깁니다."""
    TINY_W = 160

    def __init__(self, cam_key, cfg, ref, land_ref, water_ref):
        self.key, self.cfg = cam_key, cfg
        self.mode = "fixed" if str(cfg.get("mode", "vision")).lower() == "fixed" else "vision"
        self.ref, self.cur = ref, ref
        self.land_ref, self.water_ref = land_ref, water_ref
        self.found_once = self.mode == "fixed"   # vision은 첫 끝선을 찾아야 확정
        self.last_fix_t = None
        self.dirty_since = None    # 움직였는데 아직 끝선으로 다시 맞추지 못한 시작 시각
        self.next_t = 0.0
        self.prev_tiny = None
        self.shift = np.zeros(2)   # 마지막으로 끝선을 맞춘 뒤 쌓인 화면 밀림(작업 폭 px). 느린 이동도 쌓이면 잡습니다.
        self.moving_until = 0.0    # 움직임을 본 뒤 이 시각까지는 매 프레임 끝선을 찾음(밀림이 기준 아래로 쌓이는 사이 놓치지 않게)
        self.drift_since, self.warned = None, False
        self.last_found = None

    def state(self, t):
        if self.mode == "fixed":
            return "CONFIRMED"
        if not self.found_once:
            return "UNKNOWN"
        # 크레인에 달린 카메라는 계속 움직이므로, 움직이는 중이라도 방금(0.5초 안) 끝선으로 맞췄으면 확정입니다.
        if self.dirty_since is None or (self.last_fix_t is not None and t - self.last_fix_t <= 0.5):
            return "CONFIRMED"
        return "HOLDING" if t - self.dirty_since <= float(self.cfg["roi_hold_sec"]) else "UNKNOWN"

    def update(self, small, t):
        """작업 폭으로 줄인 프레임 하나로 끝선을 갱신합니다. 반환 상태."""
        c = self.cfg
        moving = False
        if self.mode == "vision":
            h, w = small.shape[:2]
            tiny = cv2.cvtColor(cv2.resize(small, (self.TINY_W, max(1, int(h * self.TINY_W / w))),
                                           interpolation=cv2.INTER_AREA), cv2.COLOR_BGR2GRAY).astype(np.float32)
            if self.prev_tiny is not None and self.prev_tiny.shape == tiny.shape:
                (dx, dy), _ = cv2.phaseCorrelate(self.prev_tiny, tiny)
                self.shift += np.array([dx, dy]) * w / self.TINY_W
                moving = float(np.linalg.norm(self.shift)) > float(c["motion_px"])
            self.prev_tiny = tiny
            if moving:
                self.moving_until = t + 1.0
                if self.dirty_since is None:
                    self.dirty_since = t
            moving = moving or t < self.moving_until
        self.last_found = None
        if t >= self.next_t or moving or self.dirty_since is not None:
            self.next_t = t + float(c["check_every_sec"] if self.mode == "fixed" else c["update_every_sec"])
            self._check(small, t, moving)
        return self.state(t)

    def _pick(self, small, gray, band):
        """끝선 후보 중 "육지가 끝나고 물이 시작되는 선"을 고릅니다. 반환 EdgeLine 또는 None.
        바다 쪽 옆(sea_px)은 물, 육지 쪽 바로 옆(land_near_px)은 물이 아님, 그 안쪽(land_px)은 육지인 후보 중
        가장 바다 쪽. 물속 무늬 선(육지 쪽 옆이 물), 선박 난간(바다 쪽이 선체), 노란 안전선(바다 쪽이 육지)이 걸러집니다.
        TODO(현장): 물 색은 파도·반사·선박 그림자·야간에 바뀝니다. 실제 화면으로 color_tol과 띠 폭을 다시 정합니다."""
        c = self.cfg
        tol = float(c["color_tol"])

        def frac(line, d, ref):
            samples = seaside_strip(small, line, float(d[0]), float(d[1]))
            return float(np.mean(np.linalg.norm(samples - ref, axis=1) < tol)) if len(samples) >= 8 else None

        best, best_off = None, None
        for p1, p2, conf in seaside_edge_candidates(gray, self.cur, band):
            if conf < float(c["min_confidence"]):
                continue
            line = EdgeLine(p1, p2, land_n=self.cur.n)
            sea = frac(line, (-c["sea_px"][1], -c["sea_px"][0]), self.water_ref)
            near = frac(line, c["land_near_px"], self.water_ref)
            land = frac(line, c["land_px"], self.land_ref)
            if None in (sea, near, land) or sea < 0.6 or near > 0.3 or land < 0.5:
                continue
            off = self.cur.offset((line.p1 + line.p2) / 2)   # 지금 끝선보다 육지 쪽 +, 바다 쪽 -
            if best is None or off < best_off:
                best, best_off = line, off
        return best

    def _check(self, small, t, moving):
        c = self.cfg
        gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
        # 띠 폭: fixed 점검은 크게 틀어진 카메라도 잡도록 3배. vision에서 위치를 모르면(시작·미확정) 화면 전체.
        if self.mode == "fixed":
            band = float(c["band_px"]) * 3.0
        elif self.state(t) == "UNKNOWN":
            band = float(small.shape[0])
        else:
            band = float(c["band_px"])
        line = self._pick(small, gray, band)
        if line is None:
            return
        self.last_found = line
        mid = (line.p1 + line.p2) / 2
        if self.mode == "fixed":
            drift = abs(self.ref.offset(mid))
            if drift <= float(c["drift_px"]):
                self.drift_since, self.warned = None, False
            else:
                self.drift_since = self.drift_since or t
                if not self.warned and t - self.drift_since >= float(c["drift_sec"]):
                    self.warned = True
                    logger.warning(f"[SEASIDE] cam={self.key} 카메라 틀어짐 의심: 찾은 끝선이 설치 끝선과 {drift:.0f}px"
                                   f"(가로 {SEASIDE_WORK_W} 기준) 벌어진 지 {t - self.drift_since:.0f}초. 마법사에서 다시 그리세요.")
            return
        jump = abs(self.cur.offset(mid))
        if self.state(t) != "UNKNOWN" and jump > float(c["max_jump_px"]):
            logger.debug(f"[SEASIDE] cam={self.key} 끝선이 {jump:.0f}px 튀어 버립니다")
            return
        if self.found_once and not moving and jump <= float(c["smooth_within_px"]):
            # 멈춰 있을 때 작은 흔들림은 조금씩만 따라갑니다(같은 방향 직선이라 끝점을 섞어도 됨).
            a = float(c["smooth_alpha"])
            p1 = self.cur.foot(line.p1) * (1 - a) + line.p1 * a
            p2 = self.cur.foot(line.p2) * (1 - a) + line.p2 * a
            line = EdgeLine(p1, p2, land_n=self.cur.n)
        self.cur = line
        self.found_once = True
        self.last_fix_t = t
        self.shift[:] = 0.0
        self.dirty_since = t if moving else None

    def map_points(self, points):
        """설치 화면의 점들(작업 폭 px)을 지금 끝선에 맞춰 옮깁니다. 설치 끝선 기준 (따라가는 거리, 육지 쪽 거리)를
        지금 끝선의 같은 자리로 옮깁니다(기준 = 설치 끝선 가운데와, 그 점에서 지금 끝선에 내린 수선의 발).
        크레인 주행으로 화면이 끝선을 따라 미끄러지는 것은 따지지 않습니다(위험 구역이 끝선을 따라 길게 그려져 있으면 같음)."""
        if self.cur is self.ref:
            return np.asarray(points, np.float64)
        o_ref = (self.ref.p1 + self.ref.p2) / 2
        o_cur = self.cur.foot(o_ref)
        out = []
        for p in np.asarray(points, np.float64):
            s, d = np.dot(p - o_ref, self.ref.u), np.dot(p - o_ref, self.ref.n)
            u = self.cur.u if np.dot(self.cur.u, self.ref.u) >= 0 else -self.cur.u
            out.append(o_cur + s * u + d * self.cur.n)
        return np.asarray(out)


SEASIDE_GROUP_RECORDERS = {}   # 해측 카메라 키 -> (VideoRecorder, cam_id): 묶음 4분할 영상·함께 저장 사진


class SeaSideGroupVideo(ConeGroupVideo):
    """해측 묶음 4분할 영상(이벤트 순간 앞뒤, 단말 저장 events/seaside_group/<묶음>/videos). 이벤트 카메라는 빨간 테두리."""
    KIND = "seaside_group"
    RECORDERS = SEASIDE_GROUP_RECORDERS
    HIT_TAG = "PERSON"
    LOG = "[SEASIDE_GROUP_VIDEO]"


class SeaSideGroup:
    """같은 크레인 해측 카메라 묶음: 묶음 쿨다운·함께 저장. 같은 사람인지는 따지지 않습니다.
    묶이지 않은 카메라는 자기 키 이름의 1대 묶음입니다."""
    KIND = "seaside_group"
    RECORDERS = SEASIDE_GROUP_RECORDERS
    _groups = {}
    _registry_lock = threading.Lock()

    @classmethod
    def for_camera(cls, camera_key, cfg):
        name = next((g for g, members in (cfg.get("groups") or {}).items() if camera_key in members), camera_key)
        with cls._registry_lock:
            if name not in cls._groups:
                cls._groups[name] = cls(name, cfg)
                logger.info(f"[SEASIDE] 묶음 '{name}' 시작 (카메라 {(cfg.get('groups') or {}).get(name, [camera_key])})")
            return cls._groups[name]

    def __init__(self, name, cfg):
        self.name, self.cfg = name, cfg
        self.lock = threading.Lock()
        self.last_event_t = None
        self.seq = 0
        self.video = None

    @property
    def members(self):
        return (self.cfg.get("groups") or {}).get(self.name) or []

    def claim(self, camera_key, now):
        """이 카메라가 이벤트를 내도 되는지(묶음 쿨다운). 되면 지금 시각을 묶음 마지막 이벤트로 잡습니다."""
        cooldown = float(self.cfg["group_cooldown_sec"])
        with self.lock:
            if self.last_event_t is not None and now - self.last_event_t < cooldown:
                logger.debug(f"[SEASIDE] group={self.name} 묶음 쿨다운 중({now - self.last_event_t:.0f}/{cooldown:.0f}초) "
                             f"{camera_key} 조건 충족 → 새 이벤트 없음")
                return False
            self.last_event_t = now
            self.seq += 1
            return True

    def on_event(self, camera_key, event_id):
        """이벤트를 낸 뒤: 묶인 카메라가 2대 이상이면 4분할 영상을 시작하고 나머지 카메라 그 순간 사진을 남깁니다."""
        if len(self.members) < 2:
            return {}
        if self.cfg.get("group_video", True):
            self.video = SeaSideGroupVideo(self.name, self.seq, self.members, [camera_key], None)
            self.video.start()
        return save_group_companions(self, camera_key, event_id)


class SeaSidePersonDetector(BaseEventDetector):
    """해측 위험 구역(설치 때 그린 구역을 지금 끝선에 맞춰 옮긴 것) 안에 사람 발 위치가 person_hold_sec 이상 머물면
    알람합니다(카메라마다 판정). 이벤트를 낼지는 Camera가 묶음 쿨다운(SeaSideGroup.claim)으로 한 번 더 봅니다."""
    gui_name = "SEASIDE PERSON"
    # topdown(1280 입력)을 Backreach·YT와 같이 쓰므로 가로 1280으로 받습니다.
    stream_max_width = 1280

    def __init__(self, config, roi_poly=None, roi_lines=None):
        super().__init__(deep_merge_dict(SEASIDE_DEFAULT_CONFIG, config), roi_poly, roi_lines)
        c = self.config
        self.ready = all(c.get(k) for k in ("edge_norm", "sea_norm", "zone_norm", "land_ref", "water_ref"))
        self.exclude = [p for p in (c.get("exclude_polys_norm") or []) if len(p) >= 3]
        self.tracker = None       # 첫 프레임 크기로 만듦(작업 폭 px 좌표)
        self.group = None
        self.inside = {}          # topdown 추적 번호 -> {"since", "last"}
        self.near_until = 0.0     # 이 시각(monotonic)까지 매 프레임 추론(Camera.backreach_inference_due)
        self.overlay = None       # 화면 그리기(Camera.draw, 정규화 좌표)
        self.zone_now = None      # 지금 위험 구역(정규화 좌표)
        self._last_warn = 0.0

    def near(self, now):
        return now < self.near_until

    def _warn(self, t, text):
        if t - self._last_warn >= 30.0:
            self._last_warn = t
            logger.warning(text)

    def _make_tracker(self, cam_key, small):
        c = self.config
        h, w = small.shape[:2]
        px = lambda pts: [[float(x) * w, float(y) * h] for x, y in pts]
        sea = px([c["sea_norm"]])[0]
        p1, p2 = px(c["edge_norm"])
        ref = EdgeLine(p1, p2)
        ref = EdgeLine(p1, p2, land_n=-ref.n if ref.offset(sea) > 0 else ref.n)   # 바다 점의 반대쪽이 육지
        self.zone_px = np.asarray(px(c["zone_norm"]))
        self.tracker = SeaSideEdgeTracker(cam_key, c["edge"], ref, np.asarray(c["land_ref"], np.float64),
                                          np.asarray(c["water_ref"], np.float64))

    def process(self, tracks, track_map, frame, fid, **kwargs):
        cam_key = kwargs.get('camera_key')
        t = time.monotonic()
        if self.group is None:
            self.group = SeaSideGroup.for_camera(cam_key, self.config)
        if frame is None or not self.ready:
            self.overlay = None
            if not self.ready:
                self._warn(t, f"[SEASIDE] cam={cam_key} 설치 설정(끝선·바다 점·위험 구역·색)이 없어 판정하지 않습니다. "
                              f"마법사에서 설정하세요.")
            return []
        h, w = frame.shape[:2]
        small = cv2.resize(frame, (SEASIDE_WORK_W, max(1, int(h * SEASIDE_WORK_W / w))), interpolation=cv2.INTER_AREA)
        if self.tracker is None:
            self._make_tracker(cam_key, small)
        state = self.tracker.update(small, t)
        sh, sw = small.shape[:2]
        zone = self.tracker.map_points(self.zone_px) / [sw, sh]
        self.zone_now = zone
        self.overlay = self._overlay(state, sw, sh)
        if state == "UNKNOWN":
            self.inside = {}
            self._warn(t, f"[SEASIDE] cam={cam_key} 끝선을 찾지 못해 위험 구역을 모름 → 해측 판정 중지")
            return []
        contour = zone.astype(np.float32).reshape(-1, 1, 2)
        hold, gap = float(self.config["person_hold_sec"]), float(self.config["gap_tolerance_sec"])
        near_d = float(self.config["near_ratio"])
        triggered, frame_copy = [], None
        for person in _yt_parse_tracks(kwargs.get('topdown_tracks')):
            if person.name not in self.config["person_classes"]:
                continue
            # 비스듬한 시점이라 발 위치 = 박스 아래 가운데
            u, v = (person.box[0] + person.box[2]) / 2.0 / w, person.box[3] / h
            dist = cv2.pointPolygonTest(contour, (float(u), float(v)), True)   # 안 +, 밖 -(정규화 단위)
            inside = dist >= 0
            self.overlay["persons"].append((u, v, inside))
            if dist >= -near_d:
                self.near_until = t + float(self.config["near_hold_sec"])
            if not inside or any(point_in_polygon(p, u, v) for p in self.exclude):
                continue
            s = self.inside.get(person.tid)
            if s is None or t - s["last"] > gap:
                s = self.inside[person.tid] = {"since": t, "last": t}
            s["last"] = t
            if t - s["since"] < hold:
                continue
            if frame_copy is None:
                frame_copy = frame.copy()
            obj = _yt_event_object(person.name, person.box, person.score, person.tid, person.cls)
            triggered.append({
                'tid': obj['tid'],
                'bbox': [float(b) for b in person.box],
                'frame': frame_copy,
                'fid': fid,
                'privacy_tracks': kwargs.get('privacy_tracks', []),
                'privacy_fid': fid,
                'objects': [obj],
                'decision_trace': {
                    'detector': 'SeaSidePersonDetector',
                    'reason': 'person_in_seaside_zone',
                    'group': self.group.name,
                    'foot_norm': [round(u, 4), round(v, 4)],
                    'zone_norm': [[round(float(x), 4), round(float(y), 4)] for x, y in zone],
                    'edge_mode': self.tracker.mode, 'edge_state': state,
                    'inside_sec': round(t - s["since"], 2), 'person_hold_sec': hold,
                },
            })
        for tid, s in self.inside.items():
            # 위험 구역에 기준(person_hold_sec)의 절반 넘게 머물다 기준 전에 나간 사람을 한 줄 남깁니다(정탐·오탐 분석용).
            stayed = s["last"] - s["since"]
            if t - s["last"] > gap and hold / 2 <= stayed < hold:
                logger.info(f"[SEASIDE] cam={cam_key} 사람 tid={tid} 위험 구역에 {stayed:.1f}초 머물다 나감(기준 {hold:g}초)")
        self.inside = {tid: s for tid, s in self.inside.items() if t - s["last"] <= gap}
        return triggered

    def _overlay(self, state, sw, sh):
        """화면에 그릴 것(정규화 좌표): 지금 위험 구역, 지금 끝선(따라가는 선), 이번에 찾은 끝선, 사람 발 위치."""
        tr = self.tracker

        def seg(line):
            a, b = line.p1 - line.u * 2 * sw, line.p1 + line.u * 2 * sw
            ok, q1, q2 = cv2.clipLine((0, 0, sw, sh), (int(a[0]), int(a[1])), (int(b[0]), int(b[1])))
            return [[q1[0] / sw, q1[1] / sh], [q2[0] / sw, q2[1] / sh]] if ok else []

        return {"state": state, "group": self.group.name, "zone": [tuple(p) for p in self.zone_now],
                "edge": seg(tr.cur), "edge_found": seg(tr.last_found) if tr.last_found is not None else [],
                "persons": []}
# ============================================================================
# [해측 추락 방지 모듈 끝]
# ============================================================================


EVENT_REGISTRY = {
    BACKREACH_EVENT: BackreachDetector,
    SPEEDING_EVENT: SpeedingDetector,
    CONE_NG_EVENT: ConeNgDetector,
    YT_HEAD_CRUSH_EVENT: YTHeadCrushDetector,
    YT_ZONE_PERSON_EVENT: YTZonePersonDetector,
    SEASIDE_PERSON_EVENT: SeaSidePersonDetector,
}

# ==========================================
# [9] 터미널 마법사 및 설정 UI
# ==========================================
def _flush_terminal_input():
    """원격 터미널에서 다음 질문으로 넘어온 잔여 키 입력을 비웁니다."""
    try:
        if not sys.stdin or not sys.stdin.isatty():
            return
        import termios
        termios.tcflush(sys.stdin, termios.TCIFLUSH)
    except Exception as e:
        logger.debug(f"터미널 입력 버퍼 정리 실패: {e}")

def _flush_cv2_key_buffer(duration_sec=0.10):
    """OpenCV 창에 남은 Enter/ESC 키 이벤트가 다음 단계로 넘어가지 않게 비웁니다."""
    end_time = time.time() + max(0.0, float(duration_sec))
    while time.time() < end_time:
        try:
            cv2.waitKey(1)
        except Exception:
            break
        time.sleep(0.01)

def guard_interactive_input():
    # RDP/VNC/SSH 환경에서는 키 입력이 늦게 도착해 다음 input/ROI 창에 들어가는 경우가 있습니다.
    # 짧게 기다린 뒤 OpenCV 키 큐와 터미널 입력 큐를 비워 연속 Enter 오입력을 줄입니다.
    guard_sec = max(0.0, float(SYS_CFG["INTERACTIVE_INPUT_GUARD_SEC"]))
    if guard_sec > 0:
        time.sleep(guard_sec)
    _flush_cv2_key_buffer(min(0.15, guard_sec if guard_sec > 0 else 0.10))
    _flush_terminal_input()

def guarded_input(prompt):
    guard_interactive_input()
    return input(prompt)

def capture_snapshot(url):
    """설정 마법사용 스냅샷 캡처"""
    try:
        cap = cv2.VideoCapture(sanitize_camera_url(url), cv2.CAP_FFMPEG)
        cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        if not cap.isOpened():
            return None
        ret, frame = cap.read()
        cap.release()
        return frame if ret else None
    except Exception as e:
        logger.error(f"스냅샷 캡처 실패: {e}")
        return None

def get_roi_points_scaled(frame, title, required=False):
    """마우스 클릭으로 다각형 ROI(관심 영역)를 받습니다."""
    pts = []
    orig_h, orig_w = frame.shape[:2]
    scale = 960 / orig_w
    disp_h = int(orig_h * scale)
    disp_frame = cv2.resize(frame, (960, disp_h))

    guard_interactive_input()

    cv2.namedWindow(title)
    def mouse_cb(e, x, y, f, p):
        if e == cv2.EVENT_LBUTTONDOWN:
            pts.append([int(x / scale), int(y / scale)])

    cv2.setMouseCallback(title, mouse_cb)

    if required:
        logger.info(f"'{title}' 설정 - 경계를 따라 3개 이상의 점을 클릭하십시오. (Enter: 완료, ESC: 이 카메라 설정 취소)")
    else:
        logger.info(f"'{title}' 설정 - 화면을 클릭하여 점을 찍으십시오. (전체 화면 적용 시 그냥 Enter 또는 ESC)")

    while True:
        temp = disp_frame.copy()
        dp = [[int(p[0] * scale), int(p[1] * scale)] for p in pts]

        if len(dp) > 0:
            cv2.polylines(temp, [np.array(dp, np.int32)], True, (0, 255, 0), 2)

        cv2.imshow(title, temp)
        k = cv2.waitKey(1)
        if k == 13: # Enter
            if required and polygon_contour(pts) is None:
                logger.warning("면적이 있는 다각형이 필요합니다. 점을 추가하거나 ESC로 취소하십시오.")
                continue
            break
        if k == 27: # ESC
            pts = []
            break

    cv2.destroyWindow(title)
    guard_interactive_input()
    return normalize_roi_points(pts, orig_w, orig_h)

def get_numbered_points(frame, title, count):
    """번호를 붙여 보여 주며 점 count개를 클릭받습니다(Backspace 마지막 점 지우기, Enter 완료, ESC 취소).
    반환 정규화 점 목록(취소하면 None)."""
    pts = []
    orig_h, orig_w = frame.shape[:2]
    scale = 960 / orig_w
    disp_frame = cv2.resize(frame, (960, int(orig_h * scale)))
    guard_interactive_input()
    cv2.namedWindow(title)

    def mouse_cb(e, x, y, f, p):
        if e == cv2.EVENT_LBUTTONDOWN and len(pts) < count:
            pts.append([int(x / scale), int(y / scale)])

    cv2.setMouseCallback(title, mouse_cb)
    while True:
        temp = disp_frame.copy()
        for i, (x, y) in enumerate(pts):
            pt = (int(x * scale), int(y * scale))
            cv2.circle(temp, pt, 5, (0, 0, 255), -1)
            cv2.putText(temp, str(i + 1), (pt[0] + 7, pt[1] - 7), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 0), 3)
            cv2.putText(temp, str(i + 1), (pt[0] + 7, pt[1] - 7), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 255), 1)
        if len(pts) >= 2:
            cv2.line(temp, (int(pts[0][0] * scale), int(pts[0][1] * scale)),
                     (int(pts[1][0] * scale), int(pts[1][1] * scale)), (255, 0, 0), 2)
        cv2.imshow(title, temp)
        k = cv2.waitKey(1)
        if k == 13:
            if len(pts) < count:
                logger.warning(f"점 {count}개가 필요합니다(지금 {len(pts)}개).")
                continue
            break
        if k == 8 and pts:
            pts.pop()
        if k == 27:
            pts = None
            break
    cv2.destroyWindow(title)
    guard_interactive_input()
    return None if pts is None else normalize_roi_points(pts, orig_w, orig_h)


def seaside_snap_edge(small, p1, p2, band_px=15):
    """클릭한 끝선 두 점(작업 폭 px)을 근처(band_px)에서 클릭한 선에 가장 가까운 나란한 직선에 맞춥니다.
    가장 긴 직선을 고르면 바로 옆 노란 안전선·흰 테두리 반대편에 붙을 수 있습니다. 없으면 클릭 그대로."""
    gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)
    clicked = EdgeLine(p1, p2)
    cands = [c for c in seaside_edge_candidates(gray, clicked, band_px, max_angle_deg=5.0) if c[2] >= 0.2]
    if not cands:
        return p1, p2, False
    q1, q2, _ = min(cands, key=lambda c: max(abs(EdgeLine(c[0], c[1]).offset(p)) for p in (p1, p2)))
    line = EdgeLine(q1, q2)
    # 클릭한 두 점의 자리(따라가는 위치)는 그대로 두고 직선 위로만 옮깁니다.
    return line.foot(p1).tolist(), line.foot(p2).tolist(), True


def run_wizard_seaside_setup(frame, ip, old_cfg=None):
    """해측 추락 방지 카메라 설정(거리 측정 없음): 스냅샷에서 안벽 끝선 두 점 + 바다 쪽 한 점을 클릭하고, 위험 구역을
    그립니다. 끝선 양옆 육지·물 색은 자동으로 남깁니다. 반환 cameras.json event_config.seaside_person 값(취소하면 None).
    크레인이 움직이면 앱이 끝선을 다시 찾아 그린 구역을 같이 옮깁니다."""
    old_cfg = old_cfg or {}
    if old_cfg.get("edge_norm") and old_cfg.get("zone_norm") and guarded_input(
            f"[{ip}] 기존 해측 설정(끝선·위험 구역)을 그대로 쓸까요? [Y/n]: ").strip().lower() != 'n':
        return {k: old_cfg[k] for k in ("edge_norm", "sea_norm", "zone_norm", "land_ref", "water_ref",
                                       "exclude_polys_norm") if k in old_cfg}
    print(f"[{ip}] 해측 설정: 1·2번 = 안벽 끝선(바다와 안벽 경계) 위 두 점(서로 멀리), 3번 = 바다 쪽 아무 곳. Enter 완료")
    pts = get_numbered_points(frame, f"Seaside quay edge (1,2) + sea (3) - CAM: {ip}", 3)
    if pts is None:
        return None
    h, w = frame.shape[:2]
    small = cv2.resize(frame, (SEASIDE_WORK_W, max(1, int(h * SEASIDE_WORK_W / w))), interpolation=cv2.INTER_AREA)
    sh, sw = small.shape[:2]
    p1, p2, snapped = seaside_snap_edge(small, [pts[0][0] * sw, pts[0][1] * sh], [pts[1][0] * sw, pts[1][1] * sh])
    print("   끝선을 화면의 직선에 맞췄습니다." if snapped else "   근처에 직선이 없어 클릭한 그대로 씁니다.")
    sea = [pts[2][0] * sw, pts[2][1] * sh]
    line = EdgeLine(p1, p2)
    line = EdgeLine(p1, p2, land_n=-line.n if line.offset(sea) > 0 else line.n)
    land_ref, water_ref = seaside_color_refs(small, line)
    if land_ref is None:
        print("   끝선 양옆 색을 읽지 못했습니다(끝선이 화면 가장자리). 다시 설정하세요.")
        return None
    print(f"   육지 색 BGR {np.round(land_ref).astype(int).tolist()}, 물 색 BGR {np.round(water_ref).astype(int).tolist()}")
    zone = get_roi_points_scaled(frame, f"Seaside danger zone - CAM: {ip}", required=True)
    if polygon_contour(zone) is None:
        return None
    excludes = [p for p in (old_cfg.get("exclude_polys_norm") or []) if len(p) >= 3]
    ans = guarded_input(f"[{ip}] 크레인 구조물 제외 구역 (기존 {len(excludes)}개) 그대로 엔터 / 새로 그리기 y / 모두 지우기 0: ").strip().lower()
    if ans == '0':
        excludes = []
    elif ans == 'y':
        excludes = []
        while True:
            poly = get_roi_points_scaled(frame, f"Seaside exclude {len(excludes) + 1} - CAM: {ip}")
            if polygon_contour(poly) is not None:
                excludes.append(poly)
            if guarded_input("   제외 구역을 더 그릴까요? [y/N]: ").strip().lower() != 'y':
                break
    return {"edge_norm": [[round(p1[0] / sw, 6), round(p1[1] / sh, 6)], [round(p2[0] / sw, 6), round(p2[1] / sh, 6)]],
            "sea_norm": [round(pts[2][0], 6), round(pts[2][1], 6)], "zone_norm": zone,
            "land_ref": [round(float(v), 1) for v in land_ref], "water_ref": [round(float(v), 1) for v in water_ref],
            "exclude_polys_norm": excludes}


def run_wizard_speeding_directions(ip, old_cfg=None):
    """과속 단속 방향을 묻습니다: forward(카메라로 다가오는 차, 앞번호판), backward(멀어지는 차, 뒷번호판). 둘 다 켜면 양쪽.
    엔터는 예전 값(없으면 기본 설정). 반환 cameras.json event_config.speeding(측정·사진 구간 등 다른 키는 예전 값 그대로)."""
    cfg = dict(old_cfg or {})
    base = SYS_CFG["event_config"][SPEEDING_EVENT]

    def ask(key, text):
        default = bool(cfg.get(key, base.get(key, key == "forward")))
        ans = guarded_input(f"[{ip}] {text} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
        return default if not ans else ans == 'y'
    forward = ask("forward", "카메라로 다가오는 차 과속 단속(forward, 앞번호판)?")
    backward = ask("backward", "카메라에서 멀어지는 차 과속 단속(backward, 뒷번호판)?")
    if not (forward or backward):
        print("   둘 다 끄면 과속 판정을 하지 않아 다가오는 차(forward)를 켭니다.")
        forward = True
    cfg.update(forward=forward, backward=backward)
    return cfg


def run_wizard_batch_mode(rtsp_list, existing_configs=None):
    logger.info("=== 설정 마법사 시작 ===")
    # 기존 설정을 복사해 둡니다. 마법사에서 고르지 않은 카메라는 그대로 두고, 다시 정한 카메라만 새로 씁니다.
    configs = existing_configs.copy() if existing_configs else {}

    for i in range(0, len(rtsp_list), BATCH_SIZE):
        batch = rtsp_list[i : i + BATCH_SIZE]

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            frames = list(executor.map(capture_snapshot, batch))

        display = []
        for frm in frames:
            if frm is None:
                blk = np.zeros((360, 640, 3), dtype=np.uint8)
                cv2.putText(blk, "Conn Fail", (50, 180), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 1)
                display.append(blk)
            else:
                display.append(frm)

        mosaic = create_mosaic_image(display)
        cols = max(1, math.ceil(math.sqrt(len(display))))
        rows = max(1, math.ceil(len(display) / cols))
        cw = SCREEN_WIDTH // cols
        ch = SCREEN_HEIGHT // rows

        for idx in range(len(display)):
            r, c = divmod(idx, cols)
            cx, cy = c * cw, r * ch
            cv2.rectangle(mosaic, (cx, cy), (cx + 50, cy + 50), (255, 255, 255), -1)

            # 배치 안 번호가 아니라 CSV 전체 기준 번호
            abs_cam_id = i + idx + 1
            cv2.putText(mosaic, str(abs_cam_id), (cx + 10, cy + 40), cv2.FONT_HERSHEY_SIMPLEX, 1.5, (0, 0, 0), 1)

        cv2.imshow("Select Cameras", mosaic)
        cv2.waitKey(1)

        # 입력 안내도 CSV 전체 기준 번호
        example_ids = f"{i+1},{i+2}" if len(batch) > 1 else f"{i+1}"
        sel = guarded_input(f">> [Batch {i//BATCH_SIZE + 1}] 설정할 카메라 번호 (예: {example_ids} / 건너뛰기: 엔터): ").strip()
        if not sel:
            continue

        try:
            nums = [int(s.strip()) for s in sel.split(',')]
            for n in nums:
                # 입력한 CSV 기준 번호를 이 배치 안 위치로 바꿉니다.
                local_idx = n - i - 1
                if 0 <= local_idx < len(batch) and frames[local_idx] is not None:
                    url = batch[local_idx]
                    ip = extract_ip(url)

                    print(f"[{ip}] 1.Backreach 2.과속 3.콘 미제거 출차 4.YT 헤드압착 5.YT 위험구역 사람 진입 6.해측 추락 방지")
                    evts = guarded_input(f"[{ip}] 이벤트 선택 (예: 1,3): ")
                    events = []

                    selected_events = {s.strip() for s in evts.split(',') if s.strip()}

                    if '1' in selected_events: events.append(BACKREACH_EVENT)
                    if '2' in selected_events: events.append(SPEEDING_EVENT)
                    if '3' in selected_events: events.append(CONE_NG_EVENT)
                    if '4' in selected_events: events.append(YT_HEAD_CRUSH_EVENT)
                    if '5' in selected_events: events.append(YT_ZONE_PERSON_EVENT)
                    if '6' in selected_events: events.append(SEASIDE_PERSON_EVENT)

                    roi_p = []

                    if BACKREACH_EVENT in events:
                        # Backreach는 다각형 ROI가 반드시 필요합니다. ESC로 취소하면 이 카메라는 기존 설정을 유지합니다.
                        roi_p = get_roi_points_scaled(frames[local_idx], f"Backreach Polygon - CAM: {ip}", required=True)
                        if polygon_contour(roi_p) is None:
                            logger.info(f"[CAM:{ip}] Backreach ROI 설정을 취소했습니다. 기존 설정을 유지합니다.")
                            continue
                    elif YT_HEAD_CRUSH_EVENT in events:
                        # YT 작업 차선 ROI(초록, 선택): 있으면 그 안의 YT만 감시하고, 없으면 화면 전체를 감시합니다.
                        if guarded_input("YT 작업 차선 ROI(초록)를 설정하시겠습니까? (없으면 화면 전체 감시) [y/N]: ").strip().lower() == 'y':
                            roi_p = get_roi_points_scaled(frames[local_idx], f"YT Work Lane (green) - CAM: {ip}")
                    elif SPEEDING_EVENT in events:
                        roi_p = get_roi_points_scaled(frames[local_idx], f"Polygon - CAM: {ip}")
                    # 콘 미제거 출차는 ROI 없이 화면 전체에서 컨테이너·콘을 찾습니다.
                    # YT 위험구역(5번, 노랑)은 그리지 않습니다. 감시가 시작되면 빈 샤시로 자동으로 만듭니다.
                    old_conf = configs.get(ip) or {}
                    speeding_cfg = None
                    if SPEEDING_EVENT in events:
                        # 과속: ROI(단속 차로)는 위에서 그렸고, 여기서 단속 방향(다가오는 차·멀어지는 차)을 묻습니다.
                        speeding_cfg = run_wizard_speeding_directions(
                            ip, (old_conf.get("event_config") or {}).get(SPEEDING_EVENT))
                    seaside_cfg = None
                    if SEASIDE_PERSON_EVENT in events:
                        # 해측: 안벽 끝선·바다 쪽 점·위험 구역을 그립니다(거리 측정 없음). 취소하면 기존 설정 유지.
                        old_seaside = ((configs.get(ip) or {}).get("event_config") or {}).get(SEASIDE_PERSON_EVENT)
                        seaside_cfg = run_wizard_seaside_setup(frames[local_idx], ip, old_seaside)
                        if seaside_cfg is None:
                            logger.info(f"[CAM:{ip}] 해측 설정을 취소했습니다. 기존 설정을 유지합니다.")
                            continue

                    # 다시 정한 카메라는 예전 설정 위에 덧쓰지 않고 새로 씁니다.
                    # 관제 켜짐·꺼짐(receiver_event_status)은 새로 고른 이벤트 것만 남깁니다.
                    old_status = (configs.get(ip) or {}).get("receiver_event_status")
                    configs[ip] = {
                        "url": url,
                        "events": events,
                        "roi_poly_norm": roi_p,
                        "roi_lines_norm": []
                    }
                    event_cfg = {}
                    if seaside_cfg is not None:
                        event_cfg[SEASIDE_PERSON_EVENT] = seaside_cfg
                    if speeding_cfg is not None:
                        event_cfg[SPEEDING_EVENT] = speeding_cfg
                    if event_cfg:
                        configs[ip]["event_config"] = event_cfg
                    if SPEEDING_EVENT in events and old_conf.get("radar_client_id") is not None:
                        # 과속 카메라의 레이더 번호(사람이 적은 값)는 다시 정해도 그대로 둡니다.
                        configs[ip]["radar_client_id"] = old_conf["radar_client_id"]
                    if isinstance(old_status, dict):
                        kept_status = {name: on for name, on in old_status.items() if name in events}
                        if kept_status:
                            configs[ip]["receiver_event_status"] = kept_status
        except Exception as e:
            logger.error(f"마법사 설정 중 오류 발생: {e}")

    cv2.destroyWindow("Select Cameras")
    return configs


# 카메라 여러 대를 한 묶음(크레인 하나)으로 판정하는 이벤트: {이벤트: (마법사 표시 이름, 묶음 영상 칸 수)}.
# 묶음은 event_config.<이벤트>.groups {"묶음 이름": [카메라 키(IP), ...]}에 저장합니다. 다른 이벤트는 카메라마다 판정합니다.
GROUP_EVENTS = {CONE_NG_EVENT: ("콘 미제거 출차", 4), SEASIDE_PERSON_EVENT: ("해측 추락 방지", 4)}


def save_event_groups(event_name, groups):
    """event_config.<이벤트>.groups를 실행 중 설정(SYS_CFG)과 system_config.json에 함께 씁니다."""
    SYS_CFG["event_config"][event_name]["groups"] = groups
    try:
        with open(CONFIG_COMMON_FILE, 'r', encoding='utf-8') as f:
            cfg = json.load(f)
        cfg.setdefault("event_config", {}).setdefault(event_name, {})["groups"] = groups
        with open(CONFIG_COMMON_FILE, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=4, ensure_ascii=False)
    except Exception as e:
        logger.error(f"system_config.json에 묶음 저장 실패({event_name}): {e}")


def run_wizard_groups(rtsp_list, configs):
    """마법사 마지막 단계: 묶음 이벤트(GROUP_EVENTS)를 고른 카메라가 2대 이상이면 묶음을 정합니다.
    번호는 마법사·관제(cctvId)와 같은 cameras.csv 순서 번호입니다. 기본값은 지금 묶음, 없으면 전부를 크레인 하나로."""
    crane = str(SYS_CFG.get("crane_id") or "").strip() or "crane1"
    for event_name, (label, tiles) in GROUP_EVENTS.items():
        cams = {i + 1: extract_ip(url) for i, url in enumerate(rtsp_list)
                if event_name in ((configs.get(extract_ip(url)) or {}).get("events") or [])}
        if len(cams) < 2:
            continue
        num_of = {ip: n for n, ip in cams.items()}
        current = [[num_of[k] for k in members if k in num_of]
                   for members in (SYS_CFG["event_config"][event_name].get("groups") or {}).values()]
        current = [g for g in current if len(g) >= 2] or [sorted(cams)]
        show = lambda gs: "; ".join(",".join(map(str, g)) for g in gs)
        print(f"\n[{label}] 카메라 " + ", ".join(f"{n}({ip})" for n, ip in sorted(cams.items())))
        print(f"   한 묶음 = 크레인 하나(같은 작업을 함께 판정). 묶음 영상은 앞 {tiles}대를 적은 순서대로 칸에 넣습니다.")
        while True:
            ans = guarded_input(f">> 묶음 [{show(current)}] 그대로면 엔터 / 다시 정하려면 번호 "
                                f"(예: 1,2,3,4  여러 묶음 1,2;3,4  묶지 않음 0): ").strip()
            if not ans:
                chosen = current
            elif ans == "0":
                chosen = []
            else:
                try:
                    chosen = [[int(s) for s in part.split(',') if s.strip()] for part in ans.split(';') if part.strip()]
                except ValueError:
                    print("   숫자와 , ; 만 적어 주세요.")
                    continue
                flat = [n for g in chosen for n in g]
                if any(n not in cams for n in flat) or len(flat) != len(set(flat)) or any(len(g) < 2 for g in chosen):
                    print(f"   {sorted(cams)} 중에서, 한 번씩만, 묶음마다 2대 이상 적어 주세요.")
                    continue
            break
        names = [crane] if len(chosen) == 1 else [f"{crane}_{k + 1}" for k in range(len(chosen))]
        groups = {name: [cams[n] for n in g] for name, g in zip(names, chosen)}
        save_event_groups(event_name, groups)
        logger.info(f"[WIZARD] {label} 묶음: " + (", ".join(f"{name}={g}" for name, g in zip(names, chosen)) or "없음"))
        for g in chosen:
            if len(g) > tiles:
                logger.warning(f"[WIZARD] {label} 묶음 {g}: 판정은 모두 함께 하지만 묶음 영상에는 앞 {tiles}대만 들어갑니다.")

# ==========================================
# [10] 카메라 제어 (FrameReader / Camera)
# ==========================================


class PacketRing:
    """카메라 1대의 원본 HEVC 세그먼트 링버퍼.
    상시 디코딩과 같은 ffmpeg 프로세스가 -c copy로 쓰므로 세그먼트 PTS가 frame_meta['pts']와 프레임 단위로 같습니다."""
    LIST_NAME = "list.csv"

    def __init__(self, cam_key, cfg):
        self.ring_sec = max(4.0, float(cfg["ring_sec"]))
        # -c copy는 키프레임에서만 잘리므로 세그먼트는 최소 1초(실제로는 GOP 길이)입니다.
        # 개수를 ring_sec+2로 두면 보존 시간이 ring_sec 이상으로 보장됩니다.
        self.wrap = int(math.ceil(self.ring_sec)) + 2
        safe_key = re.sub(r"[^0-9A-Za-z_.-]", "_", str(cam_key))
        self.root = Path(cfg["ring_dir"]) / safe_key
        self.gen = 0

    def new_generation(self):
        """ffmpeg 재시작마다 PTS가 0부터 다시 시작하므로 실행마다 디렉터리를 새로 씁니다."""
        self.gen += 1
        self.root.mkdir(parents=True, exist_ok=True)
        self._cleanup()
        gen_dir = self.root / f"{os.getpid()}_{self.gen:04d}"
        if gen_dir.exists():
            shutil.rmtree(gen_dir, ignore_errors=True)
        gen_dir.mkdir()
        return gen_dir

    def _cleanup(self):
        """마지막 기록 후 ring_sec*3이 지난 이전 실행 디렉터리를 지웁니다(추출 대기 중인 요청 보호)."""
        now = time.time()
        for child in self.root.iterdir():
            if not child.is_dir():
                continue
            try:
                last = max([child.stat().st_mtime] + [f.stat().st_mtime for f in child.iterdir()])
            except OSError:
                continue
            if now - last > self.ring_sec * 3:
                shutil.rmtree(child, ignore_errors=True)

    def segment_args(self, gen_dir):
        # list_size를 wrap-1로 두어 지금 덮어쓰는 중인 파일이 목록에 남지 않게 합니다.
        # muxdelay/muxpreload 0: mpegts 기본 지연(1.4초) 없이 라이브 showinfo PTS와 같은 값을 씁니다.
        return ["-map", "0:v:0", "-an", "-c", "copy",
                "-f", "segment", "-segment_time", "1", "-segment_format", "mpegts",
                "-muxdelay", "0", "-muxpreload", "0", "-reset_timestamps", "0",
                "-segment_wrap", str(self.wrap),
                "-segment_list", str(gen_dir / self.LIST_NAME), "-segment_list_type", "csv",
                "-segment_list_size", str(self.wrap - 1),
                str(gen_dir / "seg_%03d.ts")]

    @staticmethod
    def read_list(gen_dir):
        """[(파일명, 시작 PTS, 끝 PTS)] 시작 순. ffmpeg가 목록을 다시 쓰는 중이면 빈 목록일 수 있습니다."""
        try:
            text = (Path(gen_dir) / PacketRing.LIST_NAME).read_text(encoding="utf-8")
        except OSError:
            return []
        entries = []
        for line in text.splitlines():
            parts = line.strip().split(",")
            if len(parts) != 3:
                continue
            try:
                entries.append((parts[0], float(parts[1]), float(parts[2])))
            except ValueError:
                continue
        return sorted(entries, key=lambda e: e[1])

    @staticmethod
    def segments_for(gen_dir, t0, t1):
        return [e for e in PacketRing.read_list(gen_dir) if e[2] > t0 and e[1] <= t1]


class FullResExtractor:
    """이벤트 구간 [t0, t1]의 원본 프레임을 링버퍼 세그먼트에서 디코딩해 이벤트 폴더에 저장합니다.
    카메라마다 워커 1개로 직렬 처리합니다(카메라끼리는 동시에). 한 건에 약 10~13초라, 워커 하나를 모든 카메라가
    나눠 쓰면 이벤트가 몰릴 때 대기 중 링버퍼가 덮어써져 번호판을 놓칩니다."""
    _SHOWINFO_PTS_RE = re.compile(r"Parsed_showinfo.*?\bn:\s*\d+\s+pts:\s*-?\d+\s+pts_time:\s*(-?[\d.]+)")

    def __init__(self, cfg):
        self.cfg = cfg
        self._workers = {}  # 카메라 ip -> (요청 큐, 워커 스레드)
        self._lock = threading.Lock()

    def request(self, job):
        key = str(job.get("ip"))
        with self._lock:
            jobs, thread = self._workers.get(key) or (queue.Queue(maxsize=32), None)
            if thread is None or not thread.is_alive():
                thread = threading.Thread(target=self._worker, args=(jobs,), name=f"fullres-{job.get('cam_id')}",
                                          daemon=True)
                thread.start()
            self._workers[key] = (jobs, thread)
        try:
            jobs.put_nowait(job)
            return True
        except queue.Full:
            logger.warning(f"[FULLRES] 요청 큐가 가득 차 건너뜁니다. event_id={job.get('event_id')}")
            return False

    def _worker(self, jobs):
        while True:
            job = jobs.get()
            try:
                self._process(job)
            except Exception as e:
                logger.error(f"[FULLRES] 처리 실패 event_id={job.get('event_id')} | {e}\n{traceback.format_exc()}")

    def _wait_segments(self, gen_dir, t0, t1, event_pts):
        """t1까지 담긴 세그먼트가 닫힐 때까지 기다립니다. 재접속 등으로 끝내 안 오면 있는 것만 씁니다."""
        deadline = time.monotonic() + max(0.0, t1 - event_pts) + 10.0
        segs = []
        while True:
            segs = PacketRing.segments_for(gen_dir, t0, t1) or segs
            if segs and segs[-1][2] >= t1:
                return segs, True
            if time.monotonic() > deadline:
                return segs, False
            time.sleep(0.5)

    def _process(self, job):
        started = time.monotonic()
        gen_dir, t0, t1 = Path(job["ring_dir"]), job["t0"], job["t1"]
        out_dir = Path(job["out_dir"])
        meta = {k: job[k] for k in ("event_id", "event_name", "cam_id", "ip", "fid", "event_pts", "t0", "t1",
                                    "every_n", "event_bbox", "inference_shape", "full_shape")}
        if job.get("follow_boxes"):
            meta["follow_boxes"] = job["follow_boxes"]
        if job.get("evidence"):
            meta["evidence"] = job["evidence"]
        for key in ("radar_path", "search_box", "before_box_search", "photo_m", "photo_source"):
            if job.get(key):
                meta[key] = job[key]
        meta.update({"ring_dir": str(gen_dir), "created_at": now_kst().isoformat(), "frames": []})

        segs, complete = self._wait_segments(gen_dir, t0, t1, job["event_pts"])
        waited = time.monotonic() - started
        out_dir.mkdir(parents=True, exist_ok=True)
        if not segs:
            meta["status"] = "no_segments"
            self._write_meta(out_dir, meta)
            logger.warning(f"[FULLRES] 링버퍼에 해당 구간이 없습니다. event_id={job['event_id']} window=[{t0:.3f},{t1:.3f}] ring={gen_dir}")
            return

        # 링이 덮어쓰기 전에 이벤트 폴더로 이어 붙여 고정합니다(mpegts는 바이트 연결로 이어집니다).
        clip = out_dir / "clip.ts"
        with open(clip, "wb") as dst:
            for name, _, _ in segs:
                with open(gen_dir / name, "rb") as src:
                    shutil.copyfileobj(src, dst)
        still_listed = {(e[0], e[1]) for e in PacketRing.read_list(gen_dir)}
        overwritten = [name for name, start, _ in segs if (name, start) not in still_listed]
        meta.update({"clip": clip.name, "segments": [list(e) for e in segs],
                     "covered": [segs[0][1], segs[-1][2]], "complete": complete,
                     "ring_overwrite_suspect": overwritten})

        frames_dir = out_dir / "frames"
        frames_dir.mkdir(exist_ok=True)
        use_hw = bool(self.cfg["hwaccel"])
        frames = self._decode(clip, job, frames_dir, use_hw)
        if not frames and use_hw:
            logger.warning(f"[FULLRES] VAAPI 디코딩 결과가 없어 CPU로 재시도합니다. event_id={job['event_id']}")
            use_hw = False
            frames = self._decode(clip, job, frames_dir, use_hw)

        meta["frames"] = frames
        meta["decode"] = {"hwaccel": use_hw, "wait_sec": round(waited, 3),
                          "decode_sec": round(time.monotonic() - started - waited, 3),
                          "elapsed_sec": round(time.monotonic() - started, 3)}
        meta["status"] = "ok" if frames else "decode_failed"
        self._write_meta(out_dir, meta)
        logger.info(
            f"[FULLRES] event_id={job['event_id']} status={meta['status']} frames={len(frames)} "
            f"window=[{t0:.3f},{t1:.3f}] covered=[{segs[0][1]:.3f},{segs[-1][2]:.3f}] complete={complete} "
            f"wait={meta['decode']['wait_sec']}s decode={meta['decode']['decode_sec']}s dir={out_dir}"
        )
        if meta["status"] == "ok":
            # 번호판 인식기(검출 NPU·OCR CPU)와 결과 CSV는 하나라 카메라 워커끼리 한 건씩 씁니다(약 3초).
            with PLR_LOCK:
                run_plr_on_fullres(out_dir, meta)

    def _decode(self, clip, job, frames_dir, use_hw):
        full_w, full_h = job["full_shape"][1], job["full_shape"][0]
        frame_size = full_w * full_h * 3 // 2
        # 두 번째 select의 n은 첫 select를 통과한 프레임만 셉니다 → 구간 안에서 every_n장마다 1장.
        vf = f"select='between(t,{job['t0']:.6f},{job['t1']:.6f})',select='not(mod(n,{int(job['every_n'])}))'"
        cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-loglevel", "info", "-copyts"]
        if use_hw:
            hw_device = str(SYS_CFG["video_decode"]["hw_device"]).strip()
            cmd.extend(["-hwaccel", "vaapi", "-hwaccel_device", hw_device, "-hwaccel_output_format", "vaapi"])
            vf += ",hwdownload,format=nv12"
        cmd.extend(["-i", str(clip), "-an", "-vf", vf + "," + showinfo_filter(), "-vsync", "passthrough",
                    "-f", "rawvideo", "-pix_fmt", "nv12", "pipe:1"])

        pts_list = []
        def drain(pipe):
            for raw_line in iter(pipe.readline, b""):
                m = self._SHOWINFO_PTS_RE.search(raw_line.decode("utf-8", "replace"))
                if m:
                    pts_list.append(float(m.group(1)))
        quality = int(self.cfg["jpeg_quality"])
        written = []
        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        drain_thread = threading.Thread(target=drain, args=(proc.stderr,), daemon=True)
        drain_thread.start()
        try:
            while True:
                raw = proc.stdout.read(frame_size)
                if len(raw) != frame_size:
                    break
                yuv = np.frombuffer(raw, dtype=np.uint8).reshape((full_h * 3 // 2, full_w))
                bgr = cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_NV12)
                name = f"frame_{len(written):03d}.jpg"
                if cv2.imwrite(str(frames_dir / name), bgr, [cv2.IMWRITE_JPEG_QUALITY, quality]):
                    written.append(name)
        finally:
            proc.stdout.close()
            proc.wait(timeout=30)
            drain_thread.join(timeout=2)
        # showinfo는 출력 직전 필터라 n번째 출력 프레임과 n번째 pts 줄이 1:1입니다.
        return [{"index": i, "file": f"frames/{name}",
                 "pts": pts_list[i] if i < len(pts_list) else None,
                 "dt_from_event": round(pts_list[i] - job["event_pts"], 4) if i < len(pts_list) else None}
                for i, name in enumerate(written)]

    @staticmethod
    def _write_meta(out_dir, meta):
        tmp = out_dir / ".fullres_meta.tmp"
        tmp.write_text(json.dumps(to_json_safe(meta), ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(out_dir / "fullres_meta.json")


FULLRES_EXTRACTOR = None
PLR_LOCK = threading.Lock()


def fullres_out_dir(cam, event_name, event_id):
    return os.path.join(EVENT_ROOT_DIR, "events", cam.ip, "fullres", str(event_name), str(event_id))


def fullres_enabled(event_name):
    cfg = SYS_CFG["fullres_capture"]
    return bool(cfg["enabled"]) and event_name in cfg["events"]


def box_cut_by_frame(box, frame_shape, pad=3):
    """박스가 화면 가장자리(왼쪽·오른쪽·아래)에 닿아 잘렸는지(추론 프레임 px). 잘린 박스는 번호판도 잘렸을 수 있습니다."""
    if frame_shape is None:
        return False
    h, w = float(frame_shape[0]), float(frame_shape[1])
    x1, _, x2, y2 = (float(v) for v in box[:4])
    return x1 <= pad - 1 or x2 >= w - pad or y2 >= h - pad


def box_cut_side(box, frame_shape, pad=3):
    """박스가 화면 왼쪽·오른쪽 끝에 닿았는지(차가 옆으로 빠져나가는 중, 번호판도 잘리기 쉬움). 위·아래는 보지 않습니다."""
    if frame_shape is None:
        return False
    w = float(frame_shape[1])
    return float(box[0]) <= pad - 1 or float(box[2]) >= w - pad


def radar_seen_points(radar_path, frame_shape):
    """레이더 경로 중 화면 안(가로 8~92%, 세로 95% 위)에 투영된 점. 화면 끝에 걸린 차는 번호판이 잘리기 쉬워 뺍니다."""
    if frame_shape is None:
        return []
    h, w = float(frame_shape[0]), float(frame_shape[1])
    return [q for q in radar_path or [] if len(q) >= 5 and q[3] is not None and q[4] is not None
            and 0.08 * w <= float(q[3]) <= 0.92 * w and float(q[4]) <= 0.95 * h]


def radar_nearer_than_box(radar_path, box_pts, frame_shape, margin_m=3.0):
    """박스로 고른 장면(box_pts)보다 레이더 경로가 화면 안에서 margin_m 넘게 더 가까이 간 적이 있는지.
    차 검출이 가까이 온 큰 트럭을 놓쳐 박스 따라가기가 일찍 끝난 경우입니다(레이더는 계속 따라감)."""
    seen = radar_seen_points(radar_path, frame_shape)
    if not seen:
        return False
    at = min(radar_path, key=lambda q: abs(float(q[0]) - box_pts))
    if abs(float(at[0]) - box_pts) > 0.5:
        return False
    return min(float(q[1]) for q in seen) + margin_m < float(at[1])


PLR_RADAR_APPROACH_M = 5.0  # 이만큼 가까워진 트랙이면 다가오는 차로 봅니다(radar_path_window).
PLR_LATE_BOX_MAX_SEC = 5.0  # 박스가 늦게 붙은 차를 레이더 구간까지 당길 때 구간 길이 상한.
PLR_NEAR_WAIT_M = 30.0      # 다가오던 차가 이보다 멀리서 레이더 기록이 끝났으면 아직 번호판 거리 전입니다(도착까지 기다림).
PLR_FAR_WAIT_MAX_SEC = 30.0  # 그 차를 기다리며 늘리는 추출 구간 길이 상한(초).
PLR_MAX_FRAMES = 60         # 원본 추출 상한 장수. 구간이 길면 every_n을 키워 맞춥니다.
PLR_SOURCE_FPS = 15.0       # 원본(링버퍼) 영상 fps. every_n 계산용.


def radar_path_window(radar_path, photo_m=(0.0, 5.0), frame_shape=None, seen_sec=1.0, seen_before_sec=3.0, min_sec=1.0, max_sec=3.0):
    """레이더 경로로 고르는 번호판 원본 추출 구간 (t0, t1)(pts).
    radar_path: [[pts, 거리 m, 속도 km/h, (화면 투영 u, v)], ...]. photo_m: 사진 구간(레이더 거리 m, [가까운, 먼]).
    화면 안(radar_seen_points: 가로 8~92%, 세로 95% 위)에 보인 점이 있으면: 그중 사진 구간 안 점들(0.5초 이상일 때), 아니면 화면 안에서 가장
    가까운 점의 앞 seen_before_sec·뒤 seen_sec 안의 화면 안 점들 → 다가오는 차는 화면 밖으로 나가기 전 3초, 멀어지는 차는 막 나타난 직후 1초.
    앞을 3초 보는 까닭: 레이더 점은 운전석 쪽이라 점이 화면 안이어도 번호판은 이미 화면 아래로 나가 있습니다(11/9 13:09:33 107).
    35m보다 멀리서 시작해야 번호판이 온전한 장을 얻습니다(cam2 기준 2초는 32%, 3초는 81%).
    투영이 없으면 거리가 사진 구간 안인 때, 그것도 없으면 마지막 거리·속도로 사진 구간에 닿는 때를 계산합니다(다가오는 차).
    길이는 min_sec~max_sec. 경로가 없으면 None."""
    if not radar_path:
        return None
    # 다가오다 가장 가까워진 뒤의 기록은 쓰지 않습니다. 다른 차가 같은 트랙에 이어 붙으면 그 뒤가 "화면 안 가장 가까운
    # 때"로 뽑혀 엉뚱한 차를 찍습니다(11/9 13:16:52 107: 다가오던 차가 나간 뒤 멀어지는 트럭). 처음부터 멀어지는 차는 그대로.
    closest = min(range(len(radar_path)), key=lambda i: float(radar_path[i][1]))
    approaching = float(radar_path[0][1]) - float(radar_path[closest][1]) >= PLR_RADAR_APPROACH_M
    if approaching:
        radar_path = radar_path[:closest + 1]
    lo, hi = sorted(float(v) for v in photo_m)
    if frame_shape is not None:
        seen = radar_seen_points(radar_path, frame_shape)
        if seen:
            ts = [float(q[0]) for q in seen if lo <= float(q[1]) <= hi]
            if not ts or max(ts) - min(ts) < 0.5:
                near = float(min(seen, key=lambda q: float(q[1]))[0])
                ts = [float(q[0]) for q in seen if -seen_before_sec <= float(q[0]) - near <= seen_sec]
                max_sec = max(max_sec, seen_before_sec + seen_sec)
                # 레이더가 차를 늦게 잡아도 앞 구간을 채웁니다. 번호판은 레이더 기록 시작 전에만 보였을 수 있습니다(11/9 13:36:26 107).
                if approaching:
                    ts.append(near - seen_before_sec)
            t0, t1 = min(ts), max(ts)
            t1 = min(t1, t0 + max_sec)
            # 다가오던 차가 번호판 거리(PLR_NEAR_WAIT_M) 밖에서 레이더 기록이 끝났으면(판정 직후 급감속해 기는
            # 차를 레이더가 놓침) 아직 안 온 것입니다. 마지막 거리·속도로 사진 구간 가까운 끝(lo)에 닿을 때까지
            # 구간을 늘려 그 장면을 기다립니다(_wait_segments가 링버퍼를 기다림. 11/11 08:55:27 96 미인식).
            last_pts, last_rng, last_kmh = (float(v) for v in radar_path[-1][:3])
            if approaching and last_rng > PLR_NEAR_WAIT_M:
                speed = max(float(last_kmh), 3.0) / 3.6
                t1 = max(t1, min(last_pts + (last_rng - lo) / speed, t0 + PLR_FAR_WAIT_MAX_SEC))
            return t0 - 0.1, t1 + 0.1
    inside = [q for q in radar_path if lo <= float(q[1]) <= hi]
    if inside:
        t0, t1 = float(inside[0][0]), float(inside[-1][0])
    else:
        pts, rng, kmh = (float(v) for v in radar_path[-1][:3])
        v = max(kmh / 3.6, 2.0)
        t0, t1 = pts + max(0.0, (rng - hi) / v), pts + max(0.0, (rng - lo) / v)
    t1 = min(max(t1, t0 + min_sec), t0 + max_sec)
    return t0 - 0.1, t1 + 0.1


def request_fullres_capture(cam, event_name, event_id, event_fid, bbox, inference_shape, pts_window=None, follow_boxes=None,
                            evidence=None, radar_path=None, photo_m=None):
    """원본 프레임 추출을 예약하고 결과 폴더 경로를 돌려줍니다.
    pts_window가 있으면(레이더 이벤트) [start_pts - pre, event_pts + post], 없으면 이벤트 프레임 PTS 기준 [pre, post].
    follow_boxes([[pts, 박스], ...], 과속 차량을 판정 전후로 따라간 박스)가 있으면 그 차가 꽤 가까워졌을 때(박스 넓이가
    가장 클 때의 25% 이상)부터 박스가 화면 바닥에 닿기 전 마지막 박스 + PLR_TAIL_SEC까지(최대 PLR_WINDOW_MAX_SEC, 최소 1초)
    추출하고, 어느 장을 읽을지는 PLR이 잘림·선명도·시각으로 고릅니다(pick_plates_for_ocr).
    박스가 끝내 안 붙은 차는 radar_path(레이더 경로)로 사진 구간(photo_m) 또는 화면 안 가장 가까운 1초만 뽑고
    (radar_path_window), 번호판은 ROI(단속 차로) 영역(search_box)에서 찾습니다."""
    global FULLRES_EXTRACTOR
    if not fullres_enabled(event_name):
        return None
    cfg = SYS_CFG["fullres_capture"]
    if not isinstance(cam.reader, SpeedFrameReader):
        # 원본 링버퍼와 PTS는 과속 카메라 수신기(SpeedFrameReader)에만 있습니다.
        logger.warning(f"[FULLRES] 이 카메라 수신기는 원본 추출을 지원하지 않아 건너뜁니다(과속 카메라 전용). event_id={event_id}")
        return None
    if pts_window:
        frame_meta = {"pts": pts_window["event_pts"], "ring_dir": pts_window["ring_dir"]}
    else:
        frame_meta = cam.reader.get_frame_meta(event_fid)
    if not frame_meta or frame_meta.get("pts") is None or not frame_meta.get("ring_dir") or not cam.reader.source_shape:
        logger.warning(f"[FULLRES] 원본 구간을 특정할 수 없어 건너뜁니다. event_id={event_id} fid={event_fid} "
                       f"pts={(frame_meta or {}).get('pts')} ring={(frame_meta or {}).get('ring_dir')}")
        return None
    event_pts = float(frame_meta["pts"])
    start_pts = float(pts_window.get("start_pts", event_pts)) if pts_window else event_pts
    out_dir = fullres_out_dir(cam, event_name, event_id)
    full_w, full_h = cam.reader.source_shape
    job = {
        "event_id": event_id, "event_name": event_name, "cam_id": cam.cam_id, "ip": cam.ip,
        "fid": int(event_fid), "event_pts": event_pts,
        "t0": start_pts - float(cfg["pre_sec"]), "t1": event_pts + float(cfg["post_sec"]),
        "every_n": max(1, int(cfg["every_n"])),
        "ring_dir": frame_meta["ring_dir"], "out_dir": out_dir,
        # bbox는 추론 프레임 좌표입니다. 원본 좌표 = bbox * (full_shape / inference_shape).
        "event_bbox": int_box(bbox), "inference_shape": list(inference_shape[:2]) if inference_shape else None,
        "full_shape": [int(full_h), int(full_w)],
    }
    if evidence:
        job["evidence"] = evidence  # 증거 3장 묶음 글자(시각·레이더 속도·영상 재검증)
    # 분석용: 레이더 경로·사진 구간과, 원본 구간을 어떻게 골랐는지(박스 따라가기 / 레이더 경로 / 판정 순간 기본 구간)
    if radar_path:
        job["radar_path"] = radar_path
    if photo_m:
        job["photo_m"] = list(photo_m)
    job["photo_source"] = "event_window"
    use_radar = bool(radar_path) and not follow_boxes
    if follow_boxes:
        # 판정 순간(멀리 있을 때) 구간은 번호판이 작아 쓰지 않습니다. 뽑는 장수와 디스크도 그만큼 줄어듭니다.
        # 화면 가장자리에 닿아 잘린 박스는 번호판도 잘렸을 수 있어 빼고 고릅니다(모두 잘렸으면 그대로).
        job["follow_boxes"] = follow_boxes
        whole = [b for b in follow_boxes if not box_cut_by_frame(b[1], inference_shape)] or follow_boxes
        areas = [max(0, b[1][2] - b[1][0]) * max(0, b[1][3] - b[1][1]) for b in whole]
        best_pts = whole[areas.index(max(areas))][0]
        # 차 검출이 가까이 온 트럭을 놓쳐 박스가 일찍 끊겼고 레이더는 화면 안에서 더 가까이 갔으면 레이더 경로로 고릅니다.
        # 단, 따라가던 박스가 화면 옆 끝에 닿으며 끝났으면(차가 옆으로 빠져나감, 11/7 17:09:51) 레이더가 더 가까이 간
        # 때는 차가 화면 밖이라 레이더 경로를 쓰지 않고 박스로 고릅니다.
        side_exit = box_cut_side(follow_boxes[-1][1], inference_shape)
        use_radar = bool(radar_path) and not side_exit and radar_nearer_than_box(radar_path, best_pts, inference_shape)
        if not use_radar:
            # 번호판은 차가 다가올수록 커집니다. 박스를 놓친 직후(PLR_TAIL_SEC)가 차가 가장 가까울 때라 그때까지 뽑고,
            # 박스 아래가 화면 바닥에 닿은 뒤는 번호판도 잘리므로 그 전 마지막 박스를 끝으로 봅니다.
            job["photo_source"] = "follow_boxes"
            all_areas = [box_area(b[1]) for b in follow_boxes]
            floor = float(inference_shape[0]) - 3 if inference_shape else None
            above = [b for b in follow_boxes if floor is None or b[1][3] < floor] or follow_boxes
            # 끝은 박스가 가장 컸던 때(가장 가까울 때). 그게 따라가기 마지막이면(다가오다 놓침) PLR_TAIL_SEC를 더 보고,
            # 그 뒤 박스가 작아졌으면(멀어지거나 다른 차로 옮겨 붙음) 그 뒤 장면은 쓰지 않습니다(11/6 15:32:55).
            above_areas = [box_area(b[1]) for b in above]
            k = above_areas.index(max(above_areas))
            peak = above[k][0]
            job["t1"] = peak + (PLR_TAIL_SEC if k == len(above) - 1 else 0.1)
            near = [b[0] for b, a in zip(follow_boxes, all_areas) if a >= 0.25 * max(all_areas) and b[0] <= peak] or [peak]
            job["t0"] = min(max(near[0] - 0.1, job["t1"] - PLR_WINDOW_MAX_SEC), job["t1"] - 1.0)
            # 박스가 늦게 붙으면 구간 앞쪽 프레임엔 박스가 없습니다. 그 프레임은 이 영역에서 레이더 점으로 찾습니다.
            if radar_path:
                job["before_box_search"] = roi_search_box(cam, inference_shape)
                # 박스가 판정 뒤에야 붙은 차는 구간도 늦어 번호판이 이미 화면 밖입니다(11/9 13:24:24 107).
                # 구간 시작을 레이더로 고른 시작까지 당깁니다(끝은 박스 기준 그대로).
                if min(b[0] for b in follow_boxes) > event_pts:
                    late = radar_path_window(radar_path, photo_m=photo_m or (0.0, 5.0), frame_shape=inference_shape)
                    if late is not None:
                        job["t0"] = min(job["t0"], max(late[0], job["t1"] - PLR_LATE_BOX_MAX_SEC))
    if use_radar:
        # 차량 검출이 놓친 차(가까이 온 큰 트럭 등): 판정 시점(멀리 있을 때) 대신 번호판 사진 구간을 뽑습니다.
        window = radar_path_window(radar_path, photo_m=photo_m or (0.0, 5.0), frame_shape=inference_shape)
        if window is not None:
            job["t0"], job["t1"] = window
            job["photo_source"] = "radar_path_box_lost" if follow_boxes else "radar_path"
            box = roi_search_box(cam, inference_shape)
            if box:
                job["search_box"] = box
    # 구간이 길면(차를 기다리는 추출) 전체 장수가 PLR_MAX_FRAMES를 넘지 않게 every_n을 키웁니다.
    job["every_n"] = max(job["every_n"],
                         int(math.ceil(max(0.0, job["t1"] - job["t0"]) * PLR_SOURCE_FPS / PLR_MAX_FRAMES)))
    if FULLRES_EXTRACTOR is None:
        FULLRES_EXTRACTOR = FullResExtractor(cfg)
    if not FULLRES_EXTRACTOR.request(job):
        return None
    logger.info(f"[FULLRES] 요청 event_id={event_id} pts={event_pts:.3f} window=[{job['t0']:.3f},{job['t1']:.3f}] dir={out_dir}")
    return out_dir


# ==========================================
# 번호판 인식(PLR): 원본 프레임 → 번호판 검출(plate.dxnn, NPU) → OCR(plate_ocr.onnx, CPU)
# ==========================================
# 한국 번호판 형식: 신형 "12가3456"/"123가4567", 구형 지역 "서울12가3456"(지역 + 1~2자리)
PLATE_REGIONS = ("서울", "부산", "대구", "인천", "광주", "대전", "울산", "세종", "경기", "강원",
                 "충북", "충남", "전북", "전남", "경북", "경남", "제주")
PLATE_HANGUL = set("가나다라마거너더러머버서어저고노도로모보소오조구누두루무부수우주아바사자배하허호육해공국합")
PLATE_RE = re.compile(r"^(?:(" + "|".join(PLATE_REGIONS) + r")(\d{1,2})|(\d{2,3}))([가-힣])(\d{4})$")


_PLATE_PREFIX_RES = [re.compile(p) for p in (
    r"^\d{0,3}$", r"^\d{2,3}[가-힣]\d{0,4}$",
    r"^(?:" + "|".join(PLATE_REGIONS) + r")\d{0,2}$", r"^(?:" + "|".join(PLATE_REGIONS) + r")\d{1,2}[가-힣]\d{0,4}$")]


def plate_prefix_ok(text):
    """text 뒤에 글자를 더 붙여 올바른 번호판이 될 수 있으면 True(형식 제한 디코딩용)."""
    if len(text) <= 2 and not text[:1].isdigit() and any(r.startswith(text) for r in PLATE_REGIONS):
        return True
    if not any(p.match(text) for p in _PLATE_PREFIX_RES):
        return False
    m = re.search(r"\d([가-힣])\d*$", text)
    return m is None or m.group(1) in PLATE_HANGUL


def plate_shape(text):
    """형식이 맞으면 자리 모양(지역/숫자/글자 구분 문자열), 아니면 None."""
    m = PLATE_RE.match(text or "")
    if not m or m.group(4) not in PLATE_HANGUL:
        return None
    head = (m.group(1) + "D" * len(m.group(2))) if m.group(1) else "D" * len(m.group(3))
    return head + "H" + "DDDD"


def plate_read_text(o, rule_min=0.5):
    """OCR 결과 하나(o)에서 다수결에 쓰는 번호 (text, conf, source).
    숫자·한글만 남기고, 형식이 깨졌으면 형식 제한 디코딩 결과(rule, 신뢰도 rule_min 이상)로 바꿉니다."""
    text = re.sub(r"[^0-9가-힣]", "", o.get("text") or "")
    conf, source = float(o.get("conf") or 0.0), "ocr"
    rule = o.get("rule") or {}
    if not plate_shape(text) and rule.get("text") and float(rule.get("conf") or 0.0) >= rule_min:
        # 가장 확률 높은 글자만 이으면 형식이 깨졌지만, 2·3등 후보로 형식을 채운 결과가 충분히 그럴듯할 때
        text, conf, source = rule["text"], float(rule["conf"]), "rule"
    return text, conf, source


def finalize_plate(rows):
    """여러 프레임의 OCR 결과를 통째 다수결로 하나로 정합니다(가장 많이 읽힌 번호를 보냄).

    1) 숫자·한글만 남깁니다. 형식(신형/구형 지역, 용도 글자)이 깨졌으면 형식 제한 디코딩 결과(rule, 신뢰도 0.5 이상)로 바꿔 봅니다.
    2) 형식에 맞는 결과가 있으면 그중에서, 없으면 읽은 결과 전체에서 가장 많이 나온 번호를 고릅니다(같으면 신뢰도 합이 큰 쪽).
       예: 5장 중 A 3장, B 2장 → A.
    3) 상태(관제 plateStatus, 참고용): 형식에 맞는 결과의 60% 이상(2장 이상)이 같거나 1장뿐인데 신뢰도 0.9 이상이면 '확정',
       형식은 맞는데 표가 갈리면 '불확실', 형식에 맞는 결과가 없으면 '형식 불일치', 번호판을 못 찾았으면 '미인식'.
       상태와 상관없이 고른 번호를 보냅니다. 후보별 표는 plr_meta.json final.candidates에 남깁니다."""
    reads = []
    for r in rows:
        for backend, o in ((r.get("plate") or {}).get("ocr") or {}).items():
            text, conf, source = plate_read_text(o)
            if text:
                reads.append({"text": text, "conf": conf, "backend": backend, "source": source, "shape": plate_shape(text)})
    if not reads:
        return {"text": "", "status": "미인식", "support": 0, "valid_reads": 0, "reads": 0}
    valid = [r for r in reads if r["shape"]]
    votes = {}
    for r in valid or reads:
        n, conf_sum = votes.get(r["text"], (0, 0.0))
        votes[r["text"]] = (n + 1, conf_sum + r["conf"])
    text = max(votes, key=lambda t: votes[t])           # 표 수, 같으면 신뢰도 합
    support = votes[text][0]
    if not valid:
        status = "형식 불일치"
    else:
        best_conf = max(r["conf"] for r in valid if r["text"] == text)
        status = "확정" if ((support >= 2 and support / float(len(valid)) >= 0.6)
                          or (len(valid) == 1 and best_conf >= 0.9)) else "불확실"
    return {"text": text, "status": status, "support": support, "valid_reads": len(valid), "reads": len(reads),
            "rule_reads": sum(1 for r in reads if r["source"] == "rule"),
            "candidates": sorted(((t, v[0]) for t, v in votes.items()), key=lambda kv: -kv[1])[:5]}


class PlateRecognizer:
    """번호판 검출(plate.dxnn, 사진 번호판 가리기와 같은 엔진)과 OCR(SVTR_LCNet CTC, CPU ONNX).

    OCR 입력: BGR 64x320(높이 64 비율 유지, 폭 <= 320, 오른쪽 128 패딩 = 정규화 후 0)을 /255, (x-0.5)/0.5, CHW로 넣습니다.
    """
    OCR_H, OCR_W = 64, 320

    def __init__(self, cfg, det):
        self.cfg = cfg
        self.det = det
        with open(resolve_model_path(cfg["ocr_dict"]), encoding="utf-8") as f:
            self.chars = ["blank"] + [line.rstrip("\n") for line in f]
        cpu_path = resolve_model_path(cfg.get("ocr_cpu_model"))
        if not cpu_path or not os.path.exists(cpu_path):
            raise RuntimeError(f"번호판 OCR 모델(ocr_cpu_model)이 없습니다: {cpu_path}")
        import onnxruntime as ort
        options = ort.SessionOptions()
        options.intra_op_num_threads = max(1, int(cfg.get("ocr_cpu_threads", 2)))
        options.inter_op_num_threads = 1
        self.ocr_cpu = ort.InferenceSession(cpu_path, options, providers=["CPUExecutionProvider"])
        self._cpu_input = self.ocr_cpu.get_inputs()[0].name
        logger.info(f"[PLR] 모델 로드: det={os.path.basename(det.engine_path)} ocr_cpu={os.path.basename(cpu_path)}")

    def detect(self, image):
        t0 = time.perf_counter()
        # 사진 가리기와 같은 엔진(model_confidences.PLATE)으로 찾고, 번호판 인식에는 plr.det_conf 이상만 씁니다.
        dets = self.det.infer(image)
        dets = dets[dets[:, 4] >= float(self.cfg.get("det_conf", 0.25))]
        return dets, (time.perf_counter() - t0) * 1000.0

    def ocr_input(self, plate):
        h, w = plate.shape[:2]
        resized_w = min(self.OCR_W, int(math.ceil(self.OCR_H * w / float(h))))
        canvas = np.full((self.OCR_H, self.OCR_W, 3), 128, np.uint8)
        canvas[:, :resized_w] = cv2.resize(plate, (resized_w, self.OCR_H))
        return canvas

    def _ctc_decode(self, prob):
        prob = np.asarray(prob, np.float32).reshape(-1, len(self.chars))
        idx, conf = prob.argmax(1), prob.max(1)
        keep = [i for i in range(len(idx)) if idx[i] != 0 and (i == 0 or idx[i] != idx[i - 1])]
        text = "".join(self.chars[idx[i]] for i in keep)
        return text, float(np.mean(conf[keep])) if keep else 0.0

    def _ctc_decode_plate(self, prob, beam=12, topk=8, min_p=1e-3):
        """번호판 형식에 맞는 문자열 중 확률이 가장 높은 것(형식 제한 CTC 빔 탐색).
        가장 확률 높은 글자만 이으면 형식이 깨질 때(자리 누락·용도 글자 오인식) 2·3등 후보로 형식을 채웁니다.
        반환 (text, conf): conf는 그 문자열 전체 확률의 글자당 기하평균. 형식에 맞는 후보가 없으면 ("", 0.0)."""
        prob = np.asarray(prob, np.float64).reshape(-1, len(self.chars))
        beams = {"": (1.0, 0.0)}  # 접두어 -> (공백으로 끝난 확률, 글자로 끝난 확률)
        for row in prob:
            cand = [int(c) for c in np.argsort(row)[::-1][:topk] if c != 0 and row[c] >= min_p]
            nxt = {}

            def add(key, pb, pn):
                ob, on = nxt.get(key, (0.0, 0.0))
                nxt[key] = (ob + pb, on + pn)
            for prefix, (pb, pn) in beams.items():
                add(prefix, (pb + pn) * row[0], 0.0)  # 공백
                last = prefix[-1] if prefix else None
                for c in cand:
                    ch, p = self.chars[c], row[c]
                    if ch == last:
                        add(prefix, 0.0, pn * p)          # 같은 글자 반복(합쳐짐)
                        new_p = pb * p                     # 공백을 사이에 둔 같은 글자 = 새 글자
                    else:
                        new_p = (pb + pn) * p
                    if new_p > 0 and plate_prefix_ok(prefix + ch):
                        add(prefix + ch, 0.0, new_p)
            beams = dict(sorted(nxt.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:beam])
        done = [(k, v[0] + v[1]) for k, v in beams.items() if plate_shape(k)]
        if not done:
            return "", 0.0
        text, p = max(done, key=lambda kv: kv[1])
        return text, float(p ** (1.0 / max(1, len(text))))

    def read_plate(self, plate):
        """번호판 크롭 → {"cpu": {"text", "conf", "ms"[, "rule"]}}."""
        x = np.ascontiguousarray(self.ocr_input(plate))
        t0 = time.perf_counter()
        xf = ((x.astype(np.float32) / 255.0 - 0.5) / 0.5).transpose(2, 0, 1)[None]
        prob = self.ocr_cpu.run(None, {self._cpu_input: xf})[0]
        text, conf = self._ctc_decode(prob)
        r = {"text": text, "conf": round(conf, 4), "ms": round((time.perf_counter() - t0) * 1000.0, 2)}
        if not plate_shape(re.sub(r"[^0-9가-힣]", "", text)):
            fixed, fconf = self._ctc_decode_plate(prob)
            r["rule"] = {"text": fixed, "conf": round(fconf, 4)}  # 형식 제한 디코딩 결과(형식이 깨졌을 때만)
        return {"cpu": r}


PLATE_RECOGNIZER = None
PLATE_RECOGNIZER_ERROR = None
PLATE_DETECTOR = None  # main이 올린 번호판 모델(models.PLATE). 사진 번호판 가리기와 PLR이 엔진 하나를 같이 씁니다.


def get_plate_recognizer():
    global PLATE_RECOGNIZER, PLATE_RECOGNIZER_ERROR
    if PLATE_RECOGNIZER is None and PLATE_RECOGNIZER_ERROR is None:
        try:
            if PLATE_DETECTOR is None:
                raise RuntimeError("번호판 모델(models.PLATE)이 올라가지 않았습니다.")
            PLATE_RECOGNIZER = PlateRecognizer(SYS_CFG["plr"], PLATE_DETECTOR)
        except Exception as e:
            PLATE_RECOGNIZER_ERROR = str(e)
            logger.error(f"[PLR] 초기화 실패 → 번호판 인식을 끕니다: {e}")
    return PLATE_RECOGNIZER


# ==========================================
# 과속 증거 보강: 영상 속도 재검증 · 번호판 촬영 구간 · 증거 3장 묶음
# ==========================================
def ground_from_box(box, shape, rc=None):
    """차량 박스 하단 중앙 → 레이더 기준 지면 위치 (x 정면 m, y 왼쪽+ m).
    같은 정면 기하 투영(radar_camera.colocated)의 역변환이며, 박스 하단은 지면(타이어 접지, 높이 0)으로 봅니다.
    호모그래피를 쓰거나 투영값이 없으면, 또는 박스 하단이 수평선 근처면 None. rc: 레이더별 보정값(없으면 radar_camera)."""
    rc = SYS_CFG["radar_camera"] if rc is None else rc
    col = rc.get("colocated") or {}
    if rc.get("homography") or not col.get("enabled") or not col.get("f_px") or shape is None:
        return None
    h, w = shape[:2]
    size = col.get("image_size") or [w, h]
    f = float(col["f_px"]) * w / float(size[0])
    cx, cy = w / 2.0, h / 2.0
    u, v = (float(box[0]) + float(box[2])) / 2.0, float(box[3])
    el = math.atan2(v - cy, f) + math.radians(float(col.get("pitch_deg", 0.0)))
    if el <= math.radians(0.5):
        return None
    x = float(col.get("cam_height_m", 4.0)) / math.tan(el)
    az = math.radians(float(col.get("yaw_deg", 0.0))) + math.atan2(cx - u, f)
    return x, x * math.tan(az)


def video_speed_check(boxes, shape, event_pts, radar_kmh, rc=None):
    """영상 기반 속도 재검증: 판정 시점 전 2초(span) 동안 그 차량 박스의 지면 위치를 시간에 대해 직선 맞춤.
    boxes: [[pts, 박스], ...] (추론 프레임 좌표). 레이더 속도 대비 비율이 0.8~1.25 안이면 "ok",
    밖이면 "mismatch"(다른 차 연결·추적 끊김·보정 오차 의심), 프레임이 모자라면 "unavailable"."""
    span, lo, hi = 2.0, 0.8, 1.25
    pts = []
    for p, b in boxes or []:
        if event_pts is not None and event_pts - span <= float(p) <= event_pts + 0.05:
            g = ground_from_box(b, shape, rc)
            if g is not None:
                pts.append((float(p), g[0], g[1]))
    res = {"status": "unavailable", "video_kmh": None, "radar_kmh": round(float(radar_kmh), 1), "ratio": None,
           "frames": len(pts), "window_sec": span}
    if len(pts) < 4 or pts[-1][0] - pts[0][0] < 0.5:
        return res
    t = np.array([q[0] for q in pts])
    vx = np.polyfit(t, [q[1] for q in pts], 1)[0]
    vy = np.polyfit(t, [q[2] for q in pts], 1)[0]
    v = math.hypot(vx, vy) * 3.6
    ratio = v / max(0.1, float(radar_kmh))
    res.update(video_kmh=round(v, 1), ratio=round(ratio, 2), status="ok" if lo <= ratio <= hi else "mismatch")
    return res


# 번호판 읽을 장 고르기(번호판 기준): 따라간 구간의 원본 프레임마다 번호판을 찾고, 화면 끝에 닿지 않은 번호판 중
# 흐린 장(가장 또렷한 것의 PLR_SHARP_MIN_FRAC 미만: 흔들림·번짐)을 빼고 가로가 큰 순으로 PLR_OCR_TOP_K장만 OCR합니다.
PLR_WINDOW_MAX_SEC = 3.0   # 원본 추출 구간 최대 길이(초, 따라가기 끝쪽)
PLR_TAIL_SEC = 0.6         # 박스가 화면 바닥에 닿기 전 마지막 박스 뒤로 더 뽑는 시간(초)
PLR_MAX_FRAMES = 26        # 번호판을 찾아볼 원본 프레임 수 상한
PLR_OCR_TOP_K = 5          # OCR할 장수(번호판 가로가 큰 순)
PLR_SHARP_MIN_FRAC = 0.35  # 가장 또렷한 번호판 선명도의 이 비율보다 흐린 장은 뺌
PLR_EDGE_FRAC = 0.005      # 번호판이 원본 화면 끝에서 가로의 이 비율 안에 닿으면 잘린 것으로 봄
PLR_REATTACH_MIN_AREA_FRAC = 0.5  # 놓친 뒤 다시 붙을 박스는 이미 따라가던 가장 큰 박스의 이 비율 이상이어야 함
PLR_RADAR_NEAR_PLATES = 12.0  # 차 박스 없는 프레임: 레이더 점에서 번호판 크기(max(폭, 높이×3))의 이 배수 안 번호판만 그 차로 봄
PLR_RADAR_MAX_DT = 0.25    # 그 프레임 시각과 이 안(초)의 레이더 점만 씀


def box_area(box):
    """박스 [x1, y1, x2, y2, ...] 넓이(px²)."""
    return max(0.0, float(box[2]) - float(box[0])) * max(0.0, float(box[3]) - float(box[1]))


PLR_TILE_PX = 800          # 번호판 찾기 조각 크기(px). 640 입력에서 번호판이 잘 찾히는 크기(100px 이상)가 되게
PLR_TILE_STRIDE = 600      # 조각 간격(겹침 200px: 폭 200px 이하 번호판은 한 조각에 온전히 들어감)


def _box_iou(a, b):
    ix = max(0.0, min(a[2], b[2]) - max(a[0], b[0])); iy = max(0.0, min(a[3], b[3]) - max(a[1], b[1]))
    inter = ix * iy
    union = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / union if union > 0 else 0.0


def tile_origins(length, tile=PLR_TILE_PX, stride=PLR_TILE_STRIDE):
    """길이 length를 덮는 조각 시작점들(마지막 조각은 끝에 맞춤)."""
    if length <= tile:
        return [0]
    starts = list(range(0, length - tile, stride))
    return starts + [length - tile]


def detect_plate_tiled(detect, crop, tile=PLR_TILE_PX, stride=PLR_TILE_STRIDE, edge=4, iou=0.5):
    """crop 전체와 tile px 조각들에서 번호판을 찾아 합칩니다(crop 좌표). detect(img) -> (dets [x1,y1,x2,y2,score,...], ms).
    조각 안쪽 경계(crop 끝이 아닌 곳)에서 edge px 안에 닿은 상자는 조각에 잘린 번호판이라 버립니다.
    반환 (dets 배열, 걸린 ms 합)."""
    h, w = crop.shape[:2]
    found, total_ms = [], 0.0
    dets, ms = detect(crop)
    total_ms += ms
    found += [list(map(float, d[:5])) for d in dets]
    if max(w, h) > tile * 1.15:
        for y0 in tile_origins(h, tile, stride):
            for x0 in tile_origins(w, tile, stride):
                x1e, y1e = min(w, x0 + tile), min(h, y0 + tile)
                dets, ms = detect(crop[y0:y1e, x0:x1e])
                total_ms += ms
                for d in dets:
                    bx1, by1, bx2, by2, sc = (float(v) for v in d[:5])
                    if ((x0 > 0 and bx1 <= edge) or (y0 > 0 and by1 <= edge)
                            or (x1e < w and bx2 >= (x1e - x0) - edge) or (y1e < h and by2 >= (y1e - y0) - edge)):
                        continue
                    found.append([bx1 + x0, by1 + y0, bx2 + x0, by2 + y0, sc])
    found.sort(key=lambda d: -d[4])
    keep = []
    for d in found:
        if all(_box_iou(d, k) < iou for k in keep):
            keep.append(d)
    return np.array(keep, dtype=float).reshape(-1, 5), total_ms


def plate_sharpness(plate):
    """번호판 크롭 선명도: 가로 160px로 맞춘 회색조의 라플라시안 분산(클수록 또렷)."""
    if plate is None or not plate.size:
        return 0.0
    gray = cv2.cvtColor(plate, cv2.COLOR_BGR2GRAY) if plate.ndim == 3 else plate
    gray = cv2.resize(gray, (160, max(1, int(round(160.0 * gray.shape[0] / max(1, gray.shape[1]))))))
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def plate_touches_edge(box_full, full_shape, frac=PLR_EDGE_FRAC):
    """번호판 상자(원본 px)가 원본 화면 끝에 닿았는지(잘렸을 수 있음)."""
    h, w = float(full_shape[0]), float(full_shape[1])
    e = frac * w
    x1, y1, x2, y2 = (float(v) for v in box_full[:4])
    return x1 <= e or y1 <= e or x2 >= w - e or y2 >= h - e


PLR_PLATE_MIN_WIDTH_FRAC = 0.6  # 가장 큰 번호판 폭의 이 비율보다 작으면 OCR하지 않습니다(pick_plates_for_ocr).


def pick_plates_for_ocr(cands, k=PLR_OCR_TOP_K):
    """cands: [{"key", "width", "edge", "sharp"}](key는 프레임 순서) → OCR할 key 목록(최신 순).
    화면 끝에 닿지 않은 번호판만(모두 닿았으면 전부), 그중 가장 또렷한 것의 PLR_SHARP_MIN_FRAC보다 흐린 것과
    가장 큰 번호판 폭의 PLR_PLATE_MIN_WIDTH_FRAC보다 작은 것(차가 나간 뒤 뒤차 번호판)은 빼고 최신 k장.
    상자 크기 순으로 고르면 상자가 실제보다 크게 잡힌 먼 장이 끼어듭니다(11/9 15:27:32 107)."""
    pool = [c for c in cands if not c["edge"]] or list(cands)
    if not pool:
        return []
    top = max(c["sharp"] for c in pool)
    pool = [c for c in pool if c["sharp"] >= PLR_SHARP_MIN_FRAC * top] or pool
    widest = max(c["width"] for c in pool)
    pool = [c for c in pool if c["width"] >= PLR_PLATE_MIN_WIDTH_FRAC * widest] or pool
    return [c["key"] for c in sorted(pool, key=lambda c: -c["key"])[:k]]


def roi_search_box(cam, inference_shape):
    """번호판을 찾을 ROI(단속 차로) 영역(추론 프레임 px, 아래는 화면 바닥까지). ROI가 없으면 None."""
    roi = getattr(cam, "roi_poly", None)
    if roi is None or len(roi) < 3 or not inference_shape:
        return None
    xs, ys = [float(q[0]) for q in roi], [float(q[1]) for q in roi]
    return [int(min(xs)), int(min(ys)), int(max(xs)), int(inference_shape[0]) - 1]


def tail_search_box(box, inference_shape, grow=0.25):
    """따라가기가 끝난 뒤(차가 더 다가와 박스가 커지고 내려옴) 번호판을 찾을 영역(추론 프레임 px):
    마지막 박스를 양옆으로 grow만큼 넓히고 아래는 화면 바닥까지."""
    x1, y1, x2, y2 = (float(v) for v in box[:4])
    h, w = float(inference_shape[0]), float(inference_shape[1])
    bw = x2 - x1
    return [max(0.0, x1 - grow * bw), y1, min(w - 1, x2 + grow * bw), h - 1]


def write_api_scene(out_dir, full, inference_shape, plate_boxes, vehicle_box):
    """관제로 보낼 과속 사진(api_scene.jpg): 증거 사진과 같은 원본 프레임을 관제 사진 크기(추론 프레임)로 줄인 것.
    번호판(plate_boxes, 원본 좌표)은 모자이크(apply_plate_blur와 같은 방식), 차량(vehicle_box, 원본 좌표)은 빨간 상자와
    이벤트 이름(_draw_event_api_image). 반환 {"path", "box": 차량 상자(관제 사진 좌표), "size": [가로, 세로]}."""
    H, W = full.shape[:2]
    img = full.copy()
    for x1, y1, x2, y2 in plate_boxes:
        pw, ph = x2 - x1, y2 - y1
        x1, y1 = max(0, int(x1 - pw * 0.1)), max(0, int(y1 - ph * 0.1))
        x2, y2 = min(W, int(x2 + pw * 0.1)), min(H, int(y2 + ph * 0.1))
        if x2 <= x1 or y2 <= y1:
            continue
        small = cv2.resize(img[y1:y2, x1:x2], (max(1, (x2 - x1) // 12), max(1, (y2 - y1) // 12)), interpolation=cv2.INTER_LINEAR)
        img[y1:y2, x1:x2] = cv2.resize(small, (x2 - x1, y2 - y1), interpolation=cv2.INTER_NEAREST)
    w, h = int(inference_shape[1]), int(inference_shape[0])
    img = cv2.resize(img, (w, h), interpolation=cv2.INTER_AREA)
    box = [max(0, int(vehicle_box[0] * w / W)), max(0, int(vehicle_box[1] * h / H)),
           min(w - 1, int(vehicle_box[2] * w / W)), min(h - 1, int(vehicle_box[3] * h / H))]
    img = _draw_event_api_image(img, SPEEDING_EVENT, box, 0, [{"box": box, "tid": 0}])
    path = str(Path(out_dir) / "api_scene.jpg")
    if not cv2.imwrite(path, img):
        raise OSError(f"cv2.imwrite 실패: {path}")
    return {"path": path, "box": box, "size": [w, h]}


def make_evidence_image(out_dir, meta, rows, final, api_scene=None):
    """증거 3장 묶음(evidence.jpg): ① 전체 장면(차량·번호판 표시) ② 차량 ③ 번호판 확대 + 시각·레이더 속도·영상 재검증·확정 번호.
    확정 번호와 같은 번호로 읽힌 장 중 가장 늦은(차가 가장 가까운) 원본 프레임 하나로 만듭니다(카메라 1대로 영국식 3장 촬영을 대신). 만들면 파일 이름, 못 만들면 None.
    api_scene(dict)을 주면 같은 프레임으로 관제 사진(write_api_scene)도 만들어 그 정보를 채웁니다."""
    out_dir = Path(out_dir)
    # OCR한 장 중 확정 번호와 같은 번호로 읽힌 장만 봅니다. 가장 늦은 장을 그냥 쓰면 번호판이 화면 밖으로 나간 뒤
    # 번호판 모델이 다른 곳(유리창 등)을 잡은 장이 증거 사진이 됩니다(11/9 15:55:37 107). 그중 가장 늦은 장,
    # 같은 번호로 읽힌 장이 없으면 검출 점수가 가장 높은 장.
    read = [r for r in rows if r.get("plate") and r["plate"].get("ocr")]
    want = (final or {}).get("text")
    same = [r for r in read if want and any(plate_read_text(o)[0] == want for o in r["plate"]["ocr"].values())]
    best = same[-1] if same else max(read, key=lambda r: r["plate"].get("det_conf") or 0.0, default=None)
    if best is None:
        return None
    full = cv2.imread(str(out_dir / best["frame"]))
    if full is None:
        return None
    H, W = full.shape[:2]
    px1, py1, px2, py2 = best["plate"]["box_full"]
    inf = meta.get("inference_shape") or [H, W]
    sx, sy = W / float(inf[1]), H / float(inf[0])
    fb = None
    if best.get("pts") is not None:
        near = min((b for b in meta.get("follow_boxes") or []), key=lambda b: abs(b[0] - best["pts"]), default=None)
        fb = near[1] if near is not None and abs(near[0] - best["pts"]) <= 0.15 else None
    # 따라간 박스가 없으면 이벤트 시점 박스는 그 프레임이 이벤트 시점 근처(0.3초 이내)일 때만 씀(그 뒤엔 차가 이동함)
    # 차 박스 없이 레이더로 고른 장(mode radar)은 이벤트 박스가 화면 전체일 수 있어 번호판 둘레로 차량 칸을 만듭니다.
    vb = fb or (meta.get("event_bbox") if abs(float(best.get("dt") or 0.0)) <= 0.3 and best.get("mode") != "radar" else None)
    if vb:
        vx1, vy1, vx2, vy2 = int(vb[0] * sx), int(vb[1] * sy), int(vb[2] * sx), int(vb[3] * sy)
    else:
        pw0, ph0 = px2 - px1, py2 - py1
        vx1, vy1, vx2, vy2 = px1 - 2 * pw0, py1 - 6 * ph0, px2 + 2 * pw0, py2 + 2 * ph0
    vx1, vy1, vx2, vy2 = max(0, vx1), max(0, vy1), min(W, vx2), min(H, vy2)
    if api_scene is not None:
        try:
            api_scene.update(write_api_scene(out_dir, full, inf, [best["plate"]["box_full"]] + list(best.get("plates_all") or []),
                                             (vx1, vy1, vx2, vy2)))
        except Exception as e:
            logger.warning(f"[PLR] 관제 사진(원본 장면) 생성 실패 event_id={meta.get('event_id')}: {e}")
    scene = full.copy()
    cv2.rectangle(scene, (vx1, vy1), (vx2, vy2), (0, 0, 255), 8)
    cv2.rectangle(scene, (px1, py1), (px2, py2), (0, 255, 255), 6)
    cw, ch = vx2 - vx1, vy2 - vy1
    veh = full[max(0, int(vy1 - ch * 0.15)):min(H, int(vy2 + ch * 0.15)), max(0, int(vx1 - cw * 0.15)):min(W, int(vx2 + cw * 0.15))]
    pw, ph = px2 - px1, py2 - py1
    plate = full[max(0, py1 - ph // 2):min(H, py2 + ph // 2), max(0, px1 - pw // 4):min(W, px2 + pw // 4)]

    def fit(img, w, h):
        c = np.full((h, w, 3), 30, np.uint8)
        if img is None or not img.size:
            return c
        s = min(w / img.shape[1], h / img.shape[0])
        r = cv2.resize(img, (max(1, int(img.shape[1] * s)), max(1, int(img.shape[0] * s))))
        y, x = (h - r.shape[0]) // 2, (w - r.shape[1]) // 2
        c[y:y + r.shape[0], x:x + r.shape[1]] = r
        return c
    canvas = np.full((900, 1600, 3), 30, np.uint8)
    canvas[60:900, 0:1000] = fit(scene, 1000, 840)
    canvas[60:520, 1010:1600] = fit(veh, 590, 460)
    canvas[540:700, 1010:1600] = fit(plate, 590, 160)
    ev = meta.get("evidence") or {}
    vc = ev.get("video_check") or {}
    vc_text = {"ok": "일치", "mismatch": "불일치", "unavailable": "확인 불가"}.get(vc.get("status"), "-")
    lines = [
        ((10, 12), f"과속 증거  {str(ev.get('ts', ''))[:19].replace('T', ' ')}  CAM{meta.get('cam_id', '')}", 30, (230, 230, 230)),
        ((1010, 26), "① 전체 장면   ② 차량   ③ 번호판 확대", 24, (180, 180, 180)),
        ((1010, 712), f"번호판  {final.get('text') or '-'}  [{final.get('status', '-')}]", 30, (120, 240, 120)),
        ((1010, 760), f"레이더 속도 {float(ev.get('speed_kmh') or 0):.1f} km/h  (기준 {float(ev.get('threshold_kmh') or 0):.0f})",
         24, (255, 120, 120)),
        ((1010, 795), f"영상 재검증 {vc.get('video_kmh') if vc.get('video_kmh') is not None else '-'} km/h  "
                      f"({vc_text}, 비율 {vc.get('ratio') if vc.get('ratio') is not None else '-'})", 24, (230, 230, 230)),
        ((1010, 830), f"레이더 R{ev.get('radar_id', '-')} · 원본 {W}x{H} · 번호판 {pw}px", 24, (180, 180, 180)),
    ]
    try:
        from PIL import Image, ImageDraw, ImageFont
        font_path = "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"  # index 1 = 한국어
        img = Image.fromarray(cv2.cvtColor(canvas, cv2.COLOR_BGR2RGB))
        d = ImageDraw.Draw(img)
        for xy, text, size, color in lines:
            try:
                font = ImageFont.truetype(font_path, size, index=1)
            except Exception:
                font = ImageFont.load_default()
            d.text(xy, text, font=font, fill=color)
        canvas = cv2.cvtColor(np.asarray(img), cv2.COLOR_RGB2BGR)
    except Exception:
        pass  # 글자 없이 이미지만 저장
    name = "evidence.jpg"
    cv2.imwrite(str(out_dir / name), canvas, [cv2.IMWRITE_JPEG_QUALITY, 90])
    return name


# 과속 관제 사진: 이벤트 → 번호판 결과(run_plr_on_fullres)를 이어 주는 Future. 사진 저장 일꾼이 결과를 받은 뒤 보냅니다.
PLATE_FUTURES = {}
PLATE_FUTURES_LOCK = threading.Lock()
# 관제로 보내지 않기로 한 과속 이벤트(차량번호 없음). 녹화가 끝난 영상도 보내지 않습니다.
# 과속 로직은 모델이 완벽하다는 전제라, 번호판을 못 찾은 차는 번호판이 안 보이는 차로 보고 로그만 남깁니다.
RECEIVER_SKIPPED_EVENTS = deque(maxlen=2000)


def plate_number_missing_reason(final):
    """관제로 보낼 차량번호가 없으면 그 까닭(미인식: 번호판 모델이 못 찾음, 시간초과: 번호판 읽기가 api_wait_sec 안에
    안 끝남), 있으면 None."""
    if (final or {}).get("text"):
        return None
    return (final or {}).get("status") or "시간초과"


def plate_future_for(event_id):
    """과속 이벤트의 번호판 결과를 받을 Future(이미 있으면 그것)."""
    with PLATE_FUTURES_LOCK:
        return PLATE_FUTURES.setdefault(str(event_id), concurrent.futures.Future())


def resolve_plate_future(event_id, final):
    """번호판 결과(finalize_plate의 final, 못 읽었거나 기다리다 시간이 지나면 None)를 넘기고 목록에서 뺍니다."""
    with PLATE_FUTURES_LOCK:
        fut = PLATE_FUTURES.pop(str(event_id), None)
    if fut is not None and not fut.done():
        try:
            fut.set_result(final)
        except concurrent.futures.InvalidStateError:
            pass


def speeding_api_objects(objects, final):
    """과속 관제 사진의 detectedObjects(관제 요청 형식):
    [{"box", "label": "speedviolation", "score", "regNo", "plateStatus", "speed"}] (관제가 TOS로 넘김).
    regNo: 차량번호. 여러 장 다수결 번호를 상태와 상관없이 넣습니다(못 읽었으면 "").
    plateStatus: 확정 / 불확실 / 형식 불일치 / 미인식(finalize_plate), 번호판 없음(기다리다 시간이 지났거나 못 읽음).
    speed: 레이더 판정 속도(측정 구간 중앙값, km/h)."""
    text, status = str((final or {}).get("text") or ""), str((final or {}).get("status") or "번호판 없음")
    return [{"box": o["box"], "label": "speedviolation", "score": o["score"], "regNo": text,
             "plateStatus": status, "speed": o.get("speed_kmh")} for o in objects]


GHOST_EVENTS = deque(maxlen=2000)   # 레이더 가짜 물체로 본 과속 event_id(영상도 ghost 폴더로 옮기고 보내지 않음)
GHOST_STATUS = "레이더 가짜"


def radar_ghost_check(meta, final):
    """레이더 가짜 물체인지(11/7: 96번 ROI 맨 위 왼쪽 분리대 옆 55m에 차 없이 시속 11~13km 트랙이 생겼다 사라짐).
    판정 뒤 차 박스가 한 번도 안 붙었고(photo_source radar_path), 레이더 트랙이 판정 뒤 ghost_radar_post_sec 안에
    사라졌고, 번호판도 못 읽었으면(확정·불확실 아님) True. 그 레이더의 speeding.ghost_filter가 켜진 때만 봅니다."""
    if meta.get("event_name") != SPEEDING_EVENT or meta.get("photo_source") != "radar_path":
        return False
    cfg = radar_speeding_cfg((meta.get("evidence") or {}).get("radar_client_id"))
    if not cfg.get("ghost_filter"):
        return False
    if (final or {}).get("status") in ("확정", "불확실"):
        return False
    path, ev_pts = meta.get("radar_path") or [], meta.get("event_pts")
    if not path or ev_pts is None:
        return False
    return max(float(q[0]) for q in path) - float(ev_pts) < float(cfg.get("ghost_radar_post_sec", 1.0))


def ghost_event_dir(ip, event_id):
    return os.path.join(EVENT_ROOT_DIR, "events", str(ip), "ghost", str(event_id))


def move_ghost_event(meta, out_dir):
    """가짜로 본 과속 이벤트의 사진(images·images_api)과 원본 추출 폴더를 events/<ip>/ghost/<event_id>/로 옮깁니다."""
    gdir = ghost_event_dir(meta.get("ip"), meta.get("event_id"))
    os.makedirs(gdir, exist_ok=True)
    for sub in ("images", "images_api"):
        src = os.path.join(EVENT_ROOT_DIR, "events", str(meta.get("ip")), sub, SPEEDING_EVENT, f"{meta.get('event_id')}.jpg")
        if os.path.exists(src):
            shutil.move(src, os.path.join(gdir, f"{sub}.jpg"))
    if out_dir and os.path.isdir(str(out_dir)):
        shutil.move(str(out_dir), os.path.join(gdir, "fullres"))
    return gdir


def run_plr_on_fullres(out_dir, meta):
    """원본 프레임에서 번호판을 읽습니다(_plr_on_fullres). 과속이면 관제 사진이 기다리는 번호판 결과를 넘깁니다
    (못 읽었으면 None). 레이더 가짜 물체(radar_ghost_check)면 결과를 GHOST_STATUS로 넘겨 관제로 보내지 않습니다."""
    result = None
    try:
        result = _plr_on_fullres(out_dir, meta)
        return result
    finally:
        if meta.get("event_name") == SPEEDING_EVENT:
            final = (result or {}).get("final")
            try:
                if radar_ghost_check(meta, final):
                    GHOST_EVENTS.append(str(meta.get("event_id")))
                    final = {"text": "", "status": GHOST_STATUS}
                    gdir = move_ghost_event(meta, out_dir)
                    logger.warning(f"[GHOST] 레이더 가짜 물체로 봅니다(차 박스 없음·레이더 트랙 바로 사라짐·번호판 없음). "
                                   f"관제로 보내지 않고 사진·원본을 옮겼습니다. event_id={meta.get('event_id')} dir={gdir}")
            except Exception as e:
                logger.error(f"[GHOST] 가짜 물체 처리 실패 event_id={meta.get('event_id')}: {e}")
            if final and final.get("text") and (result or {}).get("api_scene"):
                final = dict(final, api_scene=result["api_scene"])     # 관제 사진을 원본 장면으로(_save_and_send_task)
            resolve_plate_future(meta.get("event_id"), dict(final) if final else None)


def radar_point_at(radar_path, pts, frame_shape, max_dt=PLR_RADAR_MAX_DT):
    """그 프레임 시각(max_dt 안) 레이더 점의 화면 투영 (u, v)(추론 프레임 px).
    그 시각 점이 없거나 점이 화면에서 많이 벗어나면(가로 -15~115%, 세로 0~130% 밖) False(이 프레임은 찾지 않음),
    경로에 투영이 아예 없으면 None(거르지 않음)."""
    seen = [q for q in radar_path or [] if len(q) >= 5 and q[3] is not None and q[4] is not None]
    if pts is None or not seen or frame_shape is None:
        return None
    first = min(float(q[0]) for q in seen)
    if float(pts) < first - max_dt:
        # 레이더 기록 시작 전 프레임: 처음 1초 경로를 직선으로 뒤로 이어 위치를 추정합니다.
        head = [q for q in seen if float(q[0]) <= first + 1.0]
        ts = [float(q[0]) - first for q in head]
        if len(head) < 3 or max(ts) - min(ts) < 0.3:
            return False
        u = float(np.polyval(np.polyfit(ts, [float(q[3]) for q in head], 1), float(pts) - first))
        v = float(np.polyval(np.polyfit(ts, [float(q[4]) for q in head], 1), float(pts) - first))
    else:
        q = min(seen, key=lambda r: abs(float(r[0]) - float(pts)))
        if abs(float(q[0]) - float(pts)) > max_dt:
            return False
        u, v = float(q[3]), float(q[4])
    h, w = float(frame_shape[0]), float(frame_shape[1])
    if not (-0.15 * w <= u <= 1.15 * w and 0.0 <= v <= 1.3 * h):
        return False
    return u, v


PLR_PLATE_ZONE_MIN_Y = 0.6  # 앞번호판은 차 아래쪽에 붙습니다. 번호판 가운데가 박스 높이의 이 비율보다 아래여야 그 차 번호판으로 봅니다.


def vehicle_plate_zone(box, x_from=None):
    """그 차 번호판이 있을 곳 (왼쪽, 오른쪽, 위쪽 한계)(추론 프레임 px). x_from: 가로 범위를 따로 쓸 영역(따라가기 끝난 뒤)."""
    x1, y1, x2, y2 = (float(v) for v in box[:4])
    xs = x_from if x_from is not None else box
    return float(xs[0]), float(xs[2]), y1 + PLR_PLATE_ZONE_MIN_Y * (y2 - y1)


def plate_in_zone(box, zone):
    """번호판 상자(추론 프레임 px) 가운데가 zone 안인지."""
    cx, cy = (float(box[0]) + float(box[2])) / 2.0, (float(box[1]) + float(box[3])) / 2.0
    return zone[0] <= cx <= zone[1] and cy >= zone[2]


def plate_radar_dist(box, point):
    """번호판 상자(추론 프레임 px) 가운데와 레이더 점의 거리(px)."""
    return math.hypot((float(box[0]) + float(box[2])) / 2.0 - point[0], (float(box[1]) + float(box[3])) / 2.0 - point[1])


def plate_near_radar(box, point, k=PLR_RADAR_NEAR_PLATES):
    """번호판 상자(추론 프레임 px) 가운데가 레이더 점(point)에서 번호판 크기 max(폭, 높이×3)의 k배 안인지.
    큰 트럭은 레이더 점이 컨테이너 쪽에 찍혀 번호판과 꽤 떨어지지만, 다른 차 번호판은 그보다 훨씬 멀리 있습니다."""
    x1, y1, x2, y2 = (float(c) for c in box[:4])
    size = max(x2 - x1, 3.0 * (y2 - y1), 1.0)
    return math.hypot((x1 + x2) / 2.0 - point[0], (y1 + y2) / 2.0 - point[1]) <= k * size


def _plr_on_fullres(out_dir, meta):
    """원본 프레임에서 그 차의 번호판을 찾고, 잘림·선명도·시각으로 고른 장(pick_plates_for_ocr)만 OCR해 결과를 남깁니다."""
    cfg = SYS_CFG["plr"]
    if not cfg["enabled"]:
        return None
    bbox, inference_shape, full_shape = meta.get("event_bbox"), meta.get("inference_shape"), meta.get("full_shape")
    if not bbox or not inference_shape or not full_shape:
        logger.warning(f"[PLR] 차량 박스/크기 정보가 없어 건너뜁니다. event_id={meta.get('event_id')}")
        return None
    recognizer = get_plate_recognizer()
    if recognizer is None:
        return None

    out_dir = Path(out_dir)
    plate_dir = out_dir / "plr"
    plate_dir.mkdir(exist_ok=True)
    full_h, full_w = full_shape
    sx, sy = full_w / float(inference_shape[1]), full_h / float(inference_shape[0])
    margin = float(cfg["vehicle_margin"])

    def crop_of(box):
        vx1, vy1, vx2, vy2 = box[0] * sx, box[1] * sy, box[2] * sx, box[3] * sy
        mw, mh = (vx2 - vx1) * margin, (vy2 - vy1) * margin
        return (max(0, int(vx1 - mw)), max(0, int(vy1 - mh)), min(full_w, int(vx2 + mw)), min(full_h, int(vy2 + mh)))
    cx1, cy1, cx2, cy2 = crop_of(bbox)
    event_crop = (cx1, cy1, cx2, cy2)
    pad = float(cfg["plate_padding"])
    max_dt = float(cfg["max_dt_sec"])

    # 번호판 찾을 영역: 따라간 박스(follow_boxes)가 있으면 그 프레임 시각의 박스(0.15초 안), 따라가기가 끝난 뒤 프레임은
    # 마지막 박스를 넓힌 영역(tail_search_box). 박스가 끝내 안 붙은 차는 ROI(search_box), 둘 다 없으면 이벤트 박스
    # (이벤트 시점 ±max_dt). 프레임(최대 PLR_MAX_FRAMES)마다 번호판을 찾고, OCR은 pick_plates_for_ocr로 고른 장만 합니다.
    frames = [f for f in meta.get("frames", []) if f.get("dt_from_event") is not None]
    follow = sorted(meta.get("follow_boxes") or [], key=lambda b: b[0])
    region, mode = {}, {}
    radar_point = {}
    plate_zone = {}  # 박스로 찾는 프레임: 그 차 번호판이 있을 곳(vehicle_plate_zone). 뒤차·옆차 번호판을 거릅니다(11/9 14:36:01 96).
    pick_nearest = set()  # 첫 박스 전·레이더 기록 전 프레임: 레이더 점에서 가장 가까운 번호판을 씁니다.

    def use_radar_region(f, box=None):
        # 차 박스가 없는 프레임: ROI(search_box)에서 찾되, 그 시각 레이더 점 근처 번호판만 씁니다(plate_near_radar).
        # ROI에서 가장 확실한 번호판을 그냥 고르면 레이더가 잡은 차가 아닌 다른 차 번호판을 읽습니다
        # (11/8 16:07:24 107: 레이더는 야드 트랙터, 사진은 멀리 서 있던 승합차). 그 시각 레이더 점이 없거나
        # 화면에서 많이 벗어나 있으면 이 프레임은 찾지 않습니다.
        point = radar_point_at(meta.get("radar_path"), f.get("pts"), inference_shape)
        if point is False or (box is not None and point is None):
            return
        region[f["file"]], mode[f["file"]] = box or meta["search_box"], "radar"
        if point is not None:
            radar_point[f["file"]] = point
            if float(f["pts"]) < float(meta["radar_path"][0][0]) - PLR_RADAR_MAX_DT:
                pick_nearest.add(f["file"])
    if follow:
        for f in frames:
            if f.get("pts") is None:
                continue
            near = min(follow, key=lambda b: abs(b[0] - f["pts"]))
            if abs(near[0] - f["pts"]) <= 0.15:
                region[f["file"]], mode[f["file"]] = near[1], "follow"
                plate_zone[f["file"]] = vehicle_plate_zone(near[1])
            elif meta.get("search_box"):
                # 박스를 일찍 놓쳐 레이더 경로로 고른 구간(radar_path_box_lost): ROI에서 찾되 레이더 점 근처 번호판만.
                use_radar_region(f)
            elif meta.get("before_box_search") and f["pts"] < follow[0][0]:
                # 박스가 늦게 붙은 차(11/9 11:28:12 107): 차가 화면 안에 온전한 앞쪽 프레임도 레이더 점으로 찾습니다.
                use_radar_region(f, meta["before_box_search"])
                pick_nearest.add(f["file"])
            elif f["pts"] > follow[-1][0]:
                region[f["file"]], mode[f["file"]] = tail_search_box(follow[-1][1], inference_shape), "follow_tail"
                plate_zone[f["file"]] = vehicle_plate_zone(follow[-1][1], x_from=region[f["file"]])
        frames = [f for f in frames if f["file"] in region]
        if meta.get("search_box"):
            event_crop = crop_of(meta["search_box"])
    elif meta.get("search_box"):
        event_crop = crop_of(meta["search_box"])
        for f in frames:
            use_radar_region(f)
        frames = [f for f in frames if f["file"] in region]
    else:
        frames = [f for f in frames if abs(f["dt_from_event"]) <= max_dt]
        for f in frames:
            region[f["file"]], mode[f["file"]] = bbox, "event"
    frames = frames[::max(1, int(math.ceil(len(frames) / float(PLR_MAX_FRAMES))))]

    started = time.monotonic()
    rows, plates = [], {}
    for frame in frames:
        cx1, cy1, cx2, cy2 = crop_of(region[frame["file"]])
        image = cv2.imread(str(out_dir / frame["file"]))
        if image is None or cx2 <= cx1 or cy2 <= cy1:
            continue
        # 크롭 전체 + 800px 조각에서 찾음(번호판 모델이 중간 크기를 놓치는 문제, detect_plate_tiled)
        dets, det_ms = detect_plate_tiled(recognizer.detect, image[cy1:cy2, cx1:cx2])
        row = {"frame": frame["file"], "pts": frame.get("pts"), "dt": frame.get("dt_from_event"),
               "det_ms": round(det_ms, 2), "plate": None, "mode": mode[frame["file"]],
               "vehicle_crop_full": [cx1, cy1, cx2, cy2],
               # 이 프레임에서 찾은 번호판 전부(원본 좌표): 관제 사진(api_scene)에서 다른 차 번호판도 모자이크합니다.
               "plates_all": [[int(cx1 + d[0]), int(cy1 + d[1]), int(cx1 + d[2]), int(cy1 + d[3])] for d in dets]}
        def in_view(d):
            return [(cx1 + d[0]) / sx, (cy1 + d[1]) / sy, (cx1 + d[2]) / sx, (cy1 + d[3]) / sy]
        zone = plate_zone.get(frame["file"])
        if zone is not None and len(dets):
            on_vehicle = [d for d in dets if plate_in_zone(in_view(d), zone)]
            row["plates_off_vehicle"] = len(dets) - len(on_vehicle)
            dets = on_vehicle
        point = radar_point.get(frame["file"])
        if point is not None and len(dets):
            # 레이더 점 근처 번호판만(plate_near_radar). 거른 개수는 분석용으로 남깁니다.
            near = [d for d in dets if plate_near_radar(in_view(d), point)]
            row["radar_uv"] = [round(float(point[0]), 1), round(float(point[1]), 1)]
            row["plates_off_radar"] = len(dets) - len(near)
            dets = near
            if frame["file"] in pick_nearest and len(dets) > 1:
                dets = [min(dets, key=lambda d: plate_radar_dist(in_view(d), point))]
        if len(dets):
            # 영역 안에서 가장 확실한 번호판 1개를 씁니다.
            px1, py1, px2, py2, score = [float(v) for v in max(dets, key=lambda d: d[4])[:5]]
            pw, ph = px2 - px1, py2 - py1
            gx1, gy1 = max(0, int(cx1 + px1 - pw * pad)), max(0, int(cy1 + py1 - ph * pad))
            gx2, gy2 = min(full_w, int(cx1 + px2 + pw * pad)), min(full_h, int(cy1 + py2 + ph * pad))
            plate = image[gy1:gy2, gx1:gx2]
            if plate.size:
                name = f"plate_{Path(frame['file']).stem}.jpg"
                cv2.imwrite(str(plate_dir / name), plate, [cv2.IMWRITE_JPEG_QUALITY, 95])
                box_full = [int(cx1 + px1), int(cy1 + py1), int(cx1 + px2), int(cy1 + py2)]
                row["plate"] = {"file": f"plr/{name}", "det_conf": round(score, 4), "box_full": box_full,
                                "size": [int(pw), int(ph)], "sharp": round(plate_sharpness(plate), 1),
                                "edge": plate_touches_edge(box_full, full_shape)}
                plates[len(rows)] = plate
        rows.append(row)
    picked = pick_plates_for_ocr([{"key": i, "width": rows[i]["plate"]["size"][0], "edge": rows[i]["plate"]["edge"],
                                   "sharp": rows[i]["plate"]["sharp"]} for i in plates])
    for i in picked:
        rows[i]["plate"]["ocr"] = recognizer.read_plate(plates[i])

    summary = {}
    reads = [r["plate"]["ocr"]["cpu"] for r in rows if r["plate"] and "cpu" in (r["plate"].get("ocr") or {})]
    if reads:
        best = max(reads, key=lambda r: r["conf"])
        texts = [r["text"] for r in reads if r["text"]]
        summary["cpu"] = {"text": best["text"], "conf": best["conf"],
                          "vote": max(set(texts), key=texts.count) if texts else "",
                          "ms_avg": round(float(np.mean([r["ms"] for r in reads])), 2), "reads": len(reads)}
    det_ms = [r["det_ms"] for r in rows]
    result = {
        "event_id": meta.get("event_id"), "cam_id": meta.get("cam_id"), "ip": meta.get("ip"),
        "event_name": meta.get("event_name"), "created_at": now_kst().isoformat(),
        "models": {"det_model": os.path.basename(recognizer.det.engine_path), "ocr_cpu_model": cfg["ocr_cpu_model"]},
        "vehicle_crop_full": list(event_crop), "frames_used": len(rows),
        "final": finalize_plate(rows), "ocr_frames": len(picked), "follow_boxes": meta.get("follow_boxes"),
        "plates_found": sum(1 for r in rows if r["plate"]),
        "det_ms_avg": round(float(np.mean(det_ms)), 2) if det_ms else None,
        "summary": summary, "frames": rows,
        "elapsed_sec": round(time.monotonic() - started, 3),
    }
    if meta.get("evidence") is not None:  # 과속(차량을 따라간 이벤트)만 증거 3장 묶음
        try:
            api_scene = {}
            result["evidence_file"] = make_evidence_image(out_dir, meta, rows, result["final"], api_scene)
            if api_scene:
                result["api_scene"] = api_scene
        except Exception as e:
            logger.warning(f"[PLR] 증거 이미지 생성 실패 event_id={meta.get('event_id')}: {e}")
    tmp = out_dir / ".plr_meta.tmp"
    tmp.write_text(json.dumps(to_json_safe(result), ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(out_dir / "plr_meta.json")

    # 이벤트 단위 누적표 (엑셀에서 바로 열도록 UTF-8 BOM)
    csv_path = Path(LOG_DIR) / str(cfg["compare_csv"])
    new_file = not csv_path.exists()
    cpu = summary.get("cpu", {})
    final = result["final"]
    with open(csv_path, "a", encoding="utf-8-sig" if new_file else "utf-8", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(["created_at", "event_id", "cam_id", "ip", "event_name", "frames_used", "plates_found",
                             "det_ms_avg", "cpu_text", "cpu_conf", "cpu_vote", "cpu_ms_avg", "final_text", "final_status",
                             "final_support", "dir"])
        writer.writerow([result["created_at"], result["event_id"], result["cam_id"], result["ip"], result["event_name"],
                         result["frames_used"], result["plates_found"], result["det_ms_avg"],
                         cpu.get("text", ""), cpu.get("conf", ""), cpu.get("vote", ""), cpu.get("ms_avg", ""),
                         final["text"], final["status"], final["support"], str(out_dir)])
    logger.info(
        f"[PLR] event_id={result['event_id']} frames={result['frames_used']} plates={result['plates_found']} "
        f"ocr={result['ocr_frames']} "
        f"OCR='{cpu.get('text', '')}'({cpu.get('conf', '-')}, {cpu.get('ms_avg', '-')}ms) "
        f"det_avg={result['det_ms_avg']}ms elapsed={result['elapsed_sec']}s "
        f"FINAL='{final['text']}'({final['status']}, {final['support']}/{final['valid_reads']}개 일치)"
    )
    return result


class FrameReader:
    """모든 카메라 공통 영상 수신기. 이벤트가 stream_reader를 바꾸지 않으면 이 수신기로 받습니다(BaseEventDetector.stream_spec).

    카메라마다 같은 방식이고, 다른 것은 숫자 두 개(max_fps 초당 장수, source_max_width 그림 가로 상한)뿐입니다.
    카메라가 자기 이벤트들이 적어 둔 값을 모아 정합니다(Camera._reader_spec).
    - GStreamer(기본): GPU 디코딩 → GPU 메모리에 있을 때 초당 max_fps장만 남김(버리기만 하고 같은 그림을 복제하지 않음)
      → GPU에서 크기 조절·NV12 → 남긴 장만 내려받기 → 1장짜리 대기 칸(파이썬이 늦게 읽으면 다 만든 오래된 그림을
      통째로 버려 디코더·네트워크 수신이 멈추지 않음) → 파이프 → 파이썬(1MB 파이프, 미리 잡아 둔 버퍼에 바로 읽기)
    - 대체 경로(GStreamer가 안 될 때): FFmpeg VAAPI 파이프 → OpenCV. 대체 경로도 초당 max_fps장만 넘깁니다.
    - 읽는 쪽: read(include_source)는 (추론용 프레임, fid, 연결 여부[, 원본 프레임]), get_frame_meta(fid)는 받은 시각 등입니다.
    자기 수신 방식이 있는 이벤트는 이 클래스를 물려받은 수신기를 씁니다(과속: SpeedFrameReader, 프레임별 PTS)."""
    # OpenCV 대체 경로에서 프레임 PTS(CAP_PROP_POS_MSEC)도 기록할지. 과속 수신기만 켭니다.
    _OPENCV_PTS = False

    def __init__(self, url, ip, max_fps=None, source_max_width=720, inference_size=None, start=True):
        self.url = apply_camera_password(sanitize_camera_url(url))
        self.ip = ip
        # 초당 받는 장수 상한(None = 전부)과 디코더 출력(원본 프레임) 가로 상한. Camera._reader_spec이 정합니다.
        self.max_fps = max_fps
        self.source_max_width = source_max_width
        # 추론용 프레임 크기 상한 (w, h). None이면 원본 프레임을 그대로 추론에 씁니다.
        self.inference_size = inference_size
        # 추론용 프레임 축소 보간. 콘 전용 카메라는 이 프레임을 얼굴·녹화·화면에만 써서 Camera가 INTER_LINEAR로 바꿉니다.
        self.inference_interp = cv2.INTER_AREA
        self.frame = None
        self.source_frame = None
        self.source_shape = None
        self.fid = 0
        # 최신 프레임 1개만 들고, 받은 시각 같은 정보는 fid별 메타로 따로 둡니다.
        self.frame_meta = None
        self._frame_meta_history = deque(maxlen=256)
        self._seq = 0
        self._last_read_seq = 0
        self.running = True
        self.connected = False
        self.last_t = time.time()
        self.lock = threading.Lock()
        self.decode_cfg = SYS_CFG["video_decode"]
        self._decode_mode = "init"
        self._decode_restarts = 0
        self._pipeline_started_mono = time.monotonic()
        # GStreamer에서 첫 프레임을 연속으로 못 받으면 그 시각까지 GStreamer를 건너뜁니다(_run_gstreamer_pipe).
        self._gst_skip_until = 0.0
        self._gst_no_frame_count = 0
        # 끄는 중(종료 신호로 ffprobe가 끝남) 표시. _stopping이 한 번 읽고 지웁니다.
        self._probe_stopped_by_signal = False
        self._stream_warn_startup = 0
        self._stream_warn_runtime = 0
        self._stats_window_start = time.monotonic()
        self._stats_frames = 0
        self._stats_reads = 0
        self._stats_dropped = 0
        self._stats_dropped_total = 0

        if start:
            threading.Thread(target=self._run, daemon=True).start()

    def _publish_frame(self, fr, source_fr, pts=None, is_key=None):
        """디코딩된 최신 프레임으로 바꿉니다. 이전 프레임을 아무도 읽지 않았다면 드롭으로 셉니다(read).
        공통 수신기는 PTS 대신 받은 시각(recv_monotonic)을 남깁니다. 과속 수신기는 PTS를 더 기록합니다(_pts_meta)."""
        recv_mono = time.monotonic()
        with self.lock:
            self._seq += 1
            pts_meta = self._pts_meta(pts, recv_mono)
            self.fid += 1
            meta = {
                "fid": self.fid,
                "seq": self._seq,
                "pts": pts_meta["pts"],
                "pts_smoothed": pts_meta["pts_smoothed"],
                "frame_interval": pts_meta["frame_interval"],
                "pts_lag": pts_meta["pts_lag"],
                "is_key": is_key,
                "recv_monotonic": recv_mono,
                "recv_time": time.time(),
                "ring_dir": pts_meta["ring_dir"],
            }
            self.frame = fr
            self.source_frame = source_fr
            self.frame_meta = meta
            self._frame_meta_history.append(meta)
            self.last_t = time.time()
            self._stats_frames += 1
        self._maybe_log_stream_stats()

    def _pts_meta(self, pts, recv_mono):
        """프레임 메타의 PTS 칸. 공통 수신기는 PTS를 남기지 않습니다(과속 수신기가 바꿈)."""
        return {"pts": None, "pts_smoothed": None, "frame_interval": None, "pts_lag": None, "ring_dir": None}

    def _maybe_log_stream_stats(self):
        now = time.monotonic()
        elapsed = now - self._stats_window_start
        if elapsed < max(5.0, float(self.decode_cfg["stats_log_interval_sec"])):
            return
        with self.lock:
            frames, reads, dropped = self._stats_frames, self._stats_reads, self._stats_dropped
            pending = 1 if self._seq > self._last_read_seq else 0
            meta = self.frame_meta or {}
            self._stats_frames = self._stats_reads = self._stats_dropped = 0
            self._stats_window_start = now
        interval = meta.get("frame_interval")
        lag = meta.get("pts_lag")
        # 값이 있을 때만 붙입니다: PTS(GStreamer 경로는 없음), HEVC 경고(H.264면 늘 0).
        # reconnects는 처음 연결을 빼고 다시 연결한 횟수입니다(_decode_restarts는 연결할 때마다 셈).
        extra = ""
        if interval:
            extra += f" pts_interval_ms={interval * 1000:.2f}"
        if lag is not None:
            extra += f" pts_lag_ms={lag * 1000:.0f}"
        if self._stream_warn_startup or self._stream_warn_runtime:
            extra += f" hevc_warn_startup={self._stream_warn_startup} hevc_warn_runtime={self._stream_warn_runtime}"
        logger.info(
            "[STREAM STATS] CAM:%s mode=%s decode_fps=%.1f read_fps=%.1f dropped=%d dropped_total=%d "
            "pending=%d reconnects=%d%s",
            self.ip, self._decode_mode, frames / elapsed, reads / elapsed, dropped, self._stats_dropped_total,
            pending, max(0, self._decode_restarts - 1), extra
        )
    def get_frame_meta(self, fid):
        """fid에 해당하는 수신 PTS/드롭 정보를 돌려줍니다. 오래되어 밀려났으면 None입니다."""
        with self.lock:
            for meta in reversed(self._frame_meta_history):
                if meta["fid"] == fid:
                    return dict(meta)
        return None
    def _inference_frame(self, source_fr):
        """원본 프레임을 모델 입력(inference_size) 안에 비율 유지로 줄인 추론용 프레임. 이미 작거나 크기가 없으면 원본 그대로."""
        if not self.inference_size:
            return source_fr
        h, w = source_fr.shape[:2]
        in_w, in_h = self.inference_size
        ratio = min(in_w / float(w), in_h / float(h))
        if ratio >= 1.0:
            return source_fr
        size = (max(1, int(round(w * ratio))), max(1, int(round(h * ratio))))
        return cv2.resize(source_fr, size, interpolation=self.inference_interp)
    def read(self, include_source=False):
        with self.lock:
            if self._seq > self._last_read_seq:
                # 읽기 전에 덮어쓴 프레임은 최신 프레임 유지 정책으로 버려진 것입니다.
                if self._last_read_seq > 0:
                    skipped = self._seq - self._last_read_seq - 1
                    self._stats_dropped += skipped
                    self._stats_dropped_total += skipped
                self._last_read_seq = self._seq
                self._stats_reads += 1
            if include_source:
                return self.frame, self.fid, self.connected, self.source_frame
            return self.frame, self.fid, self.connected

    def _mask_pipeline_text(self, text):
        return re.sub(r"(?i)(rtsp://)([^/@\s]+)@", r"\1***@", str(text))

    def _short_external_output(self, text, limit=800):
        if isinstance(text, bytes):
            text = text.decode("utf-8", "replace")
        text = self._mask_pipeline_text(str(text or "").strip())
        text = re.sub(r"\s+", " ", text)
        if len(text) > limit:
            return text[:limit] + "..."
        return text

    def _ensure_clean_url(self, context):
        clean_url = sanitize_camera_url(self.url)
        if clean_url != self.url:
            logger.warning(
                f"[CAM:{self.ip}] {context} sanitized runtime stream URL: "
                f"{self._mask_pipeline_text(repr(self.url))} -> "
                f"{self._mask_pipeline_text(repr(clean_url))}"
            )
            self.url = clean_url
        return self.url

    def _drain_binary_log_pipe(self, pipe, line_buffer):
        try:
            for raw_line in iter(pipe.readline, b""):
                line = self._short_external_output(raw_line)
                if line:
                    line_buffer.append(line)
        except Exception:
            pass
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    def _set_decode_pipeline(self, mode):
        """수신 경로가 (다시) 시작될 때: 경로 이름(통계 로그), 연결 횟수, 시작 시각(과속 PTS 변환 기준)."""
        self._decode_mode = mode
        self._decode_restarts += 1
        self._pipeline_started_mono = time.monotonic()

    def _open_capture(self):
        cap = cv2.VideoCapture()
        cap.open(self.url, cv2.CAP_FFMPEG, [
            cv2.CAP_PROP_HW_ACCELERATION, cv2.VIDEO_ACCELERATION_NONE,
            cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 5000,
            cv2.CAP_PROP_READ_TIMEOUT_MSEC, 5000,
        ])
        if cap.isOpened():
            self._set_decode_pipeline("opencv_ffmpeg_cpu")
        return cap

    def _should_use_ffmpeg_vaapi_pipe(self):
        backend = str(self.decode_cfg["backend"]).strip().lower()
        if backend in ("opencv", "cv2", "ffmpeg_opencv", "gstreamer", "gst", "gst_vaapi"):
            return False

        mode = str(self.decode_cfg["hw_acceleration"]).strip().lower()
        if mode in ("", "none", "off", "cpu", "false", "0"):
            return False

        if not sys.platform.startswith("linux"):
            return False

        hw_device = str(self.decode_cfg["hw_device"]).strip()
        if mode not in ("auto", "vaapi") or not hw_device or not os.path.exists(hw_device):
            return False

        return bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))

    def _should_use_gstreamer_pipe(self):
        backend = str(self.decode_cfg["backend"]).strip().lower()
        if backend in ("opencv", "cv2", "ffmpeg_opencv", "ffmpeg", "ffmpeg_vaapi", "vaapi"):
            return False

        # 첫 프레임이 연속으로 안 나오면 5분 동안 GStreamer를 건너뜁니다.
        if time.time() < self._gst_skip_until:
            return False

        if backend not in ("auto", "gstreamer", "gst", "gst_vaapi"):
            return False

        if not sys.platform.startswith("linux"):
            return False

        gst_launch = shutil.which("gst-launch-1.0")
        gst_inspect = shutil.which("gst-inspect-1.0")
        if not gst_launch or not gst_inspect:
            return False

        return bool(shutil.which("ffprobe"))

    def _probe_stream_info(self):
        probe_url = self._ensure_clean_url("ffprobe")
        cmd = ["ffprobe", "-v", "error"]
        if probe_url.lower().startswith("rtsp://"):
            cmd.extend(["-rtsp_transport", "tcp", "-allowed_media_types", "video", "-stimeout", "5000000"])
        cmd.extend([
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,codec_name",
            "-of", "json",
            probe_url,
        ])

        try:
            result = subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=8,
                text=True
            )
            out = (result.stdout or "").strip()
            err = (result.stderr or "").strip()
        except Exception as e:
            if not self.running:
                return None
            logger.warning(f"[CAM:{self.ip}] ffprobe stream info failed: {e}")
            return None

        if result.returncode != 0:
            if result.returncode in (-signal.SIGINT, -signal.SIGTERM) or not self.running:
                # 프로그램을 끌 때(Ctrl+C 신호로 ffprobe가 같이 끝남, rc=-2)는 실패가 아니므로 경고하지 않고,
                # 다른 수신 방법으로 넘어가지 않게 표시합니다(_stopping).
                self._probe_stopped_by_signal = True
                logger.debug(f"[CAM:{self.ip}] ffprobe stopped rc={result.returncode}")
                return None
            logger.warning(
                f"[CAM:{self.ip}] ffprobe stream info failed: rc={result.returncode} "
                f"stderr={self._short_external_output(err)} stdout={self._short_external_output(out)}"
            )
            return None

        try:
            data = json.loads(out)
            streams = data.get("streams", [])
            if not streams:
                raise ValueError("no video stream")
            stream = streams[0]
            width, height = int(stream.get("width") or 0), int(stream.get("height") or 0)
            codec = str(stream.get("codec_name") or "").strip().lower()
            if width > 0 and height > 0:
                return width, height, codec
        except Exception as e:
            logger.warning(
                f"[CAM:{self.ip}] ffprobe stream info parse failed: {e} "
                f"stdout={self._short_external_output(out)} stderr={self._short_external_output(err)}"
            )

        logger.warning(
            f"[CAM:{self.ip}] ffprobe returned no usable video info: "
            f"stdout={self._short_external_output(out)} stderr={self._short_external_output(err)}"
        )
        return None

    def _probe_stream_shape(self):
        info = self._probe_stream_info()
        if info is None:
            return None
        width, height, _ = info
        return width, height

    def _scaled_output_shape(self, width, height):
        """디코더 출력(원본 프레임) 크기. 가로 source_max_width(기본 720) 이하로 비율 유지 축소하고 확대는 하지 않습니다.
        1280x720 출력은 30fps 카메라에서 VAAPI 다운로드가 실시간을 못 따라가 지연이 누적됩니다.
        콘은 720폭에서 너무 작아 잡히지 않아(최고 0.02, 1920폭 0.77) 콘 카메라만 원본 폭을 씁니다."""
        max_w = int(self.source_max_width)
        if width <= max_w:
            return width, height
        ratio = max_w / float(width)
        out_height = max(2, int(round((height * ratio) / 2.0) * 2))
        return max_w, out_height

    def _ffmpeg_keep_filter(self):
        """FFmpeg에서 초당 max_fps장만 남기는 select 필터(제한이 없으면 ""). 1/max_fps초 칸마다 첫 프레임 하나만 남겨
        버리기만 하고 원래 PTS는 그대로 둡니다. 15fps → 10장처럼 나눠떨어지지 않아도 초당 max_fps장이 남습니다."""
        if not self.max_fps:
            return ""
        rate = f"{float(self.max_fps):g}"
        return f"select=isnan(prev_selected_t)+gt(floor(t*{rate})\\,floor(prev_selected_t*{rate}))"

    def _gst_element_exists(self, element_name):
        try:
            subprocess.run(
                ["gst-inspect-1.0", element_name],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=3,
                check=True
            )
            return True
        except Exception:
            return False

    def _first_gst_element(self, candidates):
        for element_name in candidates:
            if self._gst_element_exists(element_name):
                return element_name
        return None

    def _select_gstreamer_decoder(self, codec):
        codec = str(codec or "").strip().lower()
        mode = str(self.decode_cfg["hw_acceleration"]).strip().lower()
        allow_hw = mode not in ("", "none", "off", "cpu", "false", "0")

        if codec in ("h264", "avc1"):
            depay, parser = "rtph264depay", "h264parse"
            # VA 플러그인(vah*)을 먼저 씁니다. 단말기(GStreamer 1.20)에서 vaapih*dec는 첫 프레임을 받지 못해 CPU로 넘어갔습니다.
            hw_candidates = ["vah264dec", "vaapih264dec"]
            cpu_candidates = ["avdec_h264", "openh264dec"]
        elif codec in ("hevc", "h265"):
            depay, parser = "rtph265depay", "h265parse"
            hw_candidates = ["vah265dec", "vaapih265dec"]
            cpu_candidates = ["avdec_h265"]
        else:
            return None

        decoder = None
        decoder_kind = "cpu"
        if allow_hw:
            decoder = self._first_gst_element(hw_candidates)
            decoder_kind = "vaapi" if decoder else "cpu"
        if decoder is None:
            decoder = self._first_gst_element(cpu_candidates)
        if decoder is None:
            return None

        return depay, parser, decoder, decoder_kind

    def _gstreamer_cmd(self, codec, out_w, out_h):
        """공통 GStreamer 수신 명령(모든 카메라 같은 순서). 이 코덱을 풀 디코더가 없으면 (None, None)."""
        decoder_info = self._select_gstreamer_decoder(codec)
        if decoder_info is None:
            return None, None
        depay, parser, decoder, decoder_kind = decoder_info
        latency_ms = int(self.decode_cfg["gstreamer_latency_ms"] or 50)
        protocols = str(self.decode_cfg["gstreamer_protocols"] or "tcp").strip().lower()

        cmd = [
            "gst-launch-1.0", "-q",
            "rtspsrc", f"location={self.url}", f"protocols={protocols}",
            f"latency={latency_ms}", "drop-on-latency=true", "tcp-timeout=3000000",
            "!", depay,
            "!", parser,
            "!", decoder
        ]
        # 1) 디코딩 직후(GPU 디코더면 GPU 메모리에 있을 때) 초당 max_fps장만 남깁니다. max-rate는 버리기만 하고
        #    같은 그림을 복제하지 않습니다. 버린 장은 크기 조절·내려받기를 하지 않습니다(1080p: CPU 29% → 16%).
        if self.max_fps:
            cmd.extend(["!", "videorate", "drop-only=true", f"max-rate={max(1, int(round(float(self.max_fps))))}"])
        # 2) 크기 조절·NV12: GPU 디코더면 vaapipostproc(GPU), 아니면 CPU. videoconvert는 GPU 패딩 메모리를
        #    파이썬이 읽기 좋은 촘촘한(Dense) NV12로 폅니다(스트라이드 찌그러짐 방지).
        if decoder_kind == "vaapi":
            cmd.extend([
                "!", "vaapipostproc",
                "!", f"video/x-raw,format=NV12,width={out_w},height={out_h}",
                "!", "videoconvert"
            ])
        else:
            cmd.extend([
                "!", "videoconvert",
                "!", "videoscale",
                "!", f"video/x-raw,format=NV12,width={out_w},height={out_h}"
            ])
        # 3) 1장짜리 대기 칸: 파이썬이 늦게 읽으면 다 만든 오래된 그림 한 장을 통째로 버립니다. 대기 칸이 없으면
        #    파이프에 막힌 동안 디코더·네트워크 수신까지 멈추고, 그 사이 패킷이 버려져 그림이 깨집니다.
        cmd.extend([
            "!", "queue", "max-size-buffers=1", "max-size-bytes=0", "max-size-time=0", "leaky=downstream",
            "!", "fdsink", "fd=1", "sync=false"
        ])
        return cmd, decoder_kind

    def _grow_pipe(self, pipe):
        """리눅스 파이프(기본 64KB)를 1MB로 키웁니다. 1920 프레임(약 3MB)을 약 48번이 아니라 3~4번에 읽어,
        읽을 때마다 파이썬 잠금(GIL)을 놓았다 다시 잡는 횟수를 줄입니다. 키우지 못하면(커널 상한 등) 그대로 둡니다."""
        if fcntl is None:
            return
        try:
            fcntl.fcntl(pipe.fileno(), getattr(fcntl, "F_SETPIPE_SZ", 1031), 1 << 20)
        except (OSError, ValueError, TypeError, AttributeError):
            pass

    @staticmethod
    def _read_frame_into(pipe, view):
        """프레임 한 장(view 크기)을 파이프에서 미리 잡아 둔 버퍼에 바로 채웁니다. 다 채우기 전에 끝나면(EOF) False."""
        size, got = len(view), 0
        while got < size:
            n = pipe.readinto(view[got:])
            if not n:
                return False
            got += n
        return True

    def _run_gstreamer_pipe(self):
        info = self._probe_stream_info()
        if info is None:
            return False

        in_w, in_h, codec = info
        out_w, out_h = self._scaled_output_shape(in_w, in_h)
        self.source_shape = (in_w, in_h)

        # 파이프 전송량 50% 감축 (BGR: 3 bytes -> NV12: 1.5 bytes)
        frame_size = out_w * out_h * 3 // 2
        cmd, _ = self._gstreamer_cmd(codec, out_w, out_h)
        if cmd is None:
            return False

        proc = None
        # GStreamer 오류 출력은 파이프가 차서 멈추지 않게 따로 계속 읽어 둡니다(최근 30줄).
        gst_stderr_lines = deque(maxlen=30)
        gst_stderr_thread = None
        first_frame_timer = None

        try:
            # bufsize=0: 파이썬 버퍼를 거치지 않고 아래에서 미리 잡아 둔 버퍼에 바로 읽습니다(_read_frame_into).
            proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    bufsize=0)
            if proc.stderr is not None:
                gst_stderr_thread = threading.Thread(target=self._drain_binary_log_pipe, args=(proc.stderr, gst_stderr_lines), daemon=True)
                gst_stderr_thread.start()
            if proc.stdout is None:
                return False
            self._grow_pipe(proc.stdout)

            self.connected = True
            self._set_decode_pipeline("gstreamer_pipe_nv12")
            self.last_t = time.time()
            first_frame_logged = False
            # 파이프는 떴는데 첫 프레임이 안 나오면 읽기가 계속 멈춰 있으므로,
            # 10초 안에 첫 프레임이 없으면 프로세스를 끝내고 실패로 돌려줍니다(→ VAAPI/OpenCV 대체).
            first_frame_seen, no_first_frame = threading.Event(), threading.Event()

            def _no_first_frame(p=proc):
                if not first_frame_seen.is_set():
                    no_first_frame.set()
                    try:
                        p.kill()
                    except Exception:
                        pass
            first_frame_timer = threading.Timer(10.0, _no_first_frame)
            first_frame_timer.daemon = True
            first_frame_timer.start()

            # 프레임 한 장 크기의 버퍼를 한 번만 잡고 매 프레임 다시 씁니다(BGR 변환 결과는 새 배열이라 덮어써도 됨).
            buf = bytearray(frame_size)
            view = memoryview(buf)
            yuv_img = np.frombuffer(buf, dtype=np.uint8).reshape((out_h * 3 // 2, out_w))
            while self.running:
                if time.time() - self.last_t > WATCHDOG_TIMEOUT:
                    break

                if not self._read_frame_into(proc.stdout, view):
                    break

                # NV12 → BGR 변환
                source_fr = cv2.cvtColor(yuv_img, cv2.COLOR_YUV2BGR_NV12)
                self._publish_frame(self._inference_frame(source_fr), source_fr)
                if not first_frame_logged:
                    first_frame_logged = True
                    first_frame_seen.set()
                    self._gst_no_frame_count = 0

            if not first_frame_logged:
                # 프레임을 하나도 못 받았으면 성공이 아니라 실패: 다음 방식(VAAPI/OpenCV)으로 넘어가게 함
                # 연속 2번이면 5분 동안 GStreamer를 건너뜁니다(_should_use_gstreamer_pipe).
                count = self._gst_no_frame_count + 1
                self._gst_no_frame_count = count
                if count >= 2:
                    self._gst_skip_until = time.time() + 300.0
                reason = "first_frame_timeout" if no_first_frame.is_set() else "no_frame_before_exit"
                logger.warning(f"[CAM:{self.ip}] GStreamer에서 영상 프레임을 받지 못했습니다({reason}, 연속 {count}회). "
                               f"다른 수신 방식으로 넘어갑니다.")
                return False
            return True
        except Exception as e:
            logger.warning(f"[CAM:{self.ip}] GStreamer pipe failed: {e}")
            return False
        finally:
            if first_frame_timer is not None:
                first_frame_timer.cancel()
            self.connected = False
            if proc is not None:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            if gst_stderr_thread is not None:
                try:
                    gst_stderr_thread.join(timeout=0.2)
                except Exception:
                    pass

    def _run_ffmpeg_vaapi_pipe(self):
        """대체 경로 1: FFmpeg VAAPI 디코딩. 초당 max_fps장만 먼저 남기고(버리기만, 복제 없음) 남은 장만 크기를 줄입니다."""
        shape = self._probe_stream_shape()
        if shape is None:
            return False

        in_w, in_h = shape
        out_w, out_h = self._scaled_output_shape(in_w, in_h)
        self.source_shape = (in_w, in_h)
        frame_size = out_w * out_h * 3
        hw_device = str(self.decode_cfg["hw_device"]).strip()

        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
        if self.url.lower().startswith("rtsp://"):
            cmd.extend(["-rtsp_transport", "tcp", "-stimeout", "3000000", "-fflags", "nobuffer", "-flags", "low_delay"])
        cmd.extend(["-hwaccel", "vaapi", "-hwaccel_device", hw_device, "-i", self.url, "-an"])

        vf_chain = []
        keep = self._ffmpeg_keep_filter()
        if keep:
            vf_chain.append(keep)
        if (out_w, out_h) != (in_w, in_h):
            vf_chain.append(f"scale={out_w}:{out_h}")
        if vf_chain:
            cmd.extend(["-vf", ",".join(vf_chain)])

        # passthrough: 고른 프레임만 내보냅니다(빈 시간을 같은 프레임 복제로 채우지 않음).
        cmd.extend(["-vsync", "passthrough", "-pix_fmt", "bgr24", "-f", "rawvideo", "pipe:1"])

        proc = None
        try:
            proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                    bufsize=0)
            if proc.stdout is None:
                return False
            self._grow_pipe(proc.stdout)

            self.connected = True
            self._set_decode_pipeline("ffmpeg_vaapi_pipe")
            self.last_t = time.time()

            buf = bytearray(frame_size)
            view = memoryview(buf)
            while self.running:
                if time.time() - self.last_t > WATCHDOG_TIMEOUT:
                    break

                if not self._read_frame_into(proc.stdout, view):
                    break

                # 버퍼는 다음 프레임에 다시 쓰므로 프레임은 복사해 넘깁니다.
                fr = np.frombuffer(buf, dtype=np.uint8).reshape((out_h, out_w, 3)).copy()
                self._publish_frame(self._inference_frame(fr), fr)

            return True
        except Exception as e:
            logger.warning(f"[CAM:{self.ip}] FFmpeg VAAPI pipe failed: {e}")
            return False
        finally:
            self.connected = False
            if proc is not None:
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

    def _stopping(self):
        """프로그램을 끄는 중인지(running False, 또는 종료 신호로 ffprobe가 끝남). 끄는 중에는 수신 경로가 끝나도
        경고하지 않고 다른 수신 방법(FFmpeg·OpenCV)으로 넘어가지 않습니다."""
        stopped = self._probe_stopped_by_signal
        self._probe_stopped_by_signal = False
        return stopped or not self.running

    def _run(self):
        while self.running:
            if self._should_use_gstreamer_pipe():
                used_gstreamer_pipe = self._run_gstreamer_pipe()
                if used_gstreamer_pipe:
                    continue
                if self._stopping():
                    time.sleep(0.2)
                    continue
                if not self.decode_cfg["fallback_to_cpu"]:
                    time.sleep(5)
                    continue
                logger.warning(f"[CAM:{self.ip}] GStreamer pipe unavailable; trying FFmpeg/OpenCV fallback")

            if self._should_use_ffmpeg_vaapi_pipe():
                used_vaapi_pipe = self._run_ffmpeg_vaapi_pipe()
                if used_vaapi_pipe:
                    continue
                if self._stopping():
                    time.sleep(0.2)
                    continue
                if not self.decode_cfg["fallback_to_cpu"]:
                    time.sleep(5)
                    continue
                logger.warning(f"[CAM:{self.ip}] VAAPI pipe unavailable; trying OpenCV FFmpeg reader")

            if self._stopping():
                continue
            self._run_opencv()

    def _run_opencv(self):
        """마지막 대체 경로: OpenCV(FFmpeg, CPU 디코딩). 프레임은 모두 디코딩(grab)하지만, 초당 max_fps장만
        BGR로 꺼내(retrieve) 줄인 뒤 넘깁니다(나머지는 BGR 변환·축소 없이 버림)."""
        cap = self._open_capture()
        if not cap.isOpened():
            logger.error(f" [CAM:{self.ip}] RTSP 연결 실패. 5초 후 재시도합니다.")
            time.sleep(5)
            return

        self.connected = True
        logger.info(f"[CAM:{self.ip}] 카메라 스트림 연결 성공.")
        self.last_t = time.time()
        last_slot = None

        try:
            while self.running and cap.isOpened():
                if time.time() - self.last_t > WATCHDOG_TIMEOUT:
                    logger.error(f" [CAM:{self.ip}] 카메라 수신 타임아웃({WATCHDOG_TIMEOUT}s). 재연결을 시도합니다.")
                    break

                if not cap.grab():
                    break
                if self.max_fps:
                    # 1/max_fps초 칸마다 첫 프레임 하나만 꺼냅니다.
                    slot = int(time.monotonic() * float(self.max_fps))
                    if slot == last_slot:
                        continue
                    last_slot = slot
                ret, fr = cap.retrieve()
                if not ret or fr is None:
                    continue

                disp_w, disp_h = self._scaled_output_shape(fr.shape[1], fr.shape[0])
                if (disp_w, disp_h) != (fr.shape[1], fr.shape[0]):
                    fr = cv2.resize(fr, (disp_w, disp_h), interpolation=cv2.INTER_AREA)
                pts = None
                if self._OPENCV_PTS:
                    pos_msec = cap.get(cv2.CAP_PROP_POS_MSEC)
                    pts = pos_msec / 1000.0 if pos_msec and pos_msec > 0 else None
                self._publish_frame(self._inference_frame(fr), fr, pts=pts)
        finally:
            self.connected = False
            try: cap.release()
            except Exception as e: logger.error(f"카메라 리소스 해제 중 예외: {e}")


class SpeedFrameReader(FrameReader):
    """과속 카메라 전용 수신기(SpeedingDetector.stream_reader). 공통 수신기 FrameReader를 물려받아 과속에 필요한 것만 더합니다.

    - FFmpeg 파이프 수신(VAAPI 또는 CPU, TCP, 첫 키프레임부터, low_delay)으로 프레임마다 PTS를 받습니다.
    - 프레임마다 PTS·드롭 정보(get_frame_meta), PTS↔PC 시각 변환(pts_at, camera_time_of): 레이더 시각 맞춤
    - 원본 링버퍼(번호판용), PTS 지연이 커지면 재접속, 수신 통계 로그
    FFmpeg가 안 되면 공통 수신기의 GStreamer → OpenCV 경로로 받습니다(이때는 PTS·링버퍼 없음).
    """
    _SHOWINFO_RE = re.compile(r"Parsed_showinfo.*?\bn:\s*(\d+)\s+pts:\s*-?\d+\s+pts_time:\s*(-?[\d.]+).*?\biskey:(\d)")
    _HEVC_PARAM_SET_RE = re.compile(r"VPS \d+ does not exist|PPS id out of range|Error parsing NAL unit|Invalid data found when processing input")
    _VAAPI_FAIL_RE = re.compile(r"(?i)vaapi|hwaccel|hardware device|Device creation failed|Failed to (initialise|create|map|transfer)|scale_vaapi|hwdownload")
    _OPENCV_PTS = True

    def __init__(self, url, ip, max_fps=None, source_max_width=720, inference_size=None):
        # 과속은 레이더 연결에 모든 프레임의 PTS가 필요해 보통 max_fps=None입니다(SpeedingDetector.stream_all_frames).
        super().__init__(url, ip, max_fps=max_fps, source_max_width=source_max_width,
                         inference_size=inference_size or (640, 640), start=False)
        fullres_cfg = SYS_CFG["fullres_capture"]
        self.packet_ring = PacketRing(ip, fullres_cfg) if fullres_cfg["enabled"] else None
        self._ring_gen_dir = None
        self._pts_window = deque(maxlen=PTS_SMOOTH_WINDOW)
        self._pts_offset_min = None
        self._vaapi_disabled_reason = None

        threading.Thread(target=self._run, daemon=True).start()
    def _set_decode_pipeline(self, mode):
        super()._set_decode_pipeline(mode)
        with self.lock:
            self._pts_window.clear()
            self._pts_offset_min = None
    def _smooth_pts(self, seq, pts):
        """수 프레임 구간의 PTS로 등간격 직선을 맞춰 흔들림을 평균한 PTS와 프레임 간격을 돌려줍니다."""
        window = self._pts_window
        if window and (pts <= window[-1][1] or pts - window[-1][1] > 5.0):
            # 카메라 PTS 역행/점프(재시작, 랩어라운드)는 새 구간으로 봅니다.
            window.clear()
            self._pts_offset_min = None
        window.append((seq, pts))
        if len(window) < 2:
            return pts, None
        (s0, p0), (s1, p1) = window[0], window[-1]
        interval = (p1 - p0) / float(s1 - s0)
        smoothed = sum(p + (seq - s) * interval for s, p in window) / len(window)
        return smoothed, interval
    def _pts_meta(self, pts, recv_mono):
        """프레임 PTS·평활 PTS·간격·누적 지연과 이 프레임 원본이 든 링버퍼 디렉터리(같은 ffmpeg 실행 = 같은 PTS 기준).
        _publish_frame이 잠금 안에서 부릅니다."""
        pts_smoothed, interval, pts_lag = None, None, None
        if pts is not None:
            pts_smoothed, interval = self._smooth_pts(self._seq, pts)
            # 수신 시각과 PTS의 차이가 처음보다 커지면 그만큼 지연이 누적된 것입니다.
            offset = recv_mono - pts
            if self._pts_offset_min is None or offset < self._pts_offset_min:
                self._pts_offset_min = offset
            pts_lag = offset - self._pts_offset_min
        return {"pts": pts, "pts_smoothed": pts_smoothed, "frame_interval": interval, "pts_lag": pts_lag,
                "ring_dir": str(self._ring_gen_dir) if self._ring_gen_dir else None}
    def pts_at(self, t_mono):
        """PC monotonic 시각에 촬영돼 도착했을 프레임의 PTS와 그 링버퍼 디렉터리. 변환할 수 없으면 (None, None).
        _pts_offset_min = min(수신 시각 - PTS)는 지연이 가장 적었던 프레임 기준의 오프셋입니다."""
        with self.lock:
            if self._pts_offset_min is None or t_mono < self._pipeline_started_mono:
                return None, None
            return float(t_mono - self._pts_offset_min), (str(self._ring_gen_dir) if self._ring_gen_dir else None)
    def camera_time_of(self, pts):
        """프레임 PTS를 PC monotonic 기준 시각으로 바꿉니다(pts_at의 역변환)."""
        with self.lock:
            if pts is None or self._pts_offset_min is None:
                return None
            return float(pts + self._pts_offset_min)
    def _should_use_ffmpeg_pipe(self):
        # 과속 카메라는 PTS·원본 링버퍼가 FFmpeg 파이프에서만 나오므로 전역 backend와 상관없이 FFmpeg를 먼저 씁니다.
        # 다른 카메라는 공통 수신기 FrameReader가 backend를 그대로 따릅니다.
        return bool(shutil.which("ffmpeg") and shutil.which("ffprobe"))
    def _ffmpeg_vaapi_enabled(self):
        if self._vaapi_disabled_reason is not None:
            return False
        mode = str(self.decode_cfg["hw_acceleration"]).strip().lower()
        if mode not in ("auto", "vaapi") or not sys.platform.startswith("linux"):
            return False
        hw_device = str(self.decode_cfg["hw_device"]).strip()
        return bool(hw_device) and os.path.exists(hw_device)
    def _should_use_gstreamer_pipe(self):
        backend = str(self.decode_cfg["backend"]).strip().lower()
        if backend in ("opencv", "cv2", "ffmpeg_opencv"):
            return False

        if backend not in ("auto", "gstreamer", "gst", "gst_vaapi", "ffmpeg", "ffmpeg_vaapi", "vaapi"):
            return False

        if not sys.platform.startswith("linux"):
            return False

        gst_launch = shutil.which("gst-launch-1.0")
        gst_inspect = shutil.which("gst-inspect-1.0")
        if not gst_launch or not gst_inspect:
            return False

        return bool(shutil.which("ffprobe"))
    def _build_ffmpeg_cmd(self, in_w, in_h, out_w, out_h, use_vaapi, ring_dir=None):
        hw_device = str(self.decode_cfg["hw_device"]).strip()
        # -nostdin: 서비스가 `yes n | python3 ...`으로 실행되므로 stdin을 ffmpeg가 가져가지 않게 합니다.
        cmd = ["ffmpeg", "-hide_banner", "-nostdin", "-nostats", "-loglevel", "info"]
        if self.url.lower().startswith("rtsp://"):
            cmd.extend([
                "-rtsp_transport", "tcp",
                "-allowed_media_types", "video",  # 카메라 메타데이터(Data) 트랙 제외
                "-stimeout", "5000000",           # FFmpeg 4.4 기준. 5.x 이상은 -timeout
                "-fflags", "nobuffer",
            ])
        if use_vaapi:
            # low_delay는 SW 디코더의 프레임 스레딩을 꺼서 5MP H.265가 실시간을 못 따라가므로 VAAPI에만 씁니다.
            cmd.extend(["-flags", "low_delay",
                        "-hwaccel", "vaapi", "-hwaccel_device", hw_device, "-hwaccel_output_format", "vaapi"])
        cmd.extend(["-i", self.url, "-an"])

        # max_fps가 있으면 디코딩 직후 초당 max_fps장만 남깁니다(_ffmpeg_keep_filter, 버리기만 함).
        # select는 원래 PTS를 그대로 두므로 아래 showinfo PTS와 출력 프레임의 1:1 대응이 유지됩니다.
        keep = self._ffmpeg_keep_filter()
        keep = keep + "," if keep else ""
        if use_vaapi:
            # GPU에서 추론 크기로 축소한 뒤 NV12로 내려받아 파이프 전송량과 CPU 사용을 줄입니다.
            vf = keep + f"scale_vaapi=w={out_w}:h={out_h}:format=nv12,hwdownload,format=nv12"
        elif (out_w, out_h) != (in_w, in_h):
            vf = keep + f"scale={out_w}:{out_h},format=nv12"
        else:
            vf = keep + "format=nv12"
        # showinfo가 프레임마다 PTS를 stderr에 남기므로 n번째 출력 프레임과 1:1로 맞춥니다.
        # fps 필터/프레임 복제를 쓰지 않고(passthrough) 모든 프레임의 원래 PTS를 유지합니다.
        cmd.extend(["-vf", vf + "," + showinfo_filter(), "-vsync", "passthrough",
                    "-f", "rawvideo", "-pix_fmt", "nv12", "pipe:1"])
        if ring_dir is not None:
            # 같은 RTSP 입력의 원본 패킷을 재인코딩 없이 링버퍼로 씁니다(두 번째 출력).
            cmd.extend(self.packet_ring.segment_args(ring_dir))
        return cmd
    def _drain_ffmpeg_stderr(self, pipe, frame_info, cond, state, error_lines):
        """showinfo PTS를 프레임 순번별로 모으고, HEVC 파라미터셋 경고를 시작/운영 중으로 나눠 집계합니다."""
        try:
            for raw_line in iter(pipe.readline, b""):
                line = raw_line.decode("utf-8", "replace").strip()
                if not line:
                    continue
                m = self._SHOWINFO_RE.search(line)
                if m:
                    with cond:
                        frame_info[int(m.group(1))] = (float(m.group(2)), m.group(3) == "1")
                        cond.notify_all()
                    continue
                if "Parsed_showinfo" in line:
                    continue
                if self._HEVC_PARAM_SET_RE.search(line):
                    if state["frames"] == 0:
                        # 첫 IDR 전 경고는 sprop-vps 누락에 따른 예상 동작입니다.
                        self._stream_warn_startup += 1
                    else:
                        self._stream_warn_runtime += 1
                        now = time.monotonic()
                        if now - state["last_runtime_warn_log"] >= 10.0:
                            state["last_runtime_warn_log"] = now
                            logger.warning(
                                f"[CAM:{self.ip}] 첫 키프레임 이후 HEVC 디코딩 경고 "
                                f"(누적 {self._stream_warn_runtime}회): {self._short_external_output(line, 200)}"
                            )
                    continue
                if state["use_vaapi"] and self._VAAPI_FAIL_RE.search(line) and re.search(r"(?i)error|fail|cannot|unable|invalid", line):
                    # iHD 드라이버의 "Failed to query surface attributes"는 무해한 경고입니다.
                    if "query surface attributes" not in line:
                        state["vaapi_error"] = self._short_external_output(line, 300)
                if re.search(r"(?i)error|fail|refused|timed out|unauthorized|not found", line):
                    error_lines.append(self._short_external_output(line, 300))
        except Exception:
            pass
        finally:
            try:
                pipe.close()
            except Exception:
                pass
    def _run_ffmpeg_pipe(self):
        shape = self._probe_stream_shape()
        if shape is None:
            return False

        in_w, in_h = shape
        out_w, out_h = self._scaled_output_shape(in_w, in_h)
        frame_size = out_w * out_h * 3 // 2
        use_vaapi = self._ffmpeg_vaapi_enabled()
        self.source_shape = (in_w, in_h)
        ring_dir = None
        if self.packet_ring is not None:
            try:
                ring_dir = self.packet_ring.new_generation()
            except Exception as e:
                logger.warning(f"[CAM:{self.ip}] 원본 링버퍼 디렉터리 준비 실패 → 이번 연결은 링버퍼 없이 진행합니다: {e}")
        cmd = self._build_ffmpeg_cmd(in_w, in_h, out_w, out_h, use_vaapi, ring_dir=ring_dir)
        self._ring_gen_dir = ring_dir
        max_lag_sec = max(1.0, float(self.decode_cfg["max_pts_lag_sec"]))
        frame_info = {}
        cond = threading.Condition()
        state = {"frames": 0, "use_vaapi": use_vaapi, "vaapi_error": None, "last_runtime_warn_log": 0.0}
        error_lines = deque(maxlen=20)
        proc = None
        stderr_thread = None
        try:
            proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    bufsize=frame_size * 2)
            stderr_thread = threading.Thread(
                target=self._drain_ffmpeg_stderr, args=(proc.stderr, frame_info, cond, state, error_lines), daemon=True
            )
            stderr_thread.start()

            self.connected = True
            mode = "ffmpeg_vaapi_pipe" if use_vaapi else "ffmpeg_cpu_pipe"
            self._set_decode_pipeline(mode)
            self.last_t = time.time()

            while self.running:
                if time.time() - self.last_t > WATCHDOG_TIMEOUT:
                    break

                raw = proc.stdout.read(frame_size)
                if len(raw) != frame_size:
                    break

                index = state["frames"]
                with cond:
                    cond.wait_for(lambda: index in frame_info or proc.poll() is not None, timeout=0.2)
                    pts, is_key = frame_info.pop(index, (None, None))
                    for stale in [k for k in frame_info if k < index]:
                        del frame_info[stale]
                state["frames"] += 1

                yuv_img = np.frombuffer(raw, dtype=np.uint8).reshape((out_h * 3 // 2, out_w))
                source_fr = cv2.cvtColor(yuv_img, cv2.COLOR_YUV2BGR_NV12)
                self._publish_frame(self._inference_frame(source_fr), source_fr, pts=pts, is_key=is_key)

                lag = (self.frame_meta or {}).get("pts_lag")
                if lag is not None and lag > max_lag_sec:
                    # 디코딩이 실시간을 못 따라가면(주로 SW 폴백) 재접속해 지연 누적을 끊습니다.
                    logger.warning(f"[CAM:{self.ip}] 수신 지연 {lag:.1f}s 누적 ({mode}) → 재접속합니다.")
                    break

            if state["frames"] == 0 and use_vaapi and state["vaapi_error"] and self.running:
                # VAAPI 초기화 실패: 같은 FFmpeg 경로의 SW 디코딩으로 전환합니다.
                self._vaapi_disabled_reason = state["vaapi_error"]
                logger.warning(
                    f"[CAM:{self.ip}] VAAPI 디코딩 초기화 실패 → FFmpeg SW 디코딩으로 폴백합니다. "
                    f"reason={self._vaapi_disabled_reason}"
                )
                return True
            if state["frames"] == 0 and error_lines:
                logger.warning(f"[CAM:{self.ip}] FFmpeg 수신 실패: {' | '.join(error_lines)}")
            return state["frames"] > 0
        except Exception as e:
            logger.warning(f"[CAM:{self.ip}] FFmpeg pipe failed: {e}")
            return False
        finally:
            self.connected = False
            self._ring_gen_dir = None
            if proc is not None:
                # stdout을 먼저 닫아야 파이프 쓰기에 막힌 ffmpeg가 SIGTERM에 바로 종료됩니다.
                try:
                    proc.stdout.close()
                except Exception:
                    pass
                try:
                    proc.terminate()
                    proc.wait(timeout=2)
                except Exception:
                    try:
                        proc.kill()
                        proc.wait(timeout=2)
                    except Exception:
                        pass
            if stderr_thread is not None:
                stderr_thread.join(timeout=0.5)
    def _run(self):
        while self.running:
            # FFmpeg VAAPI 파이프가 기본입니다. 실측: 2880x1620 H.265 30fps 실시간, 프레임별 PTS 제공.
            if self._should_use_ffmpeg_pipe():
                used_ffmpeg_pipe = self._run_ffmpeg_pipe()
                if used_ffmpeg_pipe:
                    continue
                if self._stopping():
                    time.sleep(0.2)
                    continue
                if not self.decode_cfg["fallback_to_cpu"]:
                    time.sleep(5)
                    continue
                logger.warning(f"[CAM:{self.ip}] FFmpeg pipe unavailable; trying GStreamer/OpenCV fallback")

            # 공통 수신기의 GStreamer 경로(PTS 없음)
            if self._should_use_gstreamer_pipe():
                used_gstreamer_pipe = self._run_gstreamer_pipe()
                if used_gstreamer_pipe:
                    continue
                if self._stopping():
                    time.sleep(0.2)
                    continue
                if not self.decode_cfg["fallback_to_cpu"]:
                    time.sleep(5)
                    continue
                logger.warning(f"[CAM:{self.ip}] GStreamer pipe unavailable; trying FFmpeg/OpenCV fallback")

            if self._stopping():
                continue
            self._run_opencv()


class FramePacer:
    """AI 처리 간격 맞추기(초당 target_fps번 이하, CameraWorker).

    직전 처리 뒤 1/target_fps초가 안 돼 도착한 프레임을 버리면, 도착 간격이 조금만 흔들려도 10장 중 3~4장이
    버려집니다. 그래서 버리지 않고 처리할 시각까지 기다렸다가 그때의 최신 프레임을 처리합니다.
    늦게 온 만큼은 다음 한 번에서만 당겨 써서 평균은 target_fps를 넘지 않습니다."""

    def __init__(self):
        self.next_due = None

    def delay(self, now):
        """지금 처리하려면 기다려야 할 초(0이면 바로 처리)."""
        return 0.0 if self.next_due is None else max(0.0, self.next_due - now)

    def processed(self, now, interval):
        """처리를 시작했다고 기록합니다. 다음 처리 시각 = max(예정 시각, 지금 - 간격) + 간격."""
        start = now if self.next_due is None else max(self.next_due, now - interval)
        self.next_due = start + interval


class Camera:
    # 과속 차량을 판정 뒤 따라가는 시간: 이만큼 안 보이면 끝(초), 최대 이만큼(초, 판정 전 박스도 이만큼 앞까지 씀)
    PLR_FOLLOW_LOST_SEC = 0.7
    PLR_FOLLOW_REATTACH_SEC = 0.3     # 따라가던 박스가 이만큼 안 보이면 레이더 위치로 다시 붙을 박스를 찾음
    PLR_FOLLOW_MAX_SEC = 20.0  # 급감속해 기어오는 차도 가까워질 때까지 따라갑니다(2026-11-11).
    # 멀어지는 차는 가장 가까울 때(번호판 사진)가 판정보다 먼저라, 판정 전 박스를 이만큼(초) 앞까지 씁니다(track_boxes 160장).
    PLR_FOLLOW_BACK_SEC = 16.0
    # 알람 낸 객체 기록(alerted)은 이 시간(초) 동안 이벤트 후보가 되지 않은 객체만 지웁니다. 추적기 ID는 새 번호로만 주므로
    # 지운 ID가 다시 오지 않고, 화면에 남아 계속 후보인 객체는 지우지 않아 같은 객체에 알람이 두 번 나지 않습니다.
    ALERTED_KEEP_SEC = 600.0

    def __init__(self, ip, conf, det_main_v2, det_face, det_plate, cam_id,
                 det_topdown=None, radar_provider=None):
        self.ip = ip
        self.camera_key = ip
        self.conf = conf
        self.cam_id = cam_id
        self.events = conf.get('events', [])
        # 헬스체크 스레드가 관제 On·Off(activeSettings)를 반영하는 동안 처리 중인 프레임과 겹치지 않게 잠급니다.
        self.lock = threading.RLock()
        self.receiver_event_status = dict(conf.get('receiver_event_status') or {})
        RECEIVER_EVENT_STATUS_BY_CAM[str(self.cam_id)] = self.receiver_event_status
        # 과속 이벤트는 실제 레이더(MSE)를 수신합니다. 수신 워커는 모든 카메라가 함께 쓰고(MseRadarHub),
        # 카메라마다 cameras.json의 radar_client_id 레이더를 씁니다(없으면 routing_table의 첫 레이더).
        self.radar_provider = radar_provider
        if self.radar_provider is None and SPEEDING_EVENT in self.events:
            self.radar_provider = MseRadarHub.shared().input(conf.get('radar_client_id'))

        # 과속은 메인 모델 V2(사람·하반신·승용차·트럭), Backreach·YT는 topdown, 콘 NG는 side.dxnn(ConeObserver)을 씁니다.
        # 추론은 메인 루프(run_camera_inference)에서 하고, 여기서는 수신기 크기를 정하는 데만 씁니다.
        self.det_main_v2 = det_main_v2
        self.det_face = det_face
        self.det_plate = det_plate
        self.det_topdown = det_topdown
        # Backreach 감시 ON·OFF(스프레더, BackreachSpreaderGate). Backreach가 설정된 카메라만 있습니다.
        self.backreach_gate = None
        self._last_backreach_state = None

        # 추적기는 이벤트가 아니라 카메라 한 대 × 모델(검출 묶음)마다 하나입니다. 같은 모델을 쓰는 이벤트는 추적기를 같이 써서
        # 같은 물체가 이벤트마다 다른 번호를 받지 않습니다. 추적 방식이 달라야 하는 이벤트만 자기 추적기를 둡니다(과속 trk_speed).
        self.trk_main = SimpleTracker()
        # 과속 탐지 전용 추적기(같은 검출을 받음). 과속은 레이더 점을 박스에 붙이고 판정 구간 동안 다수결하므로,
        # ID가 바뀌지 않는 MotionTracker가 필요합니다.
        self.trk_speed = MotionTracker()
        # topdown 추적기. topdown을 쓰는 이벤트(Backreach·YT)가 같이 씁니다.
        self.trk_topdown = SimpleTracker()

        # 영상 수신기는 이벤트들의 수신 이름표(stream_spec)를 모아 카메라마다 하나 만듭니다(_reader_spec).
        # 추론용 프레임은 이 카메라 모델 중 가장 큰 입력 크기 안으로 줄이고, 원본 프레임도 함께 받습니다.
        self._reader_inference_size = None
        input_sizes = [(int(d.input_width), int(d.input_height))
                       for d in (det_main_v2, det_topdown)
                       if d is not None and getattr(d, 'input_width', None) and getattr(d, 'input_height', None)]
        if CONE_NG_EVENT in self.events:
            # 콘 판정은 원본 프레임(side·cone 모델)으로 하고, 추론용 프레임(화면·녹화·NG 사진)은 640 안으로
            # 줄입니다(원본 1920 그대로면 화면·녹화 CPU가 늘어납니다).
            input_sizes.append((640, 640))
        if input_sizes:
            # 여러 모델 중 가장 큰 입력에 맞춰 축소합니다(작은 모델은 letter_box에서 한 번 더 줄입니다).
            self._reader_inference_size = (max(w for w, _ in input_sizes), max(h for _, h in input_sizes))
        self.reader = self._make_reader()
        self.recorder = VideoRecorder(ip)
        if CONE_NG_EVENT in self.events:
            CONE_GROUP_RECORDERS[self.camera_key] = (self.recorder, cam_id)
        if SEASIDE_PERSON_EVENT in self.events:
            SEASIDE_GROUP_RECORDERS[self.camera_key] = (self.recorder, cam_id)
        self.current_frame_meta = None
        self.speed_track_hist = {}                 # 과속: 움직이는 차량 판정 이력(moving_tracks)
        self.assoc_votes = deque(maxlen=4000)      # 과속: (카메라 시각, 레이더 id, 박스 tid) 프레임별 연결 → 이벤트 차량 다수결
        self.track_boxes = {}                      # 과속: tid -> deque[[pts, 박스]] 번호판 촬영 시점 고르기용
        self.plr_follow = {}                       # 과속: event_id -> 판정 후 차량 박스를 따라가며 원본 추출 대기
        self._radar_zone_key = None                # 과속: 레이더 판정 구역으로 넘긴 (ROI, 보정값) — 바뀔 때만 다시 계산
        self._last_logic_seq = None
        self.radar_display = None
        self.radar_log = None                      # 과속: 이 프레임 레이더 점(분석용 .infer.jsonl "radar")

        self.alerted = {}           # tid -> 이 객체로 이미 알람을 낸 이벤트(같은 객체에 같은 이벤트를 다시 내지 않음,
                                    # alarm_once_per_object가 False인 이벤트는 쿨다운마다 다시 냄)
        self._alerted_seen = {}     # tid -> 마지막으로 이벤트 후보가 된 시각. ALERTED_KEEP_SEC 지나면 alerted에서 지웁니다.
        self._alerted_pruned_t = 0.0
        self.last_evt_t = {}
        self.visual_alarms = {}
        self._suppress_logged = {}  # (이벤트, tid) -> 마지막으로 남긴 억제 이유. EVENT SUPPRESSED를 객체마다 한 번만 남깁니다.
        # Backreach 추론 줄이기(감시 OFF인 동안, backreach_inference_due): 마지막 추론 시각과 그 결과, 최근 추론 시각들
        self._last_backreach_infer = None
        self.last_topdown_res = None
        self._backreach_infer_times = deque(maxlen=30)

        self.fps_queue = deque(maxlen=30)
        self.current_fps = 0.0

        self._load_roi_conf(conf)
        self._rebuild_handlers()
        self._attach_backreach_gate()

    def _rebuild_handlers(self):
        self.handlers = {}

        for ename in self.events:
            if ename in EVENT_REGISTRY:
                # 카메라별 event_config가 있으면 공통 설정 위에 덮어씁니다(예: Backreach 카메라별 cooldown).
                config = deep_merge_dict(
                    SYS_CFG["event_config"].get(ename, {}),
                    (self.conf.get("event_config") or {}).get(ename, {}),
                )
                self.handlers[ename] = EVENT_REGISTRY[ename](config, self.roi_poly, self.roi_lines)
        # YT 두 이벤트는 카메라의 양하 감시 상태 하나를 같이 봅니다. 둘 중 하나만 켜도 감시 상태는 돌고,
        # 판단 설정은 헤드압착(yt_head_crush) 설정을 우선합니다(사람 진입만 켜면 그 설정).
        yt_handlers = [self.handlers[name] for name in YT_EVENTS if name in self.handlers]
        self.yt_monitor = YTDischargeMonitor(yt_handlers[0].config, f"CAM:{self.cam_id}({self.ip})") \
            if yt_handlers else None

    def update_config(self, new_conf):
        with self.lock:
            old_base = {k: v for k, v in (self.conf or {}).items() if k != "receiver_event_status"}
            new_base = {k: v for k, v in (new_conf or {}).items() if k != "receiver_event_status"}
            self.receiver_event_status = dict(new_conf.get("receiver_event_status") or {})
            RECEIVER_EVENT_STATUS_BY_CAM[str(self.cam_id)] = self.receiver_event_status
            if old_base == new_base:
                # 관제 On·Off만 바뀌었으면 추적 ID, 처리한 객체, 쿨다운을 그대로 둡니다.
                self.conf = new_conf
                return

            old_events = list(self.events)
            self.conf = new_conf
            self.events = new_conf.get('events', [])
            reader_cls, max_fps, max_width = self._reader_spec()
            if (type(self.reader) is not reader_cls or self.reader.max_fps != max_fps
                    or self.reader.source_max_width != max_width):
                # 이벤트가 바뀌어 수신 방식(수신기 종류, 초당 장수, 원본 폭)이 달라지면 수신기를 다시 만듭니다.
                # 예: 콘만 있던 카메라에 과속이 더해지면 과속 수신기(SpeedFrameReader)로 모든 프레임을 받습니다.
                self.reader.running = False
                self.reader = self._make_reader()
                logger.info(f"[CAM:{self.ip}] 수신기 교체 → {type(self.reader).__name__} "
                            f"max_fps={self.reader.max_fps or '-'} width={self.reader.source_max_width}")
            # 핫 리로드된 이벤트와 이벤트 전용 기본 레이더의 수명을 함께 맞춥니다.
            if SPEEDING_EVENT in self.events:
                radar = MseRadarHub.shared().input(new_conf.get('radar_client_id'))
                if self.radar_provider is None or (isinstance(self.radar_provider, MseRadarInput)
                                                   and self.radar_provider is not radar):
                    self.radar_provider = radar
            else:
                self.radar_provider = None
            self._load_roi_conf(new_conf)
            self._rebuild_handlers()
            self._attach_backreach_gate()

            logger.info(
                f" [CAM:{self.ip}] 무중단 설정 리로드 완료: "
                f"{old_events} -> {self.events}"
            )

    def _load_roi_conf(self, conf):
        """설정의 정규화 ROI를 읽고, 픽셀 ROI는 비워 두어 다음 프레임 크기로 다시 계산하게 합니다(_ensure_roi)."""
        self.roi_poly_norm = conf.get('roi_poly_norm', [])
        self.roi_lines_norm = conf.get('roi_lines_norm', [])
        self.roi_poly = []
        self.roi_lines = []
        self.roi_frame_shape = None

    def _ensure_roi(self, frame):
        """프레임 크기에 맞춰 정규화 ROI를 픽셀 좌표로 바꾸고 이벤트에 넣습니다."""
        if frame is None or self.roi_frame_shape == frame.shape[:2]:
            return
        h, w = frame.shape[:2]
        self.roi_poly = denormalize_roi_points(self.roi_poly_norm, w, h) if self.roi_poly_norm else []
        self.roi_lines = denormalize_roi_points(self.roi_lines_norm, w, h) if self.roi_lines_norm else []
        self.roi_frame_shape = frame.shape[:2]
        self._inject_roi_to_handlers(self.roi_poly, self.roi_lines)

    def _inject_roi_to_handlers(self, roi_poly, roi_lines):
        for handler in self.handlers.values():
            if roi_poly and len(roi_poly) >= 3:
                handler.roi_poly = np.array(roi_poly, dtype=np.int32)
            else:
                handler.roi_poly = np.empty((0, 2), dtype=np.int32)
            handler.roi_lines = roi_lines or []

    # ------------------------------------------------------------------
    # Backreach 감시 ON·OFF(스프레더)
    # ------------------------------------------------------------------
    def _attach_backreach_gate(self):
        handler = self.handlers.get(BACKREACH_EVENT)
        self.backreach_gate = BackreachSpreaderGate(handler.config, f"CAM:{self.cam_id}({self.ip})") \
            if handler is not None else None

    def update_backreach_gate(self, d_topdown, now=None):
        """topdown을 새로 추론한 프레임마다 부릅니다(run_camera_inference). 스프레더로 감시 ON·OFF를 정합니다."""
        if self.backreach_gate is not None:
            self.backreach_gate.update(d_topdown, now)

    def _read_backreach_state(self):
        """(감시 ON 1·OFF 0, 변경번호). Backreach가 없는 카메라는 (None, None)입니다."""
        if BACKREACH_EVENT not in self.handlers:
            return None, None
        return self.backreach_gate.read_state() if self.backreach_gate is not None else (0, 0)

    def _clear_backreach_alarms(self):
        self.visual_alarms = {tid: info for tid, info in self.visual_alarms.items() if info['evt'] != BACKREACH_EVENT}

    def _sync_backreach_state(self):
        state = self._read_backreach_state()
        if self._last_backreach_state is not None and state != self._last_backreach_state:
            # 감시 구간이 바뀌면 Backreach 화면 경고만 지웁니다. 추적 ID, 처리한 ID, 쿨다운은 유지해
            # OFF→ON 전환으로 중복 제한을 우회하지 못하게 합니다.
            self._clear_backreach_alarms()
        self._last_backreach_state = state
        return state

    def _reader_spec(self):
        """이 카메라 이벤트들의 수신 이름표(BaseEventDetector.stream_spec)를 합쳐 수신기 하나를 정합니다.
        반환 (수신기 클래스, 초당 장수 상한(None = 전부), 원본 가로 상한).
        - 수신기: 자기 수신기를 적은 이벤트가 있으면 그것(과속: SpeedFrameReader), 없으면 공통 수신기 FrameReader
        - 초당 장수: 하나라도 전부(None)를 원하면 전부, 아니면 가장 큰 값
        - 원본 가로: 가장 큰 값(콘 1920, Backreach 1280, 그 밖 720)"""
        specs = [EVENT_REGISTRY[e].stream_spec() for e in self.events if e in EVENT_REGISTRY] or [BaseEventDetector.stream_spec()]
        own = [name for name, _, _ in specs if name != "FrameReader"]
        if len(set(own)) > 1:
            logger.warning(f"[CAM:{self.ip}] 이벤트마다 다른 전용 수신기를 원합니다({sorted(set(own))}) → {own[0]}을 씁니다.")
        rates = [fps for _, fps, _ in specs]
        max_fps = None if any(fps is None for fps in rates) else max(rates)
        return globals()[own[0] if own else "FrameReader"], max_fps, max(width for _, _, width in specs)

    def backreach_inference_due(self, now=None):
        """topdown(Backreach·YT)을 이번 프레임에 추론할지. Backreach 감시가 ON이거나 YT 양하 감시 중이면
        매 프레임, 아니면(이벤트를 낼 수 없는 동안) backreach.idle_infer_fps(기본 3, 0 = 줄이지 않음)장만 추론하고
        그 사이는 직전 검출을 다시 씁니다(화면·추적은 계속). Backreach OFF·YT가 멈춰 10초를 기다리는 동안은 초당 3장으로
        스프레더·YT를 충분히 찾고, 감시가 시작되면 객체·5초 이탈·사람 진입을 놓치지 않게 매 프레임 추론합니다.
        1280 모델이 NPU를 많이 써서, 감시 OFF인 동안 아낀 NPU를 콘 카메라가 씁니다.
        해측 추락 방지는 위험 구역 근처(near_zone_m)에 사람이 보이는 동안 매 프레임 추론합니다."""
        now = time.monotonic() if now is None else now
        handler = self.handlers.get(BACKREACH_EVENT) or self.handlers.get(SEASIDE_PERSON_EVENT)
        idle_fps = float((handler.config if handler is not None else {}).get("idle_infer_fps", 3.0) or 0.0)
        yt_armed = self.yt_monitor is not None and self.yt_monitor.armed()
        seaside = self.handlers.get(SEASIDE_PERSON_EVENT)
        seaside_near = seaside is not None and seaside.near(now)
        due = (self._read_backreach_state()[0] == 1 or yt_armed or seaside_near or idle_fps <= 0
               or self._last_backreach_infer is None or now - self._last_backreach_infer >= 1.0 / idle_fps)
        if due:
            self._last_backreach_infer = now
            self._backreach_infer_times.append(now)
        return due

    def backreach_infer_fps(self):
        """최근 topdown 모델 실제 추론 횟수(초당, Backreach·YT 공용). AI FPS 로그에 infer_fps=값(돌린 모델)로 남깁니다."""
        times = self._backreach_infer_times
        if len(times) < 2 or times[-1] <= times[0]:
            return 0.0
        return (len(times) - 1) / (times[-1] - times[0])

    def _make_reader(self):
        url = self.conf.get('url', '')
        reader_cls, max_fps, max_width = self._reader_spec()
        reader = reader_cls(url, self.ip, max_fps=max_fps, source_max_width=max_width,
                            inference_size=self._reader_inference_size)
        if CONE_NG_EVENT in self.events and SPEEDING_EVENT not in self.events:
            # 1080p→640 INTER_AREA는 CPU를 많이 씁니다(4대 38ms). 콘 판정은 원본을 쓰므로 빠른 보간으로 충분합니다.
            reader.inference_interp = cv2.INTER_LINEAR
        return reader

    def process_frame(self, include_source=False):
        # 모든 수신기가 (추론용 프레임, fid, 연결 여부[, 원본 프레임])을 돌려줍니다(FrameReader.read).
        return self.reader.read(include_source=include_source)

    def apply_face_blur(self, frame, person_boxes):
        if frame is None or self.det_face is None:
            return frame, []

        blur_img = frame.copy()
        blurred_faces = []

        try:
            f_dets = self.det_face.infer(blur_img)  # 점수 기준 model_confidences.FACE

            for f in f_dets:
                fx1, fy1, fx2, fy2 = map(int, f[:4])
                fw, fh = fx2 - fx1, fy2 - fy1

                if fw > blur_img.shape[1] * 0.4:
                    continue

                fcx = fx1 + (fw / 2.0)
                fcy = fy1 + (fh / 2.0)
                is_valid_face = False

                matched_person_tid = -1
                for p in person_boxes:
                    px1, py1, px2, py2 = map(int, p[:4])
                    pw, ph = px2 - px1, py2 - py1

                    pad_x = pw * 0.15
                    pad_y_top = ph * 0.25
                    pad_y_bottom = ph * 0.05

                    if (px1 - pad_x) <= fcx <= (px2 + pad_x) and (py1 - pad_y_top) <= fcy <= (py2 + pad_y_bottom):
                        is_valid_face = True
                        matched_person_tid = int(p[4])
                        break

                if is_valid_face:
                    roi = blur_img[fy1:fy2, fx1:fx2]
                    if roi.size > 0:
                        small = cv2.resize(roi, (max(1, fw//15), max(1, fh//15)), interpolation=cv2.INTER_LINEAR)
                        blur_img[fy1:fy2, fx1:fx2] = cv2.resize(small, (fw, fh), interpolation=cv2.INTER_NEAREST)
                        blurred_faces.append({
                            "box": [fx1, fy1, fx2, fy2],
                            "score": round(float(f[4]), 4),
                            "class_id": int(f[5]),
                            "matched_person_tid": matched_person_tid
                        })

        except Exception as e:
            logger.error(f"모자이크 처리 실패: {e}")

        return blur_img, blurred_faces

    def apply_plate_blur(self, frame, vehicle_boxes):
        if frame is None or self.det_plate is None:
            return frame, []

        blur_img = frame.copy()
        blurred_plates = []

        try:
            p_dets = self.det_plate.infer(blur_img)  # 점수 기준 model_confidences.PLATE
            h_img, w_img = blur_img.shape[:2]

            for p in p_dets:
                px1, py1, px2, py2 = map(int, p[:4])
                px1 = max(0, min(px1, w_img - 1))
                py1 = max(0, min(py1, h_img - 1))
                px2 = max(0, min(px2, w_img))
                py2 = max(0, min(py2, h_img))
                pw = px2 - px1
                ph = py2 - py1

                if pw <= 0 or ph <= 0:
                    continue
                if pw > w_img * 0.6 or ph > h_img * 0.3:
                    continue

                pcx = px1 + pw / 2.0
                pcy = py1 + ph / 2.0

                matched_vehicle_tid = -1
                for v in vehicle_boxes:
                    vx1, vy1, vx2, vy2 = map(int, v[:4])
                    vw = vx2 - vx1
                    vh = vy2 - vy1
                    pad_x = vw * 0.10
                    pad_y = vh * 0.10
                    if (vx1 - pad_x) <= pcx <= (vx2 + pad_x) and (vy1 - pad_y) <= pcy <= (vy2 + pad_y):
                        matched_vehicle_tid = int(v[4])
                        break

                roi = blur_img[py1:py2, px1:px2]
                if roi.size > 0:
                    small_w = max(1, pw // 12)
                    small_h = max(1, ph // 12)
                    small = cv2.resize(roi, (small_w, small_h), interpolation=cv2.INTER_LINEAR)
                    blur_img[py1:py2, px1:px2] = cv2.resize(small, (pw, ph), interpolation=cv2.INTER_NEAREST)
                    blurred_plates.append({
                        "box": [px1, py1, px2, py2],
                        "score": round(float(p[4]), 4),
                        "class_id": int(p[5]),
                        "matched_vehicle_tid": matched_vehicle_tid
                    })

        except Exception as e:
            logger.error(f"번호판 모자이크 처리 실패: {e}")

        return blur_img, blurred_plates

    def apply_privacy_blur(self, frame, t_main, blur_face=True, blur_plate=True):
        privacy_meta = {
            "blur_face": bool(blur_face),
            "blur_plate": bool(blur_plate),
            "face": [],
            "plate": []
        }

        if frame is None:
            return frame, privacy_meta

        blurred_img = frame.copy()
        person_boxes = [t for t in t_main if int(t[6]) in [ID_G_PERSON, ID_PERSON_LOW]]
        vehicle_boxes = [t for t in t_main if int(t[6]) in TARGET_VEHICLES]

        if blur_face:
            blurred_img, face_blurs = self.apply_face_blur(blurred_img, person_boxes)
            privacy_meta["face"] = face_blurs

        if blur_plate:
            blurred_img, plate_blurs = self.apply_plate_blur(blurred_img, vehicle_boxes)
            privacy_meta["plate"] = plate_blurs

        privacy_meta["applied"] = bool(privacy_meta["face"] or privacy_meta["plate"])
        return blurred_img, privacy_meta

    def _serialize_detection(self, det):
        return {
            "box": [int(round(float(v))) for v in det[:4]],
            "score": round(float(det[4]), 4),
            "class_id": int(det[5])
        }

    def _serialize_track(self, track):
        return {
            "box": [int(round(float(v))) for v in track[:4]],
            "tid": int(track[4]),
            "score": round(float(track[5]), 4),
            "class_id": int(track[6])
        }

    def _serialize_event_objects(self, objects):
        safe_objects = []
        for obj in objects or []:
            safe_objects.append({
                "label": str(obj.get("label", "")),
                "box": [int(round(float(v))) for v in obj.get("box", [])],
                "score": round(float(obj.get("score", 0.0)), 4),
                "tid": int(obj.get("tid", -1)),
                "class_id": int(obj.get("class_id", -1))
            })
        return safe_objects

    def build_inference_log(self, fid, frame, d_main_res, d_topdown, t_main, t_topdown, alarms, new_events):
        h, w = frame.shape[:2] if frame is not None else (0, 0)
        backreach_on, signal_generation = self._last_backreach_state or (None, None)
        cone_overlay = getattr(self.handlers.get(CONE_NG_EVENT), "overlay", None)
        return {
            "ts": now_kst().isoformat(),
            "fid": int(fid),
            "cam_id": int(self.cam_id),
            "ip": str(self.ip),
            "frame_shape": [int(h), int(w)],
            "events": list(self.events),
            "roi_poly": [[int(p[0]), int(p[1])] for p in (self.roi_poly or [])],
            "roi_lines": [[int(p[0]), int(p[1])] for p in (self.roi_lines or [])],
            # Backreach 카메라의 감시 ON(1)·OFF(0, 스프레더로 정함)와 구간 변경 번호입니다. 그 외 카메라는 null입니다.
            "backreach_on": backreach_on,
            "signal_generation": signal_generation,
            "detections": {
                "main": [self._serialize_detection(d) for d in d_main_res],
                **({TOPDOWN_MODEL: [self._serialize_detection(d) for d in d_topdown]} if d_topdown is not None else {}),
            },
            "tracks": {
                "main": [self._serialize_track(t) for t in t_main],
                **({TOPDOWN_MODEL: [self._serialize_track(t) for t in t_topdown]} if t_topdown is not None else {}),
            },
            "alarms": {str(int(tid)): evt for tid, evt in (alarms or {}).items()},
            "new_events": [
                {
                    "event_id": str(ev.get("event_id", "")),
                    "ts": str(ev.get("ts", "")),
                    "event_name": str(ev.get("event_name", "")),
                    "objects": self._serialize_event_objects(ev.get("objects", [])),
                    "privacy_blur": to_json_safe(ev.get("privacy_blur", {})),
                    "decision_trace": to_json_safe(ev.get("decision_trace", {}))
                }
                for ev in (new_events or [])
            ],
            # YT 양하 감시 상태(정차 초·감시 여부·적재영역·하락 예측·이탈 시간)와 콘 카메라 관측(side·콘, 원본 좌표,
            # t = 관측 시각)을 프레임마다 남겨 이벤트 앞뒤 1분을 숫자로 다시 볼 수 있게 합니다(그 이벤트가 없는 카메라는 없음).
            **({"yt": to_json_safe(self.yt_monitor.snapshot)} if self.yt_monitor is not None else {}),
            **({"cone": to_json_safe(cone_overlay)} if cone_overlay is not None else {}),
            # 과속: 이 프레임에 맞춘 레이더 점 [id, 거리, x, y, 속도, 차로 안, 화면 u, v, 붙은 박스 tid](radar_log_points)
            **({"radar": to_json_safe(self.radar_log)} if self.radar_log is not None else {}),
        }

    def _radar_judge_kwargs(self, fr, t_main, track_map_main):
        """실제 MSE 레이더면 트랙 기준 판정기를 연결하고, 화면 표시용 레이더↔박스 연결과 보정 데이터 수집을 합니다.

        규칙:
        - 투영: 호모그래피가 있으면 그것, 없으면 radar_camera.colocated(같은 정면 기하 모델)와 그 지연값
        - 레이더 프레임: 카메라 시각 - 지연값에 맞는 프레임(MseRadarInput.frame_at), 표시·연결이 같은 프레임 사용
        - 연결 후보: 움직이는 차량 박스 중 하단 띠가 ROI(단속 차로)에 걸친 것만(moving_tracks, box_band_in_roi),
          박스 하단 띠 + 레이더 거리-박스 크기 검사, 화면 밖 점 제외
        - 프레임마다 연결을 assoc_votes에 기록 → 과속 이벤트 차량은 판정 구간 다수결(SpeedingDetector)"""
        self.radar_display = None
        self.radar_log = None
        if not isinstance(self.radar_provider, MseRadarInput):
            return {}
        # 레이더 프로세스가 죽었거나 시작에 실패했으면 다시 띄웁니다(실패 후 5초 간격).
        self.radar_provider.start()
        judge = RadarSpeedJudge.for_radar(self.radar_provider)
        judge.register(self.camera_key)
        rc = radar_camera_cfg(self.radar_provider.client_id)
        col = rc.get("colocated") or {}
        use_col = not rc.get("homography") and col.get("enabled")
        offset = float(col.get("latency_offset_sec", 0.0)) if use_col else float(rc.get("latency_offset_sec", 0.0) or 0.0)
        # 추정 보정값(colocated.estimated)이면 박스 하단 띠 검사 대신 거의 모든 점을 후보로 둡니다(5000px).
        gate = 5000.0 if use_col and col.get("estimated") else float(rc.get("assoc_gate_px", 80))
        project = radar_camera_projector(fr.shape if fr is not None else None, rc)
        self._update_radar_zone(judge, rc)
        meta = self.current_frame_meta or {}
        camera_time = self.reader.camera_time_of(meta.get("pts"))
        now_t = camera_time if camera_time is not None else time.monotonic()
        moving = moving_tracks(self.speed_track_hist, t_main, now_t)
        # 박스 연결은 ROI(단속 차로)에 걸친 차량만(옆 차로·방벽 너머 차에 붙지 않게)
        roi = self.roi_poly if self.roi_poly is not None and len(self.roi_poly) >= 3 else None
        self._update_plr_follow(t_main, meta, {
            'judge': judge, 'project': project, 'offset': offset, 'camera_time': camera_time, 'moving': moving,
            'track_map': track_map_main, 'gate': gate, 'frame_shape': fr.shape if fr is not None else None,
            'focal_px': projection_focal_px(fr.shape if fr is not None else None, rc), 'roi_poly': roi})

        if project is not None and fr is not None:
            radar_t = camera_time - offset if camera_time is not None else None
            radar_frame = self.radar_provider.frame_at(radar_t)
            all_rows = judge.rows_for_frame(radar_frame) if radar_frame is not None else []
            rows = [r for r in all_rows if r['in_zone']]
            matched, projected = associate_radar_tracks(moving, track_map_main, rows, project, gate, frame_shape=fr.shape,
                                                        focal_px=projection_focal_px(fr.shape, rc), roi_poly=roi)
            self.radar_log = {"radar_arrival": round(float(radar_frame['arrival']), 3) if radar_frame is not None else None,
                              "points": radar_log_points(all_rows, project, matched)}
            if camera_time is not None:
                for tid, (row, _) in matched.items():
                    self.assoc_votes.append((camera_time, int(row['radar_id']), int(tid)))
            if rc.get("display_speed", True):
                speed_boxes = {int(x[4]): [float(c) for c in x[:4]] for x in moving}
                self.radar_display = {
                    'by_tid': {tid: row for tid, (row, _) in matched.items()},   # 과속 추적기(trk_speed) tid 기준
                    'by_box': [(speed_boxes[tid], row) for tid, (row, _) in matched.items() if tid in speed_boxes],
                    'points': [(uv, row) for row, uv in projected],
                }

        if rc.get("calibration_collect", False):
            recorder = RadarCameraCalibrationRecorder.get()
            judge.add_frame_hook(recorder.radar_hook)
            # 서 있는 차(주차 트럭 등)는 레이더에 잡히지 않아 엉뚱한 레이더 기록과 짝지어지므로, 움직이는 차량 박스만 기록합니다.
            vehicles = [t for t in moving if track_map_main.get(int(t[4])) in TARGET_VEHICLES]
            if len(vehicles) == 1 and camera_time is not None and fr is not None:
                recorder.camera_obs(self.camera_key, camera_time, meta.get("pts"), vehicles[0], fr.shape)

        return {
            'radar_judge': judge,
            'radar_camera_cfg': rc,
            'radar_projector': project,
            'radar_latency_offset': offset,
            'radar_assoc_gate_px': gate,
            'camera_time': camera_time,
            'pts_at': self.reader.pts_at,
            'radar_moving_tracks': moving,
            'radar_votes': self.assoc_votes,
        }

    def _update_radar_zone(self, judge, rc):
        """카메라 ROI(단속 차로)와 이 카메라의 과속 방향·측정·사진 구간(cameras.json event_config.speeding)을 레이더 판정기
        차로로 넘깁니다. ROI·보정값·설정이 바뀔 때만 다시 계산합니다."""
        settings = speeding_lane_settings(deep_merge_dict(radar_speeding_cfg(self.radar_provider.client_id),
                                                          (self.conf.get("event_config") or {}).get(SPEEDING_EVENT) or {}))
        key = json.dumps([self.roi_poly_norm, rc.get("homography"), rc.get("image_size"), settings])
        if key == self._radar_zone_key:
            return
        self._radar_zone_key = key
        poly = roi_radar_polygon(self.roi_poly_norm, rc)
        judge.set_camera_zone(self.camera_key, poly, settings)
        dirs = ", ".join(f"{d} 측정 {settings[d + '_measure'][0]:g}~{settings[d + '_measure'][1]:g}m "
                         f"사진 {settings[d + '_photo'][0]:g}~{settings[d + '_photo'][1]:g}m"
                         for d in ("forward", "backward") if settings[d]) or "방향 없음(판정 안 함)"
        logger.info(f"[RADAR ZONE] CAM:{self.cam_id}({self.camera_key}) 레이더 {self.radar_provider.client_id} 과속: {dirs}")
        if poly:
            logger.info(f"[RADAR ZONE] CAM:{self.cam_id}({self.camera_key}) ROI를 지나간 차만 판정합니다"
                        f"(ROI 레이더 좌표, x 앞 m, y 왼쪽+ m): {poly}")
        elif self.roi_poly_norm:
            logger.warning(f"[RADAR ZONE] CAM:{self.cam_id}({self.camera_key}) 보정값(호모그래피)이 없거나 망가졌거나 ROI가 "
                           f"수평선 너머까지 있어 ROI를 레이더 구역으로 못 바꿉니다. 설정 구역(event_config.speeding.zone)으로 판정합니다.")

    @staticmethod
    def _radar_follow_tid(fl, radar):
        """판정 순간 박스를 못 찾은 과속 차량: 지금 레이더 위치를 화면에 투영해 붙는 차량 박스를 찾습니다.
        반환 (지금 레이더 위치 (x, y, speed_kmh), 붙은 박스 tid 또는 None). 레이더가 그 차를 더 못 보면 (None, None)."""
        if not radar or radar.get('project') is None or radar.get('camera_time') is None or fl.get("radar_seq") is None:
            return None, None
        pos = radar['judge'].position_at(fl["radar_seq"], radar['camera_time'] - radar['offset'], max_dt=0.3)
        if pos is None:
            return None, None
        row = {'radar_id': fl.get("radar_id"), 'x_m': pos[0], 'y_m': pos[1], 'speed_kmh': pos[2]}
        matched, _ = associate_radar_tracks(radar['moving'], radar['track_map'], [row], radar['project'], radar['gate'],
                                            frame_shape=radar['frame_shape'], focal_px=radar['focal_px'],
                                            roi_poly=radar.get('roi_poly'))
        return pos, next(iter(matched), None)

    def _update_plr_follow(self, t_main, meta, radar=None):
        """차량 박스 이력을 남기고, 과속 차량이 화면에서 PLR_FOLLOW_LOST_SEC 넘게 사라지거나 PLR_FOLLOW_MAX_SEC가
        지나면 원본 추출을 요청합니다. 판정 전후 그 차량 박스 중 가장 클 때(가장 가까울 때)의 원본 프레임으로 번호판을 읽게 됩니다.
        판정 순간 박스를 못 찾은 차량(tid None)은 레이더 트랙을 따라가다 투영점에 박스가 붙는 순간부터 그 박스를 따라갑니다
        (멀리서 판정해 박스가 작거나 가려졌던 차). 끝내 박스가 안 붙으면 따라가며 남긴 레이더 거리(radar_path)로
        차가 번호판 촬영 구간에 올 때를 계산해 원본을 뽑습니다(radar_path_window).
        radar: _radar_judge_kwargs가 넘기는 판정기·투영·지연·움직이는 박스·ROI."""
        now_m, pts = time.monotonic(), meta.get("pts")
        by_tid = {int(t[4]): t for t in t_main}
        if pts is not None:
            for tid, t in by_tid.items():
                self.track_boxes.setdefault(tid, deque(maxlen=160)).append([round(float(pts), 4), int_box(t[:4])])
            for tid in [k for k in self.track_boxes if k not in by_tid and pts - self.track_boxes[k][-1][0] > 2.0]:
                del self.track_boxes[tid]
        for event_id in list(self.plr_follow):
            fl = self.plr_follow[event_id]
            if fl["tid"] is None:
                pos, tid = self._radar_follow_tid(fl, radar)
                if pos is not None:
                    fl["last_seen"] = now_m
                    if pts is not None:
                        # [pts, 거리 m, 속도 km/h, (화면 투영 u, v)]: 원본 구간을 그 차가 화면에 보인 마지막 순간으로 고릅니다.
                        uv = radar['project'](pos[0], pos[1]) if radar.get('project') is not None else None
                        fl.setdefault("radar_path", []).append(
                            [round(float(pts), 4), round(math.hypot(pos[0], pos[1]), 2), round(float(pos[2]), 1)]
                            + ([round(float(uv[0]), 1), round(float(uv[1]), 1)] if uv is not None else []))
                if (tid is not None and fl["boxes"] and int(tid) in by_tid and box_area(by_tid[int(tid)])
                        < PLR_REATTACH_MIN_AREA_FRAC * max(box_area(b) for _, b in fl["boxes"])):
                    # 따라가던 차보다 훨씬 작은 박스는 멀리 있거나 멀어지는 다른 차입니다(레이더가 차를 놓친 뒤 옮겨 붙는 것 방지).
                    logger.info(f"[PLR FOLLOW] event_id={event_id} 레이더 위치의 박스 tid={tid}가 따라가던 차보다 훨씬 작아 붙지 않습니다.")
                    tid = None
                if tid is not None:
                    fl["tid"] = int(tid)
                    logger.info(f"[PLR FOLLOW] event_id={event_id} 레이더 위치에 차량 박스가 붙어 따라갑니다. tid={tid}")
            t = by_tid.get(fl["tid"]) if fl["tid"] is not None else None
            if t is not None and pts is not None:
                fl["boxes"].append([round(float(pts), 4), int_box(t[:4])])
                fl["last_seen"] = now_m
            if t is not None:
                fl["box_seen"] = now_m
            elif (fl["tid"] is not None and fl.get("radar_seq") is not None
                  and now_m - fl.get("box_seen", fl["started"]) > self.PLR_FOLLOW_REATTACH_SEC):
                # 따라가던 차 박스를 놓침(추적기가 차를 잃거나 번호가 바뀜): 끝내지 않고 레이더 위치로 다시 붙을 박스를 찾습니다.
                # 끝내 버리면 차가 아직 멀 때 장면으로 번호판을 읽게 됩니다(작게 찍혀 형식 불일치).
                logger.info(f"[PLR FOLLOW] event_id={event_id} 따라가던 박스 tid={fl['tid']}를 놓쳐 레이더 위치로 다시 찾습니다.")
                fl["tid"] = None
                fl["last_seen"] = now_m
            lost = now_m - fl["last_seen"] > self.PLR_FOLLOW_LOST_SEC
            if lost or now_m - fl["started"] > self.PLR_FOLLOW_MAX_SEC:
                fl = self.plr_follow.pop(event_id)
                request_fullres_capture(self, fl["event_name"], event_id, fl["event_fid"], fl["bbox"], fl["inference_shape"],
                                        pts_window=fl["pts_window"], follow_boxes=fl["boxes"], evidence=fl.get("evidence"),
                                        radar_path=fl.get("radar_path"), photo_m=fl.get("photo_m"))

    def _frame_meta_for_logic(self, fid):
        """과속 측정용 수신 PTS와 직전 처리 프레임 이후 건너뛴 프레임 수를 돌려줍니다."""
        meta = self.reader.get_frame_meta(fid)
        if meta is None:
            return None
        seq = meta.get("seq")
        last_seq = self._last_logic_seq
        skipped = seq - last_seq - 1 if last_seq is not None and seq > last_seq else 0
        self._last_logic_seq = seq
        meta["skipped_since_prev"] = max(0, skipped)
        return meta

    def run_logic(self, fr, fid, d_main_res, d_topdown=None):
        # 헬스체크 스레드(관제 On·Off 반영)와 설정 반영이 프레임 처리와 겹치지 않게 잠급니다.
        with self.lock:
            return self._run_logic(fr, fid, d_main_res, d_topdown)

    def _run_logic(self, fr, fid, d_main_res, d_topdown):
        """d_topdown: topdown 검출(topdown 이벤트가 없는 카메라는 None)."""
        if fr is None:
            return [], None, {}, []

        now_t = time.time()
        self.fps_queue.append(now_t)
        if len(self.fps_queue) > 1:
            time_diff = self.fps_queue[-1] - self.fps_queue[0]
            self.current_fps = len(self.fps_queue) / time_diff if time_diff > 0 else 0.0
        self.current_frame_meta = self._frame_meta_for_logic(fid)
        br_state = self._sync_backreach_state()
        if br_state[0] != 1:
            # 감시 OFF면 Backreach 경고를 다음 처리 프레임에서 지웁니다(검출·추적·표시는 계속).
            self._clear_backreach_alarms()

        self._ensure_roi(fr)

        d_main_res = d_main_res if d_main_res is not None and len(d_main_res) else np.empty((0, 6))
        t_main = self.trk_main.update(d_main_res)
        t_main = t_main if len(t_main) else np.empty((0, 7))
        t_speed = self.trk_speed.update(d_main_res) if SPEEDING_EVENT in self.handlers else np.empty((0, 7))
        t_speed = t_speed if len(t_speed) else np.empty((0, 7))
        # topdown 추적은 프레임마다 한 번만 하고, topdown을 쓰는 이벤트가 이 결과(t_topdown)를 같이 받습니다.
        t_topdown = None
        if d_topdown is not None:
            tracks = self.trk_topdown.update(d_topdown)
            t_topdown = tracks if len(tracks) else np.empty((0, 7))
        # YT 양하 감시 상태는 프레임마다 한 번 갱신하고, YT 두 이벤트가 그 결과(yt_snapshot)를 같이 받습니다.
        # 관제에서 YT 이벤트를 꺼도 화면 표시와 추론 빈도 판단을 위해 상태는 계속 갱신합니다.
        # PLC 양하 신호는 없다고 보고 영상으로만 판단합니다.
        yt_snapshot = self.yt_monitor.update(fid, now_t, t_topdown, self.roi_poly) \
            if self.yt_monitor is not None else None

        now = time.time()
        current_alarms = {}
        track_map_main = {int(t[4]): int(t[6]) for t in t_main}
        track_map_speed = {int(t[4]): int(t[6]) for t in t_speed}
        newly_triggered_events = []

        for ename, handler in self.handlers.items():
            # 관제실에서 끈 이벤트(헬스체크 activeSettings)도 판정·녹화·번호판 읽기는 그대로 합니다.
            # 관제 전송(사진·영상)만 _send_receiver_file이 막습니다(2026-11-11).
            # 1. 이벤트 핸들러에 전달할 인자 세팅(과속만 전용 추적기 trk_speed의 트랙을 받음)
            handler_tracks, handler_track_map = t_main, track_map_main
            if ename == SPEEDING_EVENT:
                kwargs = {
                    'privacy_tracks': t_main,
                    'camera_key': self.camera_key,
                }
                # 과속은 전용 추적기(trk_speed) 박스로 레이더를 연결합니다. 개인정보 블러는 trk_main 기준 그대로.
                kwargs.update(self._radar_judge_kwargs(fr, t_speed, track_map_speed))
                handler_tracks, handler_track_map = t_speed, track_map_speed
            elif ename == CONE_NG_EVENT:
                # 콘·컨테이너는 원본 프레임에서 찾고, 카메라 여러 대를 같은 시간축(수신 PTS 기준 시각)으로 묶습니다.
                meta = self.current_frame_meta or {}
                camera_time_of = getattr(self.reader, 'camera_time_of', None)
                kwargs = {
                    'privacy_tracks': t_main,
                    'camera_key': self.camera_key,
                    'source_frame': self.reader.source_frame,
                    # PTS가 없는 GStreamer 수신(콘 카메라 기본)은 같은 기준(PC monotonic)의 받은 시각을 씁니다.
                    'camera_time': (camera_time_of(meta.get("pts")) if callable(camera_time_of) and meta.get("pts") is not None
                                    else meta.get("recv_monotonic")),
                }
            elif ename == BACKREACH_EVENT:
                # topdown의 모든 클래스를 감시 ON·OFF와 함께 넘깁니다. 블러는 topdown 트랙을 메인 모델 번호로 바꿔 씁니다.
                kwargs = {
                    'privacy_tracks': topdown_privacy_tracks(t_topdown),
                    'topdown_tracks': t_topdown,
                    'backreach_on': br_state[0],
                    'signal_generation': br_state[1],
                }
            elif ename in YT_EVENTS:
                # YT: Backreach와 같은 topdown 추적 결과와 이번 프레임의 양하 감시 상태를 받습니다. 블러도 Backreach와 같습니다.
                kwargs = {
                    'privacy_tracks': topdown_privacy_tracks(t_topdown),
                    'topdown_tracks': t_topdown,
                    'yt_snapshot': yt_snapshot,
                    'camera_key': self.camera_key,
                }
            elif ename == SEASIDE_PERSON_EVENT:
                # 해측: 같은 topdown 추적 결과의 사람 발 위치를 끝선에 맞춰 옮긴 위험 구역으로 판정합니다. 블러도 Backreach와 같습니다.
                kwargs = {
                    'privacy_tracks': topdown_privacy_tracks(t_topdown),
                    'topdown_tracks': t_topdown,
                    'camera_key': self.camera_key,
                }

            try:
                # 핸들러 실행 (내부에서 과거 시점의 privacy_tracks를 저장하고 ev에 담아 반환해야 함)
                triggered = handler.process(handler_tracks, handler_track_map, fr, fid, **kwargs)
            except Exception as e:
                logger.error(f"🚨 [CAM:{self.ip}] {ename} 핸들러 처리 중 예외 발생: {e}\n{traceback.format_exc()}")
                continue

            for ev in triggered:
                tid = ev['tid']
                bbox = ev['bbox']
                ev_frame = ev.get('frame') if ev.get('frame') is not None else fr
                cooldown = handler.config.get("cooldown_sec", 600)
                objects_meta = ev['objects']

                # ---------------------------------------------------------
                # 과거 프레임의 모든 객체(privacy_tracks) 복원
                # ---------------------------------------------------------
                event_frame_privacy_tracks = ev['privacy_tracks']
                privacy_reference_fid = ev['privacy_fid']

                # 타임캡슐(privacy_tracks)이 온전히 반환되었다면 그것을 사용 (과거의 전체 객체)
                if event_frame_privacy_tracks is not None and len(event_frame_privacy_tracks) > 0:
                    privacy_reference_tracks = event_frame_privacy_tracks
                    privacy_reference_tracks_label = "event_frame_tracks"
                else:
                    # 타임캡슐이 없을 경우 위반 객체로 축소하지 않고 현재 프레임의 전체 객체(t_main)를 마스킹 대상으로 삼음
                    privacy_reference_tracks = t_main
                    privacy_reference_tracks_label = "current_tracks"

                decision_trace = to_json_safe(ev['decision_trace'])

                if tid in self.alerted:
                    self._alerted_seen[tid] = now
                already = handler.alarm_once_per_object and ename in self.alerted.get(tid, ())
                if not already and (now - self.last_evt_t.get(ename, 0) >= cooldown):
                    if ename == BACKREACH_EVENT and self._read_backreach_state() != br_state:
                        # 추론 중 감시 구간(ON·OFF)이 바뀌었으면 이전 구간의 결과로 이벤트를 만들지 않습니다.
                        break
                    if ename == SEASIDE_PERSON_EVENT and not handler.group.claim(self.camera_key, now):
                        # 묶음 쿨다운: 같은 크레인의 다른 카메라가 방금 이벤트를 냈습니다(겹치는 화면의 같은 사람일 수 있음).
                        continue
                    event_ts_dt = now_kst()
                    event_ts = event_ts_dt.isoformat()
                    event_fid = int(ev.get('fid', fid))
                    event_id = make_event_id(self.cam_id, self.ip, ename, tid, event_fid, event_ts_dt)

                    objs_log_str = " | ".join([f"{o['label']}({o['score']:.2f}): {o['box']}" for o in objects_meta])
                    log_msg = (
                        f"🔥 [EVENT TRIGGERED] event_id={event_id} CAM:{self.cam_id}({self.ip}) | Event:{ename} | "
                        f"CraneID:{SYS_CFG['crane_id']} | TID:{tid} | FID:{event_fid} | FPS:{self.current_fps:.1f} | "
                        f"Reason:{decision_trace.get('reason', '-')} | "
                        f"Objects -> {objs_log_str}"
                    )
                    logger.warning(log_msg)

                    blur_face_option = handler.config.get("blur_face", True)
                    blur_plate_option = handler.config.get("blur_plate", True)

                    # 완벽하게 동기화된 트랙(privacy_reference_tracks)으로 다단계 블러 적용
                    saved_img, privacy_blur_meta = self.apply_privacy_blur(
                        ev_frame,
                        privacy_reference_tracks,
                        blur_face=blur_face_option,
                        blur_plate=blur_plate_option
                    )
                    if ename == BACKREACH_EVENT and self._read_backreach_state() != br_state:
                        # 블러 처리 중 감시 구간이 바뀌어도 이전 구간 결과는 저장하지 않습니다.
                        break

                    # 블러 처리 메타데이터 로깅 (추적용)
                    privacy_blur_meta["scope"] = "event_snapshot"
                    privacy_blur_meta["reference_tracks"] = privacy_reference_tracks_label
                    privacy_blur_meta["reference_fid"] = int(privacy_reference_fid)

                    logger.info(
                        f"[PRIVACY BLUR] event_id={event_id} cam={self.cam_id} event={ename} "
                        f"face_enabled={blur_face_option} plate_enabled={blur_plate_option} "
                        f"face_count={len(privacy_blur_meta['face'])} "
                        f"plate_count={len(privacy_blur_meta['plate'])} "
                        f"reference_tracks={privacy_blur_meta['reference_tracks']}"
                    )

                    event_trajectories = {}
                    for obj in objects_meta:
                        obj_tid = obj.get('tid')
                        if obj.get('raw_tid') in self.trk_topdown.tracks:  # topdown 객체(Backreach·YT)
                            event_trajectories[obj_tid] = list(self.trk_topdown.tracks[obj['raw_tid']]['history'])
                        elif ename == SPEEDING_EVENT and obj_tid in self.trk_speed.tracks:
                            event_trajectories[obj_tid] = list(self.trk_speed.tracks[obj_tid]['history'])
                        elif obj_tid in self.trk_main.tracks:
                            event_trajectories[obj_tid] = list(self.trk_main.tracks[obj_tid]['history'])

                    api_event_type = receiver_event_type(ename, handler.config.get("api_event_type"))
                    event_meta = {
                        'event_id': event_id,
                        'ts': event_ts,
                        'event_name': ename,
                        # 새 receiver API 필드. 녹화 영상도 사진과 같은 collectedAt으로 보냅니다.
                        'crane_id': str(SYS_CFG["crane_id"]),
                        'cctv_server_id': str(SYS_CFG["cctv_server_id"]),
                        'collected_at': receiver_collected_at(event_ts),
                        'api_event_type': api_event_type,
                        'cctv_id': int(self.cam_id),
                        'ip': str(self.ip),
                        'tid': int(tid),
                        'bbox': int_box(bbox),
                        'fid': event_fid,
                        'objects': self._serialize_event_objects(objects_meta),
                        'trajectories': to_json_safe(event_trajectories),
                        'privacy_blur': to_json_safe(privacy_blur_meta),
                        'decision_trace': decision_trace
                    }

                    ocr_dir = ocr_future = None
                    if ename == CONE_NG_EVENT:
                        # NG 컨테이너의 번호: 착지 중 모아 둔 원본 크롭으로 별도 스레드에서 읽습니다(container_ocr.json).
                        # 사진 저장·전송보다 먼저 예약해, 관제 사진의 detectedObjects에 번호를 넣어 보냅니다.
                        # 착지 OCR(ocr_on "LANDED")로 이미 읽고 있으면 그 결과를 씁니다.
                        ocr_dir, ocr_future = (ev.get('container_ocr_request')
                                               or request_container_ocr(self, event_id, ename, ev.get('container_ocr_frames')))
                        if ocr_dir:
                            event_meta['container_ocr_dir'] = ocr_dir

                    evidence_paths = save_event_image_with_mark(
                        saved_img, self.ip, ename, bbox, tid,
                        crane_id=SYS_CFG["crane_id"], cctv_id=self.cam_id,
                        objects_meta=objects_meta,
                        event_id=event_id, event_ts=event_ts, api_event_type=api_event_type,
                        ocr_future=ocr_future,
                        # 과속: 관제 사진은 번호판(원본 추출 뒤 PLR)을 읽은 뒤 regNo를 넣어 보냅니다.
                        plate_future=plate_future_for(event_id) if ename == SPEEDING_EVENT and fullres_enabled(ename) else None,
                    )
                    if evidence_paths:
                        event_meta.update(evidence_paths)
                    group_name = decision_trace.get('group')
                    if ename == CONE_NG_EVENT and group_name and group_name != self.camera_key:
                        # 묶음 카메라: 나머지 카메라도 같은 이벤트로 그 순간 화면을 함께 남깁니다.
                        event_meta['group_companions'] = save_group_companions(ConeNgGroup._groups.get(group_name),
                                                                               self.camera_key, event_id)
                    elif ename == SEASIDE_PERSON_EVENT:
                        # 해측 묶음: 나머지 카메라 그 순간 사진과 4분할 영상을 같은 이벤트로 남깁니다(묶음이 2대 이상일 때).
                        companions = handler.group.on_event(self.camera_key, event_id)
                        if companions:
                            event_meta['group_companions'] = companions

                    follow_obj = next((o for o in objects_meta if o.get('class_id', -1) != -1 and o.get('tid') is not None), None) \
                        if ename == SPEEDING_EVENT else None
                    evidence = None
                    if ename == SPEEDING_EVENT:
                        # 영상 속도 재검증: 판정 전 2초 동안 그 차량 박스(trk_speed)의 지면 위치로 속도를 다시 재서 레이더 값과 비교
                        vc_boxes = list(self.track_boxes.get(int(follow_obj['tid']), [])) if follow_obj is not None else []
                        vc_pts = float(ev['fullres_window']['event_pts']) if ev.get('fullres_window') else None
                        radar_client_id = decision_trace.get('radar_client_id')
                        video_check = video_speed_check(vc_boxes, ev_frame.shape, vc_pts, decision_trace.get('speed_kmh', 0.0),
                                                        radar_camera_cfg(radar_client_id))
                        decision_trace['video_check'] = video_check
                        evidence = {"ts": event_ts, "speed_kmh": decision_trace.get('speed_kmh'),
                                    "threshold_kmh": decision_trace.get('threshold_kmh'),
                                    "radar_id": decision_trace.get('radar_id'), "radar_client_id": radar_client_id,
                                    "video_check": video_check}
                    if ename == SPEEDING_EVENT and ev.get('fullres_window') and fullres_enabled(ename):
                        # 과속 판정은 레이더가 멀리서 먼저 내리므로, 그 차량을 따라가 가장 가까울 때의 원본으로 번호판을
                        # 읽습니다(_update_plr_follow가 차량이 사라지거나 시간이 지나면 추출 요청). 판정 순간 박스를 못 찾았으면
                        # (ftid None) 레이더 트랙을 따라가다 박스가 붙는 순간부터 그 박스를 따라갑니다.
                        ftid = int(follow_obj['tid']) if follow_obj is not None else None
                        ev_pts = float(ev['fullres_window']['event_pts'])
                        # 멀어지는 차는 가장 가까울 때가 판정보다 먼저라 판정 전 박스를 더 길게 씁니다.
                        past_sec = self.PLR_FOLLOW_BACK_SEC if ev.get('direction') == 'backward' else self.PLR_FOLLOW_MAX_SEC
                        past = [b for b in self.track_boxes.get(ftid, []) if b[0] >= ev_pts - past_sec] \
                            if ftid is not None else []
                        now_m = time.monotonic()
                        self.plr_follow[event_id] = {
                            "tid": ftid, "boxes": past, "started": now_m, "last_seen": now_m,
                            "radar_seq": decision_trace.get('radar_track_seq'), "radar_id": decision_trace.get('radar_id'),
                            "event_name": ename, "event_fid": event_fid, "bbox": bbox,
                            "inference_shape": ev_frame.shape, "pts_window": ev.get('fullres_window'), "evidence": evidence,
                            # 판정 전 레이더 경로(박스가 끝내 안 붙은 차의 사진 구간용)와 방향별 사진 구간
                            "radar_path": list(ev.get('radar_path') or []), "photo_m": ev.get('photo_m')}
                        fullres_dir = fullres_out_dir(self, ename, event_id)
                    else:
                        fullres_dir = request_fullres_capture(
                            self, ename, event_id, event_fid, bbox, ev_frame.shape,
                            pts_window=ev.get('fullres_window'), evidence=evidence
                        )
                    if fullres_dir:
                        event_meta['fullres_dir'] = fullres_dir

                    self.recorder.trigger(ename, event_meta=event_meta)
                    self.alerted.setdefault(tid, set()).add(ename)
                    self._alerted_seen[tid] = now
                    self.last_evt_t[ename] = now

                    newly_triggered_events.append({
                        'event_id': event_id,
                        'ts': event_ts,
                        'event_name': ename,
                        'objects': objects_meta,
                        'privacy_blur': privacy_blur_meta,
                        'decision_trace': decision_trace
                    })

                else:
                    cooldown_remaining = max(0.0, cooldown - (now - self.last_evt_t.get(ename, 0)))
                    reason = "alerted" if already else "cooldown"
                    # 같은 객체가 ROI에 머무는 동안 프레임마다 같은 줄이 반복되므로, 객체마다 이유가 바뀔 때만 남깁니다.
                    if self._suppress_logged.get((ename, tid)) != reason:
                        if len(self._suppress_logged) > 2000:
                            self._suppress_logged.clear()
                        self._suppress_logged[(ename, tid)] = reason
                        logger.debug(
                            f"[EVENT SUPPRESSED] cam={self.cam_id} ip={self.ip} event={ename} tid={tid} "
                            f"fid={int(ev.get('fid', fid))} reason={reason} alerted={ename in self.alerted.get(tid, ())} "
                            f"cooldown_remaining={cooldown_remaining:.1f}s"
                        )

                current_alarms[tid] = ename

        alarm_duration = SYS_CFG["VISUAL_ALARM_DURATION"]
        for tid, ename in current_alarms.items():
            self.visual_alarms[tid] = {'evt': ename, 'expire': now + alarm_duration}

        for tid in list(self.visual_alarms.keys()):
            if now > self.visual_alarms[tid]['expire']:
                del self.visual_alarms[tid]

        if now - self._alerted_pruned_t >= 60.0:
            # 1분마다: ALERTED_KEEP_SEC 동안 이벤트 후보가 되지 않은 객체를 알람 기록에서 지웁니다(오래 켜 둬도 쌓이지 않게).
            self._alerted_pruned_t = now
            for tid in [t for t, seen in self._alerted_seen.items() if now - seen > self.ALERTED_KEEP_SEC]:
                self.alerted.pop(tid, None)
                del self._alerted_seen[tid]

        return t_main, t_topdown, {t: info['evt'] for t, info in self.visual_alarms.items()}, newly_triggered_events

    def draw(self, fr, t_main, t_topdown, alarms, connected=True, inference_shape=None, for_record=False):
        """카메라 한 대의 화면(박스·ROI·상태 글자)을 그립니다.

        과속 카메라는 GUI에서 받은 원본(화면용) 프레임에 추론 좌표를 변환해 그리고 레이더 속도·투영점을 표시합니다.
        그 외 카메라는 추론 프레임에 그대로 그립니다(inference_shape 없음 = 배율 1).
        for_record=True는 관제 영상용 녹화 그림입니다. 관제 요청으로 AI FPS와 객체 점수(conf)를 적지 않습니다."""
        if fr is None or not connected:
            blank = np.zeros((360, 640, 3), dtype=np.uint8)
            cv2.putText(blank, f"CAM {self.cam_id} NO SIGNAL", (50, 180), cv2.FONT_HERSHEY_SIMPLEX, 1, (0,0,255), 1)
            cv2.putText(blank, self.ip, (50, 220), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255,255,255), 1)
            return blank

        h_frame, w_frame = fr.shape[:2]
        infer_h, infer_w = inference_shape[:2] if inference_shape is not None else (h_frame, w_frame)
        scale = np.array([w_frame / infer_w, h_frame / infer_h])

        def display_point(x, y):
            return int(x * scale[0]), int(y * scale[1])

        # 글자·선 크기는 그리는 화면 폭에 비례합니다(가로 720 = 1배). 모자이크가 화면마다 다른 비율로 줄여도
        # 칸 안에서 같은 크기로 보이게 합니다(콘 카메라는 1280에 그려 절반으로 줄어듦).
        ui = max(1.0, w_frame / 720.0)

        def fs(v):
            return v * ui

        def th(v):
            return max(1, int(round(v * ui)))

        def px(v):
            return int(round(v * ui))

        def hud_text(text, org, color, size, align_right=False):
            """검은 바탕 위에 상태 글자를 씁니다(영상 밝기와 상관없이 읽히게)."""
            (tw, tht), base = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, fs(size), th(1))
            x, y = (org[0] - tw, org[1]) if align_right else org
            cv2.rectangle(fr, (x - px(4), y - tht - px(4)), (x + tw + px(4), y + base + px(2)), (0, 0, 0), -1)
            cv2.putText(fr, text, (x, y), cv2.FONT_HERSHEY_SIMPLEX, fs(size), color, th(1), cv2.LINE_AA)

        if len(alarms) > 0:
            cv2.rectangle(fr, (0, 0), (w_frame, h_frame), (0, 0, 255), th(20))

        if len(self.roi_poly) > 2:
            # YT 작업 차선 ROI(4번)는 초록입니다(노랑은 5번 위험구역). Backreach가 있으면 Backreach 감시 영역이라 노랑 그대로입니다.
            roi_color = (0, 255, 0) if self.yt_monitor is not None and BACKREACH_EVENT not in self.handlers else (0, 255, 255)
            cv2.polylines(fr, [(np.asarray(self.roi_poly) * scale).astype(np.int32)], True, roi_color, th(2))
        zone_handler = self.handlers.get(YT_ZONE_PERSON_EVENT)
        if zone_handler is not None and self.yt_monitor is not None:
            # YT 위험구역(5번, 노랑): 양하 감시 중인 YT마다 빈 샤시로 만든 구역을 반투명으로 칠하고 테두리를 그립니다.
            for _, zone in zone_handler.danger_zones(self.yt_monitor.snapshot):
                (x1, y1), (x2, y2) = display_point(zone[0], zone[1]), display_point(zone[2], zone[3])
                area = fr[max(0, y1):max(0, y2), max(0, x1):max(0, x2)]
                if area.size:
                    area[:] = cv2.addWeighted(area, 0.7, np.full_like(area, (0, 255, 255)), 0.3, 0)
                cv2.rectangle(fr, (x1, y1), (x2, y2), (0, 255, 255), th(2))
        for p1, p2 in zip(self.roi_lines[0::2], self.roi_lines[1::2]):
            cv2.line(fr, display_point(*p1), display_point(*p2), (0, 0, 255), th(2))

        radar_display = (self.radar_display or {}) if SPEEDING_EVENT in self.events else {}
        radar_by_box = radar_display.get('by_box', [])
        radar_points = radar_display.get('points', [])

        def radar_row_for(box):
            # 레이더 연결은 과속 전용 추적기 박스 기준이므로, 화면의 trk_main 박스와 가장 많이 겹치는 것을 씁니다.
            best = max(((calculate_iou(box, b), row) for b, row in radar_by_box), key=lambda x: x[0], default=(0.0, None))
            return best[1] if best[0] >= 0.5 else None

        # 메인 모델 트랙은 과속 카메라에만 있고, 차량(승용차·트럭)만 표시합니다.
        allowed_classes = set(TARGET_VEHICLES)

        for t in t_main:
            tid = int(t[4])
            cls_id = int(t[6])
            is_alarmed = tid in alarms

            if not is_alarmed and cls_id not in allowed_classes:
                continue

            color = (0, 0, 255) if is_alarmed else (0, 255, 0)
            thickness = th(1)

            if tid in self.trk_main.tracks:
                hist = list(self.trk_main.tracks[tid]['history'])
                if len(hist) > 1:
                    cv2.polylines(fr, [(np.asarray(hist) * scale).astype(np.int32)], False, color, th(1), cv2.LINE_AA)

            if cls_id == ID_G_PERSON: label = f"Person [{tid}]"
            elif cls_id == ID_PERSON_LOW: label, color = f"LowBody [{tid}]", (0, 150, 0)
            elif cls_id == ID_G_CAR: label, color = f"Car [{tid}]", (255, 100, 0)
            elif cls_id == ID_G_TRUCK: label, color = f"LineTruck [{tid}]", (255, 100, 0)
            else: label = f"OBJ [{tid}]"
            # 객체 점수(conf): 추적이 마지막으로 맞춘 검출의 점수입니다(관제 영상에는 적지 않음).
            if not for_record:
                label = f"{label} {float(t[5]):.2f}"

            if is_alarmed:
                color = (0, 0, 255)
                label = f"ALARM: {label}"

            p1, p2 = display_point(t[0], t[1]), display_point(t[2], t[3])
            cv2.rectangle(fr, p1, p2, color, thickness)
            cv2.putText(fr, label, (p1[0], p1[1] - px(5)), cv2.FONT_HERSHEY_SIMPLEX, fs(0.6), color, th(1))
            radar_row = radar_row_for([float(c) for c in t[:4]]) if radar_by_box else None
            if radar_row is not None:
                # 레이더와 연결된 차량: 박스 위에 레이더 속도(최근 0.5초 중앙값). 기준 초과면 빨간색.
                speed_color = (0, 0, 255) if radar_row['over'] else (0, 255, 255)
                cv2.putText(fr, f"{radar_row['speed_kmh']:.0f} km/h", (p1[0], max(px(15), p1[1] - px(28))),
                            cv2.FONT_HERSHEY_SIMPLEX, fs(0.8), speed_color, th(2))

        for uv, row in radar_points:
            # 투영된 레이더 점(보정 확인용). 연결되지 않은 점도 표시합니다.
            pt = display_point(*uv)
            cv2.circle(fr, pt, px(5), (255, 0, 255), -1)
            cv2.putText(fr, f"R{row['radar_id']}", (pt[0] + px(6), pt[1] - px(4)), cv2.FONT_HERSHEY_SIMPLEX, fs(0.4),
                        (255, 0, 255), th(1))

        # topdown의 모든 객체를 같은 형식(클래스 [ID] 점수)으로 표시합니다. 경고 객체는 빨간색.
        for t in (t_topdown if t_topdown is not None else []):
            tid, cls_id = int(t[4]), int(t[6])
            is_alarmed = (TOPDOWN_TID_BASE + tid) in alarms
            name = TOPDOWN_CLASS_NAMES[cls_id] if 0 <= cls_id < len(TOPDOWN_CLASS_NAMES) else "OBJ"
            if name == "PERSON": label, color = f"Person [{tid}]", (0, 255, 0)
            elif name == "VEHICLE": label, color = f"Car [{tid}]", (255, 100, 0)
            else: label, color = f"{name} [{tid}]", (0, 255, 0)
            if not for_record:
                label = f"{label} {float(t[5]):.2f}"
            if is_alarmed:
                color = (0, 0, 255)
                label = f"ALARM: {label}"
            if tid in self.trk_topdown.tracks:
                hist = list(self.trk_topdown.tracks[tid]['history'])
                if len(hist) > 1:
                    cv2.polylines(fr, [(np.asarray(hist) * scale).astype(np.int32)], False, color, th(1), cv2.LINE_AA)
            p1, p2 = display_point(t[0], t[1]), display_point(t[2], t[3])
            cv2.rectangle(fr, p1, p2, color, th(1))
            cv2.putText(fr, label, (p1[0], p1[1] - px(5)), cv2.FONT_HERSHEY_SIMPLEX, fs(0.6), color, th(1))

        seaside_handler = self.handlers.get(SEASIDE_PERSON_EVENT)
        seaside_overlay = getattr(seaside_handler, "overlay", None)
        if seaside_overlay:
            # 해측 가변 ROI(정규화 좌표): 위험 구역(반투명 빨강), 따라가는 끝선(파랑), 이번에 찾은 끝선(하늘색), 사람 발 위치
            def norm_pts(pts):
                return np.array([[int(u * w_frame), int(v * h_frame)] for u, v in pts], np.int32)
            if len(seaside_overlay["zone"]) >= 3:
                poly = norm_pts(seaside_overlay["zone"])
                layer = fr.copy()
                cv2.fillPoly(layer, [poly], (0, 0, 255))
                fr[:] = cv2.addWeighted(fr, 0.75, layer, 0.25, 0)
                cv2.polylines(fr, [poly], True, (0, 0, 255), th(1))
            if len(seaside_overlay["edge"]) >= 2:
                cv2.polylines(fr, [norm_pts(seaside_overlay["edge"])], False, (255, 0, 0), th(2))
            if seaside_overlay["edge_found"]:
                cv2.polylines(fr, [norm_pts(seaside_overlay["edge_found"])], False, (255, 255, 0), th(2))
            for u, v, inside in seaside_overlay["persons"]:
                pt = (int(u * w_frame), int(v * h_frame))
                cv2.circle(fr, pt, px(4), (0, 0, 255) if inside else (0, 255, 0), -1)
                cv2.putText(fr, "IN" if inside else "OUT", (pt[0] + px(6), pt[1]), cv2.FONT_HERSHEY_SIMPLEX, fs(0.45),
                            (0, 0, 255) if inside else (0, 255, 0), th(1))
            state_color = {"CONFIRMED": (0, 255, 0), "HOLDING": (0, 200, 255)}.get(seaside_overlay["state"], (0, 0, 255))
            hud_text(f"SEASIDE {seaside_overlay['group']} {seaside_overlay['state']}", (px(10), h_frame - px(12)),
                     state_color, 0.55)

        if self.yt_monitor is not None:
            # YT 양하 감시: 적재영역(빨강)·하락 예측 바닥면(정상 초록, 이탈 자홍)·대기 시간(위험구역 노랑은 위에서 그림)
            yt_draw_overlay(fr, self.yt_monitor.snapshot, scale=scale, ui=ui)

        cone_handler = self.handlers.get(CONE_NG_EVENT)
        cone_overlay = getattr(cone_handler, "overlay", None)
        if cone_overlay:
            # 콘 판정 관측(원본 좌표): 컨테이너·탱크(판정 대상 초록, 그 외 회색)·모서리 크롭(노랑)·콘(강 빨강, 약 주황,
            # 모서리 밖 회색), side의 나머지 클래스(스프레더 파랑, 사람 분홍, YT_HEADER 하늘색)
            ow, oh = cone_overlay["wh"]
            sx, sy = w_frame / float(ow), h_frame / float(oh)

            def cone_pt(x, y):
                return int(x * sx), int(y * sy)

            picked = cone_overlay["picked"]
            for c in cone_overlay["containers"]:
                is_picked = picked is not None and calculate_iou(c["xyxy"], picked) > 0.5
                color = (0, 255, 0) if is_picked else (160, 160, 160)
                p1, p2 = cone_pt(*c["xyxy"][:2]), cone_pt(*c["xyxy"][2:])
                cv2.rectangle(fr, p1, p2, color, th(2) if is_picked else th(1))
                name = "TANK" if c.get("cls") == 1 else "CONT"
                cont_label = name if for_record else f"{name} {c['conf']:.2f}"
                cv2.putText(fr, cont_label, (p1[0] + px(4), p1[1] + px(16)), cv2.FONT_HERSHEY_SIMPLEX, fs(0.5), color, th(1))
            for o in cone_overlay.get("others", ()):
                name, color = SIDE_OTHER_DRAW.get(o["cls"], (str(o["cls"]), (200, 200, 200)))
                p1, p2 = cone_pt(*o["xyxy"][:2]), cone_pt(*o["xyxy"][2:])
                cv2.rectangle(fr, p1, p2, color, th(2))
                label = name if for_record else f"{name} {o['conf']:.2f}"
                cv2.putText(fr, label, (p1[0] + px(4), p1[1] + px(16)), cv2.FONT_HERSHEY_SIMPLEX, fs(0.5), color, th(1))
            for roi in cone_overlay["rois"]:
                cv2.rectangle(fr, cone_pt(*roi[:2]), cone_pt(*roi[2:]), (0, 255, 255), th(1))
            for k in cone_overlay["cones"]:
                if not k["corner"]:
                    color = (160, 160, 160)
                elif k["conf"] >= cone_overlay["strong"]:
                    color = (0, 0, 255)
                else:
                    color = (0, 165, 255)
                p1, p2 = cone_pt(*k["xyxy"][:2]), cone_pt(*k["xyxy"][2:])
                cv2.rectangle(fr, p1, p2, color, th(2))
                if not for_record:
                    cv2.putText(fr, f"{k['conf']:.2f}", (p1[0], max(px(12), p1[1] - px(4))), cv2.FONT_HERSHEY_SIMPLEX, fs(0.5), color, th(1))
            banner_color = {"NG": (0, 0, 255), "ARMED": (0, 165, 255), "DEPARTING": (255, 0, 255)}.get(
                cone_overlay["state"], (0, 255, 0))
            hud_text(f"CONE_NG #{cone_overlay['session']} {cone_overlay['state']} ({cone_overlay['stage']})",
                     (px(10), h_frame - px(12)), banner_color, 0.6)

        hud_y = px(20)
        hud_text(f"CAM {self.cam_id}", (px(10), hud_y), (0, 255, 255), 0.55)
        hud_y += px(24)
        if not for_record:
            # AI FPS가 목표(10) 이상이면 초록, 미만이면 빨강
            fps_color = (0, 255, 0) if self.current_fps >= 10.0 else (0, 0, 255)
            hud_text(f"AI FPS: {self.current_fps:.1f}", (px(10), hud_y), fps_color, 0.55)
            hud_y += px(24)

        if BACKREACH_EVENT in self.handlers:
            backreach_on = (self._last_backreach_state or (None, None))[0]
            monitoring = backreach_on == 1 and self.receiver_event_status.get(BACKREACH_EVENT, True) is not False
            # Backreach 감시 ON·OFF(ON = 스프레더로 감시 ON이고 관제 On)
            hud_text(f"BACKREACH {'ON' if monitoring else 'OFF'}", (px(10), hud_y),
                     (0, 255, 0) if monitoring else (0, 200, 255), 0.5)
            hud_y += px(24)

        if self.yt_monitor is not None:
            # 보이는 YT 수와 그중 양하 감시 중인 수
            units = self.yt_monitor.snapshot.get("units", [])
            armed = sum(1 for unit in units if unit["armed"])
            hud_text(f"YT DISCHARGE: {armed} / {len(units)}", (px(10), hud_y),
                     (0, 255, 0) if armed else (0, 200, 255), 0.5)
            hud_y += px(24)

        # 센서 객체 ID/속도는 별도 상태로 표시합니다. RGB 박스에 임의로 연결하지 않습니다.
        radar_status_lines = getattr(self.radar_provider, 'get_status_lines', None) if SPEEDING_EVENT in self.events else None
        if callable(radar_status_lines):
            for text in radar_status_lines():
                hud_text(text, (px(10), hud_y), (0, 215, 255), 0.45)
                hud_y += px(20)

        active_alarms = set(alarms.values())

        y_pos = px(20)
        for evt in self.events:
            display_name = EVENT_REGISTRY[evt].gui_name if evt in EVENT_REGISTRY else evt.upper()
            color = (0, 0, 255) if evt in active_alarms else (0, 255, 0)
            prefix = "[!] " if evt in active_alarms else " -  "
            if self.receiver_event_status.get(evt, True) is False:
                # 관제실에서 끈 이벤트: 관제 전송만 꺼짐(판정은 계속).
                color, prefix = (160, 160, 160), "[API OFF] "
            hud_text(f"{prefix}{display_name}", (w_frame - px(10), y_pos), color, 0.5, align_right=True)
            y_pos += px(24)

        return fr
# ==========================================
# [11]  Platform 송수신 모듈
# ==========================================
def get_system_temperature():
    """OS 환경(Linux, Edge Device 등)에 맞게 시스템 온도를 안전하게 수집합니다."""
    try:
        # 1차 시도: psutil 센서. CPU 센서(coretemp 등)를 먼저 읽습니다. 메인보드 ACPI 센서(acpitz)는 값이 고정인
        # 장비가 있어(단말기 .54는 늘 27.8°C, 실제 CPU는 약 58°C) CPU 센서가 없을 때만 씁니다.
        if hasattr(psutil, "sensors_temperatures"):
            temps = psutil.sensors_temperatures() or {}
            for name in ("coretemp", "k10temp", "zenpower", "cpu_thermal", "cpu-thermal", "soc_thermal"):
                entries = temps.get(name) or []
                package = [e for e in entries if str(e.label or "").lower().startswith(("package", "tctl", "tdie"))]
                if package or entries:
                    return float((package or entries)[0].current)
            for name, entries in temps.items():
                for entry in entries:
                    return float(entry.current)

        # 2차 시도: 리눅스/엣지 단말(Jetson, Raspberry Pi 등)의 하드웨어 파일 직접 참조
        temp_path = "/sys/class/thermal/thermal_zone0/temp"
        if os.path.exists(temp_path):
            with open(temp_path, "r") as f:
                return float(f.read().strip()) / 1000.0
    except Exception as e:
        logger.debug(f"온도 센서 읽기 실패 (해당 OS 미지원): {e}")

    return 0.0 # 센서가 없는 PC 환경 등의 폴백(Fallback)

def get_npu_temperature():
    """dxrt-cli 명령어를 통해 DeepX NPU의 최대 온도를 파싱하여 반환합니다."""
    if not HAS_DX_ENGINE:
        return 0.0
    try:
        # 터미널 명령어 실행 (응답 지연 방지를 위해 2초 타임아웃)
        output = subprocess.check_output(["dxrt-cli", "-s"], stderr=subprocess.DEVNULL, text=True, timeout=2)
        # 정규식으로 "temperature XX'C" 패턴을 모두 검색
        temps = re.findall(r"temperature\s+(\d+)'C", output)
        if temps:
            return float(max(int(t) for t in temps))
    except Exception:
        pass
    return 0.0

def get_npu_usage():
    """NPU 코어별 사용률(%) 목록과 NPU 메모리(사용 GiB, 전체 GiB, 사용 %)를 돌려줍니다. 못 읽으면 ([], None).
    dxrt-cli와 파이썬 API에는 이 값이 없어서, dxtop을 가상 터미널(script)로 2초 띄워 받은 화면에서 읽습니다(약 4초)."""
    if not HAS_DX_ENGINE or shutil.which("dxtop") is None or shutil.which("script") is None:
        return [], None
    path = f"/tmp/dxtop_status_{os.getpid()}.txt"
    try:
        subprocess.run(["timeout", "2", "script", "-qfc", "dxtop", path], stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                       env=dict(os.environ, TERM="xterm"), timeout=10)
        with open(path, "rb") as f:
            text = re.sub(r"\x1b\[[0-9;?]*[A-Za-z]|\x1b[()][A-Z0-9]|\x1b[=>]", "", f.read().decode("utf-8", "ignore"))
    except Exception:
        return [], None
    finally:
        try:
            os.remove(path)
        except OSError:
            pass
    util = [float(v) for v in re.findall(r"Util:\s*([0-9.]+)%", text)[-3:]]  # 마지막 화면의 코어 3개
    mem = re.findall(r"([0-9.]+) ([KMG]iB) / ([0-9.]+) GiB \(([0-9.]+)%\)", text)
    if not mem:
        return util, None
    used, unit, total, pct = mem[-1]
    used_gib = float(used) * {"KiB": 1 / 1048576, "MiB": 1 / 1024, "GiB": 1.0}[unit]
    return util, (used_gib, float(total), float(pct))

class HealthCheckDaemon:
    def __init__(self, crane_id, version, interval_sec, cams, config_file=CONFIG_CAMERAS_FILE):
        self.crane_id = crane_id
        self.version = version
        self.interval = max(1, int(interval_sec))
        self.running = True
        self.url = receiver_url("health_check")
        self.cams = list(cams)
        self.config_file = config_file
        self._config_lock = threading.Lock()
        self._roi_snapshot_refresh_cctv_ids = set()
        self._roi_snapshot_refresh_lock = threading.Lock()
        self._consecutive_failures = 0
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    @staticmethod
    def _clean_cctv_ids(cctv_ids):
        return {str(cctv_id).strip() for cctv_id in (cctv_ids or []) if str(cctv_id or "").strip()}

    def request_roi_snapshot_refresh(self, cctv_ids=None, reason=""):
        ids = self._clean_cctv_ids(cctv_ids)
        if not ids:
            return False

        with self._roi_snapshot_refresh_lock:
            before = set(self._roi_snapshot_refresh_cctv_ids)
            self._roi_snapshot_refresh_cctv_ids.update(ids)
            pending = set(self._roi_snapshot_refresh_cctv_ids)

        logger.info(
            f"[Health Check] ROI snapshot refresh requested | "
            f"craneId={self.crane_id} cctvIds={','.join(sorted(ids))} "
            f"pending={','.join(sorted(pending))} reason={reason or 'unspecified'}"
        )
        return pending != before

    def get_roi_snapshot_refresh_cctv_ids(self):
        with self._roi_snapshot_refresh_lock:
            return set(self._roi_snapshot_refresh_cctv_ids)

    def clear_roi_snapshot_refresh(self, cctv_ids=None, reason=""):
        ids = self._clean_cctv_ids(cctv_ids)
        if not ids:
            return False

        with self._roi_snapshot_refresh_lock:
            before = set(self._roi_snapshot_refresh_cctv_ids)
            self._roi_snapshot_refresh_cctv_ids.difference_update(ids)
            cleared = before - set(self._roi_snapshot_refresh_cctv_ids)
            pending = set(self._roi_snapshot_refresh_cctv_ids)

        if cleared:
            logger.info(
                f"[Health Check] ROI snapshot refresh cleared | "
                f"craneId={self.crane_id} cctvIds={','.join(sorted(cleared))} "
                f"pending={','.join(sorted(pending)) or '-'} reason={reason or 'unspecified'}"
            )
            return True
        return False

    @staticmethod
    def _decode_jsonish(value):
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return {}
            return json.loads(text)
        return value

    @staticmethod
    def _coerce_roi_norm_points(value):
        value = HealthCheckDaemon._decode_jsonish(value)
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError(f"ROI norm value must be a list, got {type(value).__name__}")

        points = []
        for point in value:
            if isinstance(point, dict):
                x = point.get("x")
                y = point.get("y")
            elif isinstance(point, (list, tuple)) and len(point) >= 2:
                x, y = point[0], point[1]
            else:
                raise ValueError(f"Invalid ROI point: {point!r}")
            points.append([round(float(x), 6), round(float(y), 6)])
        return points

    @classmethod
    def _extract_roi_norm_updates(cls, roi_json):
        payload = cls._decode_jsonish(roi_json)
        if not isinstance(payload, dict):
            return None

        candidates = [payload]
        for nested_key in ("roiInfo", "roi_info"):
            if nested_key not in payload:
                continue
            nested = cls._decode_jsonish(payload.get(nested_key))
            if isinstance(nested, dict):
                candidates.append(nested)

        for candidate in candidates:
            updates = {}
            if "roi_poly_norm" in candidate:
                updates["roi_poly_norm"] = cls._coerce_roi_norm_points(candidate.get("roi_poly_norm"))
            if "roi_lines_norm" in candidate:
                updates["roi_lines_norm"] = cls._coerce_roi_norm_points(candidate.get("roi_lines_norm"))
            if updates:
                return updates
        return None

    def _find_camera_by_cctv_id(self, cctv_id):
        cctv_text = str(cctv_id or "").strip()
        if not cctv_text:
            return None

        for cam in self.cams:
            if cctv_text in (str(cam.cam_id), str(cam.ip)):
                return cam
        return None

    def _apply_roi_settings_from_response(self, response_payload):
        if not isinstance(response_payload, dict):
            return []

        data = response_payload.get("data")
        if not isinstance(data, dict):
            return []

        roi_settings = data.get("roiSettings")
        if not roi_settings:
            return []
        if not isinstance(roi_settings, list):
            logger.warning(f"[Health Check] roiSettings ignored because it is not a list: {type(roi_settings).__name__}")
            return []

        handled_cctv_ids = []
        changed = False
        runtime_updates = []

        with self._config_lock:
            try:
                if os.path.exists(self.config_file):
                    with open(self.config_file, "r", encoding="utf-8") as f:
                        camera_configs = json.load(f)
                else:
                    camera_configs = {}
            except Exception as e:
                logger.error(f"[Health Check] failed to load cameras config for ROI update: {e}")
                return []

            if not isinstance(camera_configs, dict):
                logger.error("[Health Check] cameras config is not an object; ROI update skipped")
                return []

            for item in roi_settings:
                if not isinstance(item, dict):
                    logger.warning(f"[Health Check] invalid roiSettings item ignored: {item!r}")
                    continue

                cam = self._find_camera_by_cctv_id(item.get("cctvId"))
                if cam is None or not receiver_camera_allowed(cam.cam_id):
                    logger.warning(f"[Health Check] ROI settings camera not found or excluded: cctvId={item.get('cctvId')!r}")
                    continue

                try:
                    roi_updates = self._extract_roi_norm_updates(item.get("roiJson"))
                except Exception as e:
                    logger.warning(
                        f"[Health Check] ROI settings parse failed: "
                        f"cctvId={item.get('cctvId')!r} error={e}"
                    )
                    continue

                if not roi_updates:
                    logger.info(
                        f"[Health Check] ROI settings has no norm keys; treated as initial setup: "
                        f"cctvId={item.get('cctvId')!r}"
                    )
                    continue

                disk_conf = camera_configs.get(cam.ip)
                new_conf = dict(cam.conf)
                if isinstance(disk_conf, dict):
                    new_conf.update(disk_conf)

                item_changed = False
                for key, value in roi_updates.items():
                    key_was_present = key in new_conf
                    old_value = new_conf.get(key)
                    try:
                        old_norm = self._coerce_roi_norm_points(old_value) if key_was_present else None
                    except Exception:
                        old_norm = old_value

                    if (not key_was_present) or old_norm != value:
                        new_conf[key] = value
                        item_changed = True

                if item_changed:
                    camera_configs[cam.ip] = new_conf
                    runtime_updates.append((cam, new_conf, roi_updates))
                    handled_cctv_ids.append(str(cam.cam_id))
                    changed = True
                else:
                    logger.info(
                        f"[Health Check] ROI settings unchanged; pending kept: "
                        f"cctvId={item.get('cctvId')!r} keys={','.join(sorted(roi_updates.keys()))}"
                    )

            if changed:
                try:
                    temp_path = f"{self.config_file}.tmp"
                    with open(temp_path, "w", encoding="utf-8") as f:
                        json.dump(camera_configs, f, indent=4, ensure_ascii=False)
                    os.replace(temp_path, self.config_file)
                except Exception as e:
                    logger.error(f"[Health Check] failed to write cameras config for ROI update: {e}")
                    return []

        for cam, new_conf, roi_updates in runtime_updates:
            try:
                cam.update_config(new_conf)
                logger.info(
                    f"[Health Check] ROI settings applied: "
                    f"cctvId={cam.cam_id} camera={cam.ip} keys={','.join(sorted(roi_updates.keys()))} "
                    f"poly={len(new_conf.get('roi_poly_norm', []) or [])} "
                    f"lines={len(new_conf.get('roi_lines_norm', []) or [])}"
                )
            except Exception as e:
                logger.error(f"[Health Check] runtime ROI update failed: cctvId={cam.cam_id} error={e}")

        return handled_cctv_ids

    def build_event_status(self):
        statuses = {}
        for cam in self.cams:
            if not receiver_camera_allowed(cam.cam_id):
                continue
            with cam.lock:
                for name in cam.events:
                    config = deep_merge_dict(SYS_CFG["event_config"].get(name, {}),
                                             cam.conf.get("event_config", {}).get(name, {}))
                    try:
                        event_type = receiver_event_type(name, config.get("api_event_type"))
                    except (TypeError, ValueError):
                        continue
                    remote_name = RECEIVER_EVENT_NAMES.get(event_type)
                    if remote_name:
                        # 이벤트 설정 상태입니다. Backreach 감시 ON·OFF(스프레더)나 현재 검출 유무와는 별개입니다.
                        enabled = name in cam.handlers and cam.receiver_event_status.get(name, True) is not False
                        statuses[remote_name] = statuses.get(remote_name, False) or enabled
        return [{"event_type": name, "event_status": enabled} for name, enabled in sorted(statuses.items())]

    def _apply_active_settings_from_response(self, payload):
        data = payload.get("data") if isinstance(payload, dict) else None
        settings = data.get("activeSettings") if isinstance(data, dict) else None
        if not isinstance(settings, list):
            return []
        statuses = {item["eventType"]: item["eventStatus"] for item in settings
                    if isinstance(item, dict) and isinstance(item.get("eventType"), str)
                    and type(item.get("eventStatus")) is bool}
        if not statuses:
            return []
        updates = []
        with self._config_lock:
            configs = load_camera_configs(self.config_file)
            for cam in self.cams:
                if not receiver_camera_allowed(cam.cam_id):
                    continue
                with cam.lock:
                    conf = copy.deepcopy(cam.conf)
                    conf.update(configs.get(cam.camera_key, {}))
                    flags = copy.deepcopy(conf.get("receiver_event_status", {}))
                    if not isinstance(flags, dict):
                        flags = {}
                    changed = False
                    for name in cam.events:
                        config = deep_merge_dict(SYS_CFG["event_config"].get(name, {}),
                                                 conf.get("event_config", {}).get(name, {}))
                        try:
                            remote_name = RECEIVER_EVENT_NAMES.get(receiver_event_type(name, config.get("api_event_type")))
                        except (TypeError, ValueError):
                            continue
                        if remote_name in statuses and flags.get(name) is not statuses[remote_name]:
                            flags[name] = statuses[remote_name]
                            changed = True
                    if changed:
                        conf["receiver_event_status"] = flags
                        configs[cam.camera_key] = conf
                        updates.append((cam, conf))
            if updates:
                temp_path = self.config_file + ".tmp"
                with open(temp_path, "w", encoding="utf-8") as handle:
                    json.dump(configs, handle, ensure_ascii=False, indent=4)
                os.replace(temp_path, self.config_file)
        for cam, conf in updates:
            cam.update_config(conf)
            logger.info("[Health Check] activeSettings applied cam=%s status=%s", cam.cam_id, cam.receiver_event_status)
        return [str(cam.cam_id) for cam, _ in updates]

    def post_health(self, data):
        if not self.url or not receiver_send_enabled():
            return None
        return requests.post(self.url, headers={"accept": "application/json"}, data=data, timeout=10,
                             verify=SYS_CFG["health_check"]["verify_tls"])

    def _run(self):
        while self.running:
            try:
                # 1. 시스템 자원 및 하드웨어 정보 수집
                cpu = psutil.cpu_percent(interval=1.0)
                mem = psutil.virtual_memory().percent
                sys_temp = get_system_temperature()
                npu_temp = get_npu_temperature()
                npu_util, npu_mem = get_npu_usage()

                # 디스크 용량 수집
                disk_usage = shutil.disk_usage(PROJECT_ROOT)
                disk_free_gb = disk_usage.free / (1024 ** 3)
                disk_total_gb = disk_usage.total / (1024 ** 3)

                # 카메라 연결 상태 확인
                total_cams = len(self.cams)
                active_cams = 0
                zombie_cams = 0
                current_time_for_check = time.time()

                for cam in self.cams:
                    if cam.reader.connected:
                        last_t = cam.reader.last_t
                        # 마지막 프레임 수신 시간이 WATCHDOG_TIMEOUT(기본 30초)을 초과했다면 데드락(좀비)으로 간주
                        if current_time_for_check - last_t > WATCHDOG_TIMEOUT:
                            zombie_cams += 1
                        else:
                            active_cams += 1

                # ISO 8601 포맷 타임스탬프
                reported_at = now_kst().strftime('%Y-%m-%dT%H:%M:%S')

                # API 데이터 (API 서버가 NPU 온도를 모를 수 있으므로, 둘 중 가장 높은 위험 온도를 대표로 보냅니다)
                data = {
                    "craneId": str(self.crane_id),
                    "reportedAt": reported_at,
                    "cpuUsage": round(cpu, 1),
                    "memoryUsage": round(mem, 1),
                    "temperature": round(max(sys_temp, npu_temp), 1),
                    "softwareVersion": self.version,
                    "isRoiSetupRequired": False,
                    "eventStatus": json.dumps(self.build_event_status(), ensure_ascii=False),
                }

                response = self.post_health(data)
                api_status = "DISABLED"

                # 2. 결과 종합 및 로깅
                if response is not None and receiver_response_ok(response):
                    api_status = "OK"
                    if self._consecutive_failures > 0:
                        api_status = f"RECOVERED({self._consecutive_failures})"
                    self._consecutive_failures = 0

                    try:
                        payload = response.json()
                        logger.debug("[Health Check] response=%s", json.dumps(to_json_safe(payload), ensure_ascii=False)[:2000])
                        self._apply_active_settings_from_response(payload)
                        updated_ids = self._apply_roi_settings_from_response(payload)
                        if updated_ids:
                            self.request_roi_snapshot_refresh(updated_ids, reason="roi_settings_applied_from_health_response")
                    except Exception:
                        logger.exception("[Health Check] response settings could not be applied")
                elif response is not None:
                    self._consecutive_failures += 1
                    api_status = f"FAIL({response.status_code})"
                    logger.error("[Health Check] HTTP=%s response=%s", response.status_code, response.text[:1000])

                # 좀비 스레드(Deadlock) 상태를 명시적으로 모니터링 로그에 추가
                # NPU: 코어 3개 사용률, NPU_MEM: NPU 메모리 사용/전체(못 읽으면 -)
                npu_util_text = "/".join(f"{u:.0f}%" for u in npu_util) or "-"
                npu_mem_text = f"{npu_mem[0]:.2f}/{npu_mem[1]:.2f}GiB({npu_mem[2]:.0f}%)" if npu_mem else "-"
                logger.info(
                    f"📊 [SYSTEM STATUS] API:{api_status} | CAM:{active_cams}/{total_cams} (Zombie:{zombie_cams}) | "
                    f"CPU:{cpu:.1f}% | MEM:{mem:.1f}% | DISK:{disk_free_gb:.1f}/{disk_total_gb:.1f}GB | "
                    f"TEMP(Sys/NPU):{sys_temp:.1f}'C/{npu_temp:.1f}'C | NPU:{npu_util_text} | NPU_MEM:{npu_mem_text}"
                )

            except Exception as e:
                self._consecutive_failures += 1
                logger.error(f"🚨 [Health Check] 네트워크/데이터 수집 실패 (누적:{self._consecutive_failures}): {e}")

            # interval(기본 60초) 대기하되, 프로세스 종료 신호(running)를 1초마다 감시
            for _ in range(self.interval):
                if not self.running:
                    break
                time.sleep(1)

    def stop(self):
        self.running = False
        if self.thread.is_alive():
            self.thread.join(timeout=2.0)


def load_camera_configs(path=CONFIG_CAMERAS_FILE):
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        configs = json.load(handle)
    if not isinstance(configs, dict):
        raise ValueError("cameras.json must contain an object")
    return configs

def main():
    parser = argparse.ArgumentParser(description="Edge AI CCTV Event Detection (DeepX NPU)")
    parser.add_argument('--gui', action='store_true', help="GUI 모드를 활성화하여 모니터에 영상을 렌더링합니다.")
    args = parser.parse_args()

    is_gui_mode = args.gui

    if not is_gui_mode:
        logger.info("[시스템 모드] CLI (Headless) 모드로 동작합니다. (렌더링 생략으로 CPU 부하 최소화)")
    else:
        logger.info("[시스템 모드] GUI 모드로 동작합니다. (--gui 플래그 활성화됨)")

    rtsp_list = load_rtsp_list_from_csv(CAMERA_LIST_FILE)
    if not rtsp_list:
        logger.error(f"카메라 목록 파일({CAMERA_LIST_FILE})을 확인하십시오.")
        return

    config_file = CONFIG_CAMERAS_FILE
    camera_configs = {}

    # 파일 로그는 늘 DEBUG까지 남기고, 콘솔 출력만 사용자 선택에 따릅니다.
    if guarded_input(">> CLI 디버그 출력을 활성화하시겠습니까? (파일 로그는 항상 상세히 기록됩니다) [y/N]: ").strip().lower() == 'y':
        stream_handler.setLevel(logging.DEBUG)
        logger.debug("디버그 모드가 활성화되었습니다. 콘솔 상세 로깅이 시작됩니다.")

    run_wizard = not os.path.exists(config_file)
    if run_wizard:
        logger.warning("설정 파일(cameras.json)이 없어 터미널 마법사를 실행합니다.")
    else:
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                camera_configs = json.load(f)
        except Exception as e:
            logger.error(f"cameras.json 로드 실패: {e}")
        if guarded_input(">> 기존 설정(cameras.json)을 무시하고 ROI 및 이벤트를 재설정하시겠습니까? [y/N]: ").strip().lower() == 'y':
            logger.info("기존 설정을 무시하고 터미널 마법사를 실행합니다.")
            run_wizard = True
    if run_wizard:
        camera_configs = run_wizard_batch_mode(rtsp_list, camera_configs)
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(camera_configs, f, indent=4)
        except Exception as e:
            logger.error(f"cameras.json 저장 실패: {e}")
        run_wizard_groups(rtsp_list, camera_configs)

    models_cfg = SYS_CFG["models"]
    # 설정된 이벤트가 쓰는 모델만 올립니다(event_model_keys). 과속: 메인 모델 V2(사람·하반신·승용차·트럭),
    # Backreach·YT: topdown(같은 엔진을 같이 씀). 콘 NG의 side·cone은 첫 콘 프레임에서 ConeObserver가 올립니다.
    # 실행 중 cameras.json에 이벤트가 추가되면 load_event_models가 그때 모델을 올립니다.
    event_models = {}

    def load_event_models(events):
        for key in event_model_keys(events):
            if key in event_models:
                continue
            topdown = key == TOPDOWN_MODEL
            event_models[key] = YoLoDeepX(
                resolve_model_path(models_cfg[key]),
                output_format=get_model_output_format(key),
                pool_size=get_model_engine_pool_size(key),
                use_ort=SYS_CFG["model_use_ort"].get(key) if topdown else None,
                match_input_shape=topdown,
                max_det=1000 if topdown else None,
            )
            if key == TOPDOWN_MODEL:
                # 올린 topdown이 9클래스(이름표)인지 봅니다. 다르면 클래스 번호가 어긋나 경고합니다.
                check_topdown_class_count(event_models[key].raw_yolo_num_classes())
            apply_class_confidences(event_models[key], key)

    configured_events = {evt for conf in camera_configs.values() if isinstance(conf, dict) for evt in conf.get('events', [])}
    try:
        logger.info(f"DeepX 모델을 NPU 메모리로 할당 중... (설정된 이벤트: {sorted(configured_events)})")
        load_event_models(configured_events)

        # 얼굴(face.dxnn, PPU)과 번호판(plate.dxnn, raw YOLO: 앞 4칸이 중심 x·y·폭·높이) 모델.
        # 번호판 엔진은 과속 번호판 인식(PLR)도 같이 씁니다(PLATE_DETECTOR).
        global PLATE_DETECTOR
        d_face = YoLoDeepX(
            resolve_model_path(models_cfg["FACE"]),
            output_format=get_model_output_format("FACE"),
            pool_size=get_model_engine_pool_size("FACE")
        )
        d_plate = PLATE_DETECTOR = YoLoDeepX(
            resolve_model_path(models_cfg["PLATE"]),
            output_format=get_model_output_format("PLATE"),
            pool_size=get_model_engine_pool_size("PLATE")
        )
        apply_class_confidences(d_face, "FACE")
        apply_class_confidences(d_plate, "PLATE")
    except Exception as e:
        logger.error(f"모델 로드 실패. 경로를 확인하십시오: {e}")
        return

    def make_camera(ip, conf, cam_id):
        events = conf.get('events', [])
        return Camera(
            ip, conf,
            event_models.get("MAIN_V2") if "MAIN_V2" in event_model_keys(events) else None,
            d_face, d_plate, cam_id=cam_id,
            det_topdown=event_models.get(TOPDOWN_MODEL) if any(name in events for name in PERSON_TOPDOWN_EVENTS) else None,
        )

    cams = []
    for i, rtsp in enumerate(rtsp_list):
        ip = extract_ip(rtsp)
        conf = camera_configs.get(ip)

        if not conf or not conf.get('events'):
            continue
        conf['url'] = rtsp
        cams.append(make_camera(ip, conf, i + 1))
        # 카메라 이름(변수 ip)은 IP가 아니라 RTSP 주소로 만든 이름(예: 173_cam1)입니다.
        logger.info(
            f"[CAMERA LOADED] cam={i+1} name={ip} events={','.join(conf.get('events', [])) or '-'} "
            f"roi_poly_points={len(conf.get('roi_poly_norm', []) or [])} "
            f"roi_line_points={len(conf.get('roi_lines_norm', []) or [])}"
        )

    if not cams:
        logger.error("[SYSTEM STARTUP] no active cameras loaded; check cameras.csv and cameras.json events.")
        return

    if any(SPEEDING_EVENT in c.events for c in cams):
        logger.info('[Radar] input=real MSE sensors=%s | %s', MseRadarHub.shared().client_ids, ', '.join(
            f"CAM:{c.cam_id}→{c.radar_provider.client_id}" for c in cams if isinstance(c.radar_provider, MseRadarInput)))

    perf_cfg = SYS_CFG["system_performance"]
    sys_target_fps = float(perf_cfg["target_fps"])
    dynamic_cpu_adjust = bool(perf_cfg["dynamic_cpu_adjust_enabled"])

    loop_count = 0
    fps_calc_interval = 30

    current_fps_log_interval_sec = max(1.0, float(SYS_CFG["CURRENT_FPS_LOG_INTERVAL_SEC"]))
    current_fps_last_print = {}

    crane_id = str(SYS_CFG["crane_id"])
    software_version = "v1.1.0"

    log_disk_health([("event_root", EVENT_ROOT_DIR), ("log_dir", LOG_DIR)])
    logger.info(f"[RECEIVER] enabled={receiver_send_enabled()} craneId={crane_id} "
                f"cctvIds={SYS_CFG['receiver']['cctv_ids']}")
    health_daemon = HealthCheckDaemon(
        crane_id,
        version=software_version,
        interval_sec=SYS_CFG["health_check"]["interval_sec"],
        cams=cams,
        config_file=config_file
    )

    last_config_mtime = 0
    if os.path.exists(config_file):
        last_config_mtime = os.path.getmtime(config_file)

    RAM_DISK_DIR = "/dev/shm/cctv_frames"
    if not os.path.exists(RAM_DISK_DIR):
        try: os.makedirs(RAM_DISK_DIR, exist_ok=True)
        except OSError: RAM_DISK_DIR = "./web_frames"

    output_retention_days = float(SYS_CFG["OUTPUT_RETENTION_DAYS"])
    output_cleanup_interval_sec = float(SYS_CFG["OUTPUT_CLEANUP_INTERVAL_SEC"])
    last_output_cleanup_time = time.time()
    run_output_retention_cleanup(output_retention_days)


    def run_camera_inference(cam, fr):
        """카메라에 설정된 이벤트가 쓰는 모델만 추론합니다. 과속: 메인 모델 V2, Backreach·YT: topdown.
        topdown 검출은 topdown 이벤트가 없는 카메라(또는 모델을 못 올렸을 때) None입니다."""
        d_main_res = np.empty((0, 6))
        d_topdown = None
        h, w = fr.shape[:2]
        if SPEEDING_EVENT in cam.events and "MAIN_V2" in event_models:
            # 과속은 메인 V2 하나에서 사람·하반신·승용차·트럭(6번)을 모두 가져옵니다.
            # 점수 기준은 model_confidences.MAIN_V2(클래스별)이고 infer가 클래스마다 거릅니다.
            d_main_res = filter_speed_main_detections(event_models["MAIN_V2"].infer(fr),
                                                      max_area_threshold=(h * w) * SPEEDING_MAX_BOX_AREA_FRAC)
        if any(name in cam.events for name in PERSON_TOPDOWN_EVENTS):
            # Backreach 감시 OFF이고 YT 양하 감시 중도 아닌 동안은 초당 idle_infer_fps장만 추론하고,
            # 그 사이 프레임은 직전 검출을 다시 씁니다(Camera.backreach_inference_due).
            if cam.backreach_inference_due():
                model = event_models.get(TOPDOWN_MODEL)
                cam.last_topdown_res = detection_array(model.infer(fr)) if model is not None else None
                # 새로 추론한 검출의 스프레더로 Backreach 감시 ON·OFF를 정합니다(다시 쓰는 검출은 세지 않음).
                cam.update_backreach_gate(cam.last_topdown_res)
            d_topdown = cam.last_topdown_res
        return d_main_res, d_topdown

    # 메인 스레드와 워커 스레드 간 목표 FPS를 안전하게 공유하기 위한 상태 객체
    system_runtime_state = {"target_fps": sys_target_fps}

    class LatestItemBuffer:
        def __init__(self):
            self.item = None
            self.has_new = False
            self.lock = threading.Lock()

        def put(self, item):
            with self.lock:
                self.item = item
                self.has_new = True

        def get(self):
            with self.lock:
                if not self.has_new:
                    return None
                self.has_new = False
                return self.item

    class CameraWorker(threading.Thread):
        def __init__(self, cam):
            super().__init__(daemon=True)
            self.cam = cam
            self.frame_buffer = LatestItemBuffer()
            self.result_buffer = LatestItemBuffer()
            self.running = True
            self.pacer = FramePacer()  # AI 처리 간격 맞추기(일찍 온 프레임은 버리지 않고 기다림)

        def run(self):
            while self.running:
                item = self.frame_buffer.get()

                # 새로운 프레임이 없다면 즉시 GIL을 해제(Release)하여 다른 스레드에 양보
                if item is None:
                    time.sleep(0.005)
                    continue

                # 초당 target_fps번 이하로 처리합니다. 일찍 온 프레임은 버리지 않고 처리 시각까지 기다렸다가,
                # 그 사이 더 새 프레임이 왔으면 그것을 처리합니다(FramePacer).
                interval = 1.0 / max(1.0, system_runtime_state.get("target_fps", 10.0))
                wait = self.pacer.delay(time.monotonic())
                if wait > 0:
                    time.sleep(wait)
                    item = self.frame_buffer.get() or item
                self.pacer.processed(time.monotonic(), interval)
                fr, fid, connected, source_fr = item

                if not connected or fr is None or not self.cam.events:
                    self.result_buffer.put((fr, fid, connected, [], None, {}, [], None, source_fr))
                    continue

                try:
                    # 추론과 로직을 한 워커에서 순차 처리하여 스레드 통신 오버헤드 제거
                    d_main_res, d_topdown = run_camera_inference(self.cam, fr)
                    t_main, t_topdown, alarms, new_events = self.cam.run_logic(fr, fid, d_main_res, d_topdown)
                    infer_meta = self.cam.build_inference_log(fid, fr, d_main_res, d_topdown, t_main, t_topdown, alarms, new_events)
                    # 수신기의 프레임 메타(받은 시각, 과속 수신기는 PTS)를 덧붙입니다.
                    infer_meta["frame_timing"] = to_json_safe(self.cam.current_frame_meta)

                    self.result_buffer.put((fr, fid, connected, t_main, t_topdown, alarms, new_events, infer_meta, source_fr))
                except Exception as e:
                    logger.error(f"[Worker Error] CAM {self.cam.cam_id}: {e}\n{traceback.format_exc()}")
                    self.result_buffer.put((fr, fid, connected, [], None, {}, [], None, source_fr))

    camera_workers = []
    last_rendered_frames = {}

    for c in cams:
        worker = CameraWorker(c)
        worker.start()
        camera_workers.append(worker)

        blank = np.zeros((360, 640, 3), dtype=np.uint8)
        cv2.putText(blank, "WAITING...", (50, 180), cv2.FONT_HERSHEY_SIMPLEX, 1, (150, 150, 150), 2)
        last_rendered_frames[c.ip] = blank

    # 카메라마다 마지막 ROI 사진 시각(roi_snapshot.interval_sec마다 보냄. 처음은 켠 뒤 약 10초)
    roi_snapshot_interval = float(SYS_CFG["roi_snapshot"]["interval_sec"])
    last_roi_snapshot_times = {c.ip: time.time() - roi_snapshot_interval + 10.0 for c in cams}
    # 켠 뒤 아직 ROI 사진을 한 번도 안 보낸 카메라(첫 사진은 로그에 '시작 ROI 스냅샷'으로 적음).
    startup_roi_snapshot_pending = {c.ip for c in cams}
    last_worker_active_times = {c.ip: time.time() for c in cams}

    target_fps = sys_target_fps
    last_processed_fids = {}
    if is_gui_mode and any(SPEEDING_EVENT in c.events for c in cams):
        # 과속 카메라가 있을 때만 고해상도 표시용 창 크기 지정(그 외는 리모트와 같이 imshow 기본 창)
        cv2.namedWindow("Monitor", cv2.WINDOW_NORMAL)
        cv2.resizeWindow("Monitor", SCREEN_WIDTH, SCREEN_HEIGHT)

    try:
        psutil.cpu_percent(interval=None)

        while True:
            start_time = time.time()

            if output_cleanup_interval_sec > 0 and (start_time - last_output_cleanup_time) >= output_cleanup_interval_sec:
                run_output_retention_cleanup(output_retention_days)
                last_output_cleanup_time = start_time

            if loop_count > 0 and loop_count % 45 == 0 and os.path.exists(config_file):
                current_mtime = os.path.getmtime(config_file)
                if current_mtime > last_config_mtime:
                    try:
                        with open(config_file, 'r', encoding='utf-8') as f:
                            new_configs = json.load(f)
                        # 새로 추가된 이벤트(과속·콘 NG·Backreach·YT)가 쓰는 모델을 먼저 올립니다.
                        load_event_models({evt for conf in new_configs.values() if isinstance(conf, dict)
                                           for evt in conf.get('events', [])})
                        for c in cams:
                            if c.ip in new_configs:
                                c.update_config(new_configs[c.ip])
                                if c.det_main_v2 is None and "MAIN_V2" in event_model_keys(c.events):
                                    c.det_main_v2 = event_models.get("MAIN_V2")
                        last_config_mtime = current_mtime
                    except Exception as e:
                        logger.error(f"cameras.json 다시 읽기 실패: {e}")

            loop_count += 1

            if loop_count % fps_calc_interval == 0:
                cpu_usage = psutil.cpu_percent(interval=None)

                if dynamic_cpu_adjust:
                    if cpu_usage < 75:
                        target_fps = min(sys_target_fps, target_fps + 1.0)
                    elif cpu_usage > 90:
                        target_fps = max(8.0, target_fps - 1.0)
                else:
                    target_fps = sys_target_fps

                # 계산된 타겟 FPS를 상태 객체에 담아 워커 스레드들과 동기화
                system_runtime_state["target_fps"] = target_fps

            if loop_count % 300 == 0:
                gc.collect()
                log_disk_health([("event_root", EVENT_ROOT_DIR), ("log_dir", LOG_DIR)])

            # ---------------------------------------------------------
            # Stage 1 (Producer): 프레임 캡처 및 버퍼 공급
            # ---------------------------------------------------------
            for worker in camera_workers:
                fr, fid, connected, source_fr = worker.cam.process_frame(include_source=True)
                last_fid = last_processed_fids.get(worker.cam.ip, -1)
                if fid != last_fid:
                    worker.frame_buffer.put((fr, fid, connected, source_fr if is_gui_mode else None))
                    last_processed_fids[worker.cam.ip] = fid

            # ---------------------------------------------------------
            # Stage 2 (Consumer): 최신 결과 회수, 마스킹 스냅샷, 및 최적화 렌더링
            # ---------------------------------------------------------
            final_imgs = []
            gui_updated = False
            now_time = time.time()

            roi_snapshot_refresh_cctv_ids = health_daemon.get_roi_snapshot_refresh_cctv_ids()
            refreshed_cctv_ids = set()

            for idx, worker in enumerate(camera_workers):
                c = worker.cam
                res = worker.result_buffer.get()

                if res is None:
                    # 좀비 스레드(Deadlock) 감지 및 핫스왑 복구
                    if (now_time - last_worker_active_times.get(c.ip, now_time)) > WATCHDOG_TIMEOUT:
                        logger.error(f"🚨 [WATCHDOG] CAM:{c.cam_id}({c.ip}) 스레드 데드락(Zombie) 감지! 핫스왑(Hot-Swap) 복구를 진행합니다.")

                        worker.running = False
                        c.reader.running = False
                        c.recorder.running = False
                        worker.join(timeout=1.0)

                        conf = camera_configs.get(c.ip, c.conf)
                        new_cam = make_camera(c.ip, conf, c.cam_id)
                        new_worker = CameraWorker(new_cam)
                        new_worker.start()

                        camera_workers[idx] = new_worker
                        for i_cam, old_cam in enumerate(cams):
                            if old_cam.ip == c.ip:
                                cams[i_cam] = new_cam
                                break

                        health_daemon.cams = cams

                        last_worker_active_times[c.ip] = time.time()
                        last_processed_fids[c.ip] = -1
                        logger.info(f"✅ [WATCHDOG] CAM:{c.cam_id}({c.ip}) 핫스왑 복구 완료. 모니터링을 재개합니다.")

                    if is_gui_mode: final_imgs.append(last_rendered_frames[c.ip])
                    continue

                # 정상 수신 시 활성 시간 갱신
                last_worker_active_times[c.ip] = now_time
                fr, fid, connected, t_main, t_topdown, alarms, new_events, infer_meta, source_fr = res

                # 개별 카메라 1시간 타이머 검사
                cctv_id_text = str(c.cam_id)
                force_camera_snapshot = cctv_id_text in roi_snapshot_refresh_cctv_ids
                periodic_roi_snapshot_due = (now_time - last_roi_snapshot_times.get(c.ip, 0.0)) >= roi_snapshot_interval

                if connected and fr is not None and (periodic_roi_snapshot_due or force_camera_snapshot):
                    # 과속 카메라는 메인 모델 트랙, Backreach·YT 카메라는 topdown 트랙(사람·차량 포함)으로 얼굴·번호판을 가립니다.
                    snapshot_tracks = np.concatenate([np.asarray(t_main, dtype=float).reshape(-1, 7),
                                                      topdown_privacy_tracks(t_topdown)])
                    blurred_snap, _ = c.apply_privacy_blur(fr.copy(), snapshot_tracks, blur_face=True, blur_plate=True)
                    c._ensure_roi(blurred_snap)
                    snap_img = create_roi_snapshot(c, blurred_snap)
                    h, w = snap_img.shape[:2]
                    roi_info = {"roi_poly_norm": c.roi_poly_norm, "roi_lines_norm": c.roi_lines_norm}

                    # 켠 뒤 카메라마다 처음 보내는 사진은 1시간 정기 전송이 아니라 시작 사진입니다(로그 이름만 다름).
                    snapshot_send_type = ("roi_refresh" if force_camera_snapshot
                                          else "startup" if c.ip in startup_roi_snapshot_pending else "hourly")
                    submit_event_send(_send_roi_snapshot_task, c.cam_id, crane_id, snap_img,
                                      json.dumps(roi_info), w, h, snapshot_send_type)

                    # 큐에 넣은 즉시 이 카메라 타이머만 갱신
                    last_roi_snapshot_times[c.ip] = now_time
                    startup_roi_snapshot_pending.discard(c.ip)

                    if force_camera_snapshot: refreshed_cctv_ids.add(cctv_id_text)

                if connected and fr is not None and loop_count % 100 == 0:
                    try:
                        small_fr = cv2.resize(fr, (640, 360))
                        save_path = os.path.join(RAM_DISK_DIR, f"{c.ip}.jpg")
                        cv2.imwrite(save_path, small_fr, [cv2.IMWRITE_JPEG_QUALITY, 70])
                    except Exception: pass

                if not connected or fr is None or not c.events:
                    if is_gui_mode:
                        display_fr = c.draw(None, [], None, {}, False)
                        last_rendered_frames[c.ip] = display_fr
                        final_imgs.append(display_fr)
                        gui_updated = True
                    continue

                # 켜자마자는 화면 1~2장으로 계산한 값(수천 또는 0)이라, 카메라마다 첫 로그는 한 간격 뒤에 남깁니다.
                current_fps_last_print.setdefault(c.ip, now_time)
                if now_time - current_fps_last_print[c.ip] >= current_fps_log_interval_sec:
                    current_fps_last_print[c.ip] = now_time
                    # 카메라마다 AI FPS(초당 추론·이벤트 판정 횟수)를 CURRENT_FPS_LOG_INTERVAL_SEC(기본 60초)마다 남깁니다.
                    logger.info(format_ai_fps_log(c))

                # 녹화 영상은 GUI 화면과 같은 그림입니다(상자·이름, 알람 빨간 테두리, ROI·YT 구역, 상태 글자).
                # 관제 요청으로 녹화에는 AI FPS·객체 점수(conf)를 빼고 따로 그립니다(GUI 화면은 그대로).
                # GUI를 켜지 않고 돌려도 녹화할 화면(infer_meta 있음)은 같은 방법으로 그립니다.
                if is_gui_mode or infer_meta:
                    inference_shape = None
                    if SPEEDING_EVENT in c.events:
                        # 과속 카메라: 고해상도 원본에 그림(번호판·레이더 점 확인용)
                        display_source = source_fr if source_fr is not None else fr
                        inference_shape = fr.shape
                    elif CONE_NG_EVENT in c.events and source_fr is not None:
                        # 콘 카메라: 원본을 가로 1280으로 줄여 그림(콘이 추론 프레임 640폭에서는 너무 작음)
                        sh, sw = source_fr.shape[:2]
                        dw = min(1280, sw)
                        display_source = cv2.resize(source_fr, (dw, int(round(sh * dw / float(sw)))), interpolation=cv2.INTER_AREA)
                        inference_shape = fr.shape
                    else:
                        display_source = fr
                    if is_gui_mode:
                        display_fr = c.draw(display_source.copy(), t_main, t_topdown, alarms, True, inference_shape=inference_shape)
                    if infer_meta:
                        record_fr = c.draw(display_source.copy(), t_main, t_topdown, alarms, True,
                                           inference_shape=inference_shape, for_record=True)
                        c.recorder.update(record_frame_from_display(record_fr, (fr.shape[1], fr.shape[0])),
                                          infer_meta, timestamp=now_time)

                if is_gui_mode:
                    last_rendered_frames[c.ip] = display_fr
                    final_imgs.append(display_fr)
                    gui_updated = True

            if refreshed_cctv_ids:
                health_daemon.clear_roi_snapshot_refresh(cctv_ids=refreshed_cctv_ids, reason="roi_snapshot_sent")

            if is_gui_mode:
                if any(SPEEDING_EVENT in c.events for c in cams):
                    # 과속 카메라가 있으면 새 프레임이 있을 때만 갱신하고, 1대면 원본 크기 그대로(모자이크 축소 없이) 표시
                    if final_imgs and gui_updated:
                        display_image = final_imgs[0] if len(final_imgs) == 1 else create_mosaic_image(final_imgs)
                        cv2.imshow("Monitor", display_image)
                elif final_imgs: cv2.imshow("Monitor", create_mosaic_image(final_imgs))
                if cv2.waitKey(1) == ord('q'): break

            time.sleep(0.001)

    except KeyboardInterrupt:
        logger.info("[종료] 사용자에 의해 시스템이 중단되었습니다.")
    except Exception as e:
        logger.error(f"[치명적 오류] {e}\n{traceback.format_exc()}")
    finally:
        health_daemon.stop()

        for c in cams:
            c.reader.running = False
            c.recorder.running = False

        for w in camera_workers:
            w.running = False

        if not MseRadarHub.close_shared():
            logger.error('[Radar] MSE receiver shutdown did not finish')

        for model in [*event_models.values(), d_face, d_plate]:
            try:
                model.release()
            except Exception:
                pass

        if is_gui_mode:
            cv2.destroyAllWindows()

if __name__ == "__main__":
    main()
