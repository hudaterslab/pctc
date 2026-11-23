# -*- coding: utf-8 -*-
"""과속 로직 검증 도구: 저장된 재현 묶음(과속 이벤트 폴더의 clip.ts + radar.jsonl)을 multi_event_port.py에 그대로 다시 넣어
같은 판정이 나오는지 봅니다(오탐 재현 → 원인 확인 → 코드 수정 뒤 다시 돌려 확인).

쓰는 법(옵션 없음):
  1) 단말기의 과속 이벤트 폴더를 통째로 ./test_videos 에 넣습니다.
       events/<카메라>/fullres/speeding/<이벤트>  또는  events/<카메라>/ghost·excluded/<이벤트>
  2) python test_port.py
  3) ./test_results/<이벤트>/ 에 결과가 생깁니다.
       result.mp4(상자·레이더 점을 그린 화면), summary.txt(원래 결과와 다시 돌린 결과, 단계별 로그),
       CCTV_EVENT_ALERT/(단말기와 같은 모양의 로그·이벤트 폴더), api_posts.jsonl(관제로 보냈을 내용)
  모니터가 있으면 화면에 띄워 보면서 돌고(q를 누르면 다음 묶음으로), 없으면 화면 없이 돕니다.
  단말기에서 돌릴 때는 실행 중인 프로그램을 먼저 꺼야 합니다(NPU를 같이 쓸 수 없음).

multi_event_port.py는 고치지 않고 불러와 main()을 돌립니다. 바꿔 끼우는 것은 바깥과 닿는 곳뿐입니다.
  1) 카메라 → clip.ts(ffmpeg -re로 실제 속도 재생)
  2) 레이더 → radar.jsonl(영상 프레임과 저장 때의 시간 관계 그대로 넣음)
  3) 관제 서버 → 보내는 내용을 api_posts.jsonl에 적고 성공 응답만 돌려줌(실제로는 아무 데도 안 보냄). 스피커는 끔
  4) NPU가 없는 PC·서버에서는 dxnn 대신 원본 pt를 GPU로 계산(test_port_models.json 표가 있어야 함)
설정은 저장 때 값을 씁니다: 묶음에 든 설정(radar.jsonl 첫 줄의 system_config) → 없으면 묶음 폴더의 system_config.json
→ 그것도 없으면 이 폴더의 system_config.json. 판정 기준·ROI·보정값은 늘 묶음에 저장된 값입니다. 모델 파일은 이 폴더의 것을 씁니다.
"""
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ==========================================
# 환경 설정
# ==========================================
TEST_DIR = HERE / "test_videos"               # 재현 묶음(과속 이벤트 폴더)을 넣는 곳
TEST_RESULT_DIR = HERE / "test_results"       # 결과가 생기는 곳
CODE_FILE = HERE / "multi_event_port.py"      # 검증할 코드
MODELS_FILE = HERE / "test_port_models.json"  # NPU가 없을 때만: dxnn 이름 → pt 표
PT_DEVICE = "cuda:0"                          # NPU가 없을 때만: pt를 계산할 장치
WAIT_AFTER_VIDEO_SEC = 180.0                  # 영상이 끝난 뒤 번호판 읽기·전송 판단을 기다리는 최대 시간(초)
MPEGTS_PTS_WRAP_SEC = 2 ** 33 / 90000.0
LOG_KEYS = ("[RADAR SPEEDING]", "EVENT TRIGGERED", "[GHOST]", "[EXCLUDED]", "[PLR FOLLOW]", "[FULLRES] event_id",
            "[PLR] event_id", "[SPEEDING API]", "[SITE SOUND]", "[TEST_PORT]")


# ---------------------------------------------------------------------------
# NPU가 없는 곳: dx_engine 자리에 pt 계산을 끼웁니다(출력 모양은 dxnn의 raw YOLO와 같음).
# ---------------------------------------------------------------------------
def install_pt_engine(models, device):
    """models: {"speeding.dxnn": {"pt": "/…/speeding.pt", "h": 384, "w": 640}, "face.dxnn": {"kind": "empty", "h":…, "w":…}}"""
    import numpy as np
    loaded, load_lock = {}, threading.Lock()
    half = device.startswith("cuda")

    def load(pt):
        import torch
        from ultralytics import YOLO
        with load_lock:
            if pt not in loaded:
                model = YOLO(pt).model.float().eval().to(device)
                if half:
                    model = model.half()
                for prm in model.parameters():
                    prm.requires_grad_(False)
                loaded[pt] = (model, threading.Lock())
        return loaded[pt]

    class InferenceOption:
        use_ort = False

    class InferenceEngine:
        def __init__(self, path, option=None):
            name = os.path.basename(str(path))
            if name not in models:
                raise RuntimeError(f"--models 표에 {name}이 없습니다: {sorted(models)}")
            spec = models[name]
            self.name, self.height, self.width = name, int(spec["h"]), int(spec["w"])
            self.kind = spec.get("kind", "yolo")
            self.model, self.lock = load(spec["pt"]) if self.kind == "yolo" else (None, None)

        def get_input_tensors_info(self):
            return [{"shape": [1, self.height, self.width, 3]}]

        def run(self, inputs):
            if self.kind == "empty":
                return [np.zeros(0, dtype=np.uint8)]
            import torch
            x = np.asarray(inputs[0])
            x = x[None] if x.ndim == 3 else x
            with self.lock, torch.inference_mode():
                t = torch.from_numpy(np.ascontiguousarray(x)).to(device).permute(0, 3, 1, 2)
                t = (t.half() if half else t.float()).div_(255.0)
                y = self.model(t)
                y = y[0] if isinstance(y, (list, tuple)) else y
                return [y.float().cpu().numpy()]

        def dispose(self):
            pass

    fake = types.ModuleType("dx_engine")
    fake.InferenceEngine, fake.InferenceOption = InferenceEngine, InferenceOption
    sys.modules["dx_engine"] = fake


def has_npu():
    try:
        import dx_engine  # noqa: F401
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# 묶음 읽기와 실행 폴더 준비
# ---------------------------------------------------------------------------
def load_bundle(folder):
    top = folder = Path(folder).resolve()
    if not (folder / "clip.ts").exists() and (folder / "fullres" / "clip.ts").exists():
        folder = folder / "fullres"                      # ghost·excluded/<이벤트> 폴더를 준 경우
    clip, radar_file = folder / "clip.ts", folder / "radar.jsonl"
    if not clip.exists() or not radar_file.exists():
        sys.exit(f"[오류] 재현 묶음이 아닙니다(clip.ts·radar.jsonl이 있어야 함): {folder}")
    header, frames = None, []
    with open(radar_file, encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("type") == "header":
                header = rec
            elif rec.get("type") == "radar":
                frames.append(rec)
    if header is None:
        sys.exit(f"[오류] radar.jsonl 첫 줄(header)이 없습니다: {radar_file}")
    meta = json.loads((folder / "fullres_meta.json").read_text(encoding="utf-8")) if (folder / "fullres_meta.json").exists() else {}
    return {"top": top, "folder": folder, "clip": clip, "header": header, "frames": frames, "meta": meta}


def clip_shift(bundle):
    """ffmpeg는 재생할 때 입력의 첫 PTS를 0으로 당깁니다. 저장 때의 PTS = 재생 PTS + 이 값.
    clip.ts 안의 시각은 26.5시간마다 0으로 돌아가므로, 저장 때 구간(clip_covered)으로 몇 바퀴 돌았는지 맞춥니다."""
    out = subprocess.run(["ffprobe", "-v", "error", "-show_entries", "format=start_time", "-of", "csv=p=0",
                          str(bundle["clip"])], capture_output=True, text=True).stdout.strip()
    start = float(out)
    covered0 = float(bundle["header"]["clip_covered"][0])
    return start + round((covered0 - start) / MPEGTS_PTS_WRAP_SEC) * MPEGTS_PTS_WRAP_SEC


def find_bundles():
    """./test_videos 안의 재현 묶음 폴더들(이름 순). ghost·excluded에서 가져온 폴더는 그 안의 fullres를 봅니다."""
    found = []
    for d in sorted(TEST_DIR.iterdir()):
        if d.is_dir() and any((x / "clip.ts").exists() and (x / "radar.jsonl").exists() for x in (d, d / "fullres")):
            found.append(d)
    return found


def bundle_config(bundle):
    """설정과 그 출처: 묶음에 든 설정 → 묶음 폴더의 system_config.json → 이 폴더의 system_config.json."""
    if bundle["header"].get("system_config"):
        return json.loads(json.dumps(bundle["header"]["system_config"])), "묶음에 저장된 설정"
    for path, name in ((bundle["folder"] / "system_config.json", "묶음 폴더의 system_config.json"),
                       (bundle["top"] / "system_config.json", "묶음 폴더의 system_config.json"),
                       (HERE / "system_config.json", "이 폴더의 system_config.json(저장 때 설정과 다를 수 있음)")):
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8")), name
    sys.exit("[오류] system_config.json을 찾지 못했습니다.")


def build_run_dir(bundle, npu):
    header, meta = bundle["header"], bundle["meta"]
    event_id = str(meta.get("event_id") or bundle["top"].name)
    ip = str(meta.get("ip") or "cam")
    run = (TEST_RESULT_DIR / bundle["top"].name).resolve()
    if run.exists():
        shutil.rmtree(run)
    run.mkdir(parents=True)
    shutil.copyfile(CODE_FILE, run / "multi_event_port.py")
    for name in ("pose_plate.onnx", "plate_rec_flat_64x320.onnx", "plate_dict.txt"):
        if (HERE / name).exists():
            shutil.copyfile(HERE / name, run / name)
    cfg, cfg_from = bundle_config(bundle)
    # 번호판 모델 이름은 묶음에 저장된 것 대신 지금 코드의 기본값을 씁니다(모델 파일은 이 폴더의 것).
    for key in ("pose_model", "ocr_cpu_model"):
        (cfg.get("plr") or {}).pop(key, None)
    cid = str(header["client_id"])
    # 판정 기준·보정값은 저장 때 값 그대로(이 레이더에만 덮어쓴 설정은 이미 합쳐져 있어 지움)
    cfg.setdefault("event_config", {})["speeding"] = header["speeding"]
    cfg["radar_camera"] = dict(header["radar_camera"], calibration_collect=False)
    unit = (cfg.setdefault("radars", {}).get(cid) or {})
    unit.pop("speeding", None)
    unit.pop("radar_camera", None)
    cfg["radars"] = {cid: unit}
    cfg.setdefault("receiver", {}).update(enabled=True, base_url="http://test-port.invalid/receiver")
    cfg["receiver"].setdefault("cctv_ids", [1])
    fr_cfg = cfg.setdefault("fullres_capture", {})
    fr_cfg.update(ring_dir=f"/dev/shm/test_port_ring/{event_id}_{os.getpid()}", replay_pre_sec=0.0)
    models = {}
    if not npu:
        if not MODELS_FILE.exists():
            sys.exit(f"[오류] NPU(dx_engine)가 없습니다. pt로 돌리려면 {MODELS_FILE.name}(dxnn 이름 → pt 표)이 있어야 합니다.")
        models = json.loads(MODELS_FILE.read_text(encoding="utf-8"))
        for key in (cfg.get("models") or {}):
            cfg.setdefault("model_output_formats", {})[key] = "yolo"       # pt 출력은 raw YOLO
        cfg.setdefault("video_decode", {})["hw_acceleration"] = "none"
        fr_cfg["hwaccel"] = False
        for name in (cfg.get("models") or {}).values():                     # 모델 파일 자리(이름만 씀)
            if name in models:
                (run / name).touch()
    else:
        for name in (cfg.get("models") or {}).values():                     # 모델 파일은 이 폴더의 것
            if (HERE / name).exists() and not (run / name).exists():
                os.symlink(HERE / name, run / name)
    (run / "system_config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=1), encoding="utf-8")
    conf = dict(header["camera"])
    for drop in ("receiver_event_off_hours", "ip_speaker", "sound"):        # 재생 시각은 저장 때와 달라 시간대 규칙을 뺌
        conf.pop(drop, None)
    conf.update(speaker_enable=False, radar_client_id=int(cid), url=str(bundle["clip"]))
    (run / "cameras.json").write_text(json.dumps({ip: conf}, ensure_ascii=False, indent=1), encoding="utf-8")
    (run / "cameras.csv").write_text(str(bundle["clip"]) + "\n", encoding="utf-8")
    return run, ip, models, event_id, cfg_from


class FakeResponse:
    status_code = 200
    text = '{"success": true}'

    def json(self):
        return {"httpStatus": 200, "success": True, "message": "test_port"}


class FakeRequests:
    """requests 대신: post는 보낼 내용을 파일에 적고 성공 응답을 돌려줍니다. 나머지는 원래 requests 그대로."""

    def __init__(self, real, path):
        self._real, self._path, self._lock = real, path, threading.Lock()

    def __getattr__(self, name):
        return getattr(self._real, name)

    def post(self, url, data=None, files=None, **kw):
        record = {"t": time.time(), "url": str(url), "files": sorted((files or {}).keys()),
                  "data": {k: (v if isinstance(v, (int, float, str, bool, type(None))) else str(v))
                           for k, v in (data.items() if isinstance(data, dict) else [])}}
        with self._lock, open(self._path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return FakeResponse()


# ---------------------------------------------------------------------------
# 결과 정리
# ---------------------------------------------------------------------------
def original_result(bundle):
    folder, meta = bundle["folder"], bundle["meta"]
    kind = "ghost(레이더 가짜)" if "ghost" in folder.parts else "excluded(과속 대상 아님)" if "excluded" in folder.parts else "과속(번호판 읽기까지 감)"
    lines = [f"폴더: {folder}", f"종류: {kind}"]
    ev = meta.get("evidence") or {}
    for key in ("speed_kmh", "median_kmh", "radar_speed_kmh", "decided_by", "lane_hits", "vehicle_box", "excluded_class"):
        if ev.get(key) is not None:
            lines.append(f"{key}: {ev[key]}")
    plr = folder / "plr_meta.json"
    if plr.exists():
        try:
            p = json.loads(plr.read_text(encoding="utf-8"))
            final = p.get("final") or {}
            lines.append(f"번호판: {final.get('text') or '(없음)'} 상태: {final.get('status')} "
                         f"(찾은 번호판 {p.get('plates_found')}장, 본 장 {p.get('frames_used')}장)")
        except Exception as e:
            lines.append(f"plr_meta.json을 읽지 못함: {e}")
    return lines


def write_summary(run, bundle, started_line):
    logs = sorted((run / "CCTV_EVENT_ALERT" / "logs").glob("cctv_*.log"))
    picked = []
    for path in logs:
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            if any(k in line for k in LOG_KEYS):
                picked.append(line)
    posts = run / "api_posts.jsonl"
    sent = [json.loads(x) for x in posts.read_text(encoding="utf-8").splitlines()] if posts.exists() else []
    sent_img = [p for p in sent if p["url"].endswith("/img") and "roi" not in p["url"]]
    out = ["=== 원래 결과(단말기) ==="] + original_result(bundle)
    out += ["", "=== 다시 돌린 결과 ===", started_line,
            f"과속 판정 {sum('EVENT TRIGGERED' in x for x in picked)}건, 관제 사진 전송 {len(sent_img)}건"]
    for p in sent_img:
        out.append("  관제 전송 내용: " + json.dumps(p["data"], ensure_ascii=False)[:400])
    out += ["", "=== 단계별 로그 ==="] + picked
    (run / "summary.txt").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("\n".join(out[:out.index("=== 단계별 로그 ===")]))
    print(f"단계별 로그 {len(picked)}줄 → {run / 'summary.txt'}")


# ---------------------------------------------------------------------------
def run_bundle(folder):
    """묶음 하나를 다시 돌립니다(묶음마다 새 프로세스: multi_event_port.py의 main()을 처음부터 돌리기 때문)."""
    bundle = load_bundle(folder)
    npu = has_npu()
    run, ip, models, event_id, cfg_from = build_run_dir(bundle, npu)
    if not npu:
        install_pt_engine(models, PT_DEVICE)
    shift = clip_shift(bundle)
    header, frames = bundle["header"], bundle["frames"]
    gui = bool(os.environ.get("DISPLAY"))
    engine = "NPU(dxnn)" if npu else "pt " + PT_DEVICE
    started_line = (f"코드 {CODE_FILE} | 계산 {engine} | 설정 {cfg_from} | "
                    f"영상 {header['clip_covered'][1] - header['clip_covered'][0]:.1f}초 | 레이더 {len(frames)}프레임")

    os.environ.setdefault("NUMPY_MADVISE_HUGEPAGE", "0")
    os.chdir(run)
    sys.argv = [str(run / "multi_event_port.py")] + (["--gui"] if gui else [])
    spec = importlib.util.spec_from_file_location("multi_event_port", run / "multi_event_port.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["multi_event_port"] = mod
    spec.loader.exec_module(mod)
    log = mod.logger

    # --- 카메라: 묶음의 clip.ts 하나 ---
    url = str(bundle["clip"])
    orig_extract = mod.extract_ip
    mod.load_rtsp_list_from_csv = lambda path: [url]
    mod.extract_ip = lambda u: ip if u == url else orig_extract(u)
    mod.guarded_input = lambda prompt: "n"               # 시작 질문(디버그 출력·다시 설정)은 모두 아니오
    mod.requests = FakeRequests(mod.requests, run / "api_posts.jsonl")

    readers, eof = {}, threading.Event()
    orig_cmd = mod.SpeedFrameReader._build_ffmpeg_cmd
    orig_pipe = mod.SpeedFrameReader._run_ffmpeg_pipe

    def build_cmd(self, *a, **kw):
        cmd = orig_cmd(self, *a, **kw)
        i = cmd.index("-i")
        return cmd[:i] + ["-re"] + cmd[i:]               # 실제 속도로 재생

    def run_pipe(self):
        readers[self.ip] = self
        try:
            return orig_pipe(self)
        finally:
            self.running = False                         # 파일이 끝나면 다시 틀지 않음
            eof.set()
            log.info("[TEST_PORT] 영상 재생이 끝났습니다.")

    mod.SpeedFrameReader._build_ffmpeg_cmd = build_cmd
    mod.SpeedFrameReader._run_ffmpeg_pipe = run_pipe

    # --- 결과 영상: 녹화기에 들어가는 화면(상자·레이더 점을 그린 것)을 처음부터 끝까지 그대로 씀 ---
    import cv2
    writer = {"w": None}
    orig_update = mod.VideoRecorder.update

    def update(self, frame, infer_meta=None, timestamp=None):
        if frame is not None:
            if writer["w"] is None:
                h, w = frame.shape[:2]
                writer["w"] = cv2.VideoWriter(str(run / "result.mp4"), cv2.VideoWriter_fourcc(*"mp4v"),
                                              float(mod.SYS_CFG["system_performance"]["target_fps"]), (w, h))
            writer["w"].write(frame)
        return orig_update(self, frame, infer_meta, timestamp=timestamp)

    mod.VideoRecorder.update = update

    # --- 레이더: SDK 워커 대신 묶음의 기록을 넣음 ---
    cid = int(header["client_id"])
    mod.mse_client_ids = lambda path: [cid]
    mod.MseRadarHub.start = lambda self: not self._closed

    def feed():
        """저장 때: 카메라 시각 = PTS + pts_to_mono, 레이더 프레임 도착 = arrival(같은 시계).
        재생 때: 프레임 도착 = 재생 PTS + (수신기 기준 _pts_offset_min), 재생 PTS = 저장 PTS - shift.
        그래서 레이더 프레임은 (arrival - pts_to_mono - shift) + 수신기 기준 시각에 넣습니다."""
        hub = mod.MseRadarHub.shared()
        while ip not in readers or readers[ip]._pts_offset_min is None:
            if eof.is_set():
                return
            time.sleep(0.02)
        reader, fed, early = readers[ip], 0, 0
        for rec in frames:
            if eof.is_set():
                break
            base = reader._pts_offset_min
            if base is None:
                continue
            wait = (rec["arrival"] - header["pts_to_mono"] - shift) + base - time.monotonic()
            if wait < -0.3:
                early += 1                               # 영상 시작 전의 기록: 넣지 않음
                continue
            if wait > 0:
                time.sleep(wait)
            try:
                hub._accept({"kind": "objects", "client_id": cid, "timestamp_us": rec["timestamp_us"],
                             "objects": list(rec["objects"]) + list(rec.get("dropped") or [])})
                fed += 1
            except Exception as e:
                log.error(f"[TEST_PORT] 레이더 기록 넣기 실패: {e}")
        log.info(f"[TEST_PORT] 레이더 {cid} 기록 {fed}프레임 넣음(영상 시작 전 {early}프레임은 안 넣음)")

    threading.Thread(target=feed, name="test-port-radar", daemon=True).start()

    # --- 끝내기: 영상이 끝나고, 과속 판정마다 전송 판단([SPEEDING API])이 나올 때까지 기다림 ---
    done = threading.Lock()

    def finalize():
        """결과 영상을 닫고 요약을 쓴 뒤 끝냅니다(한 번만)."""
        if not done.acquire(blocking=False):
            return
        if writer["w"] is not None:
            writer["w"].release()
        import logging
        logging.shutdown()
        write_summary(run, bundle, started_line)
        shutil.rmtree(mod.SYS_CFG["fullres_capture"]["ring_dir"], ignore_errors=True)
        print(f"결과 폴더: {run}")
        sys.stdout.flush()
        os._exit(0)

    def finish():
        t0 = time.time()
        while not eof.is_set():
            time.sleep(0.5)
            if time.time() - t0 > 60 and not readers:
                log.error("[TEST_PORT] 60초 동안 영상이 시작되지 않아 끝냅니다.")
                break
        deadline = time.time() + WAIT_AFTER_VIDEO_SEC
        log_dir = run / "CCTV_EVENT_ALERT" / "logs"
        while time.time() < deadline:
            time.sleep(3.0)
            text = "".join(q.read_text(encoding="utf-8", errors="replace") for q in log_dir.glob("cctv_*.log"))
            ids = set(re.findall(r"EVENT TRIGGERED\] event_id=(\S+)", text))
            decided = {i for i in ids if re.search(r"\[SPEEDING API\][^\n]*" + re.escape(i), text)}
            following = "[PLR FOLLOW]" in text and len(re.findall(r"\[FULLRES\] 요청 event_id", text)) < len(ids)
            if ids == decided and not following:
                break
        time.sleep(2.0)
        finalize()

    threading.Thread(target=finish, name="test-port-finish", daemon=True).start()
    log.info(f"[TEST_PORT] 재현 시작 event_id={event_id} | {started_line}")
    try:
        mod.main()
    finally:
        finalize()                                       # 화면에서 q를 눌러 끝낸 경우


def main():
    if os.environ.get("TEST_PORT_BUNDLE"):
        return run_bundle(os.environ["TEST_PORT_BUNDLE"])
    if not TEST_DIR.exists():
        TEST_DIR.mkdir()
        print(f"[{TEST_DIR}] 폴더를 만들었습니다. 다시 돌려 볼 과속 이벤트 폴더(clip.ts·radar.jsonl)를 넣고 다시 실행하세요.")
        return
    bundles = find_bundles()
    if not bundles:
        print(f"[{TEST_DIR}] 폴더에 재현 묶음(clip.ts·radar.jsonl이 든 과속 이벤트 폴더)이 없습니다.")
        return
    TEST_RESULT_DIR.mkdir(exist_ok=True)
    print("=====================================")
    print(f"과속 로직 검증 시작: 묶음 {len(bundles)}개 | 코드 {CODE_FILE}")
    print(f"저장 경로: {TEST_RESULT_DIR}")
    print("=====================================")
    for i, folder in enumerate(bundles, 1):
        print(f"\n▶ [{i}/{len(bundles)}] {folder.name}")
        sys.stdout.flush()
        subprocess.run([sys.executable, str(Path(__file__).resolve())], env=dict(os.environ, TEST_PORT_BUNDLE=str(folder)))
    print("\n✅ 모든 묶음을 다시 돌렸습니다. 결과:", TEST_RESULT_DIR)


if __name__ == "__main__":
    main()
