#!/bin/bash
# 항만 AI CCTV(multi_event_port.py) 단말기 설치 스크립트.
#   bash install.sh                 : 전체 설치(NPU 드라이버·펌웨어, 패키지, 설정). 크레인 ID를 묻습니다.
#   bash install.sh --config-only   : 관제 설정만 다시 합니다(크레인 ID, cameras.csv → cctv 번호). 카메라를 바꾼 뒤에 씁니다.
#   CRANE_ID=1 bash install.sh      : 크레인 ID를 묻지 않고 이 값으로 넣습니다.
# cctv 번호는 묻지 않습니다. 프로그램이 cameras.csv 줄 순서대로 1, 2, 3...을 붙이므로, 그 번호를 모두 관제 전송 대상으로 넣습니다.
# 모델은 download.sh로 허깅페이스(HudatersU/port)에서 받습니다(8단계에서 실행, 같은 파일은 건너뜀).
# 설치가 끝나면 AI 프로그램이 부팅 때마다 백그라운드(화면 없이)로 자동 실행됩니다(port_cctv_ai.service, 10단계).
#   상태: systemctl status port_cctv_ai     출력 보기: journalctl -u port_cctv_ai -f
#   화면(--gui)으로 직접 실행하려면 먼저 멈추세요: sudo systemctl stop port_cctv_ai (같이 돌면 NPU 메모리가 넘칩니다)

# 1. 사용자 및 경로 설정
ACTUAL_USER=${SUDO_USER:-$USER}
USER_HOME=$(getent passwd "$ACTUAL_USER" | cut -d: -f6)

# [수정] 스크립트가 실제 위치한 절대 경로를 동적으로 가져옵니다. (어느 위치에서 실행해도 무방함)
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AI_SERVICE_NAME="port_cctv_ai.service"
AI_SERVICE_PATH="/etc/systemd/system/$AI_SERVICE_NAME"
OLD_AI_SERVICE_NAME="cctv_ai.service"   # 예전 HAI 설치의 자동 실행 서비스(~/hai/multi_event.py), 10단계에서 지움
NIC_STABILITY_SERVICE_NAME="hai-nic-stability.service"
NIC_STABILITY_SERVICE_PATH="/etc/systemd/system/$NIC_STABILITY_SERVICE_NAME"
NIC_STABILITY_SCRIPT_PATH="/usr/local/sbin/hai-nic-stability.sh"
POWER_LIMIT_SERVICE_NAME="hai-power-limit.service"
POWER_LIMIT_SERVICE_PATH="/etc/systemd/system/$POWER_LIMIT_SERVICE_NAME"
POWER_LIMIT_SCRIPT_PATH="/usr/local/sbin/hai-power-limit.sh"
CAM_CONFIG_FILE="$PROJECT_DIR/cameras.json"
CAM_CSV_FILE="$PROJECT_DIR/cameras.csv"
SYS_CONFIG_FILE="$PROJECT_DIR/system_config.json"
TARGET_SCRIPT="$PROJECT_DIR/multi_event_port.py"
DX_DIR="$USER_HOME/dx-runtime"
NUMPY_VERSION="1.26.4"
DX_FW_TAG="v2.5.6"
DX_FW_VERSION="2.5.6"
DX_FW_DIR="$DX_DIR/dx_fw"
NETWORK_TUNING_FILE="/etc/sysctl.d/99-network-tuning.conf"

# sudo로 실행했으면 원래 사용자로, 아니면 그대로 실행합니다(설정 파일 소유자를 사용자로 유지).
as_user() {
    if [ "$(id -u)" -eq 0 ] && [ "$ACTUAL_USER" != "root" ]; then
        sudo -u "$ACTUAL_USER" "$@"
    else
        "$@"
    fi
}

# system_config.json이 없으면 통합본을 import해 기본값으로 만듭니다(load_system_config(), 카메라 처리는 시작하지 않음).
ensure_system_config() {
    if [ ! -f "$SYS_CONFIG_FILE" ]; then
        echo "-> multi_event_port.py 기본값으로 system_config.json을 만듭니다..."
        (cd "$PROJECT_DIR" && as_user python3 -c 'import multi_event_port' > /dev/null 2>&1) || true
    fi
    if [ ! -f "$SYS_CONFIG_FILE" ]; then
        echo "⚠️ system_config.json을 만들지 못했습니다. multi_event_port.py를 한 번 직접 실행해 주세요."
        return 1
    fi
    echo "-> system_config.json: $SYS_CONFIG_FILE"
}

# 관제 전송 설정: 크레인 ID(crane_id)를 묻고, cctv 번호(receiver.cctv_ids)는 cameras.csv에서 자동으로 채웁니다.
# 둘 다 있으면 관제 전송(receiver.enabled)을 켜고, 하나라도 없으면 끕니다. 바뀌면 원래 파일을 .bak.날짜로 남깁니다.
configure_receiver() {
    [ -f "$SYS_CONFIG_FILE" ] || return 1
    local current_crane crane
    current_crane=$(python3 -c 'import json, sys; print(str(json.load(open(sys.argv[1], encoding="utf-8")).get("crane_id") or ""))' "$SYS_CONFIG_FILE" 2>/dev/null)
    crane="${CRANE_ID:-}"
    if [ -z "$crane" ] && [ -t 0 ]; then
        read -r -p ">> 이 단말기의 크레인 ID(crane_id)를 입력하세요 (예: 1) [현재: ${current_crane:-없음}, 엔터=그대로]: " crane
    fi
    crane="${crane:-$current_crane}"
    as_user python3 - "$SYS_CONFIG_FILE" "$CAM_CSV_FILE" "$crane" << 'PY'
import json
import os
import re
import shutil
import sys
import time

cfg_path, csv_path, crane = sys.argv[1], sys.argv[2], sys.argv[3].strip()
with open(cfg_path, encoding="utf-8") as f:
    cfg = json.load(f)
before = json.dumps(cfg, sort_keys=True, ensure_ascii=False)

# cameras.csv 읽기: multi_event_port.py의 load_rtsp_list_from_csv와 같은 규칙입니다
# (빈 줄·# 줄·제목 줄 제외, 첫 칸, 공백·비ASCII 제거, 중복 제거). 프로그램은 이 순서로 cctv 번호 1, 2, 3...을 붙입니다.
urls = []
if os.path.exists(csv_path):
    with open(csv_path, encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            first = line.split(",")[0].strip()
            if first.lower() in ("url", "rtsp", "rtsp_url", "camera_url"):
                continue
            url = re.sub(r"\s+", "", first.encode("ascii", "ignore").decode("ascii").strip())
            if url and url not in urls:
                urls.append(url)
else:
    print(f"⚠️ cameras.csv가 없어 cctv 번호를 채우지 못했습니다: {csv_path}")
    print("   cameras.csv를 만든 뒤 'bash install.sh --config-only'를 실행하세요.")

receiver = cfg.get("receiver")
if not isinstance(receiver, dict):
    receiver = cfg["receiver"] = {}
cfg["crane_id"] = crane
if os.path.exists(csv_path):
    receiver["cctv_ids"] = list(range(1, len(urls) + 1))
cctv_ids = receiver.get("cctv_ids") or []
receiver["enabled"] = bool(crane) and bool(cctv_ids)

if json.dumps(cfg, sort_keys=True, ensure_ascii=False) != before:
    backup = f"{cfg_path}.bak.{time.strftime('%Y%m%d_%H%M%S')}"
    shutil.copy2(cfg_path, backup)
    tmp = cfg_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=4, ensure_ascii=False)
    os.replace(tmp, cfg_path)
    print(f"-> system_config.json 관제 설정을 저장했습니다 (이전 파일: {os.path.basename(backup)})")
else:
    print("-> system_config.json 관제 설정이 이미 같습니다.")

def masked(url):
    return re.sub(r"//([^:/@]+):[^@/]*@", r"//\1:***@", url)

print(f"   crane_id: {crane or '(비어 있음)'}")
for number, url in enumerate(urls, 1):
    print(f"   cctv {number}: {masked(url)}")
print(f"   receiver.cctv_ids: {cctv_ids}")
if receiver["enabled"]:
    print("   관제 전송: 켜짐")
else:
    print("⚠️ 관제 전송: 꺼짐 (크레인 ID와 cameras.csv 카메라가 모두 있어야 켜집니다)")
PY
}

if [ "${1:-}" = "--config-only" ]; then
    echo "-----------------------------------------------------"
    echo " 관제 설정만 다시 합니다 (크레인 ID, cameras.csv → cctv 번호)"
    echo "-----------------------------------------------------"
    ensure_system_config || exit 1
    configure_receiver
    exit $?
fi

sudo apt install -y ssh ethtool

echo "-----------------------------------------------------"
echo " Network kernel tuning and PCIe ASPM configuration"
echo "-----------------------------------------------------"
sudo bash -c "cat > $NETWORK_TUNING_FILE" << 'EOL'
# Network interface backlog queue and socket buffer limits.
net.core.netdev_max_backlog = 65536
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.ipv4.tcp_rmem = 4096 87380 16777216
net.ipv4.tcp_wmem = 4096 65536 16777216
EOL

if sudo sysctl -p "$NETWORK_TUNING_FILE" > /dev/null 2>&1; then
    echo "-> Network kernel tuning applied."
else
    echo "[경고] Network kernel tuning could not be applied."
fi

if [ -f /etc/default/grub ] && command -v update-grub > /dev/null 2>&1; then
    if grep -q '^GRUB_CMDLINE_LINUX_DEFAULT=' /etc/default/grub; then
        if grep -q 'pcie_aspm=off' /etc/default/grub; then
            echo "-> pcie_aspm=off is already configured."
        else
            sudo sed -i 's/^\(GRUB_CMDLINE_LINUX_DEFAULT="[^"]*\)"/\1 pcie_aspm=off"/' /etc/default/grub
            sudo update-grub > /dev/null 2>&1
            echo "-> pcie_aspm=off will be applied after the next reboot."
        fi
    else
        echo "[경고] GRUB_CMDLINE_LINUX_DEFAULT was not found; ASPM setting skipped."
    fi
else
    echo "-> GRUB or update-grub is unavailable; ASPM setting skipped."
fi

echo "====================================================="
echo " Raspberry Pi Edge AI CCTV 서비스 하이브리드 설치"
echo "====================================================="

if [ ! -f "$TARGET_SCRIPT" ]; then
    echo "❌ [설치 중단] 현재 폴더에 'multi_event_port.py' 파일이 존재하지 않습니다."
    exit 1
fi

# 2. system_config.json은 multi_event_port.py가 직접 만듭니다(처음 실행할 때 기본값으로 생성하고,
#    이후 실행 때마다 빠진 기본값을 채워 저장). 예전 multi_event.py용 템플릿(침입·안전모 등)은 통합본과 맞지 않아
#    쓰지 않습니다. 파일이 없으면 아래 7-1단계에서 통합본 기본값으로 만듭니다.

echo "-----------------------------------------------------"
echo " 3. Python 환경 및 필수 패키지 설치"
echo "-----------------------------------------------------"
# python 명령어를 python3로 연결 (이미 존재해도 오류 안 나도록 -f 옵션 추가)
echo "-> 'python' 심볼릭 링크를 생성합니다..."
sudo ln -sf /usr/bin/python3 /usr/bin/python

# 필수 파이썬 패키지 설치 (최신 우분투/데비안의 PEP-668 제약 우회를 위해 break-system-packages 옵션 추가 대비)
echo "-> 필수 파이썬 패키지를 설치합니다 (numpy==$NUMPY_VERSION, pytz, psutil, requests, opencv-python)..."
python -m pip install --upgrade "numpy==$NUMPY_VERSION" pytz psutil requests opencv-python || \
python -m pip install --upgrade "numpy==$NUMPY_VERSION" pytz psutil requests opencv-python --break-system-packages

echo "-> FFmpeg VAAPI 하드웨어 디코딩 패키지를 설치합니다..."
# Ubuntu 기본 intel-media-va-driver(무료 빌드)에는 GPU 영상 처리(크기 조절)용 커널이 빠져 있어
# scale_vaapi·vaapipostproc가 실패하고 카메라 영상이 CPU 디코딩으로 넘어갑니다(2026-10-20, i3-1215U 단말기 확인).
# 커널이 들어 있는 non-free 빌드를 설치합니다. 비용은 없고(MIT 라이선스), multiverse 저장소에 있습니다.
# 설치하면 무료 빌드(intel-media-va-driver)는 자동으로 제거됩니다.
if ! apt-cache show intel-media-va-driver-non-free > /dev/null 2>&1; then
    echo "-> multiverse 저장소를 켭니다 (intel-media-va-driver-non-free)..."
    sudo add-apt-repository -y multiverse
fi
sudo apt install -y ffmpeg intel-media-va-driver-non-free vainfo

echo "-> GStreamer RTSP/VAAPI decode plugins install..."
sudo apt install -y \
    gstreamer1.0-tools \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-vaapi \
    libgstreamer1.0-0 \
    libgstreamer-plugins-base1.0-0

if command -v gst-inspect-1.0 >/dev/null 2>&1; then
    gst-inspect-1.0 rtspsrc >/dev/null 2>&1 || echo "[GStreamer] rtspsrc plugin missing"
    gst-inspect-1.0 rtph264depay >/dev/null 2>&1 || echo "[GStreamer] rtph264depay plugin missing"
    gst-inspect-1.0 avdec_h264 >/dev/null 2>&1 || echo "[GStreamer] avdec_h264 plugin missing"
    gst-inspect-1.0 vaapih264dec >/dev/null 2>&1 || gst-inspect-1.0 vah264dec >/dev/null 2>&1 || \
        echo "[GStreamer] VAAPI H264 decoder missing; CPU decoder fallback will be used"
fi

# GPU 크기 조절(VAAPI VideoProc) 확인: 실패하면 카메라 영상이 CPU 디코딩으로 동작합니다.
if command -v ffmpeg > /dev/null 2>&1 && [ -e /dev/dri/renderD128 ]; then
    if LIBVA_DRIVER_NAME=iHD ffmpeg -hide_banner -nostdin -loglevel error \
        -init_hw_device vaapi=va:/dev/dri/renderD128 -filter_hw_device va \
        -f lavfi -i testsrc=size=640x360:rate=30 \
        -vf "format=nv12,hwupload,scale_vaapi=w=320:h=180,hwdownload,format=nv12" \
        -frames:v 5 -f null - > /dev/null 2>&1; then
        echo "-> [VAAPI] GPU 크기 조절(scale_vaapi) 확인: OK"
    else
        echo "[경고] VAAPI GPU 크기 조절(scale_vaapi)이 실패했습니다. 카메라 영상이 CPU 디코딩으로 동작합니다."
        echo "       intel-media-va-driver-non-free 설치 상태를 확인하세요 (dpkg -l | grep intel-media)."
    fi
fi
sudo usermod -aG render,video "$ACTUAL_USER" 2>/dev/null || true

echo "-----------------------------------------------------"
echo " 4. DX runtime precheck"
echo "-----------------------------------------------------"

DX_ENGINE_READY=false
DXRT_CLI_READY=false
DXRT_SERVICE_ACTIVE=false

if python3 -c 'import dx_engine' > /dev/null 2>&1; then
    DX_ENGINE_READY=true
    echo "-> dx_engine import: OK"
else
    echo "-> dx_engine import: missing"
fi

if command -v dxrt-cli > /dev/null 2>&1; then
    DXRT_CLI_READY=true
    echo "-> dxrt-cli: OK ($(command -v dxrt-cli))"
else
    echo "-> dxrt-cli: missing"
fi

if systemctl is-active --quiet dxrt.service 2>/dev/null; then
    DXRT_SERVICE_ACTIVE=true
    echo "-> dxrt.service: active"
else
    echo "-> dxrt.service: inactive or not installed"
fi

echo "-----------------------------------------------------"
echo " 5. DeepX 드라이버 및 런타임 설치/복구"
echo "-----------------------------------------------------"
if [ "$DX_ENGINE_READY" != "true" ]; then
    echo "-> 시스템에서 'dx_engine'을 찾을 수 없어 공식 패키지를 다운로드합니다..."
    DOWNLOAD_DIR="$USER_HOME/Downloads"
    sudo -u "$ACTUAL_USER" mkdir -p "$DOWNLOAD_DIR"

    sudo -u "$ACTUAL_USER" wget -q --show-progress -P "$DOWNLOAD_DIR" https://github.com/DEEPX-AI/dx_rt_npu_linux_driver/raw/refs/heads/main/release/2.4.0/dxrt-driver-dkms_2.4.0-2_all.deb
    sudo -u "$ACTUAL_USER" wget -q --show-progress -P "$DOWNLOAD_DIR" https://github.com/DEEPX-AI/dx_rt/raw/refs/heads/main/release/3.3.2/libdxrt_3.3.2_all.deb

    sudo apt install -y "$DOWNLOAD_DIR/dxrt-driver-dkms_2.4.0-2_all.deb"
    sudo apt install -y "$DOWNLOAD_DIR/libdxrt_3.3.2_all.deb"

    # 설치 직후 서비스 강제 재시작 (장치 인식 유도)
    sudo systemctl restart dxrt.service 2>/dev/null || true
else
    echo "✅ [확인] dx_engine (런타임)이 이미 글로벌 환경에 설치되어 있습니다."
fi

echo "-----------------------------------------------------"
echo " 6. dx-runtime 설치 보강 및 펌웨어 플래싱"
echo "-----------------------------------------------------"
if [ ! -d "$DX_DIR" ]; then
    echo "-> 펌웨어 및 라이브러리 파일을 가져오기 위해 저장소를 클론합니다 (서브모듈 포함)..."
    # [수정] 서브모듈(fw.bin 등)을 함께 다운로드하기 위해 --recurse-submodules 옵션 추가
    sudo -u "$ACTUAL_USER" git clone --recurse-submodules https://github.com/DEEPX-AI/dx-runtime.git "$DX_DIR"
else
    echo "-> 기존 dx-runtime 저장소를 사용합니다: $DX_DIR"
    sudo -u "$ACTUAL_USER" git -C "$DX_DIR" submodule update --init --recursive || true
fi

if [ "$DX_ENGINE_READY" != "true" ] || [ "$DXRT_CLI_READY" != "true" ]; then
    echo "-> dx-runtime 내장 설치 스크립트(install.sh)를 실행하여 누락 항목을 보강합니다..."
    (cd "$DX_DIR" && sudo bash install.sh)
else
    echo "-> dx_engine과 dxrt-cli가 이미 확인되어 dx-runtime install.sh 실행을 건너뜁니다."
fi

echo "-> numpy 버전을 $NUMPY_VERSION 로 고정하고 검증합니다..."
python -m pip install --upgrade "numpy==$NUMPY_VERSION" || \
python -m pip install --upgrade "numpy==$NUMPY_VERSION" --break-system-packages

if ! python - "$NUMPY_VERSION" << 'PY'
import sys
import numpy

expected_version = sys.argv[1]
if numpy.__version__ != expected_version:
    print(f"[numpy] expected {expected_version}, found {numpy.__version__}")
    sys.exit(1)

print(f"[numpy] version verified: {numpy.__version__}")
PY
then
    echo "[오류] numpy 버전 고정 또는 검증에 실패했습니다."
    exit 1
fi

if [ ! -d "$DX_FW_DIR" ]; then
    echo "[오류] dx_fw 서브모듈 경로를 찾을 수 없습니다: $DX_FW_DIR"
    exit 1
fi

echo "-> dx_fw 펌웨어를 $DX_FW_TAG 태그로 고정합니다..."
if ! sudo -u "$ACTUAL_USER" git -C "$DX_FW_DIR" fetch --tags --force; then
    echo "[오류] dx_fw 태그 정보를 가져오지 못했습니다."
    exit 1
fi

if ! sudo -u "$ACTUAL_USER" git -C "$DX_FW_DIR" checkout --detach "$DX_FW_TAG"; then
    echo "[오류] dx_fw $DX_FW_TAG 고정에 실패했습니다."
    exit 1
fi

if [ ! -f "$DX_FW_DIR/m1/$DX_FW_VERSION/mdot2/fw.bin" ] || \
   [ ! -f "$DX_FW_DIR/m1m/$DX_FW_VERSION/mdot2/fw.bin" ]; then
    echo "[오류] dx_fw $DX_FW_VERSION 펌웨어 파일을 찾을 수 없습니다."
    exit 1
fi

if command -v dxrt-cli &> /dev/null; then
    echo "-> M.2 / PCIe 기반 펌웨어(FW) $DX_FW_VERSION 업데이트를 시도합니다..."
    dxrt-cli -u "$DX_FW_DIR/m1/$DX_FW_VERSION/mdot2/fw.bin" || echo "⚠️ [안내] 펌웨어 업데이트 건너뜀 (동일 버전이거나 재부팅 필요)"
    dxrt-cli -u "$DX_FW_DIR/m1m/$DX_FW_VERSION/mdot2/fw.bin" > /dev/null 2>&1 || true
else
    echo "⚠️ 'dxrt-cli' 명령어를 찾을 수 없어 펌웨어 업데이트를 건너뜁니다."
fi

echo "-----------------------------------------------------"
echo " 7. dx_engine 최종 검증"
echo "-----------------------------------------------------"
if ! python3 -c 'import dx_engine' > /dev/null 2>&1; then
    echo "❌ [에러] 드라이버 설치 후에도 모듈을 로드할 수 없습니다."
    echo "   >> NPU 장치가 커널에 등록되지 않았습니다."
    echo "   >> 지금 즉시 'sudo reboot' 명령어로 PC를 재부팅한 뒤, 이 스크립트를 다시 실행해 주세요!"
    exit 1
else
    echo "✅ [검증 성공] 로컬(Global) 파이썬 환경에서 'dx_engine'이 완벽하게 로드됩니다."
fi

echo "-----------------------------------------------------"
echo " 7-1. system_config.json 준비 · 관제 설정(크레인 ID, cctv 번호)"
echo "-----------------------------------------------------"
if ensure_system_config; then
    configure_receiver
fi

echo "-----------------------------------------------------"
echo " 8. 모델 받기·확인"
echo "-----------------------------------------------------"
# 허깅페이스(HudatersU/port)에서 모델을 받습니다(download.sh). 같은 파일(sha256)은 건너뛰고, 내용이 다르면 허깅페이스
# 것으로 바꿉니다. 받은 뒤 system_config.json의 models가 이 폴더에 다 있는지 봅니다.
if [ -f "$PROJECT_DIR/download.sh" ]; then
    as_user bash "$PROJECT_DIR/download.sh" || echo "⚠️ 모델 받기가 끝나지 않았습니다. 인터넷 연결을 확인하고 $PROJECT_DIR 에서 bash download.sh 를 다시 실행하세요."
else
    echo "⚠️ download.sh가 없어 모델을 받지 않았습니다: $PROJECT_DIR/download.sh"
fi
if ! python3 - "$SYS_CONFIG_FILE" "$PROJECT_DIR" << 'PY'
import json
import os
import sys

config_path, project_dir = sys.argv[1], sys.argv[2]
try:
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
except Exception as exc:
    print(f"[model-check] system_config.json read failed: {exc}")
    sys.exit(1)

missing = []
for key, value in sorted((cfg.get("models") or {}).items()):
    if not value:
        continue
    path = value if os.path.isabs(value) else os.path.join(project_dir, value)
    if not os.path.exists(path):
        missing.append(f"{key}={value}")

if missing:
    print("[model-check] missing model files: " + ", ".join(missing))
    sys.exit(1)

print("[model-check] all configured model files exist")
PY
then
    echo "⚠️ 모델 파일이 부족합니다. $PROJECT_DIR 에서 bash download.sh 로 받으세요."
fi

echo "-----------------------------------------------------"
echo " 9. 랜카드 안정화 서비스 등록 (NIC stability)"
echo "-----------------------------------------------------"
sudo bash -c "cat > $NIC_STABILITY_SCRIPT_PATH" << 'EOL'
#!/bin/bash
set -u

ETHTOOL_BIN="$(command -v ethtool || true)"

if [ -z "$ETHTOOL_BIN" ]; then
    echo "[hai-nic-stability] ethtool is not installed"
    exit 0
fi

for DEVICE_PATH in /sys/class/net/*; do
    IFACE="$(basename "$DEVICE_PATH")"

    case "$IFACE" in
        lo|docker*|br-*|veth*|virbr*|tailscale*|tun*|tap*)
            continue
            ;;
    esac

    # Only handle physical Ethernet interfaces. This excludes Wi-Fi and virtual adapters.
    if [ ! -e "$DEVICE_PATH/device" ] || [ -e "$DEVICE_PATH/wireless" ]; then
        continue
    fi

    if [ "$(cat "$DEVICE_PATH/type" 2>/dev/null || true)" != "1" ]; then
        continue
    fi

    "$ETHTOOL_BIN" -G "$IFACE" rx 4096 tx 4096 \
        && echo "[hai-nic-stability] $IFACE ring: RX/TX 4096" \
        || echo "[hai-nic-stability] $IFACE ring setting skipped or unsupported"

    "$ETHTOOL_BIN" -K "$IFACE" tso off gso off gro off \
        && echo "[hai-nic-stability] $IFACE TSO/GSO/GRO: disabled" \
        || echo "[hai-nic-stability] $IFACE offload setting skipped or unsupported"

    "$ETHTOOL_BIN" --set-eee "$IFACE" eee off \
        && echo "[hai-nic-stability] $IFACE EEE: disabled" \
        || echo "[hai-nic-stability] $IFACE EEE setting skipped or unsupported"
done
EOL

sudo chmod 0755 "$NIC_STABILITY_SCRIPT_PATH"

sudo bash -c "cat > $NIC_STABILITY_SERVICE_PATH" << EOL
[Unit]
Description=HAI Ethernet NIC stability settings
After=NetworkManager.service

[Service]
Type=oneshot
ExecStart=$NIC_STABILITY_SCRIPT_PATH

[Install]
WantedBy=multi-user.target
EOL

sudo systemctl daemon-reload
sudo systemctl enable "$NIC_STABILITY_SERVICE_NAME"
sudo systemctl start "$NIC_STABILITY_SERVICE_NAME"

echo "-----------------------------------------------------"
echo " 9-1. CPU 전력 제한 해제 서비스 등록 (power limit)"
echo "-----------------------------------------------------"
# 일부 미니PC(PL64 등)는 RAPL MMIO 쪽 단기 전력 한도(PL2)가 0W로 들어가 있어, 부하가 걸려도 CPU가 최저 400MHz에 묶입니다.
# MMIO PL2를 MSR 쪽 PL2와 같은 값(없으면 39W)으로 올리고, 부팅 때마다 다시 적용합니다(재부팅하면 원래 값으로 돌아가기 때문).
# 이미 그 값 이상인 단말기(예: .54 i3-1215U, PL2 39W)는 바뀌는 것이 없습니다.
sudo bash -c "cat > $POWER_LIMIT_SCRIPT_PATH" << 'EOL'
#!/bin/bash
set -u

MMIO=/sys/class/powercap/intel-rapl-mmio:0
MSR=/sys/class/powercap/intel-rapl:0
DEFAULT_PL2_UW=39000000

if [ ! -d "$MMIO" ]; then
    echo "[hai-power-limit] RAPL MMIO not found; nothing to do"
    exit 0
fi

TARGET="$(cat "$MSR/constraint_1_power_limit_uw" 2>/dev/null || echo 0)"
[ "${TARGET:-0}" -gt 0 ] || TARGET=$DEFAULT_PL2_UW
CUR="$(cat "$MMIO/constraint_1_power_limit_uw" 2>/dev/null || echo 0)"

if [ "${CUR:-0}" -lt "$TARGET" ]; then
    echo "$TARGET" > "$MMIO/constraint_1_power_limit_uw" \
        && echo "[hai-power-limit] MMIO PL2: $((CUR / 1000000))W -> $((TARGET / 1000000))W" \
        || echo "[hai-power-limit] MMIO PL2 setting failed"
else
    echo "[hai-power-limit] MMIO PL2 already $((CUR / 1000000))W; unchanged"
fi
EOL

sudo chmod 0755 "$POWER_LIMIT_SCRIPT_PATH"

sudo bash -c "cat > $POWER_LIMIT_SERVICE_PATH" << EOL
[Unit]
Description=HAI CPU power limit unlock (RAPL MMIO PL2)

[Service]
Type=oneshot
ExecStart=$POWER_LIMIT_SCRIPT_PATH

[Install]
WantedBy=multi-user.target
EOL

sudo systemctl daemon-reload
sudo systemctl enable "$POWER_LIMIT_SERVICE_NAME"
sudo systemctl start "$POWER_LIMIT_SERVICE_NAME"
if [ -r /sys/class/powercap/intel-rapl-mmio:0/constraint_1_power_limit_uw ]; then
    echo "-> CPU 단기 전력 한도(MMIO PL2): $(( $(cat /sys/class/powercap/intel-rapl-mmio:0/constraint_1_power_limit_uw) / 1000000 ))W"
fi

echo "-----------------------------------------------------"
echo " 10. AI 프로그램 자동 실행 서비스 ($AI_SERVICE_NAME)"
echo "-----------------------------------------------------"
# 부팅 때마다 multi_event_port.py를 백그라운드(화면 없이, --gui 없음)로 실행하고, 꺼지면 15초 뒤 다시 켭니다.
# 시작할 때 묻는 질문(CLI 디버그 출력, 설정 다시 하기)에는 엔터(기본값 = 아니오)를 넣습니다. 카메라 설정 마법사는 화면이
# 필요하므로, cameras.json이 없으면 서비스가 시작하지 않습니다(먼저 --gui로 직접 실행해 마법사로 cameras.json을 만드세요).
# 프로그램 로그는 지금처럼 CCTV_EVENT_ALERT/logs에 남고, 화면 출력은 journalctl -u port_cctv_ai 로 봅니다.
# 예전 HAI 설치의 cctv_ai.service(~/hai/multi_event.py, 더 이상 안 씀)는 끄고 지웁니다(같이 돌면 NPU 메모리가 넘칩니다).
if [ -f "/etc/systemd/system/$OLD_AI_SERVICE_NAME" ]; then
    sudo systemctl disable --now "$OLD_AI_SERVICE_NAME" 2>/dev/null || true
    sudo rm -f "/etc/systemd/system/$OLD_AI_SERVICE_NAME"
    sudo systemctl daemon-reload
    sudo systemctl reset-failed "$OLD_AI_SERVICE_NAME" 2>/dev/null || true
    echo "-> 예전 자동 실행 서비스($OLD_AI_SERVICE_NAME)를 지웠습니다."
fi

sudo bash -c "cat > $AI_SERVICE_PATH" << EOL
[Unit]
Description=Port AI CCTV event detection (multi_event_port.py, background)
Requires=dxrt.service
After=network-online.target dxrt.service
Wants=network-online.target
ConditionPathExists=$TARGET_SCRIPT
ConditionPathExists=$CAM_CSV_FILE
ConditionPathExists=$CAM_CONFIG_FILE

[Service]
Type=simple
User=$ACTUAL_USER
WorkingDirectory=$PROJECT_DIR
Environment=PYTHONUNBUFFERED=1 LANG=ko_KR.UTF-8
# NPU·랜카드가 자리 잡을 때까지 잠깐 기다립니다.
ExecStartPre=/bin/sleep 15
ExecStart=/bin/bash -c 'yes "" | exec /usr/bin/python3 -u $TARGET_SCRIPT'
Restart=always
RestartSec=15
TimeoutStopSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOL

sudo systemctl daemon-reload
sudo systemctl enable "$AI_SERVICE_NAME"
if pgrep -u "$ACTUAL_USER" -f "python3? .*multi_event_port\.py" > /dev/null && ! systemctl is-active --quiet "$AI_SERVICE_NAME"; then
    # 직접 실행한 프로그램이 이미 돌고 있으면 지금은 켜지 않습니다(같이 돌면 NPU 메모리가 넘칩니다). 다음 부팅부터 자동 실행.
    echo "⚠️ 직접 실행한 multi_event_port.py가 돌고 있어 서비스는 지금 켜지 않았습니다(다음 부팅부터 자동 실행)."
    echo "   바로 서비스로 바꾸려면 직접 실행한 프로그램을 끈 뒤: sudo systemctl start $AI_SERVICE_NAME"
elif [ ! -f "$CAM_CONFIG_FILE" ]; then
    echo "⚠️ cameras.json이 없어 서비스가 시작하지 않습니다. 먼저 화면으로 직접 실행해 카메라 설정 마법사를 마치세요:"
    echo "   cd $PROJECT_DIR && python3 multi_event_port.py --gui"
    echo "   마법사가 끝나면 프로그램을 끄고: sudo systemctl start $AI_SERVICE_NAME"
else
    sudo systemctl restart "$AI_SERVICE_NAME"
    echo "-> $AI_SERVICE_NAME 을 켰습니다(부팅 때마다 자동 실행)."
fi
echo "   상태: systemctl status $AI_SERVICE_NAME   출력: journalctl -u $AI_SERVICE_NAME -f"
echo "   화면(--gui)으로 직접 실행할 때는 먼저 멈추세요: sudo systemctl stop $AI_SERVICE_NAME"

sudo apt install -y openssh-server
sudo systemctl enable ssh
sudo systemctl start ssh
echo "====================================================="
echo " 🎉 설치 완료! 쾌적하고 안정적인 Global 환경으로 설정되었습니다."
echo " ※ 만약 AI 인퍼런스가 정상 작동하지 않는다면, PC를 1회 껐다 켜주세요 (Cold Boot 권장)."
echo " ※ PCIe 절전 끄기(pcie_aspm=off)와 render/video 그룹은 재부팅 후 적용됩니다."
echo " ※ 카메라(cameras.csv)를 바꾸면 'bash install.sh --config-only'로 cctv 번호를 다시 맞추세요."
echo "====================================================="
