#!/bin/bash
# 항만 AI CCTV(multi_event_port.py) 모델 받기 스크립트.
#   bash download.sh : 허깅페이스 HudatersU/port의 모델(.dxnn, .onnx)을 이 폴더(multi_event_port.py 옆)에 받습니다.
# 공개 저장소라 로그인은 필요 없습니다. 받을 목록과 sha256은 허깅페이스에서 읽으므로, 모델을 바꿔 올려도 이 파일은 그대로 씁니다.
# 이미 같은 파일(sha256 같음)이 있으면 건너뛰고, 내용이 다르면 허깅페이스 것으로 바꿉니다.
#   HF_REPO=다른/저장소 HF_REVISION=브랜치 bash download.sh : 다른 저장소·브랜치에서 받습니다.

set -u

HF_REPO="${HF_REPO:-HudatersU/port}"
HF_REVISION="${HF_REVISION:-main}"
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
API_URL="https://huggingface.co/api/models/$HF_REPO/tree/$HF_REVISION"
FILE_URL="https://huggingface.co/$HF_REPO/resolve/$HF_REVISION"

for cmd in curl python3 sha256sum; do
    if ! command -v "$cmd" > /dev/null 2>&1; then
        echo "❌ '$cmd' 명령이 없습니다. 먼저 설치하세요(예: sudo apt install -y $cmd)."
        exit 1
    fi
done

echo "====================================================="
echo " 모델 받기: $HF_REPO ($HF_REVISION) → $PROJECT_DIR"
echo "====================================================="

# 허깅페이스 파일 목록에서 모델(.dxnn, .onnx)만: "이름 sha256 크기" 한 줄씩
if ! LISTING=$(curl -fsSL --retry 3 --connect-timeout 20 "$API_URL"); then
    echo "❌ 허깅페이스 목록을 읽지 못했습니다. 인터넷 연결과 저장소 이름($HF_REPO)을 확인하세요."
    exit 1
fi
MODELS=$(printf '%s' "$LISTING" | python3 -c '
import json, sys
for f in json.load(sys.stdin):
    lfs = f.get("lfs") or {}
    if f.get("type") == "file" and lfs.get("oid") and f["path"].endswith((".dxnn", ".onnx")):
        print(f["path"], lfs["oid"], lfs["size"])
')
if [ -z "$MODELS" ]; then
    echo "❌ $HF_REPO 에 모델 파일(.dxnn, .onnx)이 없습니다."
    exit 1
fi

SKIPPED=0
DOWNLOADED=0
FAILED=()
tmp=""
# Ctrl+C 등으로 멈추면 받던 임시 파일만 지웁니다(기존 모델 파일은 그대로).
trap 'rm -f "$tmp"; echo; echo "⚠️ 중단했습니다. 받던 파일은 지웠고 기존 파일은 그대로입니다."; exit 130' INT TERM
while read -r name sha size; do
    target="$PROJECT_DIR/$name"
    if [ -f "$target" ] && [ "$(sha256sum "$target" | cut -d' ' -f1)" = "$sha" ]; then
        echo "-> $name: 이미 같은 파일이 있어 건너뜁니다."
        SKIPPED=$((SKIPPED + 1))
        continue
    fi
    if [ -f "$target" ]; then
        echo "-> $name: 내용이 달라 허깅페이스 것으로 바꿉니다. ($((size / 1048576))MB)"
    else
        echo "-> $name: 받습니다. ($((size / 1048576))MB)"
    fi
    mkdir -p "$(dirname "$target")"
    # 임시 파일로 받고 sha256이 맞을 때만 바꿉니다(중간에 끊기거나 깨져도 기존 파일은 그대로).
    tmp="$target.part"
    if curl -fL --retry 3 --connect-timeout 20 --progress-bar -o "$tmp" "$FILE_URL/$name" \
        && [ "$(sha256sum "$tmp" | cut -d' ' -f1)" = "$sha" ]; then
        mv -f "$tmp" "$target"
        DOWNLOADED=$((DOWNLOADED + 1))
    else
        rm -f "$tmp"
        echo "   ❌ $name 받기 실패(연결이 끊겼거나 sha256이 맞지 않음). 기존 파일은 그대로 둡니다."
        FAILED+=("$name")
    fi
done <<< "$MODELS"

echo "-----------------------------------------------------"
echo " 받음 $DOWNLOADED개, 건너뜀 $SKIPPED개, 실패 ${#FAILED[@]}개"
if [ ${#FAILED[@]} -gt 0 ]; then
    echo " 실패: ${FAILED[*]} (다시 실행하면 실패한 것만 다시 받습니다)"
    exit 1
fi
echo " ✅ 모델이 모두 준비됐습니다."
