#!/bin/bash
# 항만 AI CCTV 단말기 원격 접속 설정 (Lubuntu 22.04 + SDDM 미니PC 기준).
# 예전 AI CCTV의 setup_remote_new.sh를 바탕으로, 항만 단말기(.54)에서 실제로 잘 돌고 있는 방식에 맞췄습니다.
#
#   bash setup_remote.sh                  : sudo 없이 일반 사용자로 실행합니다. 중간에 sudo 비밀번호, VNC 비밀번호,
#                                           Tailscale 로그인(처음 한 번)을 묻습니다. 끝나면 재부팅하세요.
#   VNC_LISTEN=all bash setup_remote.sh   : VNC를 Tailscale뿐 아니라 모든 네트워크(같은 랜 포함)에 엽니다.
#
# 하는 일:
#   1. xrdp 제거: 실제 화면과 다른 세션을 따로 만들어, GUI 프로그램 화면과 어긋나고 VNC 화면 공유와 겹칩니다.
#   2. 자동 로그인(SDDM): 모니터 없이도 실제 화면(:0)이 떠서 GUI 프로그램과 VNC가 동작합니다.
#   3. Tailscale 설치·로그인: 원격 접속용 사설망(100.x 주소).
#   4. 절전·화면 꺼짐 막기, 모니터가 없으면 화면을 1920x1080으로(그대로 두면 1024x768로 떠서 GUI가 잘림).
#   5. VNC(x11vnc): 실제 화면 :0을 5900번으로 공유하는 시스템 서비스(remote-x11vnc.service).
#      로그인 시작 프로그램이 아니라 서비스라서 부팅 때 켜지고, 죽으면 3초 뒤 다시 켭니다.
#      기본은 Tailscale 주소에만 엽니다(1분 안에 주소를 못 찾으면 모든 네트워크).
#   6. SSH 서버.

ACTUAL_USER="$USER"
USER_HOME=$(getent passwd "$ACTUAL_USER" | cut -d: -f6)
VNC_LISTEN_MODE="${VNC_LISTEN:-tailscale}"
VNC_RESOLUTION="${VNC_RESOLUTION:-1920x1080}"
VNC_PASSWD="$USER_HOME/.vnc/passwd"
VNC_SERVICE_NAME="remote-x11vnc.service"
VNC_SERVICE_PATH="/etc/systemd/system/$VNC_SERVICE_NAME"
VNC_SCRIPT_PATH="/usr/local/sbin/remote-x11vnc.sh"
AUTOSTART_DIR="$USER_HOME/.config/autostart"
BIN_DIR="$USER_HOME/.local/bin"

# xrdp 세션 안에서 실행하면 아래에서 xrdp를 지우는 순간 접속과 이 스크립트가 같이 끊깁니다.
in_xrdp_session() {
    [ -n "${XRDP_SESSION:-}${XRDP_SOCKET_PATH:-}" ] && return 0
    local pid=$$ comm
    while [ -n "$pid" ] && [ "$pid" -gt 1 ]; do
        comm=$(ps -o comm= -p "$pid" 2>/dev/null)
        case "$comm" in xrdp*) return 0 ;; esac
        pid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
    done
    return 1
}

if [ "$(id -u)" -eq 0 ]; then
    echo "❌ sudo 없이 일반 사용자(예: hudaters)로 실행하세요: bash setup_remote.sh"
    exit 1
fi
if in_xrdp_session; then
    echo "❌ 원격 데스크톱(xrdp) 세션에서는 실행하지 마세요. xrdp를 지우는 순간 접속과 설정이 중간에 끊깁니다."
    echo "   SSH나 VNC로 접속하거나 단말기 앞에서 실행하세요."
    exit 1
fi
if [ ! -t 0 ]; then
    echo "❌ 비밀번호와 로그인을 물어야 해서 터미널에서 직접 실행해야 합니다."
    exit 1
fi

echo "====================================================="
echo " 항만 AI CCTV 단말기 원격 접속 설정 (사용자: $ACTUAL_USER)"
echo "====================================================="
echo " xrdp 제거, 자동 로그인, Tailscale, VNC(5900번, $VNC_LISTEN_MODE), 절전 끄기, SSH를 설정합니다."
read -r -p ">> 계속할까요? [y/N]: " answer
case "$answer" in
    y|Y) ;;
    *) echo "취소했습니다."; exit 0 ;;
esac

echo "-----------------------------------------------------"
echo " 1. 필수 패키지 설치"
echo "-----------------------------------------------------"
sudo apt-get update
sudo apt-get install -y curl gpg lsb-release x11vnc xauth net-tools x11-xserver-utils openssh-server

echo "-----------------------------------------------------"
echo " 2. xrdp 제거"
echo "-----------------------------------------------------"
if dpkg -s xrdp > /dev/null 2>&1; then
    sudo systemctl stop xrdp xrdp-sesman 2> /dev/null || true
    sudo apt-get purge -y xrdp xorgxrdp
    sudo apt-get autoremove -y
    echo "-> xrdp를 지웠습니다."
else
    echo "-> xrdp가 없습니다."
fi
# 예전 setup_remote_new.sh가 만든 로그인 시작 VNC가 있으면 지웁니다(아래 서비스와 5900번이 겹침).
rm -f "$AUTOSTART_DIR/x11vnc.desktop"

echo "-----------------------------------------------------"
echo " 3. 자동 로그인 (SDDM)"
echo "-----------------------------------------------------"
DISPLAY_MANAGER=$(basename "$(cat /etc/X11/default-display-manager 2> /dev/null)" 2> /dev/null)
if [ "$DISPLAY_MANAGER" = "sddm" ]; then
    SESSION_NAME="lxqt"
    [ -f /usr/share/xsessions/Lubuntu.desktop ] && SESSION_NAME="Lubuntu"
    sudo mkdir -p /etc/sddm.conf.d
    printf "[Autologin]\nUser=%s\nSession=%s\n" "$ACTUAL_USER" "$SESSION_NAME" | sudo tee /etc/sddm.conf.d/autologin.conf > /dev/null
    echo "-> 자동 로그인: $ACTUAL_USER ($SESSION_NAME)"
    # /etc/sddm.conf가 sddm.conf.d보다 우선하므로, 거기에 다른 사용자가 있으면 알려 줍니다.
    OTHER_USER=$(awk -F= '/^\[Autologin\]/{f=1; next} /^\[/{f=0} f && $1=="User"{print $2}' /etc/sddm.conf 2> /dev/null)
    if [ -n "$OTHER_USER" ] && [ "$OTHER_USER" != "$ACTUAL_USER" ]; then
        echo "⚠️ /etc/sddm.conf의 자동 로그인 사용자($OTHER_USER)가 우선합니다. 그 파일의 [Autologin]을 고쳐 주세요."
    fi
else
    echo "⚠️ 화면 관리자가 SDDM이 아닙니다(${DISPLAY_MANAGER:-알 수 없음}). 자동 로그인은 직접 켜 주세요."
fi

echo "-----------------------------------------------------"
echo " 4. Tailscale 설치·로그인"
echo "-----------------------------------------------------"
if ! command -v tailscale > /dev/null 2>&1; then
    CODENAME=$(lsb_release -cs)
    curl -fsSL "https://pkgs.tailscale.com/stable/ubuntu/$CODENAME.noarmor.gpg" | sudo tee /usr/share/keyrings/tailscale-archive-keyring.gpg > /dev/null
    curl -fsSL "https://pkgs.tailscale.com/stable/ubuntu/$CODENAME.tailscale-keyring.list" | sudo tee /etc/apt/sources.list.d/tailscale.list > /dev/null
    sudo apt-get update && sudo apt-get install -y tailscale
fi
sudo systemctl enable --now tailscaled
if [ -z "$(tailscale ip -4 2> /dev/null)" ]; then
    echo "-> Tailscale 로그인이 필요합니다. 아래에 나오는 주소를 다른 PC 브라우저로 열어 로그인하세요."
    sudo tailscale up
fi
TAILSCALE_IP=$(tailscale ip -4 2> /dev/null | head -n 1)
echo "-> Tailscale 주소: ${TAILSCALE_IP:-(없음)}"

echo "-----------------------------------------------------"
echo " 5. 절전·화면 꺼짐 막기, 모니터 없을 때 화면 크기"
echo "-----------------------------------------------------"
sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target > /dev/null 2>&1
mkdir -p "$BIN_DIR" "$AUTOSTART_DIR"

cat > "$BIN_DIR/disable-screen-blanking" << 'EOF'
#!/bin/sh
xset s off
xset s noblank
xset -dpms
xscreensaver-command -exit >/dev/null 2>&1 || true
EOF

cat > "$BIN_DIR/set-vnc-resolution" << EOF
#!/bin/sh
# 모니터가 없을 때만 화면 크기를 정합니다(모니터가 있으면 모니터 해상도 그대로).
export DISPLAY=:0
if xrandr 2>/dev/null | grep -q " connected"; then
    exit 0
fi
xrandr --fb $VNC_RESOLUTION

# LXQt panel may keep the old 1024x768 geometry if xrandr runs after login.
sleep 1
pkill -u "\$USER" -x lxqt-panel 2>/dev/null || true
sleep 1
nohup lxqt-panel >/tmp/lxqt-panel-autostart.log 2>&1 &
EOF
chmod 755 "$BIN_DIR/disable-screen-blanking" "$BIN_DIR/set-vnc-resolution"

cat > "$AUTOSTART_DIR/disable-screen-blanking.desktop" << EOF
[Desktop Entry]
Type=Application
Name=Disable Screen Blanking
Exec=$BIN_DIR/disable-screen-blanking
Terminal=false
EOF

cat > "$AUTOSTART_DIR/set-vnc-resolution.desktop" << EOF
[Desktop Entry]
Type=Application
Name=Set VNC Resolution
Exec=$BIN_DIR/set-vnc-resolution
Terminal=false
EOF
echo "-> 절전 끔, 로그인 때 화면 꺼짐 끄기·화면 크기($VNC_RESOLUTION, 모니터 없을 때) 등록"

echo "-----------------------------------------------------"
echo " 6. VNC 비밀번호"
echo "-----------------------------------------------------"
mkdir -p "$USER_HOME/.vnc"
chmod 700 "$USER_HOME/.vnc"
if [ ! -f "$VNC_PASSWD" ]; then
    echo "-> VNC 접속 비밀번호를 정합니다."
    x11vnc -storepasswd "$VNC_PASSWD"
else
    echo "-> 기존 VNC 비밀번호를 그대로 씁니다: $VNC_PASSWD"
fi
chmod 600 "$VNC_PASSWD"

echo "-----------------------------------------------------"
echo " 7. VNC 서비스 등록 ($VNC_SERVICE_NAME)"
echo "-----------------------------------------------------"
sudo tee "$VNC_SCRIPT_PATH" > /dev/null << 'EOF'
#!/bin/sh
# remote-x11vnc.service가 실행합니다. 실제 화면(:0)이 뜨면 x11vnc로 5900번에 공유합니다.
# VNC_LISTEN=tailscale: Tailscale 주소에만 엽니다(1분 안에 주소를 못 찾으면 모든 네트워크). all: 모든 네트워크.
for i in $(seq 1 90); do
    [ -S /tmp/.X11-unix/X0 ] && break
    sleep 2
done
if [ ! -S /tmp/.X11-unix/X0 ]; then
    echo "실제 화면(:0)이 뜨지 않았습니다(자동 로그인을 확인하세요)." >&2
    exit 1
fi

AUTH_FILE=""
for f in /var/run/sddm/* /run/sddm/* /run/user/*/gdm/Xauthority /run/gdm3/auth-for-*/database; do
    if [ -r "$f" ]; then
        AUTH_FILE="$f"
        break
    fi
done
if [ -z "$AUTH_FILE" ]; then
    echo "화면 관리자의 Xauthority 파일을 찾지 못했습니다: /var/run/sddm/* /run/sddm/* /run/user/*/gdm/Xauthority /run/gdm3/auth-for-*/database" >&2
    exit 1
fi

LISTEN=""
if [ "${VNC_LISTEN:-tailscale}" = "tailscale" ]; then
    for i in $(seq 1 30); do
        LISTEN=$(tailscale ip -4 2>/dev/null | head -n 1)
        [ -n "$LISTEN" ] && break
        sleep 2
    done
    [ -n "$LISTEN" ] || echo "Tailscale 주소를 못 찾아 모든 네트워크에 엽니다." >&2
fi

exec /usr/bin/x11vnc -display :0 -auth "$AUTH_FILE" -rfbauth "$VNC_PASSWD" -rfbport 5900 \
    -listen "${LISTEN:-0.0.0.0}" -forever -shared -repeat -noxdamage
EOF
sudo chmod 755 "$VNC_SCRIPT_PATH"

sudo tee "$VNC_SERVICE_PATH" > /dev/null << EOF
[Unit]
Description=x11vnc desktop sharing for :0 (PCTC remote access)
After=network-online.target display-manager.service tailscaled.service
Wants=network-online.target

[Service]
Type=simple
Environment=DISPLAY=:0
Environment=VNC_LISTEN=$VNC_LISTEN_MODE
Environment=VNC_PASSWD=$VNC_PASSWD
ExecStart=$VNC_SCRIPT_PATH
Restart=always
RestartSec=3

[Install]
WantedBy=graphical.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable "$VNC_SERVICE_NAME"
echo "-> 재부팅하면 새 설정으로 켜집니다."

echo "-----------------------------------------------------"
echo " 8. SSH 서버"
echo "-----------------------------------------------------"
sudo systemctl enable --now ssh

echo "====================================================="
echo " 🎉 원격 접속 설정 완료. 재부팅하세요: sudo reboot"
if [ -n "$TAILSCALE_IP" ]; then
    echo "   VNC: $TAILSCALE_IP:5900"
    echo "   SSH: ssh $ACTUAL_USER@$TAILSCALE_IP"
fi
echo "====================================================="
