#!/usr/bin/env bash
# deploy.sh — 从 GitHub Release 下载 mimo2api-go 并重启 mimi3 服务
# 用法: bash scripts/deploy.sh [release_tag]
#   不传 tag 则自动拉取最新 release
set -euo pipefail

REPO="LuckyWithLab/mimo2api-go"
BINARY_NAME="mimo2api-go"
SERVICE_NAME="mimi3"
DEPLOY_DIR="/root/api/mimi3"
DEPLOY_PATH="${DEPLOY_DIR}/${BINARY_NAME}"

# 颜色
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[deploy]${NC} $*"; }
err()  { echo -e "${RED}[deploy]${NC} $*" >&2; }

# 获取 release tag
TAG="${1:-}"
if [ -z "$TAG" ]; then
    log "正在查询最新 release..."
    TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" 2>/dev/null \
        | grep '"tag_name"' | head -1 | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/' || true)
    if [ -z "$TAG" ]; then
        # API 限流时 fallback 到 releases 页面解析
        TAG=$(curl -fsSL "https://github.com/${REPO}/releases/latest" -o /dev/null -w "%{redirect_url}" 2>/dev/null \
            | sed -E 's|.*/tag/||' || true)
    fi
    if [ -z "$TAG" ]; then
        err "无法获取最新 release tag，请手动指定: bash scripts/deploy.sh v0.1.0"
        exit 1
    fi
fi
log "目标版本: ${TAG}"

# 构造下载 URL (linux amd64)
ASSET_NAME="${BINARY_NAME}-linux-amd64"
DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${TAG}/${ASSET_NAME}"
log "下载地址: ${DOWNLOAD_URL}"

# 下载到临时文件
TMPFILE=$(mktemp "/tmp/${BINARY_NAME}.XXXXXX")
trap 'rm -f "$TMPFILE"' EXIT

log "正在下载..."
if ! curl -fSL --retry 3 --retry-delay 2 -o "$TMPFILE" "$DOWNLOAD_URL"; then
    err "下载失败，请确认 release ${TAG} 存在且包含 ${ASSET_NAME} 文件"
    exit 1
fi

# 验证文件
FILESIZE=$(stat -c%s "$TMPFILE" 2>/dev/null || stat -f%z "$TMPFILE" 2>/dev/null)
if [ "${FILESIZE:-0}" -lt 1000000 ]; then
    err "文件太小 (${FILESIZE} bytes)，可能不是有效的二进制"
    exit 1
fi
log "下载完成: $(du -h "$TMPFILE" | cut -f1)"

# 备份旧二进制
if [ -f "$DEPLOY_PATH" ]; then
    BACKUP="${DEPLOY_PATH}.bak.$(date +%Y%m%d%H%M%S)"
    cp "$DEPLOY_PATH" "$BACKUP"
    log "已备份旧版本到 ${BACKUP}"
fi

# 停止服务
log "正在停止 ${SERVICE_NAME}..."
systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
sleep 2

# 替换二进制
log "正在部署..."
cp "$TMPFILE" "$DEPLOY_PATH"
chmod +x "$DEPLOY_PATH"

# 启动服务
log "正在启动 ${SERVICE_NAME}..."
systemctl start "${SERVICE_NAME}"

# 验证
sleep 2
if systemctl is-active --quiet "${SERVICE_NAME}"; then
    log "✅ 部署成功！${SERVICE_NAME} 已运行"
    systemctl status "${SERVICE_NAME}" --no-pager | head -5
else
    err "❌ 服务启动失败！正在回滚..."
    if [ -n "${BACKUP:-}" ] && [ -f "$BACKUP" ]; then
        cp "$BACKUP" "$DEPLOY_PATH"
        systemctl start "${SERVICE_NAME}"
        warn "已回滚到旧版本"
    fi
    exit 1
fi
