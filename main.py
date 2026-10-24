# main.py
"""
ImageFree 2API Platform - Enterprise Edition
=============================================
支持：
  1. API Key 自定义后台配置与调用鉴权（OpenAI Bearer Token & X-API-Key）
  2. 智能多代理池调度（轮询权重负载均衡、故障自动隔离、健康检查）
  3. OpenAI 兼容接口: POST /v1/images/generations
  4. 原生异步接口:   POST /api/generate + GET /api/tasks/{task_id}
  5. WebSocket 实时推送任务进度
  6. 完整可视化管理控制台
"""

import asyncio
import hashlib
import json
import logging
import base64
import os
import secrets
import time
import zipfile
import io
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Optional, Dict, List, Set, Any

import httpx
from fastapi import (
    FastAPI, HTTPException, BackgroundTasks,
    Request, WebSocket, WebSocketDisconnect, Header, Depends
)
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, PlainTextResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("imagefree-2api")

# ---------------------------------------------------------------------------
# 全局配置常量 (支持通过环境变量动态配置，便于 Docker 及容器化部署)
# ---------------------------------------------------------------------------
BASE_DIR = os.getenv("BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)

UPSTREAM_BASE = os.getenv("UPSTREAM_BASE", "https://imagefree.net")
CONFIG_FILE = os.getenv("CONFIG_FILE", os.path.join(DATA_DIR, "config.json"))
STORAGE_DIR = os.getenv("STORAGE_DIR", os.path.join(DATA_DIR, "storage", "images"))
HISTORY_FILE = os.getenv("HISTORY_FILE", os.path.join(DATA_DIR, "storage", "history.json"))
UI_FILE = os.getenv("UI_FILE", os.path.join(BASE_DIR, "demo.html"))

os.makedirs(STORAGE_DIR, exist_ok=True)

MAX_CACHED_IMAGES = 1000           # 本地最多缓存保留 1000 张图片 (LRU淘汰)
RATE_LIMIT_RPM = 60                # 默认总请求频率限制
TASK_EXPIRE_SEC = 900              # 任务保留 15 分钟
CACHE_EXPIRE_SEC = 3600            # 缓存 1 小时
CLEANUP_INTERVAL_SEC = 30          # 清理周期
POLL_INTERVAL_SEC = 3              # 轮询间隔
POLL_MAX_ATTEMPTS = 120            # 最大轮询次数（~6 分钟）
OPENAI_SYNC_TIMEOUT = 300          # OpenAI 同步等待超时

SUPPORTED_RATIOS = ["1:1", "16:9", "9:16", "4:3", "3:4"]
SIZE_TO_RATIO = {
    "1024x1024": "1:1",
    "1792x1024": "16:9",
    "1024x1792": "9:16",
    "1:1": "1:1",
    "16:9": "16:9",
    "9:16": "9:16",
    "4:3": "4:3",
    "3:4": "3:4",
}

# ---------------------------------------------------------------------------
# 代理池管理器 (Proxy Pool)
# ---------------------------------------------------------------------------
class ProxyNode:
    def __init__(self, url: str):
        self.url = url.strip()
        self.active = True
        self.fail_count = 0
        self.success_count = 0
        self.last_used = 0.0
        self.last_latency = 0.0

    def to_dict(self):
        return {
            "url": self.url,
            "active": self.active,
            "fail_count": self.fail_count,
            "success_count": self.success_count,
            "last_used": self.last_used,
            "last_latency": round(self.last_latency, 2)
        }

class ProxyPool:
    def __init__(self):
        self.proxies: Dict[str, ProxyNode] = {}
        self._index = 0
        self._lock = asyncio.Lock()

    def add_proxy(self, url: str) -> bool:
        url = url.strip()
        if not url or url in self.proxies:
            return False
        self.proxies[url] = ProxyNode(url)
        return True

    def remove_proxy(self, url: str) -> bool:
        url = url.strip()
        if url in self.proxies:
            del self.proxies[url]
            return True
        return False

    def toggle_proxy(self, url: str, active: bool) -> bool:
        url = url.strip()
        if url in self.proxies:
            self.proxies[url].active = active
            if active:
                self.proxies[url].fail_count = 0
            return True
        return False

    async def get_next_proxy(self) -> Optional[str]:
        """轮询选择一个健康的可用代理；若代理池为空或无可用代理，返回 None（直连）"""
        async with self._lock:
            available = [p for p in self.proxies.values() if p.active and p.fail_count < 5]
            if not available:
                return None
            self._index = (self._index + 1) % len(available)
            node = available[self._index]
            node.last_used = time.time()
            return node.url

    def report_success(self, url: Optional[str], latency: float = 0.0):
        if not url or url not in self.proxies:
            return
        node = self.proxies[url]
        node.success_count += 1
        node.fail_count = 0
        node.last_latency = latency

    def report_failure(self, url: Optional[str]):
        if not url or url not in self.proxies:
            return
        node = self.proxies[url]
        node.fail_count += 1
        if node.fail_count >= 5:
            node.active = False
            logger.warning("Proxy %s disabled due to 5 consecutive failures", url)

    def list_proxies(self) -> List[dict]:
        return [p.to_dict() for p in self.proxies.values()]

proxy_pool = ProxyPool()

# ---------------------------------------------------------------------------
# API Key 管理器 (API Key Storage, Quota & Verification)
# ---------------------------------------------------------------------------
class ApiKeyInfo:
    def __init__(
        self,
        key: str,
        name: str,
        enabled: bool = True,
        rpm_limit: int = 60,
        quota_limit: int = 0,     # 0 表示无限制，>0 表示最大调用次数
        expires_at: float = 0.0   # 0 表示永不过期，时间戳
    ):
        self.key = key
        self.name = name
        self.enabled = enabled
        self.rpm_limit = rpm_limit
        self.quota_limit = quota_limit
        self.expires_at = expires_at
        self.created_at = time.time()
        self.request_count = 0
        self.last_used_at = 0.0

    def to_dict(self):
        return {
            "key": self.key,
            "name": self.name,
            "enabled": self.enabled,
            "rpm_limit": self.rpm_limit,
            "quota_limit": self.quota_limit,
            "expires_at": self.expires_at,
            "created_at": self.created_at,
            "request_count": self.request_count,
            "last_used_at": self.last_used_at
        }

class ApiKeyManager:
    def __init__(self):
        self.keys: Dict[str, ApiKeyInfo] = {}
        self.auth_required = False  # 是否开启强制 API Key 鉴权

    def create_key(
        self,
        name: str,
        custom_key: Optional[str] = None,
        rpm_limit: int = 60,
        quota_limit: int = 0,
        expires_at: float = 0.0
    ) -> ApiKeyInfo:
        key = custom_key.strip() if custom_key and custom_key.strip() else f"sk-{secrets.token_hex(16)}"
        info = ApiKeyInfo(
            key=key,
            name=name,
            enabled=True,
            rpm_limit=rpm_limit,
            quota_limit=quota_limit,
            expires_at=expires_at
        )
        self.keys[key] = info
        return info

    def delete_key(self, key: str) -> bool:
        if key in self.keys:
            del self.keys[key]
            return True
        return False

    def toggle_key(self, key: str, enabled: bool) -> bool:
        if key in self.keys:
            self.keys[key].enabled = enabled
            return True
        return False

    def validate_key(self, raw_key: Optional[str]) -> (bool, Optional[str]):
        """
        验证 API Key 是否合法。
        返回 (is_valid, error_reason)
        """
        if not self.auth_required:
            return True, None
        if not raw_key:
            return False, "Missing API Key"
        # 去掉 Bearer 前缀
        if raw_key.startswith("Bearer "):
            raw_key = raw_key[7:].strip()
        key_info = self.keys.get(raw_key)
        if not key_info:
            return False, "Invalid API Key"
        if not key_info.enabled:
            return False, "API Key is disabled"
        # 校验到期时间
        now = time.time()
        if key_info.expires_at > 0 and now > key_info.expires_at:
            return False, "API Key has expired"
        # 校验调用额度
        if key_info.quota_limit > 0 and key_info.request_count >= key_info.quota_limit:
            return False, f"API Key quota exceeded (limit={key_info.quota_limit})"

        key_info.request_count += 1
        key_info.last_used_at = now
        return True, None

    def list_keys(self) -> List[dict]:
        return [k.to_dict() for k in self.keys.values()]

key_manager = ApiKeyManager()

# ---------------------------------------------------------------------------
# 配置持久化 (Save & Load)
# ---------------------------------------------------------------------------
def save_config():
    data = {
        "auth_required": key_manager.auth_required,
        "keys": [k.to_dict() for k in key_manager.keys.values()],
        "proxies": [p.to_dict() for p in proxy_pool.proxies.values()]
    }
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error("Failed to save config: %s", e)

def load_config():
    if not os.path.exists(CONFIG_FILE):
        # 默认生成一个体验 key
        default_k = key_manager.create_key("Default Key", custom_key="sk-imagefree-default", rpm_limit=60)
        save_config()
        return
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        key_manager.auth_required = data.get("auth_required", False)
        for kd in data.get("keys", []):
            info = ApiKeyInfo(
                key=kd["key"],
                name=kd.get("name", "Key"),
                enabled=kd.get("enabled", True),
                rpm_limit=kd.get("rpm_limit", 60),
                quota_limit=kd.get("quota_limit", 0),
                expires_at=kd.get("expires_at", 0.0)
            )
            info.created_at = kd.get("created_at", time.time())
            info.request_count = kd.get("request_count", 0)
            info.last_used_at = kd.get("last_used_at", 0.0)
            key_manager.keys[info.key] = info
        for pd in data.get("proxies", []):
            node = ProxyNode(pd["url"])
            node.active = pd.get("active", True)
            node.fail_count = pd.get("fail_count", 0)
            node.success_count = pd.get("success_count", 0)
            proxy_pool.proxies[node.url] = node
    except Exception as e:
        logger.error("Failed to load config: %s", e)

# ---------------------------------------------------------------------------
# 基础状态与统计
# ---------------------------------------------------------------------------
task_store: Dict[str, dict] = {}
rate_limits: Dict[str, List[float]] = defaultdict(list)
prompt_cache: Dict[str, dict] = {}
ws_connections: Dict[str, Set[WebSocket]] = defaultdict(set)

metrics = {
    "requests_total": 0,
    "tasks_submitted": 0,
    "tasks_completed": 0,
    "tasks_failed": 0,
    "cache_hits": 0,
    "rate_limited": 0,
    "ws_connections_total": 0,
    "boot_time": time.time(),
}

# ---------------------------------------------------------------------------
# Pydantic 模型
# ---------------------------------------------------------------------------
class NativeGenerateRequest(BaseModel):
    prompt: str
    aspect_ratio: str = "1:1"
    proxy: Optional[str] = None
    enhance_prompt: Optional[bool] = False
    auto_translate: Optional[bool] = True
    style_preset: Optional[str] = "none"
    negative_prompt: Optional[str] = None

class TaskResponse(BaseModel):
    task_id: str
    status: str
    progress: int = 0
    image_url: Optional[str] = None
    error: Optional[str] = None
    cached: bool = False
    proxy_used: Optional[str] = None
    created_at: Optional[float] = None
    completed_at: Optional[float] = None

class OpenAIImageRequest(BaseModel):
    prompt: str
    model: Optional[str] = "imagefree-v1"
    n: Optional[int] = Field(default=1, ge=1, le=4)
    size: Optional[str] = "1024x1024"
    response_format: Optional[str] = "url"  # "url" or "b64_json"
    enhance_prompt: Optional[bool] = False  # 是否启用提示词画质智能增强
    auto_translate: Optional[bool] = True   # 是否自动将非英语提示词翻译为地道英文
    style_preset: Optional[str] = "none"    # 风格预设: none/anime/cyberpunk/photo/ink/cinematic/3d
    negative_prompt: Optional[str] = None   # 负向提示词 (排除画风/缺陷)

class OpenAIImageData(BaseModel):
    url: Optional[str] = None
    b64_json: Optional[str] = None
    revised_prompt: Optional[str] = None

class OpenAIImageResponse(BaseModel):
    created: int
    data: List[OpenAIImageData]

# 管理模型
class CreateKeyRequest(BaseModel):
    name: str
    custom_key: Optional[str] = None
    rpm_limit: int = 60
    quota_limit: Optional[int] = 0     # 0 表示无限制
    expires_days: Optional[int] = 0    # 0 表示永不过期，>0 表示有效天数

class ToggleKeyRequest(BaseModel):
    key: str
    enabled: bool

class AddProxyRequest(BaseModel):
    url: str

class BatchAddProxyRequest(BaseModel):
    urls: str  # 多行或逗号分隔的代理地址

class ToggleProxyRequest(BaseModel):
    url: str
    active: bool

class AuthSettingRequest(BaseModel):
    auth_required: bool

# ---------------------------------------------------------------------------
# 鉴权依赖注入
# ---------------------------------------------------------------------------
def verify_api_key(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None)
):
    """提取 Authorization: Bearer <key> 或 X-API-Key: <key> 进行校验"""
    token = authorization or x_api_key
    valid, err_msg = key_manager.validate_key(token)
    if not valid:
        raise HTTPException(
            status_code=401,
            detail={
                "error": {
                    "message": f"Unauthorized: {err_msg}. Please provide a valid API Key.",
                    "type": "invalid_request_error",
                    "code": "invalid_api_key"
                }
            }
        )
    return True

# ---------------------------------------------------------------------------
# 图像本地下载防盗链、LRU轮转与持久化画廊历史
# ---------------------------------------------------------------------------
def rotate_image_storage():
    """按 LRU/修改时间自动淘汰最旧的本地缓存图片，保留数量限制在 MAX_CACHED_IMAGES"""
    try:
        if not os.path.exists(STORAGE_DIR):
            return
        files = []
        for f in os.listdir(STORAGE_DIR):
            if f.endswith(".png") or f.endswith(".jpg") or f.endswith(".jpeg"):
                p = os.path.join(STORAGE_DIR, f)
                try:
                    files.append((p, os.path.getmtime(p)))
                except OSError:
                    pass
        if len(files) > MAX_CACHED_IMAGES:
            # 按修改时间从旧到新排序
            files.sort(key=lambda x: x[1])
            to_remove = files[: len(files) - MAX_CACHED_IMAGES]
            for p, _ in to_remove:
                try:
                    os.remove(p)
                except OSError:
                    pass
            logger.info("Rotated image storage: cleaned %d old images, %d remaining", len(to_remove), MAX_CACHED_IMAGES)
    except Exception as e:
        logger.warning("Error during image storage rotation: %s", e)

# 历史画廊内存+文件存储
gallery_history: List[dict] = []

def load_gallery_history():
    global gallery_history
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                gallery_history = json.load(f)
        except Exception as e:
            logger.warning("Failed to load history: %s", e)
            gallery_history = []

def save_gallery_history():
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(gallery_history[:500], f, ensure_ascii=False, indent=2)  # 最多保留 500 条历史
    except Exception as e:
        logger.warning("Failed to save history: %s", e)

def record_history(prompt: str, original_prompt: str, aspect_ratio: str, image_url: str, local_url: str, model: str = "imagefree-v1", duration_sec: float = 0.0):
    entry = {
        "id": hashlib.md5(f"{time.time()}_{image_url}".encode()).hexdigest()[:12],
        "prompt": prompt,
        "original_prompt": original_prompt,
        "aspect_ratio": aspect_ratio,
        "image_url": image_url,
        "local_url": local_url,
        "model": model,
        "duration_sec": round(duration_sec, 2),
        "created_at": time.time()
    }
    gallery_history.insert(0, entry)
    save_gallery_history()

# ---------------------------------------------------------------------------
# 提示词智能增强与自动中英翻译 (Prompt Enhancement & Auto-Translation)
# ---------------------------------------------------------------------------
QUALITY_MODIFIERS = [
    "masterpiece", "highly detailed", "8k resolution", "sharp focus",
    "photorealistic", "cinematic lighting", "octane render"
]

def contains_cjk(text: str) -> bool:
    """检测字符串是否包含中文或日韩文字"""
    for ch in text:
        if '\u4e00' <= ch <= '\u9fff':
            return True
    return False

async def translate_to_english_if_needed(text: str) -> str:
    """如果检测到输入包含非英文/中文字符，自动调用轻量免费翻译层翻译为地道英文"""
    if not contains_cjk(text):
        return text
    try:
        import urllib.parse
        enc = urllib.parse.quote(text.strip())
        url = f"https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=en&dt=t&q={enc}"
        async with httpx.AsyncClient(timeout=4.0, headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                translated = "".join([part[0] for part in data[0] if part and part[0]]).strip()
                if translated:
                    logger.info("Auto-translated prompt: '%.40s' -> '%.40s'", text, translated)
                    return translated
    except Exception as e:
        logger.warning("Prompt auto-translation failed (fallback to original): %s", e)
    return text

def enhance_prompt_text(prompt: str) -> str:
    """智能提升提示词画质：若未包含画质修饰词，自动追加高质量画风标签"""
    p = prompt.strip()
    if not p:
        return p
    lower_p = p.lower()
    missing_modifiers = [mod for mod in QUALITY_MODIFIERS if mod not in lower_p]
    if missing_modifiers:
        selected_mod = ", ".join(missing_modifiers[:4])
        return f"{p}, {selected_mod}"
    return p

STYLE_PRESETS = {
    "none": "",
    "anime": "anime style, key visual, Makoto Shinkai style, vibrant colors, clean lines, highly detailed anime illustration",
    "cyberpunk": "cyberpunk style, neon glow, futuristic city, high tech low life, dark atmospheric, octane render",
    "photo": "professional photograph, 35mm lens, natural lighting, ultra realistic, hyperdetailed, 8k resolution, raw photo",
    "ink": "traditional Chinese ink painting, watercolor wash, brush strokes, elegant, artistic, atmospheric",
    "cinematic": "cinematic still, movie screencap, dramatic lighting, anamorphic lens, depth of field, blockbuster movie aesthetic",
    "3d": "3D pixar style, cute, smooth render, vibrant colors, octane render, trending on artstation"
}

def apply_style_preset(prompt: str, preset: Optional[str] = None) -> str:
    """根据选择的画风预设注入修饰语"""
    if not preset or preset not in STYLE_PRESETS or preset == "none":
        return prompt
    suffix = STYLE_PRESETS[preset]
    return f"{prompt}, {suffix}"

async def process_prompt(
    prompt: str,
    auto_translate: bool = True,
    enhance_quality: bool = False,
    style_preset: Optional[str] = "none",
    negative_prompt: Optional[str] = None
) -> str:
    """综合提示词预处理流水线：翻译 -> 画风预设 -> 画质增强 -> 负向提示词处理"""
    res = prompt.strip()
    if auto_translate:
        res = await translate_to_english_if_needed(res)
    if style_preset and style_preset != "none":
        res = apply_style_preset(res, style_preset)
    if enhance_quality:
        res = enhance_prompt_text(res)
    if negative_prompt and negative_prompt.strip():
        neg = negative_prompt.strip()
        if auto_translate:
            neg = await translate_to_english_if_needed(neg)
        res = f"{res} --no {neg}"
    return res

async def cache_and_download_image(remote_url: str) -> (str, bytes):
    """
    将远程图片异步保存到本地 /workspace/storage/images 目录。
    返回: (本地保存的文件名, 图片原始二进制字节)
    """
    if not remote_url:
        return "", b""
    # 用 url 摘要作为持久化文件名
    filename = hashlib.md5(remote_url.encode()).hexdigest() + ".png"
    filepath = os.path.join(STORAGE_DIR, filename)

    if os.path.exists(filepath):
        try:
            with open(filepath, "rb") as f:
                content = f.read()
            return filename, content
        except Exception:
            pass

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(remote_url)
            if resp.status_code == 200:
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                logger.info("Cached image locally: %s -> %s", remote_url, filename)
                rotate_image_storage()
                return filename, resp.content
    except Exception as e:
        logger.warning("Failed to cache remote image %s: %s", remote_url, e)

    return "", b""
def check_rate_limit(ip: str) -> bool:
    now = time.time()
    cutoff = now - 60
    rate_limits[ip] = [t for t in rate_limits[ip] if t > cutoff]
    if len(rate_limits[ip]) >= RATE_LIMIT_RPM:
        return False
    rate_limits[ip].append(now)
    return True

# ---------------------------------------------------------------------------
# 缓存
# ---------------------------------------------------------------------------
def _cache_key(prompt: str, ratio: str) -> str:
    raw = json.dumps({"p": prompt.strip().lower(), "r": ratio}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()

def cache_get(prompt: str, ratio: str) -> Optional[dict]:
    key = _cache_key(prompt, ratio)
    entry = prompt_cache.get(key)
    if entry and time.time() - entry["ts"] < CACHE_EXPIRE_SEC:
        return entry
    if entry:
        prompt_cache.pop(key, None)
    return None

def cache_set(prompt: str, ratio: str, task_id: str, image_url: str):
    key = _cache_key(prompt, ratio)
    prompt_cache[key] = {
        "task_id": task_id,
        "image_url": image_url,
        "ts": time.time(),
    }

# ---------------------------------------------------------------------------
# 上游交互 (集成智能代理池)
# ---------------------------------------------------------------------------
async def upstream_submit(prompt: str, aspect_ratio: str, manual_proxy: Optional[str] = None) -> (str, Optional[str]):
    """向上游提交生成任务，支持指定的代理或从代理池自动选取"""
    url = f"{UPSTREAM_BASE}/api/generate"
    payload = {"prompt": prompt, "aspect_ratio": aspect_ratio}

    chosen_proxy = manual_proxy if manual_proxy else await proxy_pool.get_next_proxy()
    transport = httpx.AsyncHTTPTransport(proxy=chosen_proxy) if chosen_proxy else None

    t0 = time.time()
    try:
        async with httpx.AsyncClient(transport=transport, timeout=30.0) as client:
            resp = await client.post(url, json=payload)
            latency = time.time() - t0
            if resp.status_code != 200:
                proxy_pool.report_failure(chosen_proxy)
                logger.error("Upstream submit failed: %d %s (proxy=%s)", resp.status_code, resp.text[:200], chosen_proxy)
                raise HTTPException(502, detail=f"Upstream imagefree error: {resp.status_code}")
            data = resp.json()
            task_id = data.get("taskId")
            if not task_id:
                proxy_pool.report_failure(chosen_proxy)
                raise HTTPException(502, detail="Upstream returned no taskId")
            
            proxy_pool.report_success(chosen_proxy, latency)
            return task_id, chosen_proxy
    except HTTPException:
        raise
    except Exception as e:
        proxy_pool.report_failure(chosen_proxy)
        logger.error("Network error on upstream submit: %s (proxy=%s)", e, chosen_proxy)
        raise HTTPException(502, detail=f"Proxy/Network error connecting to imagefree: {str(e)}")


async def upstream_poll(task_id: str, proxy: Optional[str] = None) -> dict:
    url = f"{UPSTREAM_BASE}/api/generate/status"
    params = {"taskId": task_id}
    transport = httpx.AsyncHTTPTransport(proxy=proxy) if proxy else None

    t0 = time.time()
    try:
        async with httpx.AsyncClient(transport=transport, timeout=15.0) as client:
            resp = await client.get(url, params=params)
            latency = time.time() - t0
            if resp.status_code != 200:
                return {"status": "pending", "progress": 0}
            data = resp.json()
            status = data.get("status", "pending")
            progress = data.get("progress", 0)
            image_url = data.get("image")

            if status == "completed" and image_url:
                proxy_pool.report_success(proxy, latency)
                return {"status": "completed", "progress": 100, "image_url": image_url}
            elif status == "failed":
                proxy_pool.report_success(proxy, latency)
                return {"status": "failed", "progress": progress, "error": data.get("error", "Upstream generation failed")}
            else:
                return {"status": "pending", "progress": progress}
    except Exception as e:
        proxy_pool.report_failure(proxy)
        logger.warning("Poll error with proxy %s: %s", proxy, e)
        return {"status": "pending", "progress": 0}

# ---------------------------------------------------------------------------
# WebSocket 推送
# ---------------------------------------------------------------------------
async def ws_broadcast(task_id: str, payload: dict):
    if task_id not in ws_connections:
        return
    msg = json.dumps({"task_id": task_id, **payload})
    dead: Set[WebSocket] = set()
    for ws in ws_connections[task_id]:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    ws_connections[task_id] -= dead
    if not ws_connections[task_id]:
        ws_connections.pop(task_id, None)

# ---------------------------------------------------------------------------
# 后台任务轮询
# ---------------------------------------------------------------------------
async def background_poll(task_id: str, proxy: Optional[str] = None):
    for attempt in range(1, POLL_MAX_ATTEMPTS + 1):
        try:
            result = await upstream_poll(task_id, proxy)
        except Exception as e:
            logger.warning("Poll attempt failed: %s", e)
            await asyncio.sleep(POLL_INTERVAL_SEC)
            continue

        if task_id in task_store:
            task_store[task_id]["status"] = result["status"]
            task_store[task_id]["progress"] = result.get("progress", 0)
            if result.get("image_url"):
                task_store[task_id]["image_url"] = result["image_url"]
            if result.get("error"):
                task_store[task_id]["error"] = result["error"]

        await ws_broadcast(task_id, {
            "status": result["status"],
            "progress": result.get("progress", 0),
            "image_url": result.get("image_url"),
            "error": result.get("error"),
            "attempt": attempt,
            "max_attempts": POLL_MAX_ATTEMPTS,
        })

        if result["status"] == "completed":
            if task_id in task_store:
                task_store[task_id]["completed_at"] = time.time()
            metrics["tasks_completed"] += 1
            prompt_text = task_store.get(task_id, {}).get("prompt", "")
            orig_prompt = task_store.get(task_id, {}).get("original_prompt", prompt_text)
            ratio = task_store.get(task_id, {}).get("aspect_ratio", "1:1")
            img_url = result.get("image_url", "")
            if prompt_text and img_url:
                cache_set(prompt_text, ratio, task_id, img_url)
                # 异步将图片转存本地并入画廊记录
                async def _save_gallery_async():
                    try:
                        local_fn, _ = await cache_and_download_image(img_url)
                        local_url = f"/api/images/cache/{local_fn}" if local_fn else img_url
                        duration = (task_store[task_id]["completed_at"] - task_store[task_id]["created_at"]) if task_id in task_store else 0.0
                        record_history(prompt=prompt_text, original_prompt=orig_prompt, aspect_ratio=ratio, image_url=img_url, local_url=local_url, duration_sec=duration)
                    except Exception as ex:
                        logger.warning("Failed to record gallery for async task: %s", ex)
                asyncio.create_task(_save_gallery_async())
            logger.info("Task completed: %s", task_id)
            return

        if result["status"] == "failed":
            if task_id in task_store:
                task_store[task_id]["completed_at"] = time.time()
            metrics["tasks_failed"] += 1
            return

        await asyncio.sleep(POLL_INTERVAL_SEC)

    if task_id in task_store:
        task_store[task_id]["status"] = "timeout"
        task_store[task_id]["error"] = "Task timed out after max attempts"
        task_store[task_id]["completed_at"] = time.time()
    metrics["tasks_failed"] += 1
    await ws_broadcast(task_id, {"status": "timeout", "progress": 0, "error": "Task timed out"})

# ---------------------------------------------------------------------------
# 上游模型自动探测器 (Upstream Model Auto-Discovery)
# ---------------------------------------------------------------------------
class UpstreamModelDiscovery:
    def __init__(self):
        self.model_id = "Z-Image-Turbo"
        self.model_name = "Z-Image-Turbo (Tongyi-MAI)"
        self.model_source = "https://huggingface.co/Tongyi-MAI/Z-Image-Turbo"
        self.last_checked = 0.0
        self.check_interval = 1800  # 30 分钟重新探测一次

    async def fetch_upstream_model(self):
        """自动请求并解析 imagefree.net 网站获取其真实部署的底层模型信息"""
        try:
            async with httpx.AsyncClient(timeout=10.0, headers={"User-Agent": "Mozilla/5.0"}) as client:
                resp = await client.get(UPSTREAM_BASE, follow_redirects=True)
                if resp.status_code == 200:
                    html = resp.text
                    import re
                    # 匹配 Powered by ... <a>模型名</a>
                    match = re.search(r'href=[\"\'](https?://huggingface\.co/[^\"\']+)[\"\'][^>]*>([^<]+)</a>', html)
                    if match:
                        self.model_source = match.group(1).strip()
                        self.model_id = match.group(2).strip()
                        self.model_name = f"{self.model_id} (Official Upstream)"
                        self.last_checked = time.time()
                        logger.info("Auto-discovered upstream model: %s (source: %s)", self.model_id, self.model_source)
                        return
                    # 备用匹配
                    matches = re.findall(r'([A-Za-z0-9_\-]+(?:-Turbo|-Base|-XL|-v[0-9]))', html)
                    for m in matches:
                        if "Turbo" in m or "Image" in m:
                            self.model_id = m
                            self.model_name = m
                            self.last_checked = time.time()
                            logger.info("Auto-discovered upstream model via pattern: %s", self.model_id)
                            return
        except Exception as e:
            logger.warning("Upstream model auto-discovery failed, using fallback: %s", e)
        self.last_checked = time.time()

    def get_models(self) -> List[dict]:
        return [
            {
                "id": self.model_id,
                "object": "model",
                "created": int(metrics["boot_time"]),
                "owned_by": "imagefree-upstream",
                "source": self.model_source,
                "description": "Auto-discovered real model from imagefree.net",
                "permission": [],
            },
            {
                "id": "imagefree-v1",
                "object": "model",
                "created": int(metrics["boot_time"]),
                "owned_by": "imagefree-2api",
                "source": self.model_source,
                "description": f"Default alias pointing to upstream {self.model_id}",
                "permission": [],
            }
        ]

model_discovery = UpstreamModelDiscovery()

# ---------------------------------------------------------------------------
# 代理池后台自动健康检查与自愈巡检
# ---------------------------------------------------------------------------
async def check_all_proxies_health():
    """定期对代理池中的节点发起并发健康心跳检测，自动复活或熔断"""
    if not proxy_pool.proxies:
        return

    async def _probe(url: str, node: ProxyNode):
        try:
            t0 = time.time()
            transport = httpx.AsyncHTTPTransport(proxy=url)
            async with httpx.AsyncClient(transport=transport, timeout=8.0) as client:
                resp = await client.get(f"{UPSTREAM_BASE}/", follow_redirects=True)
                latency = time.time() - t0
                if resp.status_code in (200, 301, 302, 403):  # 成功连通上游
                    if not node.active:
                        logger.info("Proxy auto-recovered and unblocked: %s (latency=%.2fs)", url, latency)
                    node.active = True
                    node.fail_count = 0
                    node.last_latency = latency
                else:
                    node.fail_count += 1
                    if node.fail_count >= 5:
                        node.active = False
        except Exception:
            node.fail_count += 1
            if node.fail_count >= 5 and node.active:
                node.active = False
                logger.warning("Proxy auto-disabled after 5 failures: %s", url)

    # 限制并发度批量探测
    tasks = [_probe(url, node) for url, node in list(proxy_pool.proxies.items())]
    await asyncio.gather(*tasks, return_exceptions=True)

# ---------------------------------------------------------------------------
# 定时清理任务
# ---------------------------------------------------------------------------
async def cleanup_loop():
    last_proxy_check = 0.0
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SEC)
        now = time.time()

        # 检查是否需要重新探测上游模型
        if now - model_discovery.last_checked > model_discovery.check_interval:
            await model_discovery.fetch_upstream_model()

        # 每 60 秒自动并发检测一次代理池健康状态并自动恢复熔断节点
        if now - last_proxy_check > 60:
            last_proxy_check = now
            if proxy_pool.proxies:
                asyncio.create_task(check_all_proxies_health())

        expired = [tid for tid, d in task_store.items() if now - d.get("created_at", now) > TASK_EXPIRE_SEC]
        for tid in expired:
            del task_store[tid]
        if expired:
            logger.info("Cleaned %d expired tasks", len(expired))

        expired_c = [k for k, v in prompt_cache.items() if now - v.get("ts", now) > CACHE_EXPIRE_SEC]
        for k in expired_c:
            del prompt_cache[k]

        empty = [ip for ip, ts_list in rate_limits.items() if not any(t > now - 60 for t in ts_list)]
        for ip in empty:
            del rate_limits[ip]

# ---------------------------------------------------------------------------
# 生命周期
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_config()
    load_gallery_history()
    # 启动时自动进行一次上游模型探测
    await model_discovery.fetch_upstream_model()
    task = asyncio.create_task(cleanup_loop())
    logger.info("ImageFree 2API Platform (Enterprise) booted.")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

# ---------------------------------------------------------------------------
# 初始化 FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title="ImageFree 2API Platform",
    description="Enterprise-grade AI Image Generation Gateway & Proxy Pool",
    version="2.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def request_logger(request: Request, call_next):
    t0 = time.time()
    response = await call_next(request)
    ms = (time.time() - t0) * 1000
    if request.url.path not in ("/health", "/favicon.ico"):
        logger.info("%s %s -> %d (%.0fms)", request.method, request.url.path, response.status_code, ms)
    return response

# =========================== 业务 API ===========================

# ---------- 1. 原生异步接口 ----------

@app.post("/api/generate", response_model=TaskResponse)
async def api_generate(
    body: NativeGenerateRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    _auth = Depends(verify_api_key)
):
    metrics["requests_total"] += 1
    ip = request.client.host if request.client else "0.0.0.0"

    if body.aspect_ratio not in SUPPORTED_RATIOS:
        raise HTTPException(400, detail=f"Unsupported aspect_ratio. Use one of {SUPPORTED_RATIOS}")

    if not check_rate_limit(ip):
        metrics["rate_limited"] += 1
        raise HTTPException(429, detail=f"Rate limit: max {RATE_LIMIT_RPM} requests/min")

    # 缓存命中
    cached = cache_get(body.prompt, body.aspect_ratio)
    if cached:
        metrics["cache_hits"] += 1
        return TaskResponse(
            task_id=cached["task_id"], status="completed", progress=100,
            image_url=cached["image_url"], cached=True,
        )

    # 提交上游并使用代理池
    effective_prompt = await process_prompt(
        prompt=body.prompt,
        auto_translate=body.auto_translate,
        enhance_quality=body.enhance_prompt,
        style_preset=body.style_preset,
        negative_prompt=body.negative_prompt
    )
    upstream_id, used_proxy = await upstream_submit(effective_prompt, body.aspect_ratio, body.proxy)
    now = time.time()
    task_store[upstream_id] = {
        "status": "pending", "progress": 0,
        "prompt": effective_prompt,
        "original_prompt": body.prompt,
        "aspect_ratio": body.aspect_ratio,
        "proxy": used_proxy, "image_url": None, "error": None,
        "created_at": now, "completed_at": None, "client_ip": ip,
    }
    metrics["tasks_submitted"] += 1

    background_tasks.add_task(background_poll, upstream_id, used_proxy)
    logger.info("Task submitted: %s prompt=%.60s (proxy=%s)", upstream_id, body.prompt, used_proxy)

    return TaskResponse(
        task_id=upstream_id, status="pending", progress=0,
        proxy_used=used_proxy, created_at=now
    )


@app.get("/api/tasks/{task_id}", response_model=TaskResponse)
async def api_task_status(task_id: str):
    if task_id not in task_store:
        raise HTTPException(404, detail="Task not found")
    d = task_store[task_id]
    return TaskResponse(
        task_id=task_id,
        status=d.get("status", "unknown"),
        progress=d.get("progress", 0),
        image_url=d.get("image_url"),
        error=d.get("error"),
        proxy_used=d.get("proxy"),
        created_at=d.get("created_at"),
        completed_at=d.get("completed_at"),
    )


async def generate_single_image(
    prompt: str,
    original_prompt: str,
    ratio: str,
    response_fmt: str,
    base_origin: str,
    model: str = "imagefree-v1"
) -> OpenAIImageData:
    """生成单张图像（复用缓存与代理池提交+轮询）"""
    t0 = time.time()
    cached = cache_get(prompt, ratio)
    if cached:
        metrics["cache_hits"] += 1
        raw_url = cached["image_url"]
        local_fn, raw_bytes = await cache_and_download_image(raw_url)
        api_full_url = f"{base_origin}/api/images/cache/{local_fn}" if local_fn else raw_url
        if response_fmt == "b64_json":
            b64_str = base64.b64encode(raw_bytes).decode("utf-8") if raw_bytes else ""
            return OpenAIImageData(b64_json=b64_str, revised_prompt=prompt)
        else:
            return OpenAIImageData(url=api_full_url, revised_prompt=prompt)

    upstream_id, used_proxy = await upstream_submit(prompt, ratio)
    metrics["tasks_submitted"] += 1

    deadline = time.time() + OPENAI_SYNC_TIMEOUT
    while time.time() < deadline:
        try:
            result = await upstream_poll(upstream_id, used_proxy)
        except Exception:
            await asyncio.sleep(POLL_INTERVAL_SEC)
            continue

        if result["status"] == "completed" and result.get("image_url"):
            metrics["tasks_completed"] += 1
            raw_url = result["image_url"]
            cache_set(prompt, ratio, upstream_id, raw_url)
            duration = time.time() - t0

            local_fn, raw_bytes = await cache_and_download_image(raw_url)
            local_url = f"/api/images/cache/{local_fn}" if local_fn else raw_url
            api_full_url = f"{base_origin}/api/images/cache/{local_fn}" if local_fn else raw_url

            # 存入历史画廊 (使用相对路径 /api/images/cache/...，保证无论是在 127.0.0.1 还是线上反向代理域名下均能正常加载图片)
            record_history(
                prompt=prompt,
                original_prompt=original_prompt,
                aspect_ratio=ratio,
                image_url=raw_url,
                local_url=local_url,
                model=model,
                duration_sec=duration
            )

            if response_fmt == "b64_json":
                b64_str = base64.b64encode(raw_bytes).decode("utf-8") if raw_bytes else ""
                return OpenAIImageData(b64_json=b64_str, revised_prompt=prompt)
            else:
                return OpenAIImageData(url=api_full_url, revised_prompt=prompt)

        if result["status"] == "failed":
            metrics["tasks_failed"] += 1
            raise HTTPException(500, detail=result.get("error", "Image generation failed"))

        await asyncio.sleep(POLL_INTERVAL_SEC)

    metrics["tasks_failed"] += 1
    raise HTTPException(504, detail="Image generation timed out")


# ---------- 2. OpenAI 兼容接口 ----------

@app.post("/v1/images/generations", response_model=OpenAIImageResponse)
async def openai_images_generations(
    body: OpenAIImageRequest,
    request: Request,
    _auth = Depends(verify_api_key)
):
    metrics["requests_total"] += 1
    ip = request.client.host if request.client else "0.0.0.0"

    # 宽高比解析与模型名称宽松容错
    size_str = body.size or "1024x1024"
    ratio = SIZE_TO_RATIO.get(size_str, "1:1")
    response_fmt = body.response_format or "url"
    n_count = body.n or 1

    if not check_rate_limit(ip):
        metrics["rate_limited"] += 1
        raise HTTPException(429, detail=f"Rate limit: max {RATE_LIMIT_RPM} requests/min")

    base_origin = f"{request.url.scheme}://{request.url.netloc}"

    effective_prompt = await process_prompt(
        prompt=body.prompt,
        auto_translate=body.auto_translate if body.auto_translate is not None else True,
        enhance_quality=body.enhance_prompt if body.enhance_prompt is not None else False,
        style_preset=body.style_preset,
        negative_prompt=body.negative_prompt
    )

    logger.info("OpenAI request: n=%d prompt=%.60s (enhanced=%s, translate=%s) model=%s fmt=%s",
                n_count, effective_prompt, body.enhance_prompt, body.auto_translate, body.model, response_fmt)

    if n_count == 1:
        img_data = await generate_single_image(
            prompt=effective_prompt,
            original_prompt=body.prompt,
            ratio=ratio,
            response_fmt=response_fmt,
            base_origin=base_origin,
            model=body.model or "imagefree-v1"
        )
        return OpenAIImageResponse(created=int(time.time()), data=[img_data])
    else:
        # 并发通过多代理调度生成多张图片 (n=2~4)
        tasks = [
            generate_single_image(
                prompt=effective_prompt,
                original_prompt=body.prompt,
                ratio=ratio,
                response_fmt=response_fmt,
                base_origin=base_origin,
                model=body.model or "imagefree-v1"
            )
            for _ in range(n_count)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        datas = []
        for r in results:
            if isinstance(r, Exception):
                logger.error("One of the parallel generation tasks failed: %s", r)
                # 若其中一个失败但有成功的，仍返回成功部分，若全失败则抛出异常
            else:
                datas.append(r)
        if not datas:
            raise HTTPException(502, detail="All parallel image generation attempts failed")

        return OpenAIImageResponse(created=int(time.time()), data=datas)


@app.get("/v1/models")
async def openai_models(_auth = Depends(verify_api_key)):
    return {
        "object": "list",
        "data": model_discovery.get_models()
    }

@app.get("/api/upstream/model")
async def get_upstream_model():
    """获取当前从上游实时探测到的模型信息"""
    return {
        "model_id": model_discovery.model_id,
        "model_name": model_discovery.model_name,
        "model_source": model_discovery.model_source,
        "last_checked": model_discovery.last_checked
    }

# ---------- 3. WebSocket 端点 ----------

@app.websocket("/ws/tasks/{task_id}")
async def ws_task_progress(websocket: WebSocket, task_id: str):
    await websocket.accept()
    ws_connections[task_id].add(websocket)
    metrics["ws_connections_total"] += 1

    try:
        if task_id in task_store:
            d = task_store[task_id]
            await websocket.send_text(json.dumps({
                "task_id": task_id,
                "status": d.get("status"),
                "progress": d.get("progress", 0),
                "image_url": d.get("image_url"),
                "error": d.get("error"),
            }))

        while True:
            msg = await websocket.receive_text()
            if msg == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        pass
    finally:
        ws_connections[task_id].discard(websocket)
        if not ws_connections[task_id]:
            ws_connections.pop(task_id, None)

# ---------- 4. 管理面板专用 API ----------

@app.get("/api/admin/keys")
async def admin_list_keys():
    return {
        "auth_required": key_manager.auth_required,
        "keys": key_manager.list_keys()
    }

@app.post("/api/admin/keys")
async def admin_create_key(req: CreateKeyRequest):
    expires_at = (time.time() + req.expires_days * 86400) if (req.expires_days and req.expires_days > 0) else 0.0
    info = key_manager.create_key(
        name=req.name,
        custom_key=req.custom_key,
        rpm_limit=req.rpm_limit,
        quota_limit=req.quota_limit or 0,
        expires_at=expires_at
    )
    save_config()
    return {"status": "ok", "key": info.to_dict()}

@app.delete("/api/admin/keys/{key}")
async def admin_delete_key(key: str):
    success = key_manager.delete_key(key)
    save_config()
    return {"status": "ok", "success": success}

@app.post("/api/admin/keys/toggle")
async def admin_toggle_key(req: ToggleKeyRequest):
    success = key_manager.toggle_key(req.key, req.enabled)
    save_config()
    return {"status": "ok", "success": success}

@app.post("/api/admin/auth-setting")
async def admin_set_auth(req: AuthSettingRequest):
    key_manager.auth_required = req.auth_required
    save_config()
    return {"status": "ok", "auth_required": key_manager.auth_required}

# 代理池管理
@app.get("/api/admin/proxies")
async def admin_list_proxies():
    return {"proxies": proxy_pool.list_proxies()}

@app.post("/api/admin/proxies")
async def admin_add_proxy(req: AddProxyRequest):
    success = proxy_pool.add_proxy(req.url)
    if not success:
        raise HTTPException(400, detail="Invalid proxy or already exists")
    save_config()
    return {"status": "ok", "proxy": req.url}

@app.post("/api/admin/proxies/batch")
async def admin_batch_add_proxies(req: BatchAddProxyRequest):
    """批量导入代理节点（支持换行或逗号分隔）"""
    added = 0
    lines = req.urls.replace(",", "\n").splitlines()
    for line in lines:
        u = line.strip()
        if u and (u.startswith("http://") or u.startswith("https://") or u.startswith("socks5://")):
            if proxy_pool.add_proxy(u):
                added += 1
    save_config()
    return {"status": "ok", "added_count": added, "total": len(proxy_pool.proxies)}

@app.delete("/api/admin/proxies")
async def admin_delete_proxy(url: str):
    success = proxy_pool.remove_proxy(url)
    save_config()
    return {"status": "ok", "success": success}

@app.post("/api/admin/proxies/toggle")
async def admin_toggle_proxy(req: ToggleProxyRequest):
    success = proxy_pool.toggle_proxy(req.url, req.active)
    save_config()
    return {"status": "ok", "success": success}

@app.post("/api/admin/proxies/test-all")
async def admin_test_all_proxies():
    """并发测试代理池内所有节点的连通性与延迟"""
    results = []
    async def test_node(url: str, node: ProxyNode):
        t0 = time.time()
        try:
            transport = httpx.AsyncHTTPTransport(proxy=url)
            async with httpx.AsyncClient(transport=transport, timeout=8.0) as client:
                resp = await client.get(f"{UPSTREAM_BASE}/", follow_redirects=True)
                latency = (time.time() - t0) * 1000
                if resp.status_code in (200, 301, 302, 403):
                    node.active = True
                    node.fail_count = 0
                    node.last_latency = latency
                    return {"url": url, "status": "ok", "latency_ms": round(latency, 1), "code": resp.status_code}
                else:
                    node.fail_count += 1
                    return {"url": url, "status": "error", "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            node.fail_count += 1
            if node.fail_count >= 5:
                node.active = False
            return {"url": url, "status": "error", "error": str(e)[:100]}

    tasks = [test_node(url, node) for url, node in proxy_pool.proxies.items()]
    if tasks:
        results = await asyncio.gather(*tasks)
    save_config()
    return {"total": len(results), "results": results}

@app.get("/api/admin/proxies/export")
async def admin_export_proxies():
    """以纯文本形式一键导出所有代理节点列表（换行符分隔）"""
    text = "\n".join([url for url in proxy_pool.proxies.keys()])
    return PlainTextResponse(content=text)

@app.delete("/api/admin/proxies/dead")
async def admin_prune_dead_proxies():
    """一键清理失效节点（fail_count >= 5 或 active == False）"""
    dead_urls = [url for url, node in proxy_pool.proxies.items() if not node.active or node.fail_count >= 5]
    for u in dead_urls:
        proxy_pool.remove_proxy(u)
    save_config()
    return {"status": "ok", "removed_count": len(dead_urls), "remaining": len(proxy_pool.proxies)}

# ---------- 5. 本地图片防盗链缓存代理端点与画廊接口 ----------

@app.get("/api/images/cache/{filename}")
async def serve_cached_image(filename: str):
    """本地提供持久化的图片，避免上游 R2 图床失效或跨域防盗链问题"""
    filepath = os.path.join(STORAGE_DIR, filename)
    if not os.path.exists(filepath):
        raise HTTPException(404, detail="Image not found in local cache")
    return FileResponse(filepath, media_type="image/png")

@app.get("/api/history")
async def get_history(limit: int = 50, offset: int = 0):
    """获取历史图库持久化记录"""
    total = len(gallery_history)
    items = gallery_history[offset : offset + limit]
    return {
        "total": total,
        "items": items,
        "storage_images_count": len([f for f in os.listdir(STORAGE_DIR) if f.endswith(('.png', '.jpg'))]) if os.path.exists(STORAGE_DIR) else 0,
        "max_cached_limit": MAX_CACHED_IMAGES
    }

@app.delete("/api/history/{item_id}")
async def delete_history_item(item_id: str):
    """从图库记录中删除指定项，并同步清理对应磁盘图片文件"""
    global gallery_history
    target = None
    new_history = []
    for x in gallery_history:
        if x.get("id") == item_id:
            target = x
        else:
            new_history.append(x)
    gallery_history = new_history
    save_gallery_history()

    # 尝试同步清理本地缓存文件
    if target and target.get("local_url"):
        try:
            local_url = target["local_url"]
            if "/api/images/cache/" in local_url:
                fn = local_url.split("/api/images/cache/")[-1]
                fp = os.path.join(STORAGE_DIR, fn)
                if os.path.exists(fp):
                    os.remove(fp)
                    logger.info("Deleted cached image file: %s", fp)
        except Exception as e:
            logger.warning("Error deleting cached image file for history item %s: %s", item_id, e)

    return {"status": "ok"}

@app.delete("/api/history")
async def clear_all_history():
    """清空全部历史画廊记录"""
    global gallery_history
    gallery_history = []
    save_gallery_history()
    return {"status": "ok"}

@app.post("/api/history/export-zip")
async def export_history_zip(body: Dict[str, List[str]]):
    """
    批量将选中的历史图片打包为 ZIP 压缩流返回
    入参: {"ids": ["id1", "id2", ...]}，若 ids 为空则打包全部
    """
    req_ids = set(body.get("ids", []))
    selected_items = [it for it in gallery_history if not req_ids or it.get("id") in req_ids]

    if not selected_items:
        raise HTTPException(400, detail="No matching images to export")

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        manifest = []
        for idx, item in enumerate(selected_items):
            local_url = item.get("local_url", "")
            fn = local_url.split("/api/images/cache/")[-1] if "/api/images/cache/" in local_url else None
            filepath = os.path.join(STORAGE_DIR, fn) if fn else None

            entry_name = f"{idx+1:03d}_{item.get('id', 'img')[:8]}.png"
            if filepath and os.path.exists(filepath):
                try:
                    with open(filepath, "rb") as img_f:
                        zf.writestr(entry_name, img_f.read())
                except Exception as e:
                    logger.warning("Failed to include %s into zip: %s", filepath, e)
            
            manifest.append({
                "file": entry_name,
                "prompt": item.get("prompt"),
                "original_prompt": item.get("original_prompt"),
                "aspect_ratio": item.get("aspect_ratio"),
                "created_at": item.get("created_at")
            })
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

    zip_buffer.seek(0)
    zip_filename = f"imagefree_export_{int(time.time())}.zip"
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{zip_filename}"'}
    )

# ---------- 6. 运营指标与监控 ----------

@app.get("/api/metrics")
async def api_metrics():
    active = sum(1 for d in task_store.values() if d.get("status") in ("pending", "processing"))
    return {
        "requests_total": metrics["requests_total"],
        "tasks_submitted": metrics["tasks_submitted"],
        "tasks_completed": metrics["tasks_completed"],
        "tasks_failed": metrics["tasks_failed"],
        "cache_hits": metrics["cache_hits"],
        "rate_limited": metrics["rate_limited"],
        "ws_connections_active": sum(len(s) for s in ws_connections.values()),
        "uptime_seconds": round(time.time() - metrics["boot_time"], 1),
        "tasks_in_store": len(task_store),
        "cache_entries": len(prompt_cache),
        "active_tasks": active,
        "proxy_pool_total": len(proxy_pool.proxies),
        "proxy_pool_active": sum(1 for p in proxy_pool.proxies.values() if p.active),
        "api_keys_total": len(key_manager.keys),
        "auth_required": key_manager.auth_required,
    }

@app.get("/metrics")
async def prometheus_metrics():
    """提供兼容 Prometheus 采集的标准文本格式指标"""
    active = sum(1 for d in task_store.values() if d.get("status") in ("pending", "processing"))
    uptime = time.time() - metrics["boot_time"]
    active_proxies = sum(1 for p in proxy_pool.proxies.values() if p.active)

    lines = [
        "# HELP imagefree_requests_total Total number of HTTP requests handled.",
        "# TYPE imagefree_requests_total counter",
        f"imagefree_requests_total {metrics['requests_total']}",
        "",
        "# HELP imagefree_tasks_submitted_total Total generation tasks submitted.",
        "# TYPE imagefree_tasks_submitted_total counter",
        f"imagefree_tasks_submitted_total {metrics['tasks_submitted']}",
        "",
        "# HELP imagefree_tasks_completed_total Total generation tasks successfully completed.",
        "# TYPE imagefree_tasks_completed_total counter",
        f"imagefree_tasks_completed_total {metrics['tasks_completed']}",
        "",
        "# HELP imagefree_tasks_failed_total Total generation tasks failed.",
        "# TYPE imagefree_tasks_failed_total counter",
        f"imagefree_tasks_failed_total {metrics['tasks_failed']}",
        "",
        "# HELP imagefree_cache_hits_total Total cache hits for image generation.",
        "# TYPE imagefree_cache_hits_total counter",
        f"imagefree_cache_hits_total {metrics['cache_hits']}",
        "",
        "# HELP imagefree_rate_limited_total Total requests rejected due to rate limiting.",
        "# TYPE imagefree_rate_limited_total counter",
        f"imagefree_rate_limited_total {metrics['rate_limited']}",
        "",
        "# HELP imagefree_active_tasks Currently active rendering tasks.",
        "# TYPE imagefree_active_tasks gauge",
        f"imagefree_active_tasks {active}",
        "",
        "# HELP imagefree_proxies_total Total proxies in the pool.",
        "# TYPE imagefree_proxies_total gauge",
        f"imagefree_proxies_total {len(proxy_pool.proxies)}",
        "",
        "# HELP imagefree_proxies_active Active and healthy proxies in the pool.",
        "# TYPE imagefree_proxies_active gauge",
        f"imagefree_proxies_active {active_proxies}",
        "",
        "# HELP imagefree_api_keys_total Total registered API keys.",
        "# TYPE imagefree_api_keys_total gauge",
        f"imagefree_api_keys_total {len(key_manager.keys)}",
        "",
        "# HELP imagefree_uptime_seconds Process uptime in seconds.",
        "# TYPE imagefree_uptime_seconds gauge",
        f"imagefree_uptime_seconds {uptime:.1f}",
        ""
    ]
    return PlainTextResponse(content="\n".join(lines), media_type="text/plain; version=0.0.4; charset=utf-8")

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.1.0"}

# ---------- 6. 控制台页面 ----------

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    try:
        with open(UI_FILE, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>ImageFree 2API</h1><p>UI not found.</p>")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3020))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
