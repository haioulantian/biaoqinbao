#!/usr/bin/env python3
"""
独立启动 Meme Manager WebUI（无需 AstrBot）
自动创建软链接解决包名中的连字符问题。
用法: cd /Users/heng/Downloads && python3 meme_webui_run.py
"""
import os
import sys
import types
import subprocess
import tempfile

PLUGIN_DIR = "/Users/heng/Downloads/biaoqinbao"
LINK_NAME = "meme_plugin"
LINK_PATH = os.path.join(tempfile.gettempdir(), LINK_NAME)

# ── 创建/更新软链接 ──
if os.path.islink(LINK_PATH):
    os.unlink(LINK_PATH)
elif os.path.exists(LINK_PATH):
    os.remove(LINK_PATH)
os.symlink(PLUGIN_DIR, LINK_PATH)

# ── Mock AstrBot（必须在 config.py 被 import 之前） ──
LOCAL_DATA_DIR = os.path.join(PLUGIN_DIR, "data", "standalone")
os.makedirs(os.path.join(LOCAL_DATA_DIR, "memes"), exist_ok=True)

class _FakeAstrbotPath:
    @staticmethod
    def get_astrbot_data_path(): return LOCAL_DATA_DIR
    @staticmethod
    def get_astrbot_plugin_data_path(): return LOCAL_DATA_DIR

_astrbot_core = types.ModuleType("astrbot.core")
_astrbot_core_utils = types.ModuleType("astrbot.core.utils")
_astrbot_path = types.ModuleType("astrbot.core.utils.astrbot_path")
_astrbot_path.get_astrbot_data_path = _FakeAstrbotPath.get_astrbot_data_path
_astrbot_path.get_astrbot_plugin_data_path = _FakeAstrbotPath.get_astrbot_plugin_data_path
_astrbot_core_utils.astrbot_path = _astrbot_path
_astrbot_core.utils = _astrbot_core_utils

sys.modules["astrbot"] = types.ModuleType("astrbot")
sys.modules["astrbot.core"] = _astrbot_core
sys.modules["astrbot.core.utils"] = _astrbot_core_utils
sys.modules["astrbot.core.utils.astrbot_path"] = _astrbot_path

_astrbot_api = types.ModuleType("astrbot.api")
_astrbot_api.logger = __import__("logging").getLogger("meme")
sys.modules["astrbot.api"] = _astrbot_api

# ── 把软链接目录加入 sys.path ──
sys.path.insert(0, tempfile.gettempdir())

# ── 导入插件模块 ──
import asyncio
import shutil
import hypercorn.asyncio
from hypercorn.config import Config as HypercornConfig

from meme_plugin.backend.api import api
from meme_plugin.backend.category_manager import CategoryManager
from meme_plugin.webui import app
from meme_plugin.config import MEMES_DIR
import meme_plugin.webui as webui_module

PORT = int(os.environ.get("PORT", 5050))
SECRET_KEY = os.environ.get("KEY", "meme123")

category_manager = CategoryManager()
app.secret_key = os.urandom(16)
app.config["PLUGIN_CONFIG"] = {
    "category_manager": category_manager,
    "webui_port": PORT,
}

webui_module.SERVER_LOGIN_KEY = None  # 无需密钥

# 仅在 MEMES_DIR 完全为空时才复制默认表情包
default_memes_src = os.path.join(PLUGIN_DIR, "memes")
existing_dirs = [
    d for d in os.listdir(MEMES_DIR)
    if os.path.isdir(os.path.join(MEMES_DIR, d))
] if os.path.isdir(MEMES_DIR) else []
if os.path.isdir(default_memes_src) and len(existing_dirs) == 0:
    for item in os.listdir(default_memes_src):
        src = os.path.join(default_memes_src, item)
        dst = os.path.join(MEMES_DIR, item)
        if os.path.isdir(src):
            shutil.copytree(src, dst)

print(f"""
╔══════════════════════════════════════════╗
║   🎭 Meme Manager WebUI (独立预览)      ║
╠══════════════════════════════════════════╣
║  地址: http://localhost:{PORT}              ║
║  密钥: {SECRET_KEY}                          ║
╚══════════════════════════════════════════╝
""")

hypercorn_config = HypercornConfig()
hypercorn_config.bind = [f"0.0.0.0:{PORT}"]
hypercorn_config.graceful_timeout = 5
hypercorn_config.accesslog = "-"

asyncio.run(hypercorn.asyncio.serve(app, hypercorn_config))
