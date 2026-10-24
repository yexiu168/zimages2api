# ImageFree 2API (Enterprise Edition)

[![FastAPI](https://img.shields.io/badge/FastAPI-0.104.1-009688.svg?style=flat&logo=fastapi)](https://fastapi.tiangolo.com)
[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg?style=flat&logo=python)](https://www.python.org)
[![Docker](https://img.shields.io/badge/Docker-Ready-2496ED.svg?style=flat&logo=docker)](https://www.docker.com)
[![OpenAI Compatible](https://img.shields.io/badge/OpenAI-Compatible-green.svg?style=flat&logo=openai)](https://platform.openai.com)

基于 [imagefree.net](https://imagefree.net) 的工业级二方 API 转发与聚合网关平台。将免费 AI 生图引擎包装为标准 **OpenAI 官方图像生成接口**（`POST /v1/images/generations`），集成多代理池调度、模型动态探测、自动中英翻译、画质增强、多图并发及本地防盗链图库。

---

## 🌟 核心特性

- 💎 **100% 兼容 OpenAI 格式**：支持 `/v1/images/generations`、`/v1/models`，支持 `dall-e-3` / `flux` 等别名，支持 `url` 与 `b64_json` 格式，原生兼容 NextChat、Dify、Lobehub、OneAPI / NewAPI。
- ⚡ **并发多图输出 (`n=1~4`)**：支持单请求并发生成多张图像，通过多代理节点并发调度加速出图。
- 🌐 **智能多代理池与健康熔断**：支持 HTTP/HTTPS/SOCKS5 代理轮询，失败 5 次自动熔断隔离，支持全节点一键测速与批量导入/导出。
- 🔍 **底层模型动态探测**：实时解析上游真实底模（识别为 Tongyi-MAI `Z-Image-Turbo`），同步呈现在模型列表与控制台。
- 🌐 **中文自动地道翻译**：针对扩散模型英文理解更优的特点，内置自动中英翻译，中文直接出高质量大片。
- 🎨 **智能画质修饰器 (Prompt Enhancement)**：可选自动注入 `masterpiece, 8k resolution, cinematic lighting` 等画质正向词。
- 💾 **本地防盗链缓存与图库画廊 (Gallery)**：自动拉取持久化到本地，防 R2 时效失效；配备 LRU 存储配额与可视化历史画廊。
- 🔑 **企业级 API Key 鉴权系统**：支持后台创建、启用/禁用自定义密钥，支持单独配置 RPM 限流，可选开启全局强制鉴权。
- 🖥️ **精美管理控制台**：自带 Playground 体验器、代理池中心、API Key 管理、历史画廊、运营指标看板及接口开发文档。

---

## 🚀 部署方式

### 方式一：Docker Compose（推荐）

```bash
# 1. 克隆代码仓库
git clone <repo-url>
cd <project-folder>

# 2. 复制环境配置（可选）
cp .env.example .env

# 3. 启动容器
docker compose up -d --build

# 4. 查看日志与健康状态
docker compose logs -f
```
启动后访问 `http://localhost:3020` 进入管理控制台。持久化数据自动保存在本地 `./data` 目录。

---

### 方式二：裸机/虚拟环境运行

```bash
# 1. 安装 Python 3.11 依赖
pip install -r requirements.txt

# 2. 启动服务
uvicorn main:app --host 0.0.0.0 --port 3020
```

---

### 方式三：PaaS 云原生部署 (Zeabur / Railway / Render)

- **Zeabur / Railway**: 直接导入该 GitHub 仓库，平台会自动根据 `Dockerfile` 或 `Procfile` 构建运行。
- **Render**: 项目内置 `render.yaml`，可在 Render 控制台选择 Blueprint 一键部署。

---

## 🔌 接入三方客户端指引

### 1. Dify 接入

1. 进入 Dify **设置 -> 模型供应商 -> 添加模型**。
2. 模型类型选择：`Text2Image`。
3. 模型名称填写：`imagefree-v1` 或 `dall-e-3`。
4. API 域名：`http://<your-server-ip>:3020/v1`。
5. API Key：填入在控制台生成的 API Key（默认测试 key 为 `sk-imagefree-default`）。

### 2. NextChat / ChatGPT-Next-Web 接入

1. 设置中配置自定义接口地址：`http://<your-server-ip>:3020`。
2. API Key：`sk-imagefree-default`。
3. 自定义模型：追加 `imagefree-v1`。

### 3. OneAPI / NewAPI 聚合中转

1. 渠道类型：选择 **OpenAI**。
2. 代理地址：`http://<your-server-ip>:3020`。
3. 密钥：填入后台创建的 API Key。
4. 模型重定向：将 `dall-e-3` 映射至 `imagefree-v1`。

---

## 📖 核心 API 端点一览

| 端点 | 方法 | 鉴权 | 说明 |
| :--- | :--- | :--- | :--- |
| `/v1/images/generations` | POST | 可选/强制 | OpenAI 官方图像生成兼容端点（支持 `n`、`size`、`b64_json`） |
| `/v1/models` | GET | 可选/强制 | OpenAI 官方模型列表端点（自动返回探测到的真实底模） |
| `/api/generate` | POST | 可选/强制 | 原生异步提交任务接口 |
| `/api/tasks/{task_id}` | GET | 无 | 原生异步任务轮询端点 |
| `/ws/tasks/{task_id}` | WS | 无 | 任务进度 WebSocket 实时推送 |
| `/api/history` | GET | 无 | 获取持久化画廊历史作品列表 |
| `/api/history/export-zip` | POST | 无 | 批量将选中的画廊图片及清单打包导出为 ZIP |
| `/api/images/cache/{filename}` | GET | 无 | 本地防盗链图片静态代理服务 |
| `/api/admin/*` | ANY | 管理 | API Key 与代理池增删改查、失效清理与批量测速 |
| `/metrics` | GET | 无 | Prometheus / OpenMetrics 标准监控指标采集端点 |
| `/health` | GET | 无 | 容器健康探针 |

---

## 🛡️ 环境变量说明

| 变量名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `PORT` | `3020` | 容器向外映射的端口 |
| `DATA_DIR` | `/app/data` (Docker) / 项目目录 | 数据持久化根目录（存放 `config.json` 与图片文件） |
| `UPSTREAM_BASE` | `https://imagefree.net` | 上游镜像服务基地址 |
