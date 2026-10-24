FROM python:3.11-slim

# 设置工作目录与 Python 环境变量
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    BASE_DIR=/app \
    DATA_DIR=/app/data

# 安装基础依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝代码与静态文件
COPY main.py .
COPY demo.html .

# 创建持久化数据目录
RUN mkdir -p /app/data/storage/images

# 暴露端口
EXPOSE 3020

# 健康检查
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:3020/health')" || exit 1

# 启动服务
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3020"]
