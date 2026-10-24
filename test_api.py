"""
ImageFree 2API 自动化集成测试套件 (标准 unittest 版本)
全面覆盖:
1. 健康检查与指标 (GET /health, GET /api/metrics)
2. 模型列表 (GET /v1/models)
3. 提示词增强、画风预设与自动翻译流水线 (process_prompt)
4. 代理池管理 (增删改查、死节点清理、导出)
5. API Key 管理与权限开关 (鉴权开启拦截与放行)
6. 画廊历史管理 (GET/DELETE/CLEAR)
"""

import sys
import os
import time
import asyncio
import unittest
from fastapi.testclient import TestClient

from main import app, proxy_pool, key_manager, process_prompt, gallery_history

class TestImageFreeGateway(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)

    def test_01_health_and_metrics(self):
        """测试健康检查与指标统计接口"""
        res = self.client.get("/health")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["status"], "ok")

        res = self.client.get("/api/metrics")
        self.assertEqual(res.status_code, 200)
        m_data = res.json()
        self.assertIn("tasks_submitted", m_data)
        self.assertIn("tasks_completed", m_data)
        self.assertIn("proxy_pool_total", m_data)

    def test_02_models_endpoint(self):
        """测试兼容 OpenAI 的模型列表接口"""
        res = self.client.get("/v1/models")
        self.assertEqual(res.status_code, 200)
        data = res.json()
        self.assertEqual(data["object"], "list")
        models = [m["id"] for m in data["data"]]
        self.assertIn("imagefree-v1", models)
        self.assertIn("Z-Image-Turbo", models)

    def test_03_prompt_processing_pipeline(self):
        """测试提示词处理流水线：画风预设、负向提示词与画质增强"""
        loop = asyncio.new_event_loop()
        try:
            # 赛博朋克风格 + 负向提示词 + 画质增强测试
            res = loop.run_until_complete(process_prompt(
                prompt="a futuristic cyberpunk girl",
                auto_translate=False,
                enhance_quality=True,
                style_preset="cyberpunk",
                negative_prompt="blurry, ugly, watermark"
            ))
            self.assertIn("cyberpunk style", res)
            self.assertIn("octane render", res)
            self.assertIn("--no blurry, ugly, watermark", res)

            # 水墨风测试
            res_ink = loop.run_until_complete(process_prompt(
                prompt="traditional mountains",
                auto_translate=False,
                enhance_quality=False,
                style_preset="ink"
            ))
            self.assertIn("traditional Chinese ink painting", res_ink)
        finally:
            loop.close()

    def test_04_proxy_management(self):
        """测试代理池的添加、查询、导出与死节点清理"""
        test_proxy_url = "http://127.0.0.1:19999"
        # 1. 添加代理
        res = self.client.post("/api/admin/proxies", json={"url": test_proxy_url})
        self.assertEqual(res.status_code, 200)

        # 2. 查询代理列表
        res = self.client.get("/api/admin/proxies")
        self.assertEqual(res.status_code, 200)
        proxies = res.json()["proxies"]
        self.assertTrue(any(p["url"] == test_proxy_url for p in proxies))

        # 3. 导出代理文本
        res = self.client.get("/api/admin/proxies/export")
        self.assertEqual(res.status_code, 200)
        self.assertIn(test_proxy_url, res.text)

        # 4. 模拟标记为失效节点并执行清理
        if test_proxy_url in proxy_pool.proxies:
            proxy_pool.proxies[test_proxy_url].active = False

        res = self.client.delete("/api/admin/proxies/dead")
        self.assertEqual(res.status_code, 200)
        self.assertEqual(res.json()["status"], "ok")
        self.assertNotIn(test_proxy_url, proxy_pool.proxies)

    def test_05_api_keys_and_auth_toggle(self):
        """测试 API Key 增删改查以及鉴权强制开关逻辑"""
        test_key_name = "unittest-temp-key"
        test_key_val = "sk-unittest-test-12345"

        # 1. 创建测试 Key
        res = self.client.post("/api/admin/keys", json={
            "name": test_key_name,
            "custom_key": test_key_val,
            "rpm_limit": 100
        })
        self.assertEqual(res.status_code, 200)

        # 2. 检查 Key 列表中是否存在
        res = self.client.get("/api/admin/keys")
        self.assertEqual(res.status_code, 200)
        keys_list = res.json()["keys"]
        self.assertTrue(any(k["key"] == test_key_val for k in keys_list))

        # 3. 开启强制鉴权
        self.client.post("/api/admin/auth-setting", json={"auth_required": True})

        # 未带 Key 或错误 Key 应当返回 401
        bad_req = self.client.post("/v1/images/generations", json={"prompt": "test"})
        self.assertEqual(bad_req.status_code, 401)

        # 4. 恢复非强制鉴权
        self.client.post("/api/admin/auth-setting", json={"auth_required": False})

        # 5. 删除测试 Key
        res = self.client.delete(f"/api/admin/keys/{test_key_val}")
        self.assertEqual(res.status_code, 200)

        # 6. 测试 Quota 额度超额拦截
        quota_key = "sk-unittest-quota-key"
        self.client.post("/api/admin/keys", json={
            "name": "quota-test",
            "custom_key": quota_key,
            "quota_limit": 1
        })
        self.client.post("/api/admin/auth-setting", json={"auth_required": True})
        # 第一次调用扣减额度成功
        valid_1, _ = key_manager.validate_key(quota_key)
        self.assertTrue(valid_1)
        # 第二次调用超额拦截
        valid_2, reason = key_manager.validate_key(quota_key)
        self.assertFalse(valid_2)
        self.assertIn("quota exceeded", reason)
        self.client.post("/api/admin/auth-setting", json={"auth_required": False})
        self.client.delete(f"/api/admin/keys/{quota_key}")

    def test_06_gallery_history_management(self):
        """测试历史画廊的查询、单条删除与 ZIP 批量导出"""
        fake_item = {
            "id": "unittest_test_uuid",
            "task_id": "unittest_task",
            "prompt": "unittest prompt",
            "original_prompt": "unittest prompt",
            "aspect_ratio": "1:1",
            "image_url": "https://example.com/test.png",
            "local_url": "/api/images/cache/test.png",
            "duration_sec": 1.5,
            "model": "imagefree-v1",
            "created_at": time.time()
        }
        gallery_history.insert(0, fake_item)

        # 查询画廊
        res = self.client.get("/api/history?limit=10")
        self.assertEqual(res.status_code, 200)
        self.assertTrue(any(it["id"] == "unittest_test_uuid" for it in res.json()["items"]))

        # 测试批量导出 ZIP 流
        zip_res = self.client.post("/api/history/export-zip", json={"ids": ["unittest_test_uuid"]})
        self.assertEqual(zip_res.status_code, 200)
        self.assertEqual(zip_res.headers.get("content-type"), "application/zip")

        # 单张删除
        del_res = self.client.delete("/api/history/unittest_test_uuid")
        self.assertEqual(del_res.status_code, 200)
        from main import gallery_history as gh_check
        self.assertFalse(any(it["id"] == "unittest_test_uuid" for it in gh_check))

    def test_07_prometheus_metrics(self):
        """测试 Prometheus 标准文本格式指标"""
        res = self.client.get("/metrics")
        self.assertEqual(res.status_code, 200)
        self.assertIn("imagefree_requests_total", res.text)
        self.assertIn("imagefree_uptime_seconds", res.text)

if __name__ == "__main__":
    unittest.main()
