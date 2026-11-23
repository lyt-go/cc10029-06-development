"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, window_export

# 闭合窗口明细导出等专用路由先注册，通用资源路由兜底。
ROUTERS = [window_export, resource]
