"""请求追踪中间件 - 为每个请求添加唯一 ID"""

import uuid
from contextvars import ContextVar
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from app.core.logger import logger


# 上下文变量，用于在请求生命周期内传递 request_id
request_id_ctx: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


def get_request_id() -> Optional[str]:
    """获取当前请求的 ID"""
    return request_id_ctx.get()


class RequestTracingMiddleware(BaseHTTPMiddleware):
    """
    请求追踪中间件
    
    功能：
    - 为每个请求生成或提取唯一 ID
    - 在响应头中返回请求 ID
    - 通过 contextvars 在请求生命周期内共享 ID
    """
    
    async def dispatch(self, request: Request, call_next) -> Response:
        # 尝试从请求头获取 request_id，否则生成新的
        request_id = request.headers.get("X-Request-Id")
        
        if not request_id:
            request_id = str(uuid.uuid4())[:8]  # 使用短格式 ID
        
        # 设置到上下文变量
        token = request_id_ctx.set(request_id)
        
        try:
            # 记录请求开始
            logger.debug(f"[{request_id}] {request.method} {request.url.path}")
            
            # 处理请求
            response = await call_next(request)
            
            # 添加响应头
            response.headers["X-Request-Id"] = request_id
            
            return response
            
        finally:
            # 重置上下文变量
            request_id_ctx.reset(token)
