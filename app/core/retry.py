"""通用重试装饰器 - 统一的HTTP请求重试逻辑"""

import asyncio
from typing import Callable, List, Optional, TypeVar, Any
from functools import wraps

from app.core.logger import logger
from app.core.config import setting


T = TypeVar('T')


class RetryConfig:
    """重试配置"""
    
    def __init__(
        self,
        max_outer_retry: int = 3,
        max_403_retry: int = 5,
        retry_codes: Optional[List[int]] = None,
        base_delay: float = 0.1,
        module_name: str = "Retry"
    ):
        self.max_outer_retry = max_outer_retry
        self.max_403_retry = max_403_retry
        self.retry_codes = retry_codes or [401, 429]
        self.base_delay = base_delay
        self.module_name = module_name


async def with_retry(
    request_func: Callable[..., Any],
    get_proxy_func: Callable[[], Any],
    force_refresh_proxy_func: Optional[Callable[[], Any]] = None,
    proxy_pool_enabled: bool = False,
    config: Optional[RetryConfig] = None,
    **request_kwargs
) -> Any:
    """
    通用重试包装器
    
    Args:
        request_func: 执行请求的异步函数，接收 proxy 参数
        get_proxy_func: 获取代理的异步函数
        force_refresh_proxy_func: 强制刷新代理的异步函数（可选）
        proxy_pool_enabled: 是否启用代理池
        config: 重试配置
        **request_kwargs: 传递给 request_func 的额外参数
        
    Returns:
        请求结果
        
    Raises:
        最后一次失败的异常
    """
    if config is None:
        config = RetryConfig()
    
    # 从配置读取重试状态码
    retry_codes = setting.grok_config.get("retry_status_codes", config.retry_codes)
    
    last_error = None
    
    for outer_retry in range(config.max_outer_retry + 1):
        retry_403_count = 0
        
        while retry_403_count <= config.max_403_retry:
            try:
                # 获取代理
                if retry_403_count > 0 and proxy_pool_enabled and force_refresh_proxy_func:
                    logger.info(f"[{config.module_name}] 403重试 {retry_403_count}/{config.max_403_retry}，刷新代理...")
                    proxy = await force_refresh_proxy_func()
                else:
                    proxy = await get_proxy_func()
                
                # 执行请求
                result = await request_func(proxy=proxy, **request_kwargs)
                
                # 检查响应状态码（如果返回的是 Response 对象）
                if hasattr(result, 'status_code'):
                    status_code = result.status_code
                    
                    # 403 内层重试
                    if status_code == 403 and proxy_pool_enabled:
                        retry_403_count += 1
                        if retry_403_count <= config.max_403_retry:
                            logger.warning(f"[{config.module_name}] 遇到403错误，正在重试 ({retry_403_count}/{config.max_403_retry})...")
                            await asyncio.sleep(0.5)
                            continue
                        logger.error(f"[{config.module_name}] 403错误，已重试{retry_403_count-1}次，放弃")
                    
                    # 可配置状态码外层重试
                    if status_code in retry_codes:
                        if outer_retry < config.max_outer_retry:
                            delay = (outer_retry + 1) * config.base_delay
                            logger.warning(f"[{config.module_name}] 遇到{status_code}错误，外层重试 ({outer_retry+1}/{config.max_outer_retry})，等待{delay}s...")
                            await asyncio.sleep(delay)
                            break  # 跳出内层循环
                        else:
                            logger.error(f"[{config.module_name}] {status_code}错误，已重试{outer_retry}次，放弃")
                
                # 成功或非重试状态码
                if outer_retry > 0 or retry_403_count > 0:
                    logger.info(f"[{config.module_name}] 重试成功！")
                
                return result
                
            except Exception as e:
                last_error = e
                if outer_retry < config.max_outer_retry - 1:
                    logger.warning(f"[{config.module_name}] 异常: {e}，外层重试 ({outer_retry+1}/{config.max_outer_retry})...")
                    await asyncio.sleep(0.5)
                    break
                raise
        
        # 内层循环正常结束（非break），说明403重试全部失败
        else:
            continue
    
    # 所有重试都失败
    if last_error:
        raise last_error
    return None


def retry_on_error(
    max_retries: int = 3,
    retry_exceptions: tuple = (Exception,),
    base_delay: float = 0.5,
    module_name: str = "Retry"
):
    """
    简单重试装饰器（用于非HTTP请求场景）
    
    Args:
        max_retries: 最大重试次数
        retry_exceptions: 需要重试的异常类型
        base_delay: 基础延迟时间
        module_name: 模块名称（用于日志）
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            last_error = None
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retry_exceptions as e:
                    last_error = e
                    if attempt < max_retries:
                        delay = (attempt + 1) * base_delay
                        logger.warning(f"[{module_name}] 第{attempt+1}次尝试失败: {e}，{delay}s后重试...")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"[{module_name}] 已重试{max_retries}次，放弃: {e}")
            
            if last_error:
                raise last_error
            return None
        
        return wrapper
    return decorator
