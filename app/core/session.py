"""会话管理器 - 安全的会话管理"""

import time
import secrets
import hashlib
import asyncio
from collections import OrderedDict
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

from app.core.logger import logger


@dataclass
class SessionData:
    """会话数据"""
    user: str
    fingerprint: str  # IP + User-Agent 的哈希
    expires: datetime
    ip: str
    created_at: datetime


class SessionManager:
    """
    安全的会话管理器
    
    特性:
    - 会话过期自动清理
    - 登录失败次数限制（防暴力破解）
    - IP + User-Agent 绑定
    - 最大会话数限制
    """
    
    _instance: Optional['SessionManager'] = None
    
    # 配置
    MAX_SESSIONS = 1000  # 最大会话数
    MAX_LOGIN_ATTEMPTS = 5  # 最大登录失败次数
    LOCKOUT_DURATION = 900  # 锁定时间（秒）= 15分钟
    SESSION_EXPIRE_HOURS = 24  # 会话有效期（小时）
    CLEANUP_INTERVAL = 300  # 清理间隔（秒）= 5分钟
    
    def __new__(cls) -> 'SessionManager':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        # 会话存储（有序字典，便于 LRU 淘汰）
        self._sessions: OrderedDict[str, SessionData] = OrderedDict()
        
        # 登录尝试记录：{user:ip -> (attempts, last_attempt_time)}
        self._login_attempts: Dict[str, Tuple[int, float]] = {}
        
        # 清理任务
        self._cleanup_task: Optional[asyncio.Task] = None
        
        self._initialized = True
        logger.info("[Session] 会话管理器初始化完成")
    
    def _generate_fingerprint(self, ip: str, user_agent: str) -> str:
        """生成客户端指纹（只使用 User-Agent，避免 CDN/代理导致的 IP 变化问题）"""
        # 注意：不再使用 IP，因为 Cloudflare 等 CDN 会导致 IP 频繁变化
        data = f"{user_agent}"
        return hashlib.sha256(data.encode()).hexdigest()[:32]
    
    def _get_attempt_key(self, username: str, ip: str) -> str:
        """生成登录尝试的 key"""
        return f"{username}:{ip}"
    
    def is_locked_out(self, username: str, ip: str) -> bool:
        """检查是否被锁定"""
        key = self._get_attempt_key(username, ip)
        if key not in self._login_attempts:
            return False
        
        attempts, last_attempt = self._login_attempts[key]
        
        # 检查是否超过限制
        if attempts >= self.MAX_LOGIN_ATTEMPTS:
            # 检查锁定是否过期
            if time.time() - last_attempt < self.LOCKOUT_DURATION:
                remaining = int(self.LOCKOUT_DURATION - (time.time() - last_attempt))
                logger.warning(f"[Session] 账户锁定中: {username} from {ip}, 剩余 {remaining}s")
                return True
            else:
                # 锁定已过期，重置计数
                del self._login_attempts[key]
                return False
        
        return False
    
    def record_failed_attempt(self, username: str, ip: str) -> int:
        """记录登录失败，返回剩余尝试次数"""
        key = self._get_attempt_key(username, ip)
        now = time.time()
        
        if key in self._login_attempts:
            attempts, _ = self._login_attempts[key]
            attempts += 1
        else:
            attempts = 1
        
        self._login_attempts[key] = (attempts, now)
        remaining = max(0, self.MAX_LOGIN_ATTEMPTS - attempts)
        
        logger.warning(f"[Session] 登录失败: {username} from {ip}, 剩余尝试: {remaining}")
        return remaining
    
    def clear_failed_attempts(self, username: str, ip: str) -> None:
        """清除登录失败记录（登录成功时调用）"""
        key = self._get_attempt_key(username, ip)
        if key in self._login_attempts:
            del self._login_attempts[key]
    
    def create_session(
        self, 
        username: str, 
        ip: str, 
        user_agent: str = ""
    ) -> Optional[str]:
        """
        创建会话
        
        Args:
            username: 用户名
            ip: 客户端 IP
            user_agent: 客户端 User-Agent
            
        Returns:
            会话令牌，如果被锁定则返回 None
        """
        # 检查是否被锁定
        if self.is_locked_out(username, ip):
            return None
        
        # 生成会话令牌
        token = secrets.token_urlsafe(32)
        fingerprint = self._generate_fingerprint(ip, user_agent)
        now = datetime.now()
        
        # 创建会话数据
        session = SessionData(
            user=username,
            fingerprint=fingerprint,
            expires=now + timedelta(hours=self.SESSION_EXPIRE_HOURS),
            ip=ip,
            created_at=now
        )
        
        # 存储会话
        self._sessions[token] = session
        
        # 清除失败记录
        self.clear_failed_attempts(username, ip)
        
        # 检查是否需要清理
        if len(self._sessions) > self.MAX_SESSIONS:
            self._cleanup_oldest()
        
        logger.info(f"[Session] 创建会话: {username} from {ip}")
        return token
    
    def validate_session(
        self, 
        token: str, 
        ip: Optional[str] = None, 
        user_agent: Optional[str] = None
    ) -> Optional[SessionData]:
        """
        验证会话
        
        Args:
            token: 会话令牌
            ip: 当前客户端 IP（可选，用于指纹验证）
            user_agent: 当前客户端 User-Agent（可选）
            
        Returns:
            会话数据，如果无效则返回 None
        """
        if token not in self._sessions:
            return None
        
        session = self._sessions[token]
        
        # 检查是否过期
        if datetime.now() > session.expires:
            del self._sessions[token]
            logger.debug(f"[Session] 会话已过期: {session.user}")
            return None
        
        # 可选：验证指纹（如果提供了 IP 和 User-Agent）
        if ip and user_agent:
            current_fingerprint = self._generate_fingerprint(ip, user_agent)
            if current_fingerprint != session.fingerprint:
                logger.warning(f"[Session] 指纹不匹配: {session.user}, IP: {ip}")
                # 不删除会话，只是拒绝这次请求（可能是网络变化）
                return None
        
        # 刷新会话位置（LRU）
        self._sessions.move_to_end(token)
        
        return session
    
    def destroy_session(self, token: str) -> bool:
        """销毁会话"""
        if token in self._sessions:
            session = self._sessions[token]
            del self._sessions[token]
            logger.debug(f"[Session] 销毁会话: {session.user}")
            return True
        return False
    
    def _cleanup_oldest(self) -> None:
        """清理最旧的会话（超过限制时）"""
        # 删除超出限制的部分
        while len(self._sessions) > self.MAX_SESSIONS:
            oldest_token = next(iter(self._sessions))
            session = self._sessions[oldest_token]
            del self._sessions[oldest_token]
            logger.debug(f"[Session] 清理旧会话: {session.user}")
    
    def cleanup_expired(self) -> int:
        """清理所有过期会话，返回清理数量"""
        now = datetime.now()
        expired_tokens = [
            token for token, session in self._sessions.items()
            if now > session.expires
        ]
        
        for token in expired_tokens:
            del self._sessions[token]
        
        # 同时清理过期的登录尝试记录
        now_ts = time.time()
        expired_attempts = [
            key for key, (_, last_attempt) in self._login_attempts.items()
            if now_ts - last_attempt > self.LOCKOUT_DURATION
        ]
        
        for key in expired_attempts:
            del self._login_attempts[key]
        
        if expired_tokens or expired_attempts:
            logger.debug(f"[Session] 清理过期: {len(expired_tokens)} 会话, {len(expired_attempts)} 登录记录")
        
        return len(expired_tokens)
    
    async def start_cleanup_task(self) -> None:
        """启动定期清理任务"""
        if self._cleanup_task is not None:
            return
        
        async def _cleanup_loop():
            while True:
                try:
                    await asyncio.sleep(self.CLEANUP_INTERVAL)
                    self.cleanup_expired()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"[Session] 清理任务异常: {e}")
        
        self._cleanup_task = asyncio.create_task(_cleanup_loop())
        logger.info("[Session] 定期清理任务已启动")
    
    async def stop_cleanup_task(self) -> None:
        """停止清理任务"""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
            logger.info("[Session] 定期清理任务已停止")
    
    def get_stats(self) -> Dict:
        """获取会话统计"""
        now = datetime.now()
        active = sum(1 for s in self._sessions.values() if now < s.expires)
        
        return {
            "total_sessions": len(self._sessions),
            "active_sessions": active,
            "pending_lockouts": len(self._login_attempts)
        }


# 全局实例
session_manager = SessionManager()
