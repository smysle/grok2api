"""请求统计服务 - 记录和分析API使用情况（支持Redis持久化）"""

import os
import time
import asyncio
import bisect
from collections import deque, defaultdict
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

from app.core.logger import logger


@dataclass
class RequestRecord:
    """单次请求记录"""
    timestamp: float
    model: str
    success: bool
    duration: float  # 秒
    error_code: Optional[str] = None


@dataclass
class ErrorRecord:
    """错误记录"""
    timestamp: float
    model: str
    error_code: str
    error_message: str
    count: int = 1  # 合并后的次数


class StatsService:
    """统计服务（单例）- 支持Redis持久化"""
    
    _instance: Optional['StatsService'] = None
    
    # 保留24小时的详细数据
    MAX_RECORDS = 100000
    # 最多保留1000条错误记录
    MAX_ERRORS = 1000
    
    # Redis keys
    REDIS_KEY_RECORDS = "grok:stats:records"
    REDIS_KEY_COUNTERS = "grok:stats:counters"
    REDIS_KEY_START_TIME = "grok:stats:start_time"
    REDIS_KEY_DAILY_STATS = "grok:stats:daily"  # 每日统计
    REDIS_KEY_HOURLY_STATS = "grok:stats:hourly"  # 每小时统计（7天）
    REDIS_KEY_ERRORS = "grok:stats:errors"  # 错误记录
    
    # 同步间隔（秒）
    SYNC_INTERVAL = 60
    
    def __new__(cls) -> 'StatsService':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        
        # 使用 deque 自动限制大小
        self._records: deque[RequestRecord] = deque(maxlen=self.MAX_RECORDS)
        # 时间戳索引（用于二分查找）
        self._timestamps: deque[float] = deque(maxlen=self.MAX_RECORDS)
        self._lock = asyncio.Lock()
        
        # 实时计数器（用于RPM计算）
        self._minute_requests: deque[float] = deque()  # 最近1分钟的请求时间戳
        
        # 累计统计（不会丢失）
        self._total_requests = 0
        self._total_success = 0
        self._total_failed = 0
        self._start_time = time.time()
        
        # 每日统计 {日期: {total, success, failed}}
        self._daily_stats: Dict[str, Dict[str, int]] = {}
        
        # 每小时统计 {小时key: {total, success, failed}} - 保留7天(168小时)
        self._hourly_stats: Dict[str, Dict[str, int]] = {}
        
        # 错误收集 {error_key: ErrorRecord}
        self._errors: Dict[str, ErrorRecord] = {}
        self._error_list: deque[ErrorRecord] = deque(maxlen=self.MAX_ERRORS)
        
        # Redis 相关
        self._redis = None
        self._sync_task: Optional[asyncio.Task] = None
        self._use_redis = os.getenv("STORAGE_MODE", "file").lower() == "redis"
        
        self._initialized = True
        logger.info(f"[Stats] 统计服务初始化完成 (Redis持久化: {self._use_redis})")
    
    async def init_redis(self) -> None:
        """初始化Redis连接并恢复数据"""
        if not self._use_redis:
            return
        
        try:
            import redis.asyncio as aioredis
            
            redis_url = os.getenv("DATABASE_URL", "")
            if not redis_url:
                logger.warning("[Stats] Redis URL未配置，禁用持久化")
                self._use_redis = False
                return
            
            self._redis = aioredis.Redis.from_url(
                redis_url, encoding="utf-8", decode_responses=True
            )
            await self._redis.ping()
            logger.info("[Stats] Redis连接成功")
            
            # 恢复历史数据
            await self._restore_from_redis()
            
            # 启动定时同步任务
            self._sync_task = asyncio.create_task(self._periodic_sync())
            
        except ImportError:
            logger.warning("[Stats] redis未安装，禁用持久化")
            self._use_redis = False
        except Exception as e:
            logger.error(f"[Stats] Redis初始化失败: {e}，禁用持久化")
            self._use_redis = False
    
    async def _restore_from_redis(self) -> None:
        """从Redis恢复统计数据"""
        if not self._redis:
            return
        
        try:
            import orjson
            
            # 恢复累计计数器
            counters = await self._redis.hgetall(self.REDIS_KEY_COUNTERS)
            if counters:
                self._total_requests = int(counters.get("total_requests", 0))
                self._total_success = int(counters.get("total_success", 0))
                self._total_failed = int(counters.get("total_failed", 0))
                logger.info(f"[Stats] 恢复累计统计: 总请求={self._total_requests}, 成功={self._total_success}, 失败={self._total_failed}")
            
            # 恢复启动时间（使用原始启动时间，不是当前时间）
            start_time = await self._redis.get(self.REDIS_KEY_START_TIME)
            if start_time:
                self._start_time = float(start_time)
                logger.info(f"[Stats] 恢复启动时间: {datetime.fromtimestamp(self._start_time).strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                # 首次启动，保存启动时间
                await self._redis.set(self.REDIS_KEY_START_TIME, str(self._start_time))
            
            # 恢复最近24小时的请求记录
            now = time.time()
            cutoff = now - 86400  # 24小时前
            
            records_json = await self._redis.lrange(self.REDIS_KEY_RECORDS, 0, -1)
            restored_count = 0
            
            for record_str in records_json:
                try:
                    record_dict = orjson.loads(record_str)
                    # 只恢复24小时内的记录
                    if record_dict["timestamp"] >= cutoff:
                        record = RequestRecord(**record_dict)
                        self._records.append(record)
                        self._timestamps.append(record.timestamp)
                        restored_count += 1
                except Exception as e:
                    logger.warning(f"[Stats] 解析记录失败: {e}")
            
            if restored_count > 0:
                logger.info(f"[Stats] 恢复 {restored_count} 条请求记录")
            
            # 恢复每日统计
            daily_data = await self._redis.get(self.REDIS_KEY_DAILY_STATS)
            if daily_data:
                self._daily_stats = orjson.loads(daily_data)
                logger.info(f"[Stats] 恢复 {len(self._daily_stats)} 天的每日统计")
            
            # 恢复每小时统计
            hourly_data = await self._redis.get(self.REDIS_KEY_HOURLY_STATS)
            if hourly_data:
                self._hourly_stats = orjson.loads(hourly_data)
                logger.info(f"[Stats] 恢复 {len(self._hourly_stats)} 小时的统计")
            
            # 恢复错误记录
            errors_data = await self._redis.get(self.REDIS_KEY_ERRORS)
            if errors_data:
                errors_list = orjson.loads(errors_data)
                for e in errors_list:
                    error_record = ErrorRecord(**e)
                    hour = datetime.fromtimestamp(error_record.timestamp).strftime("%Y-%m-%d %H")
                    error_key = f"{hour}:{error_record.model}:{error_record.error_code}"
                    self._errors[error_key] = error_record
                logger.info(f"[Stats] 恢复 {len(self._errors)} 条错误记录")
                
        except Exception as e:
            logger.error(f"[Stats] 从Redis恢复数据失败: {e}")
    
    async def _periodic_sync(self) -> None:
        """定期同步数据到Redis"""
        while True:
            try:
                await asyncio.sleep(self.SYNC_INTERVAL)
                await self._sync_to_redis()
            except asyncio.CancelledError:
                # 服务关闭，最后同步一次
                await self._sync_to_redis()
                break
            except Exception as e:
                logger.error(f"[Stats] 定时同步失败: {e}")
    
    async def _sync_to_redis(self) -> None:
        """同步统计数据到Redis"""
        if not self._redis:
            return
        
        try:
            import orjson
            
            async with self._lock:
                # 保存累计计数器
                await self._redis.hset(self.REDIS_KEY_COUNTERS, mapping={
                    "total_requests": str(self._total_requests),
                    "total_success": str(self._total_success),
                    "total_failed": str(self._total_failed)
                })
                
                # 保存最近的请求记录（只保留24小时内的）
                now = time.time()
                cutoff = now - 86400
                
                # 清空旧记录，重新写入
                pipe = self._redis.pipeline()
                pipe.delete(self.REDIS_KEY_RECORDS)
                
                for record in self._records:
                    if record.timestamp >= cutoff:
                        record_json = orjson.dumps(asdict(record)).decode()
                        pipe.rpush(self.REDIS_KEY_RECORDS, record_json)
                
                # 设置过期时间（25小时，留点余量）
                pipe.expire(self.REDIS_KEY_RECORDS, 90000)
                
                await pipe.execute()
                
                # 保存每日统计（保留35天）
                if self._daily_stats:
                    # 清理超过35天的数据
                    cutoff_date = datetime.now().strftime("%Y-%m-%d")
                    keys_to_delete = []
                    for date_str in list(self._daily_stats.keys()):
                        try:
                            date = datetime.strptime(date_str, "%Y-%m-%d")
                            if (datetime.now() - date).days > 35:
                                keys_to_delete.append(date_str)
                        except:
                            pass
                    for k in keys_to_delete:
                        del self._daily_stats[k]
                    
                    await self._redis.set(
                        self.REDIS_KEY_DAILY_STATS, 
                        orjson.dumps(self._daily_stats).decode(),
                        ex=86400 * 35  # 35天过期
                    )
                
                # 保存每小时统计（保留8天）
                if self._hourly_stats:
                    self._cleanup_hourly_stats()
                    await self._redis.set(
                        self.REDIS_KEY_HOURLY_STATS,
                        orjson.dumps(self._hourly_stats).decode(),
                        ex=86400 * 8  # 8天过期
                    )
                
                # 保存错误记录（保留7天）
                if self._errors:
                    errors_list = [asdict(e) for e in self._errors.values()]
                    await self._redis.set(
                        self.REDIS_KEY_ERRORS,
                        orjson.dumps(errors_list).decode(),
                        ex=86400 * 7  # 7天过期
                    )
                
            logger.debug(f"[Stats] 同步到Redis完成: {len(self._records)} 条记录")
            
        except Exception as e:
            logger.error(f"[Stats] 同步到Redis失败: {e}")
    
    def record_request(
        self, 
        model: str, 
        success: bool, 
        duration: float,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> None:
        """记录一次请求（同步方法，快速返回）"""
        now = time.time()
        today = datetime.fromtimestamp(now).strftime("%Y-%m-%d")
        
        record = RequestRecord(
            timestamp=now,
            model=model,
            success=success,
            duration=duration,
            error_code=error_code
        )
        
        self._records.append(record)
        self._timestamps.append(now)
        self._minute_requests.append(now)
        
        # 更新累计统计
        self._total_requests += 1
        if success:
            self._total_success += 1
        else:
            self._total_failed += 1
        
        # 更新每日统计
        if today not in self._daily_stats:
            self._daily_stats[today] = {"total": 0, "success": 0, "failed": 0}
        self._daily_stats[today]["total"] += 1
        if success:
            self._daily_stats[today]["success"] += 1
        else:
            self._daily_stats[today]["failed"] += 1
        
        # 更新每小时统计（用于7天曲线）
        hour_key = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:00")
        if hour_key not in self._hourly_stats:
            self._hourly_stats[hour_key] = {"total": 0, "success": 0, "failed": 0}
        self._hourly_stats[hour_key]["total"] += 1
        if success:
            self._hourly_stats[hour_key]["success"] += 1
        else:
            self._hourly_stats[hour_key]["failed"] += 1
        
        # 清理过期的小时统计（保留8天，多留1天余量）
        self._cleanup_hourly_stats()
        
        # 记录错误
        if not success and error_code:
            self._record_error(now, model, error_code, error_message or error_code)
        
        # 清理过期的分钟数据
        cutoff = now - 60
        while self._minute_requests and self._minute_requests[0] < cutoff:
            self._minute_requests.popleft()
    
    def _cleanup_hourly_stats(self) -> None:
        """清理过期的小时统计"""
        cutoff = datetime.now() - timedelta(days=8)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:00")
        keys_to_delete = [k for k in self._hourly_stats if k < cutoff_str]
        for k in keys_to_delete:
            del self._hourly_stats[k]
    
    def _record_error(self, timestamp: float, model: str, error_code: str, error_message: str) -> None:
        """记录错误（归纳合并相同错误）"""
        # 生成错误 key（模型+错误码，1小时内合并）
        hour = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H")
        error_key = f"{hour}:{model}:{error_code}"
        
        if error_key in self._errors:
            # 已有相同错误，增加计数
            self._errors[error_key].count += 1
            self._errors[error_key].timestamp = timestamp  # 更新最后发生时间
        else:
            # 新错误
            error_record = ErrorRecord(
                timestamp=timestamp,
                model=model,
                error_code=error_code,
                error_message=error_message[:200] if error_message else error_code,  # 限制长度
                count=1
            )
            self._errors[error_key] = error_record
            self._error_list.append(error_record)
        
        # 清理过期错误（保留7天）
        cutoff = timestamp - 7 * 86400
        expired_keys = [k for k, v in self._errors.items() if v.timestamp < cutoff]
        for k in expired_keys:
            del self._errors[k]
    
    def get_rpm(self) -> float:
        """获取当前 RPM（每分钟请求数）"""
        now = time.time()
        cutoff = now - 60
        
        # 清理过期数据
        while self._minute_requests and self._minute_requests[0] < cutoff:
            self._minute_requests.popleft()
        
        return len(self._minute_requests)
    
    def _get_records_since(self, cutoff: float) -> List[RequestRecord]:
        """使用二分查找获取指定时间之后的记录"""
        if not self._timestamps:
            return []
        
        # 转换为列表进行二分查找
        timestamps_list = list(self._timestamps)
        idx = bisect.bisect_left(timestamps_list, cutoff)
        
        # 返回从idx开始的所有记录
        records_list = list(self._records)
        return records_list[idx:]
    
    def get_stats(self, hours: int = 24) -> Dict[str, Any]:
        """获取统计数据
        
        Args:
            hours: 统计时间范围（小时），默认24小时
        """
        now = time.time()
        cutoff = now - (hours * 3600)
        
        # 使用二分查找获取时间范围内的记录
        recent_records = self._get_records_since(cutoff)
        
        # 基础统计
        total = len(recent_records)
        success = sum(1 for r in recent_records if r.success)
        failed = total - success
        success_rate = (success / total * 100) if total > 0 else 0
        
        # 按模型统计
        model_stats: Dict[str, Dict[str, int]] = {}
        for r in recent_records:
            if r.model not in model_stats:
                model_stats[r.model] = {"total": 0, "success": 0, "failed": 0}
            model_stats[r.model]["total"] += 1
            if r.success:
                model_stats[r.model]["success"] += 1
            else:
                model_stats[r.model]["failed"] += 1
        
        # 按小时统计（用于图表）
        hourly_stats: Dict[str, Dict[str, int]] = {}
        for r in recent_records:
            hour_key = datetime.fromtimestamp(r.timestamp).strftime("%Y-%m-%d %H:00")
            if hour_key not in hourly_stats:
                hourly_stats[hour_key] = {"total": 0, "success": 0, "failed": 0}
            hourly_stats[hour_key]["total"] += 1
            if r.success:
                hourly_stats[hour_key]["success"] += 1
            else:
                hourly_stats[hour_key]["failed"] += 1
        
        # 错误统计
        error_stats: Dict[str, int] = {}
        for r in recent_records:
            if not r.success and r.error_code:
                error_stats[r.error_code] = error_stats.get(r.error_code, 0) + 1
        
        # 平均响应时间
        durations = [r.duration for r in recent_records if r.success]
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        # RPM
        rpm = self.get_rpm()
        
        # 运行时间
        uptime_seconds = now - self._start_time
        uptime_hours = uptime_seconds / 3600
        
        return {
            "period_hours": hours,
            "total_requests": total,
            "success_count": success,
            "failed_count": failed,
            "success_rate": round(success_rate, 2),
            "rpm": rpm,
            "avg_duration": round(avg_duration, 2),
            "model_stats": model_stats,
            "hourly_stats": hourly_stats,
            "error_stats": error_stats,
            "uptime_hours": round(uptime_hours, 2),
            "lifetime_stats": {
                "total_requests": self._total_requests,
                "total_success": self._total_success,
                "total_failed": self._total_failed,
                "success_rate": round(self._total_success / self._total_requests * 100, 2) if self._total_requests > 0 else 0
            }
        }
    
    def get_realtime_stats(self) -> Dict[str, Any]:
        """获取实时统计（轻量级，用于频繁刷新）"""
        now = time.time()
        
        # 最近5分钟的统计（使用二分查找）
        cutoff_5min = now - 300
        recent_5min = self._get_records_since(cutoff_5min)
        
        total_5min = len(recent_5min)
        success_5min = sum(1 for r in recent_5min if r.success)
        
        return {
            "rpm": self.get_rpm(),
            "requests_5min": total_5min,
            "success_5min": success_5min,
            "failed_5min": total_5min - success_5min,
            "success_rate_5min": round(success_5min / total_5min * 100, 2) if total_5min > 0 else 0,
            "total_requests": self._total_requests,
            "total_success": self._total_success,
            "total_failed": self._total_failed
        }
    
    def get_period_stats(self) -> Dict[str, Any]:
        """获取24小时/7天/30天统计（含时间曲线）"""
        now = time.time()
        today = datetime.fromtimestamp(now).strftime("%Y-%m-%d")
        
        # 计算各时间段的统计
        stats_24h = {"total": 0, "success": 0, "failed": 0}
        stats_7d = {"total": 0, "success": 0, "failed": 0}
        stats_30d = {"total": 0, "success": 0, "failed": 0}
        
        # 30天按天曲线 (30个点)
        daily_30d: Dict[str, Dict[str, int]] = {}
        
        # 从每日统计中累加
        for date_str, daily in self._daily_stats.items():
            try:
                date = datetime.strptime(date_str, "%Y-%m-%d")
                days_ago = (datetime.now() - date).days
                
                if days_ago < 30:
                    stats_30d["total"] += daily["total"]
                    stats_30d["success"] += daily["success"]
                    stats_30d["failed"] += daily["failed"]
                    daily_30d[date_str] = daily.copy()
                    
                    if days_ago < 7:
                        stats_7d["total"] += daily["total"]
                        stats_7d["success"] += daily["success"]
                        stats_7d["failed"] += daily["failed"]
                        
                        if days_ago < 1:
                            stats_24h["total"] += daily["total"]
                            stats_24h["success"] += daily["success"]
                            stats_24h["failed"] += daily["failed"]
            except:
                pass
        
        # 7天按小时曲线：直接从持久化的小时统计获取
        cutoff_7d = datetime.now() - timedelta(days=7)
        cutoff_7d_str = cutoff_7d.strftime("%Y-%m-%d %H:00")
        
        hourly_7d = {k: v.copy() for k, v in self._hourly_stats.items() if k >= cutoff_7d_str}
        
        def calc_rate(s):
            return round(s["success"] / s["total"] * 100, 2) if s["total"] > 0 else 0
        
        # 排序曲线数据
        hourly_7d_sorted = dict(sorted(hourly_7d.items()))
        daily_30d_sorted = dict(sorted(daily_30d.items()))
        
        return {
            "24h": {**stats_24h, "success_rate": calc_rate(stats_24h)},
            "7d": {**stats_7d, "success_rate": calc_rate(stats_7d), "hourly": hourly_7d_sorted},
            "30d": {**stats_30d, "success_rate": calc_rate(stats_30d), "daily": daily_30d_sorted},
            "daily_breakdown": self._daily_stats
        }
    
    def get_errors(self, hours: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
        """获取错误列表（归纳合并后）
        
        Args:
            hours: 时间范围（小时）
            limit: 最大返回数量
        """
        now = time.time()
        cutoff = now - hours * 3600
        
        # 过滤时间范围内的错误
        errors = []
        for error in self._errors.values():
            if error.timestamp >= cutoff:
                errors.append({
                    "time": datetime.fromtimestamp(error.timestamp).strftime("%Y-%m-%d %H:%M:%S"),
                    "model": error.model,
                    "error_code": error.error_code,
                    "error_message": error.error_message,
                    "count": error.count
                })
        
        # 按时间倒序，取最近的
        errors.sort(key=lambda x: x["time"], reverse=True)
        return errors[:limit]
    
    def get_error_summary(self) -> Dict[str, Any]:
        """获取错误汇总"""
        now = time.time()
        
        # 按错误码统计
        by_code: Dict[str, int] = defaultdict(int)
        # 按模型统计
        by_model: Dict[str, int] = defaultdict(int)
        # 24小时内的错误
        recent_24h = 0
        cutoff_24h = now - 86400
        
        for error in self._errors.values():
            by_code[error.error_code] += error.count
            by_model[error.model] += error.count
            if error.timestamp >= cutoff_24h:
                recent_24h += error.count
        
        return {
            "total_errors": sum(by_code.values()),
            "errors_24h": recent_24h,
            "by_error_code": dict(sorted(by_code.items(), key=lambda x: -x[1])),
            "by_model": dict(sorted(by_model.items(), key=lambda x: -x[1]))
        }
    
    async def close(self) -> None:
        """关闭服务"""
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        
        # 最后同步一次
        await self._sync_to_redis()
        
        if self._redis:
            await self._redis.close()
            logger.info("[Stats] Redis连接已关闭")


# 全局实例
stats_service = StatsService()
