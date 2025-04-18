import re
from datetime import datetime
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from pathlib import Path

# 目前用处不大

@dataclass
class LogEntry:
    """日志条目数据类"""
    timestamp: datetime
    ip: str
    method: str
    path: str
    status_code: int
    response_time: float
    user_agent: str
    referrer: str
    request_params: Dict[str, str]
    raw_line: str

class LogParser:
    """通用日志解析器"""
    
    # Nginx日志格式模式
    NGINX_PATTERN = r'(?P<ip>\S+) - - \[(?P<timestamp>.*?)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>.*?)" (?P<status>\d+) (?P<size>\d+) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'
    
    # Apache日志格式模式
    APACHE_PATTERN = r'(?P<ip>\S+) - - \[(?P<timestamp>.*?)\] "(?P<method>\S+) (?P<path>\S+) (?P<protocol>.*?)" (?P<status>\d+) (?P<size>\d+)'
    
    # MySQL慢查询日志格式模式
    MYSQL_SLOW_PATTERN = r'# Time: (?P<timestamp>.*?)\n# User@Host: (?P<user>.*?) @ (?P<host>.*?) \[(?P<ip>.*?)\]\n# Query_time: (?P<query_time>.*?) Lock_time: (?P<lock_time>.*?) Rows_sent: (?P<rows_sent>.*?) Rows_examined: (?P<rows_examined>.*?)\n(?P<query>.*?);'
    
    def __init__(self, log_type: str = 'nginx'):
        """初始化日志解析器
        
        Args:
            log_type: 日志类型，支持 'nginx', 'apache', 'mysql_slow'
        """
        self.log_type = log_type
        self.pattern = self._get_pattern(log_type)
    
    def _get_pattern(self, log_type: str) -> str:
        """获取对应日志类型的正则表达式模式"""
        patterns = {
            'nginx': self.NGINX_PATTERN,
            'apache': self.APACHE_PATTERN,
            'mysql_slow': self.MYSQL_SLOW_PATTERN
        }
        return patterns.get(log_type, self.NGINX_PATTERN)
    
    def parse_line(self, line: str) -> Optional[LogEntry]:
        """解析单行日志
        
        Args:
            line: 日志行
            
        Returns:
            解析后的日志条目，如果解析失败则返回None
        """
        try:
            match = re.match(self.pattern, line)
            if not match:
                return None
            
            data = match.groupdict()
            
            # 解析时间戳
            timestamp = datetime.strptime(
                data['timestamp'].split()[0], 
                '%d/%b/%Y:%H:%M:%S' if self.log_type in ['nginx', 'apache'] else '%y%m%d %H:%M:%S'
            )
            
            # 解析请求参数
            request_params = {}
            if '?' in data['path']:
                path, params = data['path'].split('?', 1)
                for param in params.split('&'):
                    if '=' in param:
                        key, value = param.split('=', 1)
                        request_params[key] = value
            else:
                path = data['path']
            
            return LogEntry(
                timestamp=timestamp,
                ip=data['ip'],
                method=data['method'],
                path=path,
                status_code=int(data['status']),
                response_time=float(data.get('query_time', 0)),
                user_agent=data.get('user_agent', ''),
                referrer=data.get('referrer', ''),
                request_params=request_params,
                raw_line=line
            )
        except Exception as e:
            print(f"Error parsing line: {e}")
            return None
    
    def parse_file(self, file_path: Union[str, Path]) -> List[LogEntry]:
        """解析日志文件
        
        Args:
            file_path: 日志文件路径
            
        Returns:
            解析后的日志条目列表
        """
        entries = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                entry = self.parse_line(line.strip())
                if entry:
                    entries.append(entry)
        return entries
    
    def detect_anomalies(self, entries: List[LogEntry]) -> Dict[str, List[LogEntry]]:
        """检测异常日志条目
        
        Args:
            entries: 日志条目列表
            
        Returns:
            包含异常检测结果的字典
        """
        anomalies = {
            'high_response_time': [],
            'error_status': [],
            'suspicious_ips': [],
            'suspicious_paths': []
        }
        
        for entry in entries:
            # 检测高响应时间
            if entry.response_time > 1.0:  # 超过1秒
                anomalies['high_response_time'].append(entry)
            
            # 检测错误状态码
            if entry.status_code >= 400:
                anomalies['error_status'].append(entry)
            
            # 检测可疑IP（示例：来自特定国家/地区的IP）
            if self._is_suspicious_ip(entry.ip):
                anomalies['suspicious_ips'].append(entry)
            
            # 检测可疑路径
            if self._is_suspicious_path(entry.path):
                anomalies['suspicious_paths'].append(entry)
        
        return anomalies
    
    def _is_suspicious_ip(self, ip: str) -> bool:
        """判断IP是否可疑"""
        # 这里可以添加更多的IP检测逻辑
        suspicious_ranges = [
            '192.168.0.0/16',
            '10.0.0.0/8',
            '172.16.0.0/12'
        ]
        return any(ip.startswith(prefix) for prefix in suspicious_ranges)
    
    def _is_suspicious_path(self, path: str) -> bool:
        """判断路径是否可疑"""
        suspicious_patterns = [
            r'\.\./',
            r'\.php',
            r'\.asp',
            r'\.jsp',
            r'admin',
            r'login',
            r'config'
        ]
        return any(re.search(pattern, path, re.IGNORECASE) for pattern in suspicious_patterns) 