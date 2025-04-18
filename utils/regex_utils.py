import re
from typing import List, Dict, Optional
from collections import defaultdict
import json

class SecurityPatterns:
    """安全检测相关的正则表达式模式"""
    
    # SQL注入模式
    SQL_INJECTION_PATTERNS = {
        'union_select': r'(?i)UNION\s+SELECT',
        'sleep': r'(?i)SLEEP\s*\(',
        'benchmark': r'(?i)BENCHMARK\s*\(',
        'information_schema': r'(?i)INFORMATION_SCHEMA',
        'comment': r'(?i)--\s*$|/\*.*?\*/',
        'concat': r'(?i)CONCAT\s*\(',
        'group_concat': r'(?i)GROUP_CONCAT\s*\(',
    }
    
    # XSS攻击模式
    XSS_PATTERNS = {
        'script_tag': r'<script[^>]*>.*?</script>',
        'javascript': r'javascript:',
        'on_event': r'on\w+\s*=',
        'eval': r'eval\s*\(',
        'document_cookie': r'document\.cookie',
    }
    
    # 命令注入模式
    COMMAND_INJECTION_PATTERNS = {
        'system': r'(?i)system\s*\(',
        'exec': r'(?i)exec\s*\(',
        'shell_exec': r'(?i)shell_exec\s*\(',
        'backtick': r'`.*?`',
        'pipe': r'\|.*?$',
    }
    
    # 敏感信息模式
    SENSITIVE_PATTERNS = {
        'phone': r'1[3-9]\d{9}',
        'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
        'ip': r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}',
        'id_card': r'[1-9]\d{5}(19|20)\d{2}(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{3}[\dXx]',
    }

class SecurityDetector:
    """安全检测器类"""
    
    def __init__(self):
        self.patterns = SecurityPatterns()
    
    def detect_sql_injection(self, text: str) -> Dict[str, List[str]]:
        """检测SQL注入攻击
        
        Args:
            text: 待检测文本
            
        Returns:
            包含检测结果的字典，key为攻击类型，value为匹配到的内容列表
        """
        results = {}
        for pattern_name, pattern in self.patterns.SQL_INJECTION_PATTERNS.items():
            matches = re.findall(pattern, text)
            if matches:
                results[pattern_name] = matches
        return results
    
    def detect_xss(self, text: str) -> Dict[str, List[str]]:
        """检测XSS攻击
        
        Args:
            text: 待检测文本
            
        Returns:
            包含检测结果的字典，key为攻击类型，value为匹配到的内容列表
        """
        results = {}
        for pattern_name, pattern in self.patterns.XSS_PATTERNS.items():
            matches = re.findall(pattern, text)
            if matches:
                results[pattern_name] = matches
        return results
    
    def detect_command_injection(self, text: str) -> Dict[str, List[str]]:
        """检测命令注入攻击
        
        Args:
            text: 待检测文本
            
        Returns:
            包含检测结果的字典，key为攻击类型，value为匹配到的内容列表
        """
        results = {}
        for pattern_name, pattern in self.patterns.COMMAND_INJECTION_PATTERNS.items():
            matches = re.findall(pattern, text)
            if matches:
                results[pattern_name] = matches
        return results
    
    def find_sensitive_info(self, text: str) -> Dict[str, List[str]]:
        """查找敏感信息
        
        Args:
            text: 待检测文本
            
        Returns:
            包含检测结果的字典，key为敏感信息类型，value为匹配到的内容列表
        """
        results = {}
        for pattern_name, pattern in self.patterns.SENSITIVE_PATTERNS.items():
            matches = re.findall(pattern, text)
            if matches:
                results[pattern_name] = matches
        return results
    
    def desensitize_text(self, text: str) -> str:
        """对文本中的敏感信息进行脱敏处理
        
        Args:
            text: 待脱敏文本
            
        Returns:
            脱敏后的文本
        """
        desensitized_text = text
        
        # 手机号脱敏
        desensitized_text = re.sub(
            self.patterns.SENSITIVE_PATTERNS['phone'],
            lambda m: m.group(0)[:3] + '****' + m.group(0)[-4:],
            desensitized_text
        )
        
        # 邮箱脱敏
        desensitized_text = re.sub(
            self.patterns.SENSITIVE_PATTERNS['email'],
            lambda m: m.group(0)[0] + '****' + m.group(0)[m.group(0).index('@'):],
            desensitized_text
        )
        
        # IP地址脱敏
        desensitized_text = re.sub(
            self.patterns.SENSITIVE_PATTERNS['ip'],
            lambda m: m.group(0)[:m.group(0).rindex('.')] + '.xxx',
            desensitized_text
        )
        
        return desensitized_text 
    

# 使用示例
f = open("http.log", 'r', encoding='utf-8')
target_file = f.read()

detector = SecurityDetector()
sensitive_info = detector.find_sensitive_info(target_file)
phone_counts = defaultdict(int)
for phone in sensitive_info['phone']:
    if phone in phone_counts:
        phone_counts[phone] += 1
    else:
        phone_counts[phone] = 1

# print(phone_counts)
sorted_counts = sorted(phone_counts.items(), key=lambda x: x[1], reverse=True)
print(sorted_counts)
# with open('sensitive_info_results.json', 'w', encoding='utf-8') as f:
#     json.dump(sensitive_info, f, indent=2, ensure_ascii=False)

