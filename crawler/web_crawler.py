import requests
from bs4 import BeautifulSoup
import time
import random
from typing import Dict, List, Optional, Union
from pathlib import Path
import json
import logging
from urllib.parse import urljoin, urlparse
import re
from fake_useragent import UserAgent
import pandas as pd

class WebCrawler:
    """网页爬虫类"""
    
    def __init__(self, 
                 delay: float = 1.0,
                 max_retries: int = 3,
                 timeout: int = 10,
                 use_proxy: bool = False,
                 proxy_list: Optional[List[str]] = None):
        """初始化爬虫
        
        Args:
            delay: 请求延迟时间（秒）
            max_retries: 最大重试次数
            timeout: 请求超时时间（秒）
            use_proxy: 是否使用代理
            proxy_list: 代理列表
        """
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_list = proxy_list or []
        self.ua = UserAgent()
        
        # 配置日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def get_random_headers(self) -> Dict[str, str]:
        """获取随机请求头"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    def get_random_proxy(self) -> Optional[Dict[str, str]]:
        """获取随机代理"""
        if not self.proxy_list:
            return None
        proxy = random.choice(self.proxy_list)
        return {
            'http': proxy,
            'https': proxy
        }
    
    def make_request(self, 
                    url: str, 
                    method: str = 'GET',
                    params: Optional[Dict] = None,
                    data: Optional[Dict] = None,
                    headers: Optional[Dict] = None) -> Optional[requests.Response]:
        """发送HTTP请求
        
        Args:
            url: 请求URL
            method: 请求方法
            params: URL参数
            data: 请求体数据
            headers: 请求头
            
        Returns:
            响应对象
        """
        headers = headers or self.get_random_headers()
        proxies = self.get_random_proxy() if self.use_proxy else None
        
        for attempt in range(self.max_retries):
            try:
                response = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=headers,
                    proxies=proxies,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.delay * (attempt + 1))
                else:
                    self.logger.error(f"Max retries reached for URL: {url}")
                    return None
    
    def crawl_page(self, url: str) -> Optional[Dict]:
        """爬取单个页面
        
        Args:
            url: 页面URL
            
        Returns:
            页面内容字典
        """
        response = self.make_request(url)
        if not response:
            return None
        
        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 提取页面信息
            page_info = {
                'url': url,
                'title': soup.title.string if soup.title else '',
                'text': soup.get_text(),
                'links': [a.get('href') for a in soup.find_all('a', href=True)],
                'images': [img.get('src') for img in soup.find_all('img', src=True)],
                'meta': {meta.get('name', ''): meta.get('content', '') 
                        for meta in soup.find_all('meta')},
                'status_code': response.status_code,
                'headers': dict(response.headers)
            }
            
            # 处理相对URL
            page_info['links'] = [urljoin(url, link) for link in page_info['links']]
            page_info['images'] = [urljoin(url, img) for img in page_info['images']]
            
            return page_info
        except Exception as e:
            self.logger.error(f"Error parsing page {url}: {e}")
            return None
    
    def crawl_site(self, 
                  start_url: str,
                  max_pages: int = 100,
                  allowed_domains: Optional[List[str]] = None,
                  exclude_patterns: Optional[List[str]] = None) -> List[Dict]:
        """爬取整个网站
        
        Args:
            start_url: 起始URL
            max_pages: 最大爬取页面数
            allowed_domains: 允许的域名列表
            exclude_patterns: 排除的URL模式列表
            
        Returns:
            爬取的页面列表
        """
        visited_urls = set()
        pages_to_visit = {start_url}
        crawled_pages = []
        
        # 设置允许的域名
        if allowed_domains is None:
            allowed_domains = [urlparse(start_url).netloc]
        
        # 设置排除模式
        if exclude_patterns is None:
            exclude_patterns = [
                r'\.(jpg|jpeg|png|gif|pdf|doc|docx|xls|xlsx)$',
                r'\.(css|js)$',
                r'#.*$'
            ]
        
        while pages_to_visit and len(crawled_pages) < max_pages:
            url = pages_to_visit.pop()
            
            # 检查URL是否已访问
            if url in visited_urls:
                continue
            
            # 检查URL是否在允许的域名内
            if not any(domain in url for domain in allowed_domains):
                continue
            
            # 检查URL是否匹配排除模式
            if any(re.search(pattern, url) for pattern in exclude_patterns):
                continue
            
            # 爬取页面
            self.logger.info(f"Crawling: {url}")
            page_info = self.crawl_page(url)
            
            if page_info:
                crawled_pages.append(page_info)
                visited_urls.add(url)
                
                # 添加新发现的链接
                for link in page_info['links']:
                    if link not in visited_urls:
                        pages_to_visit.add(link)
            
            # 延迟
            time.sleep(self.delay)
        
        return crawled_pages
    
    def save_results(self, 
                    results: List[Dict],
                    output_dir: Union[str, Path],
                    format: str = 'json'):
        """保存爬取结果
        
        Args:
            results: 爬取结果列表
            output_dir: 输出目录
            format: 输出格式，支持 'json', 'csv'
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        if format == 'json':
            output_file = output_dir / f'crawl_results_{timestamp}.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
        elif format == 'csv':
            # 将结果转换为适合CSV的格式
            csv_data = []
            for page in results:
                csv_data.append({
                    'url': page['url'],
                    'title': page['title'],
                    'status_code': page['status_code'],
                    'num_links': len(page['links']),
                    'num_images': len(page['images'])
                })
            
            output_file = output_dir / f'crawl_results_{timestamp}.csv'
            pd.DataFrame(csv_data).to_csv(output_file, index=False)
        else:
            raise ValueError(f"Unsupported output format: {format}")
        
        self.logger.info(f"Results saved to {output_file}") 