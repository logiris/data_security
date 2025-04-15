import requests
from bs4 import BeautifulSoup
import time
import random
from typing import Dict, List, Optional, Union
from pathlib import Path
import json
import logging
from urllib.parse import urljoin, urlparse, parse_qsl, urlunparse, urlencode
import re
from fake_useragent import UserAgent
import pandas as pd

class WebCrawler:
    """改进型分页爬虫类"""
    
    def __init__(self, 
                 delay: float = 1.0,
                 max_retries: int = 3,
                 timeout: int = 10,
                 use_proxy: bool = False,
                 proxy_list: Optional[List[str]] = None):
        """初始化爬虫参数"""
        self.delay = delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.use_proxy = use_proxy
        self.proxy_list = proxy_list or []
        self.ua = UserAgent()
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def get_random_headers(self) -> Dict[str, str]:
        """随机生成请求头"""
        return {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    def get_random_proxy(self) -> Optional[Dict[str, str]]:
        """随机获取代理"""
        return {'http': random.choice(self.proxy_list),
                'https': random.choice(self.proxy_list)} if self.use_proxy and self.proxy_list else None

    def make_request(self, 
                    url: str, 
                    method: str = 'GET',
                    params: Optional[Dict] = None,
                    data: Optional[Dict] = None,
                    headers: Optional[Dict] = None) -> Optional[requests.Response]:
        """带重试机制的请求函数"""
        headers = headers or self.get_random_headers()
        proxies = self.get_random_proxy()
        
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
                time.sleep(self.delay * (attempt + 1))
        self.logger.error(f"Max retries reached for URL: {url}")
        return None

    def get_soup(self, url: str) -> Optional[BeautifulSoup]:
        """获取BeautifulSoup对象"""
        response = self.make_request(url)
        if response:
            return BeautifulSoup(response.text, 'html.parser')
        return None

    def crawl_paginated_data(self,
                            start_url: str,
                            data_selector: str,
                            next_button_selector: Optional[str] = None,
                            page_param: Optional[str] = None,
                            max_pages: int = 10,
                            allowed_domains: Optional[List[str]] = None,
                            exclude_patterns: Optional[List[str]] = None) -> List[Dict]:
        """分页爬取特定数据
        
        Args:
            start_url: 起始URL
            data_selector: 数据选择器
            next_button_selector: 下一页按钮选择器，可选
            page_param: 分页参数名，可选（如：page、p等）
            max_pages: 最大爬取页数
            allowed_domains: 允许的域名列表
            exclude_patterns: 排除的URL模式列表
            
        Returns:
            爬取的数据列表
        """
        crawled_data = []
        current_url = start_url
        page_count = 0
        allowed_domains = allowed_domains or [urlparse(start_url).netloc]
        exclude_patterns = exclude_patterns or []
        
        while True:
            # 获取当前页面内容
            soup = self.get_soup(current_url)
            if not soup:
                break
                
            # 提取当前页面数据
            data_elements = soup.select(data_selector)
            if not data_elements:
                self.logger.error(f"No data found with selector {data_selector}")
                break
            crawled_data.append({
                'current_url': current_url,
                'data': [{
                    'html': str(e),
                    'text': e.get_text(strip=True),
                    'attributes': {attr: e[attr] for attr in e.attrs}
                } for e in data_elements]
            })
            page_count += 1
            self.logger.info(f"Page {page_count} collected successfully")
            
            # 检查是否达到最大页数
            if page_count >= max_pages:
                self.logger.info("达到最大页数限制")
                break
                
            # 获取下一页URL
            next_url = None
            if next_button_selector:
                # 通过选择器查找下一页
                next_button = soup.select_one(next_button_selector)
                if next_button and next_button.has_attr('href'):
                    next_url = urljoin(current_url, next_button['href'])
            elif page_param:
                # 通过URL参数构造下一页
                parsed_url = urlparse(current_url)
                query_params = dict(parse_qsl(parsed_url.query))
                current_page = int(query_params.get(page_param, 1))
                query_params[page_param] = str(current_page + 1)
                next_url = urlunparse((
                    parsed_url.scheme,
                    parsed_url.netloc,
                    parsed_url.path,
                    parsed_url.params,
                    urlencode(query_params),
                    parsed_url.fragment
                ))
            else:
                self.logger.info("未提供分页方式")
                break
                
            if not next_url:
                self.logger.info("未找到下一页")
                break
                
            # 检查域名和排除模式
            parsed_next = urlparse(next_url)
            if any(domain in parsed_next.netloc for domain in allowed_domains):
                # 排除模式检查
                if not any(re.search(pattern, next_url) for pattern in exclude_patterns):
                    current_url = next_url
                    time.sleep(self.delay)
                else:
                    self.logger.info("当前下一页链接被排除模式过滤")
                    break
            else:
                self.logger.info("当前下一页链接不在白名单域名中")
                break
                
        return crawled_data

    def save_results(self,
                    data: List[Dict],
                    output_dir: Union[str, Path],
                    format: str = 'json'):
        """保存爬取结果"""
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        
        if format == 'json':
            output_file = output_dir / f'crawl_results_{timestamp}.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        elif format == 'csv':
            # 转换为适合CSV的格式
            csv_rows = []
            for page in data:
                for element in page['data']:
                    row = {
                        'URL': page['current_url'],
                        'Text Content': element['text'],
                        'HTML Content': element['html']
                    }
                    row.update(element['attributes'])  # 添加元素属性
                    csv_rows.append(row)
                    
            output_file = output_dir / f'crawl_results_{timestamp}.csv'
            pd.DataFrame(csv_rows).to_csv(output_file, index=False)
        else:
            raise ValueError("支持的格式：json 或 csv")
        
        self.logger.info(f"数据已保存到 {output_file}")

# 使用示例
# 可以通过更改crawled_data.append中的内容来设置不同数据
crawler = WebCrawler()
results = crawler.crawl_paginated_data(
    start_url="http://localhost:5000",
    max_pages=3,
    data_selector=".comment",
    page_param="page"  # 使用URL参数分页
)
crawler.save_results(results, output_dir='./output', format='json')