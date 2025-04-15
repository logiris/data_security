import argparse
from pathlib import Path
import logging
from typing import Optional

from crawler.web_crawler import WebCrawler
from detection.malware_detector import MalwareDetector
from log_analysis.log_parser import LogParser
from data_cleaning.data_cleaner import DataCleaner
from utils.regex_utils import SecurityDetector

def setup_logging():
    """配置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='数据安全工具集')
    
    # 通用参数
    parser.add_argument('--output-dir', type=str, default='output',
                       help='输出目录')
    
    # 爬虫参数
    parser.add_argument('--crawl', action='store_true',
                       help='启用爬虫功能')
    parser.add_argument('--url', type=str,
                       help='要爬取的URL')
    parser.add_argument('--max-pages', type=int, default=100,
                       help='最大爬取页面数')
    
    # 日志分析参数
    parser.add_argument('--analyze-logs', action='store_true',
                       help='启用日志分析功能')
    parser.add_argument('--log-file', type=str,
                       help='日志文件路径')
    parser.add_argument('--log-type', type=str, default='nginx',
                       choices=['nginx', 'apache', 'mysql_slow'],
                       help='日志类型')
    
    # 数据清洗参数
    parser.add_argument('--clean-data', action='store_true',
                       help='启用数据清洗功能')
    parser.add_argument('--input-file', type=str,
                       help='输入文件路径')
    parser.add_argument('--file-type', type=str, default='csv',
                       choices=['csv', 'excel', 'json'],
                       help='文件类型')
    
    # 恶意代码检测参数
    parser.add_argument('--detect-malware', action='store_true',
                       help='启用恶意代码检测功能')
    parser.add_argument('--text', type=str,
                       help='要检测的文本')
    parser.add_argument('--use-bert', action='store_true',
                       help='使用BERT模型进行检测')
    
    return parser.parse_args()

def main():
    """主函数"""
    args = parse_args()
    logger = setup_logging()
    
    # 创建输出目录
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 爬虫功能
    if args.crawl and args.url:
        logger.info("开始爬取网页...")
        crawler = WebCrawler()
        results = crawler.crawl_site(
            start_url=args.url,
            max_pages=args.max_pages
        )
        crawler.save_results(results, output_dir / 'crawl_results')
        logger.info(f"爬取完成，结果保存在 {output_dir / 'crawl_results'}")
    
    # 日志分析功能
    if args.analyze_logs and args.log_file:
        logger.info("开始分析日志...")
        parser = LogParser(log_type=args.log_type)
        entries = parser.parse_file(args.log_file)
        anomalies = parser.detect_anomalies(entries)
        
        # 输出异常检测结果
        for anomaly_type, anomaly_entries in anomalies.items():
            logger.info(f"发现 {len(anomaly_entries)} 个{anomaly_type}异常")
            for entry in anomaly_entries[:5]:  # 只显示前5个异常
                logger.info(f"异常详情: {entry.raw_line}")
    
    # 数据清洗功能
    if args.clean_data and args.input_file:
        logger.info("开始清洗数据...")
        cleaner = DataCleaner()
        
        # 加载数据
        df = cleaner.load_data(args.input_file, args.file_type)
        
        # 添加清洗规则（示例）
        cleaner.add_cleaning_rule('price', 'fill_na', method='mean')
        cleaner.add_cleaning_rule('category', 'remove_duplicates')
        cleaner.add_cleaning_rule('sales', 'remove_outliers', method='zscore')
        
        # 执行清洗
        cleaned_df = cleaner.clean_data(df)
        
        # 保存结果
        output_file = output_dir / f'cleaned_{Path(args.input_file).name}'
        cleaner.save_data(cleaned_df, output_file, args.file_type)
        logger.info(f"数据清洗完成，结果保存在 {output_file}")
    
    # 恶意代码检测功能
    if args.detect_malware and args.text:
        logger.info("开始检测恶意代码...")
        detector = MalwareDetector(use_bert=args.use_bert)
        results = detector.detect(args.text)
        
        # 输出检测结果
        logger.info("检测结果:")
        for category, result in results.items():
            if category == 'is_malicious':
                logger.info(f"是否恶意: {result}")
            elif isinstance(result, dict):
                if 'is_malicious' in result:
                    logger.info(f"{category}: {result['is_malicious']}")
                    if result['is_malicious']:
                        logger.info(f"匹配项: {result['matches']}")
                elif 'malicious' in result:
                    logger.info(f"{category}: 恶意概率 {result['malicious']:.2%}")

if __name__ == '__main__':
    main() 