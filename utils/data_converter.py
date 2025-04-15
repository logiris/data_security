import json
import csv
import yaml
from pathlib import Path
from typing import Dict, List, Union, Any
import pandas as pd
import logging

class DataConverter:
    """数据格式转换工具类"""
    
    def __init__(self):
        """初始化转换器"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def load_file(self, file_path: Union[str, Path]) -> Dict[str, Any]:
        """加载文件并自动识别格式
        
        Args:
            file_path: 文件路径
            
        Returns:
            解析后的数据
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
            
        suffix = file_path.suffix.lower()
        try:
            if suffix == '.json':
                return self._load_json(file_path)
            elif suffix == '.csv':
                return self._load_csv(file_path)
            elif suffix in ['.yaml', '.yml']:
                return self._load_yaml(file_path)
            else:
                raise ValueError(f"不支持的文件格式: {suffix}")
        except Exception as e:
            self.logger.error(f"加载文件失败: {e}")
            raise
    
    def save_file(self, 
                 data: Union[Dict, List],
                 output_path: Union[str, Path],
                 format: str = None) -> None:
        """保存数据到指定格式的文件
        
        Args:
            data: 要保存的数据
            output_path: 输出文件路径
            format: 输出格式（json/csv/yaml），如果为None则根据文件后缀自动判断
        """
        output_path = Path(output_path)
        if format is None:
            format = output_path.suffix.lower().lstrip('.')
            
        try:
            if format == 'json':
                self._save_json(data, output_path)
            elif format == 'csv':
                self._save_csv(data, output_path)
            elif format in ['yaml', 'yml']:
                self._save_yaml(data, output_path)
            else:
                raise ValueError(f"不支持的输出格式: {format}")
        except Exception as e:
            self.logger.error(f"保存文件失败: {e}")
            raise
    
    def convert(self,
               input_path: Union[str, Path],
               output_path: Union[str, Path],
               output_format: str = None) -> None:
        """转换文件格式
        
        Args:
            input_path: 输入文件路径
            output_path: 输出文件路径
            output_format: 输出格式，如果为None则根据输出文件后缀自动判断
        """
        data = self.load_file(input_path)
        self.save_file(data, output_path, output_format)
        self.logger.info(f"转换完成: {input_path} -> {output_path}")
    
    def _load_json(self, file_path: Path) -> Dict[str, Any]:
        """加载JSON文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_csv(self, file_path: Path) -> List[Dict[str, Any]]:
        """加载CSV文件"""
        df = pd.read_csv(file_path)
        return df.to_dict('records')
    
    def _load_yaml(self, file_path: Path) -> Dict[str, Any]:
        """加载YAML文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    def _save_json(self, data: Union[Dict, List], output_path: Path) -> None:
        """保存为JSON文件"""
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _save_csv(self, data: Union[Dict, List], output_path: Path) -> None:
        """保存为CSV文件"""
        if isinstance(data, dict):
            data = [data]
        df = pd.DataFrame(data)
        df.to_csv(output_path, index=False, encoding='utf-8')
    
    def _save_yaml(self, data: Union[Dict, List], output_path: Path) -> None:
        """保存为YAML文件"""
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(data, f, allow_unicode=True, sort_keys=False)

# 使用示例
# if __name__ == '__main__':
#     converter = DataConverter()
    
    # # 创建测试数据
    # test_data = {
    #     "users": [
    #         {
    #             "id": 1,
    #             "name": "张三",
    #             "phone": "13800138000",
    #             "address": {
    #                 "city": "北京",
    #                 "district": "朝阳区"
    #             }
    #         },
    #         {
    #             "id": 2,
    #             "name": "李四",
    #             "phone": "13900139000",
    #             "address": {
    #                 "city": "上海",
    #                 "district": "浦东新区"
    #             }
    #         }
    #     ]
    # }
    
    # # 保存为不同格式
    # converter.save_file(test_data, 'test.json')
    # converter.save_file(test_data, 'test.csv')
    # converter.save_file(test_data, 'test.yaml')
    
    # # 格式转换
    # converter.convert('test.json', 'test_from_json.csv')
    # converter.convert('test.csv', 'test_from_csv.yaml')
    # converter.convert('test.yaml', 'test_from_yaml.json')
