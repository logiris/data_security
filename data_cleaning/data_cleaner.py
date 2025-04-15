import pandas as pd
import numpy as np
from typing import Dict, List, Union, Any, Optional
from pathlib import Path
import re
from datetime import datetime

class DataCleaner:
    """数据清洗器类"""
    
    def __init__(self):
        """初始化数据清洗器"""
        self.cleaning_rules = {}
    
    def load_data(self, file_path: Union[str, Path], file_type: str = 'csv') -> pd.DataFrame:
        """加载数据文件
        
        Args:
            file_path: 文件路径
            file_type: 文件类型，支持 'csv', 'excel', 'json'
            
        Returns:
            加载的数据框
        """
        if file_type == 'csv':
            return pd.read_csv(file_path)
        elif file_type == 'excel':
            return pd.read_excel(file_path)
        elif file_type == 'json':
            return pd.read_json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    
    def save_data(self, df: pd.DataFrame, file_path: Union[str, Path], file_type: str = 'csv'):
        """保存数据文件
        
        Args:
            df: 数据框
            file_path: 文件路径
            file_type: 文件类型，支持 'csv', 'excel', 'json'
        """
        if file_type == 'csv':
            df.to_csv(file_path, index=False)
        elif file_type == 'excel':
            df.to_excel(file_path, index=False)
        elif file_type == 'json':
            df.to_json(file_path, orient='records')
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    
    def add_cleaning_rule(self, column: str, rule_type: str, **kwargs):
        """添加数据清洗规则
        
        Args:
            column: 列名
            rule_type: 规则类型，支持以下类型：
                - 'fill_na': 填充缺失值
                - 'remove_duplicates': 删除重复值
                - 'standardize': 标准化
                - 'remove_outliers': 删除异常值
                - 'convert_type': 转换数据类型
                - 'regex_replace': 正则替换
            **kwargs: 规则参数
        """
        if column not in self.cleaning_rules:
            self.cleaning_rules[column] = []
        
        self.cleaning_rules[column].append({
            'type': rule_type,
            'params': kwargs
        })
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用所有清洗规则
        
        Args:
            df: 原始数据框
            
        Returns:
            清洗后的数据框
        """
        cleaned_df = df.copy()
        
        for column, rules in self.cleaning_rules.items():
            if column not in cleaned_df.columns:
                continue
                
            for rule in rules:
                rule_type = rule['type']
                params = rule['params']
                
                if rule_type == 'fill_na':
                    cleaned_df[column] = self._fill_na(cleaned_df[column], **params)
                elif rule_type == 'remove_duplicates':
                    cleaned_df = self._remove_duplicates(cleaned_df, column, **params)
                elif rule_type == 'standardize':
                    cleaned_df[column] = self._standardize(cleaned_df[column], **params)
                elif rule_type == 'remove_outliers':
                    cleaned_df = self._remove_outliers(cleaned_df, column, **params)
                elif rule_type == 'convert_type':
                    cleaned_df[column] = self._convert_type(cleaned_df[column], **params)
                elif rule_type == 'regex_replace':
                    cleaned_df[column] = self._regex_replace(cleaned_df[column], **params)
        
        return cleaned_df
    
    def _fill_na(self, series: pd.Series, method: str = 'mean', value: Any = None) -> pd.Series:
        """填充缺失值
        
        Args:
            series: 数据列
            method: 填充方法，支持 'mean', 'median', 'mode', 'ffill', 'bfill', 'value'
            value: 当method为'value'时的填充值
            
        Returns:
            填充后的数据列
        """
        if method == 'mean':
            return series.fillna(series.mean())
        elif method == 'median':
            return series.fillna(series.median())
        elif method == 'mode':
            return series.fillna(series.mode()[0])
        elif method == 'ffill':
            return series.fillna(method='ffill')
        elif method == 'bfill':
            return series.fillna(method='bfill')
        elif method == 'value':
            return series.fillna(value)
        else:
            raise ValueError(f"Unsupported fill method: {method}")
    
    def _remove_duplicates(self, df: pd.DataFrame, column: str, keep: str = 'first') -> pd.DataFrame:
        """删除重复值
        
        Args:
            df: 数据框
            column: 列名
            keep: 保留方式，支持 'first', 'last', False
            
        Returns:
            删除重复值后的数据框
        """
        return df.drop_duplicates(subset=[column], keep=keep)
    
    def _standardize(self, series: pd.Series, method: str = 'zscore') -> pd.Series:
        """标准化数据
        
        Args:
            series: 数据列
            method: 标准化方法，支持 'zscore', 'minmax'
            
        Returns:
            标准化后的数据列
        """
        if method == 'zscore':
            return (series - series.mean()) / series.std()
        elif method == 'minmax':
            return (series - series.min()) / (series.max() - series.min())
        else:
            raise ValueError(f"Unsupported standardization method: {method}")
    
    def _remove_outliers(self, df: pd.DataFrame, column: str, method: str = 'zscore', threshold: float = 3.0) -> pd.DataFrame:
        """删除异常值
        
        Args:
            df: 数据框
            column: 列名
            method: 异常值检测方法，支持 'zscore', 'iqr'
            threshold: 阈值
            
        Returns:
            删除异常值后的数据框
        """
        if method == 'zscore':
            z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
            return df[z_scores < threshold]
        elif method == 'iqr':
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1
            return df[~((df[column] < (Q1 - 1.5 * IQR)) | (df[column] > (Q3 + 1.5 * IQR)))]
        else:
            raise ValueError(f"Unsupported outlier detection method: {method}")
    
    def _convert_type(self, series: pd.Series, target_type: str) -> pd.Series:
        """转换数据类型
        
        Args:
            series: 数据列
            target_type: 目标类型，支持 'int', 'float', 'str', 'datetime'
            
        Returns:
            转换类型后的数据列
        """
        if target_type == 'int':
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif target_type == 'float':
            return pd.to_numeric(series, errors='coerce')
        elif target_type == 'str':
            return series.astype(str)
        elif target_type == 'datetime':
            return pd.to_datetime(series, errors='coerce')
        else:
            raise ValueError(f"Unsupported target type: {target_type}")
    
    def _regex_replace(self, series: pd.Series, pattern: str, replacement: str) -> pd.Series:
        """正则替换
        
        Args:
            series: 数据列
            pattern: 正则表达式模式
            replacement: 替换字符串
            
        Returns:
            替换后的数据列
        """
        return series.str.replace(pattern, replacement, regex=True) 