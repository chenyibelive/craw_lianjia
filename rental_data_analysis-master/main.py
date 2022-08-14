
import pandas as pd
import numpy as np

# 显示所有列
from preprocess_data import preprocess_data
from read_data import read_data
from view_data import view_data

pd.set_option('display.max_columns', None)



if __name__ == '__main__':
    df_data = read_data()

    """数据预处理"""
    df_data = preprocess_data(df_data)

    """可视化分析"""
    df_data = view_data(df_data)

