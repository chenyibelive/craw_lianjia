# encoding:utf-8

# Description: 部分工具

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib as plt

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
# pd.set_option('display.max_rows', None)


def sns_set():
    """
    sns 相关设置
    @return:
    """
    # 声明使用 Seaborn 样式
    sns.set()
    # 有五种seaborn的绘图风格，它们分别是：darkgrid, whitegrid, dark, white, ticks。默认的主题是darkgrid。
    sns.set_style("whitegrid")
    # 有四个预置的环境，按大小从小到大排列分别为：paper, notebook, talk, poster。其中，notebook是默认的。
    sns.set_context('talk')
    # 中文字体设置-黑体
    plt.rcParams['font.sans-serif'] = ['SimHei']
    # 解决保存图像是负号'-'显示为方块的问题
    plt.rcParams['axes.unicode_minus'] = False
    # 解决Seaborn中文显示问题并调整字体大小
    sns.set(font='SimHei')

    return sns

if __name__ == '__main__':
    pass