
# Description: 

import pandas as pd
import numpy as np
import warnings

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

warnings.filterwarnings('ignore')

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
# pd.set_option('display.max_rows', None)


def init_selenium():
    """
    初始化 selenium
    @return:
    """
    executable_path = "D:\software\install\chromedriver_win32\chromedriver.exe"
    # 设置不弹窗显示
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    browser = webdriver.Chrome(chrome_options=chrome_options, executable_path=executable_path)
    # 设置弹窗显示
    browser = webdriver.Chrome(executable_path=executable_path)

    return browser


if __name__ == '__main__':
    pass