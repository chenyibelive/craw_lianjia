
# Description: 数据预处理
import re
from collections import Counter

import pandas as pd
import numpy as np

from read_data import read_data

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
# pd.set_option('display.max_rows', None)


def preprocess_data(df_data):
    """
    数据清洗
    @param df_data:
    """
    print(df_data.info())

    print(df_data[df_data['house_longitude'].isnull()])




    # 即删除房源编号、供暖、房屋备注、房源维护时间和创建时间字段
    df_data.drop(columns=['city', 'house_id', 'house_update_time', 'create_time', 'house_heating', 'house_note'],
                 axis=1, inplace=True)

    df_data['station'] = df_data['house_address'].apply(lambda str: str.split('-')[0])
    df_data['area'] = df_data['house_address'].apply(lambda str: str.split('-')[1])
    df_data['address'] = df_data['house_address'].apply(lambda str: str.split('-')[2])
    df_data['house_longitude'] = df_data['house_longitude'].astype(dtype='float')
    df_data['house_latitude'] = df_data['house_latitude'].astype(dtype='float')
    df_data['area'] = df_data['area'].astype(dtype='str')
    print("房屋经度数据类型为：{0}，纬度数据类型为：{1}".format(
        df_data['house_longitude'].dtype, df_data['house_latitude'].dtype))
    """4. 房屋出租面积：剔除m²，并将数据转换成int"""
    # df_data['house_rental_area'] = df_data['house_rental_area'].str.replace('㎡', '').astype(dtype='int')
    df_data['house_rental_area'] = df_data['house_rental_area'].str.replace('㎡', '').astype(dtype='float').astype(dtype='int')
    print("房屋出租面积数据类型为：{0}".format(df_data['house_rental_area'].dtype))
    print(df_data.house_layout.value_counts())
    # 通过同小区的数据进行填充
    df_data.loc[df_data.house_layout.str.contains('未知'), 'house_layout'] = \
        df_data.loc[df_data.house_layout.str.contains('未知'), ['house_rental_area', 'address']].\
            apply(lambda str: get_mode_layout(str[0], str[1], df_data), axis=1)

    df_data['house_rental_price'] = \
        df_data['house_rental_price'].map(lambda str: re.findall(r'\d+', str)[0]).astype(dtype='int')
    print("房租价格数据类型为：{0}".format(df_data['house_rental_price'].dtype))

    # 去掉房屋标签的头尾 /
    df_data['house_tag'] = df_data['house_tag'].str.slice(1, -1)
    # 对房屋标签进行汇总统计
    print("房屋标签共有{0}个".format(
        pd.Series(np.concatenate(df_data['house_tag'].str.split('/'))).value_counts().shape[0]))

    df_data.loc[~df_data.house_floor.str.contains('未知'), 'house_floor'] = \
        df_data.loc[~df_data.house_floor.str.contains('未知'), 'house_floor'].apply(lambda str: str.split('（')[0])

    df_data.loc[df_data.house_floor.str.contains('未知'), 'house_floor'] = \
        df_data.loc[df_data.house_floor.str.contains('未知'), 'address'].apply(lambda str: get_mode_floor(str, df_data))

    df_data.loc[df_data.house_elevator == '暂无数据', ['house_elevator']] = \
        df_data.loc[df_data.house_elevator == '暂无数据', 'address'].apply(lambda str: get_mode_elevator(str, df_data))
    # 这里我们继续填充，这次根据前面的楼层范围字段进行填充，若是中高楼层则填充为有电梯，否则填充为无
    df_data.loc[df_data.house_elevator == '无法填充', 'house_elevator'] = \
        df_data.loc[df_data.house_elevator == '无法填充', 'house_floor'].apply(lambda str: get_like_elevator(str))
    # 通过水电字段分为普通住宅、商业住宅、商住两用三种
    df_data['cell_info'] = '暂无数据'
    df_data.loc[(df_data['house_water'] == '民水') & (df_data['house_electricity'] == '民电'), 'cell_info'] = '普通住宅'
    df_data.loc[(df_data['house_water'] == '商水') & (df_data['house_electricity'] == '商电'), 'cell_info'] = '商业住宅'
    df_data.loc[((df_data['house_water'] == '民水') & ~(df_data['house_electricity'] == '民电') |
                 ~(df_data['house_water'] == '民水') & (df_data['house_electricity'] == '民电')), 'cell_info'] = '商住两用'
    print(df_data['cell_info'].value_counts())


    return df_data


def get_mode_address(str_address, data):
    """
    通过同名小区的区域进行填充
    @param str_address:
    @param data:
    @return:
    """
    # 确定同名的小区，且区域不为空
    data = data[(data.address == str_address) & ~(data.area == '')]
    # 利用地址进行填充
    str_address = data.area
    if len(str_address) == 0:
        return ''
    else:
        return str_address.iloc[0]


def get_mode_layout(str_area, str_address, data):
    """
    根据近似户型+普遍标准进行填充
    @param str_area:
    @param str_address:
    @param data:
    @return:
    """
    # 确定同名的小区，且楼层数据不为未知
    data = data[(data.address == str_address) & ~(data.house_layout.str.contains('未知'))]
    # 在20m²范围内波动，则默认是同一个户型
    data_like = data.loc[abs(data.house_rental_area-str_area) <= 20, 'house_layout']

    # 如果无法根据近似户型判断，则根据面积普遍标准进行判断
    if data_like.size < 1:
        if str_area < 45:
            return "1室0厅0卫"
        elif str_area < 90:
            return "2室0厅0卫"
        elif str_area < 150:
            return "3室0厅0卫"
        else:
            return "4室0厅0卫"

    return data_like.mode()[0]


def get_mode_floor(str_address,  data):
    """
    根据众数进行填充
    @param str_address:
    @param data:
    @return:
    """
    # 确定同名的小区，且楼层数据不为未知
    data = data.loc[(data.address == str_address) & ~(data.house_floor.str.contains('未知'))]
    # 利用众数进行填充
    mode_floor = data.loc[data.address == str_address, 'house_floor'].mode()[0]
    if len(mode_floor) == 0:
        return '无法填充'
    else:
        return mode_floor


def get_mode_elevator(str_address,  data):
    """
    根据众数进行填充
    @param str_address:
    @param data:
    @return:
    """
    # 确定同名的小区，且电梯数据不为暂无数据
    data = data.loc[(data.address == str_address) & ~(data.house_elevator == '暂无数据')]
    # 利用众数进行填充
    mode_elevvator = data.loc[data.address == str_address, 'house_elevator'].mode()
    if len(mode_elevvator) == 0:
        return '无法填充'
    else:
        return mode_elevvator[0]


def get_like_elevator(str):
    """
    根据楼层字段填充电梯字段
    @param str:
    @return:
    """
    if '低' in str:
        return '无'

    return '有'


if __name__ == '__main__':
    pass