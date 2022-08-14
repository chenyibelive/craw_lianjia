# encoding:utf-8
# Description: 数据可视化
import re

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from preprocess_data import preprocess_data
from tools import sns_set

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
# pd.set_option('display.max_rows', None)


def view_data(df_data):
    """
    数据可视化探索
    @param df_data:
    @return:
    """
    # 声明使用 Seaborn 样式
    sns = sns_set()
    print(df_data.info())
    print(df_data.describe())
    print('*'*50)

    print(df_data.loc[df_data.house_rental_method == '整租'].describe())

    # seaborn 可以直接通过hue 字段对所有的数据进行分箱展示，但是由于合租和整租的数据分布差别太大，可视化效果不佳，这里分开处理
    ax = sns.boxenplot(x=np.ones(df_data[df_data.house_rental_method == '整租'].shape[0]), y='house_rental_price',
                  data=df_data[df_data.house_rental_method == '整租'])
    # 添加标题及相关
    ax.set_title('房租的箱型分布by：陈绎', fontsize=16)
    ax.set_ylabel('房租价格(元)', fontsize=12)
    # ax.set_ylim([0,10000])
    plt.show()

    """多子图的饼图绘制"""
    # 同样适合用饼图表示的特征还有：楼层、电梯、是否有停车位、水电情况、燃气情况、房屋属性等
    fig, axs = plt.subplots(nrows=2, ncols=2)
    # 饼图的数据统计
    house_floor = df_data.house_floor.value_counts()
    house_elevator = df_data.house_elevator.value_counts()
    house_parking = df_data.house_parking.value_counts()
    house_parking.loc['提供车位'] = house_parking.loc['租用车位'] + house_parking.loc['免费使用']
    del house_parking['租用车位']
    del house_parking['免费使用']
    house_gas = df_data.house_gas.value_counts()
    cell_info = df_data.cell_info.value_counts()
    # 饼图的子图显示
    axs[0, 0].pie(house_floor, labels=house_floor.index.tolist(), autopct='%.2f%%', startangle=90, shadow=True)
    axs[0, 1].pie(house_elevator, labels=house_elevator.index.tolist(), autopct='%.2f%%', startangle=90, shadow=True)
    axs[1, 0].pie(house_parking, labels=house_parking.index.tolist(), autopct='%.2f%%', startangle=90,shadow=True)
    # 数字显示有重叠，可以通过设置偏移解决，但最好的解决方式引出一条直线。另外，使用pyecharts可以很大程度避免重叠的问题
    axs[1, 1].pie(house_gas, labels=house_gas.index.tolist(), autopct='%.2f%%', startangle=90, shadow=True,
                  explode=(0, 0, 0.1))
    # 设置标题相关
    axs[0, 0].set_title('楼层高度', fontsize=11)
    axs[0, 1].set_title('是否有电梯', fontsize=11)
    axs[1, 0].set_title('是否提供停车位', fontsize=11)
    axs[1, 1].set_title('是否提供燃气', fontsize=11)
    fig.suptitle('房源特征统计    by:陈绎', fontsize=16)
    plt.show()
    plt.pie(cell_info, labels=cell_info.index.tolist(), autopct='%.2f%%', startangle=90, shadow=True,
                  explode=(0, 0, 0, 0.1))
    plt.title('房屋性质  by:陈绎', fontsize=11)
    plt.show()

    """行政区房屋数量"""
    """统计方法：这里我们直接基于sz的行政区域进行统计"""
    # 数据汇总统计
    house_station = df_data.area.value_counts(ascending=True).tail(10)
    # 绘制条形图
    ax_station = sns.barplot(x=house_station.index.tolist(),
                             y=house_station, palette="Spectral_r",)
    # 添加标题及相关
    ax_station.set_title('南京市鼓楼区房源出租数量统计  by：陈绎', fontsize=16)
    ax_station.set_xlabel('TOP10地区', fontsize=12)
    ax_station.set_ylabel('出租房源数量', fontsize=12)
    # 设置坐标轴刻度的字体大小
    ax_station.tick_params(axis='x', labelsize=8)
    # 显示数据的具体数值
    for x, y in zip(range(0, len(house_station.index.tolist())), house_station):
        ax_station.text(x - 0.2, y + 0.3, '%d' % y, color='black')
    plt.show()

    """房屋标签"""
    """统计方法：方法同区域统计"""
    # 数据汇总统计
    df_data['house_tag'] = df_data['house_tag'].str.split('/')
    house_tag = pd.Series(np.concatenate(df_data['house_tag'])).value_counts(ascending=True)
    del house_tag['']
    # 绘制条形图
    ax_tag = sns.barplot(x=house_tag.index.tolist(), y=house_tag, palette="Spectral_r")
    # 添加标题及相关
    ax_tag.set_title('出租房源标签统计    by:陈绎', fontsize=16)
    ax_tag.set_xlabel('房源标签', fontsize=12)
    ax_tag.set_ylabel('标签频次', fontsize=12)
    # 设置坐标轴刻度的字体大小
    plt.xticks(rotation=30)
    ax_tag.tick_params(axis='x', labelsize=8)
    # 显示数据的具体数值
    for x, y in zip(range(0, len(house_tag.index.tolist())), house_tag):
        ax_tag.text(x - 0.3, y + 0.3, '%d' % y, color='black')
    plt.show()

    """房屋户型"""
    """统计方法：只统计卧室的数量"""
    df_data['house_layout'] = df_data['house_layout'].map(lambda str: re.findall(r'\d+', str)[0]).astype(dtype='int')
    # 通过卧室数量进行排序
    house_layout_zz = df_data.loc[df_data.house_rental_method == '整租', 'house_layout'].value_counts().sort_index()
    # 绘制多条形图
    ax = sns.barplot(x=house_layout_zz.index.tolist(), y=house_layout_zz, palette="Spectral_r")
    # 添加标题及相关
    ax.set_title('房源卧室数量统计  by:陈绎', fontsize=13)
    ax.set_xlabel('卧室个数', fontsize=12)
    ax.set_ylabel('房源数量', fontsize=12)
    # 设置坐标轴刻度的字体大小
    ax.tick_params(axis='x', labelsize=10)
    # 显示数据的具体数值
    for x, y in zip(range(0, len(house_layout_zz.index.tolist())), house_layout_zz):
        ax.text(x - 0.3, y + 0.3, '%d' % y, color='black')
    plt.show()

    plot_station_data(df_data, '鼓楼', 2)

    """楼层、车位、燃气、电梯、小区性质"""
    df_data_zz = df_data.loc[(df_data.station == '鼓楼') & (df_data.house_rental_method == '整租') &
                             (df_data.house_rental_price < 10000), :]

    fig, axs = plt.subplots(nrows=1, ncols=2)
    sns.stripplot(x='house_gas', y='house_rental_price', data=df_data_zz, ax=axs[0])
    sns.stripplot(x='cell_info', y='house_rental_price', data=df_data_zz, ax=axs[1])
    axs[0].set_title('有无燃气与房租分布', fontsize=13)
    axs[1].set_title('小区性质与房租分布', fontsize=13)
    axs[0].set_xlabel('', fontsize=12)
    axs[1].set_xlabel('', fontsize=12)
    axs[0].set_ylabel('房租（元）', fontsize=12)
    axs[1].set_ylabel('', fontsize=12)
    plt.xticks(rotation=30)
    fig.suptitle('不同特征下房租价格分布    by:陈绎', fontsize=16)

    plt.show()
    return df_data


def plot_station_data(df_data, area, tag):
    """
    绘制各个行政区的区域房源数据图
    @param df_data:
    @param area:
    @param tag:
    @return:
    """
    # 分离出整租和合租的房源数据
    df_data_zz = df_data.loc[(df_data.station == area) & (df_data.house_rental_method == '整租') &
                             (df_data.house_rental_price < 10000), :].sort_values('area')
    # 对区域进行汇总统计
    zz_count = df_data.area.value_counts(ascending=True).tail(10)
    street = zz_count.index.tolist()
    df_data_zz = df_data_zz.loc[(df_data_zz['area'].isin(street)),:].sort_values('area')
    if tag == 1:
        ax = sns.boxenplot(x='area', y='house_rental_price', data=df_data_zz)
        # sns.boxenplot(x='area', y='house_rental_price', data=df_data_zz, ax=axs[0, 0])
    else:
        ax = sns.violinplot(x='area', y='house_rental_price', data=df_data_zz)
    # 绘制多条形图

    # 设置坐标轴刻度的字体大小
    ax.set_title('南京市鼓楼区房源价格分布   by:陈绎', fontsize=13)
    ax.set_xlabel('')
    ax.set_ylabel('房租价格（元）', fontsize=12)
    # 设置x 轴标签文字的方向
    plt.xticks(rotation=30)
    # 设置x 轴文字大小
    ax.tick_params(axis='x', labelsize=8)

    plt.show()


if __name__ == '__main__':
    pass