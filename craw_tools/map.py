# encoding:utf-8
# ------------------------------
# FileName: map
# Description: 不同类型经纬度的转换
# ------------------------------
import csv
import json
import math
from math import sqrt, atan2, cos, sin, radians, fabs, asin
# 腾讯地图经纬度转换成百度经纬度
import requests as requests
import pandas as pd

x_pi = 3.14159265358979324 * 3000.0 / 180.0
pi = 3.1415926535897932384626  # π


def map_tx2bd(lng, lat):
    '''
    腾讯转百度
    :param lng:
    :param lat:
    :return:
    '''
    x = lng
    y = lat
    z = sqrt(x * x + y * y) + 0.00002 * sin(y * pi)
    theta = atan2(y, x) + 0.000003 * cos(x * pi)
    bd_lng = z * cos(theta) + 0.0065
    bd_lat = z * sin(theta) + 0.006

    return round(bd_lng, 5), round(bd_lat, 5)


def map_gps2bd(lng, lat):
    '''
    gps 经纬度转换为 baidu 经纬度
    :param lng:
    :param lat:
    :return:
    '''
    ak = '你的ak'
    url = 'http://api.map.baidu.com/geoconv/v1/?coords=' + str(lng) + ',' + str(lat) + '&from=1&to=5&ak='+ak

    content = requests.get(url).content
    data = json.loads(content)
    result = data['result']
    lng = result[0]['x']
    lat = result[0]['y']

    return round(lng, 5), round(lat, 5)


def bd09_to_gcj02(bd_lon, bd_lat):
    """
    百度坐标系(BD-09)转火星坐标系(GCJ-02)
    百度——>谷歌、高德
    :param bd_lat:百度坐标纬度
    :param bd_lon:百度坐标经度
    :return:转换后的坐标列表形式
    """
    x = bd_lon - 0.0065
    y = bd_lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * x_pi)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * x_pi)
    lng = z * math.cos(theta)
    lat = z * math.sin(theta)

    return round(lng, 5), round(lat, 5)


def hav(theta):
    s = sin(theta / 2)
    return s * s


def get_distance(lnglat1, lnglat2):
    '''
    计算两经纬度点之间的距离
    :param lnglat1:
    :param lnglat2:
    :return:
    '''
    EARTH_RADIUS = 6371  # 地球平均半径，6371km

    "用haversine公式计算球面两点间的距离。"
    lng1 = lnglat1[0]
    lat1 = lnglat1[1]
    lng2 = lnglat2[0]
    lat2 = lnglat2[1]
    # 经纬度转换成弧度
    dlng = fabs(radians(lng1) - radians(lng2))
    dlat = fabs(radians(lat1) - radians(lat2))

    h = hav(dlat) + cos(radians(lat1)) * cos(radians(lat2)) * hav(dlng)
    distance = 2 * EARTH_RADIUS * asin(sqrt(h))

    return round(distance, 5)


if __name__ == '__main__':
    lng, lat = bd09_to_gcj02(113.331248,23.121341)
    print(lng, lat)
