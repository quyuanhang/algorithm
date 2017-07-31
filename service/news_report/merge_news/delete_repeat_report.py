#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
每个话题只推荐一个
并根据历史记录过滤
'''
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 內建库
import json
import collections
import datetime
# 自建库
import file_path_lib


def read_file(file_):
    with open(file_) as f:
        data_dict = json.load(f)
    return data_dict


def write_file(file_, obj_):
    with open(file_, mode='w') as f:
        f.write(json.dumps(obj_, ensure_ascii=False, indent=4))
    return 0


def flit_topic(topic_dict, invalid_dict):
    flit_dict = collections.OrderedDict()
    flit_list = list()
    for topic, reports in topic_dict.items():
        reports_ = [report for report in reports.values() if 'time' in report]
        reports__ = sorted(reports_, key=lambda x: x['time'])
        flit_dict[topic] = dict()
        flit_dict[topic][reports__[0]['url']] = reports__[0]
    return flit_dict


def main():
    # 声明文件路径================================================
    file_path = file_path_lib.file_path
    topic_file = file_path['data_file']['topic_file']
    history_file = file_path['data_file']['history_file']
    flit_file = file_path['data_file']['flit_file']
    # ================================================

    invalid_dict = read_file(history_file)
    topic_dict = read_file(topic_file)

    flited = flit_topic(topic_dict, invalid_dict)

    write_file(flit_file, flited)

if __name__ == "__main__":
    sys.exit(main())
