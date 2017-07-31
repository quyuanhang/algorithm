#!/usr/bin/env python
# -*- coding: utf8 -*-
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 內建库
import json
import collections
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


def evaluate(topic_dict_, invalid_urls_, invalid_dict_, 
        repeat_report_file, repeat_cluster_file, reject_file, miss_file):
    '''
    precision_num 记录包含正样本的topic
    precision_1_num 记录仅包含一条正样本的topic

    '''

    repeat_reports_dict = collections.OrderedDict()
    repeat_topic_dict = collections.OrderedDict()
    reject_dict = collections.OrderedDict()
    precision_num = 0
    prec_1_num = 0
    positive = len(invalid_urls_)
    predict = len(topic_dict_)
    invalid_urls_ = set(invalid_urls_)
    for topic, reports in topic_dict_.items():
        reports_urls = set(reports.keys())
        prec_set = (reports_urls & invalid_urls_)
        if prec_set:
            precision_num += 1
            if len(prec_set) == 1:
                prec_1_num += 1
            else:
                repeat_reports_dict[topic] = dict()
                repeat_topic_dict[topic] = topic_dict_[topic]
                for url in prec_set:
                    repeat_reports_dict[topic][url] = topic_dict_[topic][url]
            invalid_urls_ -= reports_urls
        else:
            reject_dict[topic] = topic_dict_[topic]

    if invalid_urls_:
        miss_dict = collections.OrderedDict()
        for url in invalid_urls_:
            miss_dict[url] = invalid_dict_[url]
        with open(miss_file, 'w') as file:
            file.write(json.dumps(miss_dict,
                                  ensure_ascii=False, indent=4))

    with open(repeat_report_file, 'w') as file:
        file.write(json.dumps(repeat_reports_dict,
                              ensure_ascii=False, indent=4))
    with open(repeat_cluster_file, 'w') as file:
        file.write(json.dumps(repeat_topic_dict,
                              ensure_ascii=False, indent=4))
    with open(reject_file, 'w') as file:
        file.write(json.dumps(reject_dict,
                              ensure_ascii=False, indent=4))
    precision = precision_num / (predict + 0.01)
    prec_1 = prec_1_num / (predict + 0.01)
    recall = precision_num / (positive + 0.01)
    recall_1 = prec_1_num / (positive + 0.01)
    print('precision_num, prec_1_num, positive, predict, miss num by event',
          precision_num, prec_1_num, positive, predict, len(invalid_urls_))
    return [precision, recall, prec_1, recall_1]


def main():

    # 声明文件路径================================================
    file_path = file_path_lib.file_path
    flit_file = file_path['data_file']['flit_file']
    topic_file = file_path['data_file']['topic_file']
    test_file = file_path['data_file']['test_file']
    repeat_report_file = file_path['data_file']['repeat_report_file']
    repeat_cluster_file = file_path['data_file']['repeat_cluster_file']
    reject_file = file_path['data_file']['reject_file']
    miss_file = file_path['data_file']['miss_file']
    # ================================================


    # 读取标记数据===========================================
    invalid_dict = read_file(test_file)
    invalid_urls = set()
    for url, report in invalid_dict.items():
        if report['is_invalid'] == 2:
            invalid_urls.add(url)
    base_urls = invalid_dict.keys()
    print('base_precess = %3f' % (len(invalid_urls) / (len(base_urls) + 0.1)),
          len(invalid_urls), len(base_urls))
    # ========================================================

    # 读取过滤数据============================================
    print('每个cluster选择一条report')
    topic_dict = read_file(flit_file)
    ev = evaluate(topic_dict, invalid_urls, invalid_dict, 
        repeat_report_file, repeat_cluster_file, reject_file, miss_file)
    print('precess: %3f, recall: %3f, precess_1: %3f, recall_1: %3f' % tuple(ev))

    # 读取未过滤数据==========================================
    print('返回完整cluster')
    topic_dict = read_file(topic_file)
    ev = evaluate(topic_dict, invalid_urls, invalid_dict, 
        repeat_report_file, repeat_cluster_file, reject_file, miss_file)
    print('precess: %3f, recall: %3f, precess_1: %3f, recall_1: %3f' % tuple(ev))

if __name__ == "__main__":
    sys.exit(main())
