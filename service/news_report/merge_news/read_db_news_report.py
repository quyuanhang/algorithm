#!/usr/bin/env python
# -*- coding: utf8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 內建库
import os
import json
import collections
import re
import datetime
import copy
# 第三方库
from pyquery import PyQuery as pq
import pandas as pd
from lxml import etree
# 公司开发库
sys.path.append('../../../')
import common.db_fetcher as db_fetcher
import company_recognize.report_to_company as report_to_company
sys.path.append('../../../baselib/')
import segment.chinese_segmenter as chinese_segmenter
# 自建库
import file_path_lib

fetcher = db_fetcher.DataBaseFetcher()

def read_file(file_):
    with open(file_) as f:
        data_dict = json.load(f)
    return data_dict


def write_file(file_, obj_):
    with open(file_, mode='w') as f:
        f.write(json.dumps(obj_, ensure_ascii=False, indent=4))
    return 0


class CompanyInfo(object):
    """获取公司名"""

    def __init__(self):
        sql = "select id, attach_cid, name from company"
        db_data = fetcher.get_sql_result(sql, 'mysql_crm')
        self.id_name = dict()
        self.attach_name = dict()
        for row in db_data:
            try:
                com_id, attach_cid, name = row
                if com_id != 0:
                    self.id_name[com_id] = name
                if attach_cid != 0:
                    self.attach_name[attach_cid] = name
            except Exception as e:
                print(e)
                print(row)
                pass

    def get_name_by_id(self, com_id):
        try:
            name = self.id_name[com_id]
            # if name:
            # self.id_name_num += 1
        except Exception as e:
            name = 0
        return name

    def get_name_by_attach(self, attach_cid):
        try:
            attach_cid = int(attach_cid)
            name = self.attach_name[attach_cid]
            # if name:
            # self.attach_name_num += 1
        except Exception as e:
            name = 0
        return name

com_info = CompanyInfo()


def content_format(content):
    # 格式化content
    content_ = pq(unicode(content, 'utf-8')).text().replace('\n', ' ')
    content_ = re.sub(
        "[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), " ".decode("utf8"), content_)
    return content_


def read_invalid(start_, end_):
    # 读取标记数据
    sql = "select link, title, content, website, time, is_invalid, auto_cid from company_media_reports where time >= '%s' and time < '%s' order by time" % (
        start_, end_)
    db_data = fetcher.get_sql_result(sql, 'mysql_crm')

    data_list = list()
    false_list = list()
    company_name_error_num = 0
    content_bug_num = 0
    invalid_num = 0
    invalid_dict = dict()
    for row in db_data:
        url, title, content, website, time, is_invalid, auto_cid = row
        if is_invalid == 2:
            invalid_num += 1
        try:
            content = content_format(content)
        except:
            content_bug_num += 1
        try:
            com_name = com_info.get_name_by_id(auto_cid)
        except Exception as e:
            company_name_error_num += 1
        report = dict(zip(['url', 'title', 'content', 'website', 'com_name', 'time', 'is_invalid'], [
            url, title, content, website, com_name, time.strftime("%Y-%m-%d-%H-%M-%S"), is_invalid]))
        invalid_dict[url] = report

    print('db_data', len(db_data), 'invalid_num', invalid_num, 'company_name_error_num',
          company_name_error_num, 'content_bug_num', content_bug_num)
    return invalid_dict


def main():
    # 声明文件路径================================================
    file_path = file_path_lib.file_path
    data_file = file_path['data_file']['data_file']
    wrong_file = file_path['data_file']['wrong_file']
    history_file = file_path['data_file']['history_file']
    test_file = file_path['data_file']['test_file']
    idf_file = file_path['source_file']['idf_file']
    # ================================================

    # news report时间范围=========================================
    today = \
        datetime.datetime.now().strftime("%Y-%m-%d") if sys.argv[1] == '0' \
        else sys.argv[1]
    tomorrow = \
        (datetime.datetime.strptime(today, '%Y-%m-%d') + datetime.timedelta(days=1))\
        .strftime("%Y-%m-%d")
    # media report时间范围========================================
    yesterday = \
        (datetime.datetime.strptime(today, '%Y-%m-%d') - datetime.timedelta(days=1))\
        .strftime("%Y-%m-%d")
    now = \
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") if sys.argv[1] == '0' \
        else sys.argv[1]

    comlib = report_to_company.ComLib('../../../company_recognize/segment/data',
        '/data/work/resource/com_name2id.txt', None, '/data/work/resource/com_filter.txt')

    '''
    从insight库中读取全部无标签数据用于测试
    '''

    sql = "select url, title, content, source, publish_date, reported_com, reported_coms from news_report where publish_date >= '%s' and publish_date < '%s' order by publish_date" % (
        today, tomorrow)
    db_data = fetcher.get_sql_result(sql, 'mysql_insight')

    # 读取dbdata写入list 每行是一个dict
    data_dict_list = list()
    drop_list = list()
    wrong_list = list()
    com_num = 0
    coms_num = 0
    for row in db_data:
        url, title, content, website, time, company, related_com_ids = row
        try:
            com_id, com_weight, is_confident = comlib.extract_report(title, content, '', website)
            related_com_ids = comlib.extract_report(title, content, '-1', website)
            content = content_format(content)
        except:
            com_id, com_weight, related_com_ids = 0, 0, ''
            is_confident = False
            content = ''
        com_name = com_info.get_name_by_attach(com_id)
        if len(related_com_ids) > 0:
            related_com_names = [com_info.get_name_by_attach(
                company_[0]) for company_ in related_com_ids if com_info.get_name_by_attach(company_[0]) != 0]
        else:
            related_com_names = 0
        report = dict(zip(['url', 'title', 'content', 'website', 'com_name', 'related_com_names', 'time', 'com_weight', 'is_confidence'], [
            url, title, content, website, com_name, related_com_names, time.strftime("%Y-%m-%d-%H-%M-%S"), com_weight, is_confident]))
        if report['com_name'] != 0:
            com_num += 1
        if report['related_com_names'] != 0:
            coms_num += 1
        if not content:
            wrong_list.append(report)
        data_dict_list.append(report)

    data_dict_keys = [report['url'] for report in data_dict_list]
    data_dict = dict(zip(data_dict_keys, data_dict_list))
    wrong_dict = dict(zip(range(len(wrong_list)), wrong_list))
    write_file(data_file, data_dict)
    write_file(wrong_file, wrong_dict)

    '''
    读取含有invalid标签的数据用于历史去重
    '''
    print('yesterday invalid')
    history_dict_ = read_invalid(yesterday, now)
    history_dict = {url: report for url,
                    report in history_dict_.items() if url in data_dict_keys}
    write_file(history_file, history_dict)

    '''
    读取含有invalid标签的数据用于测试评价
    '''
    print('today invalid')
    test_dict_ = read_invalid(today, tomorrow)
    test_dict = {url: report for url, report in test_dict_.items()
                 if url in data_dict_keys}
    write_file(test_file, test_dict)

    print('num of reports', len(db_data))
    print('num of wrong', len(wrong_list))
    print('num of rest', len(data_dict))
    print('num of test data', len(history_dict))
    print('report with one com', com_num)
    print('report with related coms', coms_num)


if __name__ == "__main__":
    sys.exit(main())
