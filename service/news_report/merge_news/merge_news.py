#!/usr/bin/env python
# -*- coding: utf8 -*-
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 內建库
import datetime
import json
import collections
import copy
# 第三方库
import pandas as pd
# 公司开发库
sys.path.append('../../../baselib/')
import segment.chinese_segmenter as chinese_segmenter
sys.path.append('../../../company_recognize/')
import company_normalizer.CompanyNormalizer as CompanyNormalizer
# 自建库
import file_path_lib


# 读写方法 ================================================


def read_file(file_):
    with open(file_) as f:
        data_dict = json.load(f)
    return data_dict


def write_file(file_, obj_):
    with open(file_, mode='w') as f:
        f.write(json.dumps(obj_, ensure_ascii=False, indent=4))
    return 0
# =============================================================


class WordVector(object):
    """docstring for tf_idf"""

    def __init__(self, idf_file, chunker, com_norm):
        self.com_norm = com_norm
        self.chunker = chunker
        self.idf_list = list()
        with open(idf_file) as f:
            for line in f:
                # print(line)
                line = line.lower().strip('\r\n').split()
                if len(line) == 3:
                    self.idf_list.append([line[0], float(line[2])])
        self.idf_series = pd.Series(dict(self.idf_list))

    def rank(self, text, tf_n=20, tf_idf_n=10, flit_single=False, idf=False):
        words_series = pd.Series(self.chunker.segment(text))
        if flit_single:
            words_len = words_series.apply(lambda x: len(unicode(x)))
            words_series = words_series[words_len > 1]
        tf_series = words_series.value_counts().iloc[:tf_n]
        if not idf:
            return tf_series
        cur_idf_ser = self.idf_series.reindex(tf_series.index).fillna(0.6)
        tf_idf_ser = (
            tf_series * cur_idf_ser).sort_values(ascending=False).iloc[:tf_idf_n]
        return tf_idf_ser

    def rank_del_website(self, text, website, tf_n=20, tf_idf_n=10, flit_single=False, idf=False):
        tf_idf_ser = self.rank(text)
        normed_ser = pd.Series(tf_idf_ser.index, index=tf_idf_ser.index).apply(
            self.com_norm.com_to_id())
        website_com_id = self.com_norm.com_to_id(website)
        if website_com_id != -1:
            tf_idf_ser[normed_ser == website_com_id] = 0
        tf_idf_ser = tf_idf_ser.sort_values(ascending=False)
        return tf_idf_ser


class SimLib(object):
    """docstring for sim"""

    def __init__(self):
        self.time_reindex = datetime.datetime.now() - datetime.datetime.now()
        self.time_cos = datetime.datetime.now() - datetime.datetime.now()

    def content_similarity(self, ser_1, ser_2):
        idx = ser_1.index & ser_2.index
        product = 0
        for i in idx:
            product += ser_1[i] * ser_2[i]
        cos = \
            product / ((sum(ser_1.values ** 2.0) *
                        sum(ser_2.values ** 2.0))**0.5 + 0.01)
        return cos

    def com_similarity(self, com_1, com_2):
        r = 0
        if com_1 != 0:
            if com_1 == com_2:
                r = 1
        return r

    def coms_with_topic(self, old_topic, current_report):
        def clean_coms(topic):
            coms = False
            if topic['related_com_names'] != 0:
                coms = set(topic['related_com_names'])
                website = topic['website']
                if website in coms:
                    coms.remove(website)
            return coms if coms else 0
        r = 0
        r_list = list()
        current_coms = clean_coms(current_report)
        if current_coms != 0:
            for url, old_report in old_topic.items():
                if url == 'words_rank':
                    continue
                old_coms = clean_coms(old_report)
                if old_coms != 0:
                    r_list.append(len(old_coms & current_coms) /
                                  len(old_coms | current_coms))
            r = sum(r_list) / (len(r_list) + 0.1)
        return r

    def cosin_with_topic(self, old_topic, current_report):
        topic_word_rank = old_topic['words_rank']
        current_word_rank = current_report['words_rank']
        cosin = self.content_similarity(topic_word_rank, current_word_rank)
        return cosin


class MergeNews(object):
    """docstring for MergeNews"""

    def __init__(self, my_sim, my_tfidf):
        self.sim = my_sim
        self.tfidf = my_tfidf

    def type_is_discuss(self, report):
        '''判定口水文'''
        word_rank = report['words_rank']
        words_len = word_rank.apply(lambda x: len(str(x)))
        if 1 in words_len.index:
            if words_len.value_counts()[1] >= 5:
                report['type'] = '口水'
        return True if report['type'] == '口水' else False

    def topic_add_report(self, topic, report, com_name=None):
        '''将report加入topic，更新topic词向量'''
        if len(topic) == 0:
            topic['words_rank'] = report['words_rank']
        else:
            new_word_index = topic[
                'words_rank'].index | report['words_rank'].index
            new_word_rank = topic['words_rank'].reindex(new_word_index).fillna(0)\
                + report['words_rank'].reindex(new_word_index).fillna(0)
            topic['words_rank'] = new_word_rank
        topic[report['url']] = report
        if not com_name:
            return topic
        else:
            topic['com_name'] = com_name
            return topic

    def merge(self, dict_data):
        discuss_baseline = 0.4
        com_report_baseline = 0.3
        # 对每一条新内容
        report_no_com = 0
        report_with_com = 0
        num = 0
        topic_with_com = dict()
        topic_no_com = dict()
        for i, current_report in sorted(dict_data.items(), key=lambda x: x[1]['time']):
            # 输出进度=============================
            if num % 100 == 0:
                print('merged %d reports' % num)
            num += 1
            # =====================================
            sim_with_topics = dict()
            # 没有明显公司特征的文档==================================
            if int(current_report['com_weight']) < 5:
                report_no_com += 1
                current_words_rank = self.tfidf.rank_del_website(
                    current_report['content'], flit_single=False, idf=True)
                current_report['words_rank'] = current_words_rank
                # 判定口水文=======================================
                if self.type_is_discuss(current_report):
                    for old_topic in topic_no_com:
                        sim = self.sim.cosin_with_topic(
                            topic_no_com[old_topic], current_report)
                        sim_with_topics[old_topic] = sim
                    sim_with_topics = pd.Series(sim_with_topics)
                    if sim_with_topics.max() > discuss_baseline:
                        most_sim_topic = sim_with_topics.idxmax()
                    else:
                        most_sim_topic = len(topic_no_com)
                        topic_no_com[most_sim_topic] = dict()
                # 没有置信公司的非口水文 中间情况===================
                else:
                    for old_topic in topic_no_com:
                        cosin = self.sim.cosin_with_topic(
                            topic_no_com[old_topic], current_report)
                        coms_sim = self.sim.coms_with_topic(
                            topic_no_com[old_topic], current_report)
                        sim_with_topics[old_topic] = max(cosin, coms_sim)
                    sim_with_topics = pd.Series(sim_with_topics)
                    if sim_with_topics.max() > discuss_baseline:
                        most_sim_topic = sim_with_topics.idxmax()
                    else:
                        most_sim_topic = len(topic_no_com)
                        topic_no_com[most_sim_topic] = dict()
                topic_no_com[most_sim_topic] = self.topic_add_report(
                    topic_no_com[most_sim_topic], current_report)
            # 公司文=======================================================
            else:
                # pd.series中的中文索引是string
                current_com = str(current_report['com_name'])
                # 从词向量中删除公司名
                current_words_rank = self.tfidf.rank_del_website(
                    current_report['content'], flit_single=True, idf=True)
                if current_com in current_words_rank.index:
                    current_words_rank[current_com] = 0
                    report_with_com += 1
                current_report['words_rank'] = current_words_rank
                # 与已有话题比较，若公司相同则计算相似度，否则跳过
                for old_topic in topic_with_com:
                    if current_com == topic_with_com[old_topic]['com_name']:
                        cosin = self.sim.cosin_with_topic(
                            topic_with_com[old_topic], current_report)
                        sim_with_topics[old_topic] = cosin
                    else:
                        continue
                sim_with_topics = pd.Series(sim_with_topics)
                # 若符合阈值 则归档至最相似话题
                if sim_with_topics.max() > com_report_baseline:
                    most_sim_topic = sim_with_topics.idxmax()
                # 否则新建话题
                else:
                    most_sim_topic = len(topic_with_com)
                    topic_with_com[most_sim_topic] = dict()
                topic_with_com[most_sim_topic] = self.topic_add_report(
                    topic_with_com[most_sim_topic], current_report, com_name=current_com)
        print('report_word_rank_with_com', report_with_com)
        # 合并公司文和口水文
        topic_dict = copy.deepcopy(topic_no_com)
        for key, value in topic_with_com.items():
            topic_ = len(topic_dict)
            topic_dict[topic_] = value
        return topic_dict

    def convert_series(self, dict_topic):
        '''把pd.series词向量转化为dict'''
        dict_topic_ = dict()
        for topic in dict_topic:
            dict_topic_[topic] = dict()
            for report in dict_topic[topic]:
                if report == 'com_name':
                    continue
                elif report == 'words_rank':
                    dict_topic_[topic]['words_rank'] = collections.OrderedDict(
                        dict_topic[topic]['words_rank'].sort_values(ascending=False))
                else:
                    dict_topic_[topic][report] = dict_topic[topic][report]
                    dict_topic_[topic][report]['words_rank'] = collections.OrderedDict(
                        dict_topic[topic][report]['words_rank'].sort_values(ascending=False))
        return dict_topic_

    def sort_dict(self, dict_topic_):
        '''按照话题规模排序'''
        sorted_topic_list = sorted(
            dict_topic_.items(), key=lambda x: len(x[1]), reverse=True)
        ordered_dict = collections.OrderedDict(sorted_topic_list)
        for topic, reports in ordered_dict.items():
            sorted_report_list = sorted(
                reports.items(), key=lambda x: x[1]['time'] if 'time' in x[1] else '1994-03-02-00-00-00')
            ordered_dict[topic] = collections.OrderedDict(sorted_report_list)
        return ordered_dict

    def clean_topic(self, topic_dict):
        '''整理话题，保留重要信息，以便肉眼观察'''
        topic_dict_ = collections.OrderedDict()
        for topic in topic_dict:
            topic_dict_[topic] = collections.OrderedDict()
            for report_url in topic_dict[topic]:
                current_report = topic_dict[topic][report_url]
                if report_url == 'words_rank':
                    s = ''
                    for word, rank in list(current_report.items()):
                        s = s + word + ':' + '%.2f' % rank + ' '
                    topic_dict_[topic]['words_rank'] = s
                else:

                    title = current_report['title']
                    time = current_report['time']
                    words = current_report['words_rank']
                    com_name = current_report['com_name']
                    url = current_report['url']
                    s = ''
                    for word, rank in list(words.items()):
                        s = s + word + ':' + '%.2f' % rank + ' '
                    topic_dict_[topic][report_url] = collections.OrderedDict()
                    topic_dict_[topic][report_url]['title'] = title
                    topic_dict_[topic][report_url]['time'] = time
                    topic_dict_[topic][report_url]['words_rank'] = s
                    topic_dict_[topic][report_url]['com_name'] = com_name
                    topic_dict_[topic][report_url]['url'] = url
        return topic_dict_


def main():
    # 声明文件路径================================================
    file_path = file_path_lib.file_path
    clean_file = file_path['data_file']['clean_file']
    topic_file = file_path['data_file']['topic_file']
    topic_file_clean = file_path['data_file']['topic_file_clean']

    idf_file = file_path['source_file']['idf_file']
    segment_data = file_path['source_file']['segment_data']
    stop_word_file = file_path['source_file']['stop_word_file']
    id_name_url_app_file = file_path['source_file']['id_name_url_app_file']
    com_name2id_file = file_path['source_file']['com_name2id_file']

    # ================================================
    sim_lib = SimLib()
    chunker = chinese_segmenter.ChineseSegmenter(
        str(segment_data), str(stop_word_file))
    com_norm = CompanyNormalizer(
        str(id_name_url_app_file), str(com_name2id_file), str(segment_data))
    word_vector = WordVector(idf_file, chunker, com_norm)
    merge_news = MergeNews(sim_lib, word_vector)

    dict_data = read_file(clean_file)

    dict_topic = merge_news.merge(dict_data)

    dict_topic_format = merge_news.convert_series(dict_topic)
    ordered_dict = merge_news.sort_dict(dict_topic_format)
    dict_for_look = merge_news.clean_topic(ordered_dict)

    write_file(topic_file, ordered_dict)
    write_file(topic_file_clean, dict_for_look)


if __name__ == "__main__":
    sys.exit(main())
