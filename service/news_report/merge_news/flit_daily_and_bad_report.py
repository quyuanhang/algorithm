# -*- coding: utf8 -*-
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 內建库
import json
import collections
# 公司开发库
sys.path.append('../')
import site_config as site_config
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


def get_event_type(title):
    '''
    新闻评级 用于过滤早晚报
    口水排在最后，基本不会命中
    '''
    etype = 0
    summery_words = ['晨讯', '晨读', '晨报', '早讯', '早读', '早报', '每日', '日报',
                     '晚讯', '晚读', '晚报', '8点1氪', '8点20发', '简报', '观点', '消息榜']
    finance_words = ['新三板', '挂牌', '入股', '获投', '融', '融资', '投资', '美元', '人民币', '种子轮', '天使轮',
                     'PreA', 'A轮', 'B轮', 'C轮', 'D轮', 'E轮', 'IPO', '上市', '私有化', '收购', '并购', '合并', '倒闭']
    new_words = ['发布', '推出', '研发', '启动', '新品',
                 '新产品', '版本', '业务', '进军', '入股', '战略', '上线']
    com_change_words = ['加入', '任命', '离职', '组织架构', '组织结构',
                        '事业部', '事业群', 'CEO', 'COO', 'CMO', 'CFO', 'CIO']
    discuss_words = ['为啥', '为什么', '如何', '？', '谈', '专访']
    if title:
        if etype == 0:
            for key in summery_words:
                if key in title:
                    etype = '早晚报'
        if etype == 0:
            for key in finance_words:
                if key in title:
                    etype = '金融'
        if etype == 0:
            for key in new_words:
                if key in title:
                    etype = '新品'
        if etype == 0:
            for key in com_change_words:
                if key in title:
                    etype = '公司变动'
        if etype == 0:
            for key in discuss_words:
                if key in title:
                    etype = '口水'
    return etype


def get_media_rank(m_):
    m_r = site_config.map_media_rank
    for m in m_r:
        if m in m_:
            return m_r[m]
    return 4


def get_report_rank_and_event_type(report_):
    '''
    新方法 过滤早晚报和口水
    '''
    r = 0
    event_type = get_event_type(report_["title"])
    if event_type != ('早晚报' or '口水'):
        media_rank = get_media_rank(report_["url"])
        if media_rank < 3:
            r = 1
        else:
            if event_type == '金融':
                r = 1
    return r, event_type


def get_report_rank_and_event_type_old(report_):
    '''
    原方法，同 insert_crm_media_report.py
    '''
    r = 0
    media_rank = get_media_rank(report_["url"])
    event_type = get_event_type(report_["title"])
    if media_rank != 3: 
        r = 1
    else:
        if event_type == '金融':
            r = 1
    return r, event_type


def flit_data(data_dict_, rank_method_):
    report_list = list()
    for i, report in data_dict_.items():
        report_rank, event_type = rank_method_(report)
        if report_rank == 1:
            report['type'] = event_type
            website = report['website']
            report['content'] = report['content'].replace(website, '')
            report_list.append(report)

    keys = [report['url'] for report in report_list]

    report_dict = collections.OrderedDict(
        zip(keys, report_list))

    return report_dict


def main():
    # 声明文件路径================================================
    file_path = file_path_lib.file_path
    data_file = file_path['data_file']['data_file']
    test_file = file_path['data_file']['test_file']
    clean_event_file = file_path['data_file']['clean_event_file']
    clean_file = file_path['data_file']['clean_file']
    # ================================================

    data_dict = read_file(data_file)

    report_dict_flit_daily = flit_data(
        data_dict, get_report_rank_and_event_type)

    write_file(clean_file, report_dict_flit_daily)

    report_dict_flit_old = flit_data(
        data_dict, get_report_rank_and_event_type_old)

    write_file(clean_event_file, report_dict_flit_old)

    print('from %d flit %d by site and flit %d by event' %
          (len(data_dict), len(report_dict_flit_old), len(report_dict_flit_daily)))

    invalid_dict = read_file(test_file)
    old_miss, discuss_miss = 0, 0
    for url, report in invalid_dict.items():
        if report['is_invalid'] == 2:
            if url not in report_dict_flit_old:
                old_miss += 1
            if url not in report_dict_flit_daily:
                discuss_miss += 1
    print('old miss', old_miss, 'discuss miss', discuss_miss)


if __name__ == "__main__":
    sys.exit(main())
