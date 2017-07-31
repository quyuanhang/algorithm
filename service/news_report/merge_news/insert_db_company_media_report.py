#!/usr/bin/env python
# -*- coding: utf8 -*-import sys
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
# 內建库
import json
import datetime
# 公司开发库
sys.path.append('../../../')
import common.db_fetcher as db_fetcher
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


def get_company_id(attach_cid):
    # 根据company表的attach_cid找到主键id
    if attach_cid == 0:
        return 0
    else:
        try:
            sql = "select id from company where attach_cid = '%s'" % (
                attach_cid)
            db_data = fetcher.get_sql_result(sql, 'mysql_crm')
            cid = db_data[0][0]
            return cid
        except:
            return 0

def main():

    # 声明文件路径================================================
    file_path = file_path_lib.file_path
    flit_file = file_path['data_file']['flit_file']
    # ================================================

    topic_dict = read_file(flit_file)
    report_dict = {report.keys()[0]: report.values()[0]
                   for report in topic_dict.values()}
    urls = tuple(report_dict.keys())

    read_sql = 'select source, publish_date, url, title, content, headimg, reported_com, type from news_report where url in (%s)' % (("%s," * len(urls))[:-1])
    insert_sql = "insert into crm.company_media_reports (title,type,link,website,time,create_time,auto_cid,content,headimg) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"

    db_data = fetcher.get_sql_result(read_sql, 'mysql_insight', params=urls)
    print('insert num', len(db_data))
    repeat_num = 0
    for row in db_data:
        source, publish_date, url, title, content, headimg, reported_com, event_type = row

        content = content.replace('\t', '')
        com_id = get_company_id(reported_com)
        create_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        params = (title, event_type, url, source, publish_date,
                  create_time, com_id, content, headimg)

        crm_repeat = fetcher.commit_sql_cmd(
            insert_sql, "mysql_crm", params=params)
        if crm_repeat == -1:
            repeat_num += 1
            update_sql = "update company_media_reports set title=%s,content=%s,website=%s where link=%s"
            params = (title, content, source, url)
            fetcher.commit_sql_cmd(update_sql, "mysql_crm", params=params)
    print('repeat num', repeat_num)


if __name__ == "__main__":
    sys.exit(main())
