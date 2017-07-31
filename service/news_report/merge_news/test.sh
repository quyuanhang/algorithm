#!/bin/bash

python read_db_news_report.py '2017-6-16'
python flit_daily_and_bad_report.py
python merge_news.py
python delete_repeat_report.py
python evaluate.py
