#!/bin/bash

path=`pwd`
to=quyuanhang@36kr.com,cuiquan@36kr.com
day=`date '+%Y-%m-%d'`
subject="[媒体新闻流监控-$day]"

cp $path/data/topic_clean.json $path/data/topic_clean.$day
cp $path/data/topic.json $path/data/topic.$day
cp $path/data/flit_topic.json $path/data/flit_topic.$day

python /usr/bin/sendmail.py --subject="$subject" --body-file='见附件' --to="$to" -a="$path/data/topic_clean.$day" -a="$path/data/topic.$day" -a="$path/data/flit_topic.$day"

