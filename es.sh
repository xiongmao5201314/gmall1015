#!/bin/bash
es_Home=/opt/module/elasticsearch-6.3.1
kibana_Home=/opt/module/kibana-6.3.1-linux-x86_64
case $1 in
start)

  for host in hadoop104 hadoop106 hadoop108 ; do
      echo "启动es $host 服务"
      ssh $host "source /etc/profile ; nohup $es_Home/bin/elasticsearch 1>$es_Home/es.log 2>$es_Home/error.log &"
  done
      echo "在hadoop106上启动kb服务"
      nohup $kibana_Home/bin/kibana 1>$kibana_Home/kb.log 2>$kibana_Home/error.log &
  ;;
stop)
      echo "在hadoop106上停止kb服务"
       ps -ef | grep kibana | grep -v grep | awk '{print $2}'| xargs kill -9
      for host in hadoop104 hadoop106 hadoop108 ; do
      echo "停止es $host 服务"
      ssh $host "source /etc/profile;jps | grep Elasticsearch | awk '{print \$1}' | xargs kill -9"
  done
  ;;
*)
  echo "请输入start或者stop来启动或停止脚本"
  ;;
esac

