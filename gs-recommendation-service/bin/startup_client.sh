#!/bin/bash

CLASSPATH=conf:gs-recommendation-service.jar
for f in lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

gs_recommendation_client_pid=`ps -ef |grep "RecommendClient" |grep -v grep |grep java |awk '{print $2}'`

start_run () {
    java -DAPP_NAME=GSRecommendClient -cp $CLASSPATH com.ctrip.gs.recommendation.api.RecommendClient >> /var/log/gs_recommendation/gs_recommendation_startup_client.log 2>&1 &
}

case "$1" in
start)
        start_run
        ;;
stop)
        if [[ $gs_recommendation_client_pid == "" ]]; then
            echo "gs recommendation client was stoped."
          else
            kill -9 $gs_recommendation_client_pid
        fi
        ;;
restart)
        kill -9 $gs_recommendation_client_pid
        start_run
        ;;
status)
        if [[ $gs_recommendation_client_pid -gt 1 ]]; then
            echo "gs recommendation client  is running."
          else
            echo "gs recommendation client  is stop."
        fi
        ;;
  *)
        echo $"Usage: $0 {start|stop|restart}"
esac