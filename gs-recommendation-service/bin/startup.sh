#!/bin/bash

CLASSPATH=conf:gs-recommendation-service.jar
for f in lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

gs_recommendation_service_pid=`ps -ef |grep "RecommendServer" |grep -v grep |grep java |awk '{print $2}'`
JAVA_OPTS="-Dfile.encoding=UTF-8 -Xms2G -Xmx2G -XX:NewSize=512M -XX:MaxNewSize=512M -XX:PermSize=256M -XX:MaxPermSize=256M"
JAVA_OPTS=$JAVA_OPTS" -Xloggc:/var/log/gs_recommendation/gs_recommendation_service_gc.log -XX:+PrintGCDetails -XX:+PrintGC -XX:+PrintGCTimeStamps -XX:+PrintHeapAtGC -XX:+PrintTenuringDistribution"
JAVA_OPTS=$JAVA_OPTS" -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/var/log/gs_recommendation/gs_recommendation_service_gc<pid>.hprof"
JAVA_OPTS=$JAVA_OPTS" -Djava.awt.headless=true -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+DisableExplicitGC"

start_run () {
    java $JAVA_OPTS -DAPP_NAME=gsRecommendService -cp $CLASSPATH com.ctrip.gs.recommendation.api.RecommendServer >> /var/log/gs_recommendation/gs_recommendation_startup.log 2>&1 &
}

case "$1" in
start)
        start_run
        ;;
stop)
        if [[ $gs_recommendation_service_pid == "" ]]; then
            echo "gs recommendation service was stoped."
          else
            kill -9 $gs_recommendation_service_pid
        fi
        ;;
restart)
        kill -9 $gs_recommendation_service_pid
        start_run
        ;;
status)
        if [[ $gs_recommendation_service_pid -gt 1 ]]; then
            echo "gs recommendation service  is running."
          else
            echo "gs recommendation service  is stop."
        fi
        ;;
  *)
        echo $"Usage: $0 {start|stop|restart}"
esac