#! /usr/bin/env bash
##########################################################################
#
# runJob.sh:  Launch Recommender Job
#
# Pre-requisites:
# 1)  JAVA_HOME is set
# 2)  HADOOP_HOME is set, and $HADOOP_HOME/conf contains your cluster
#     configuration
#
#
##########################################################################

APP_HOME=`pwd`

cd $APP_HOME
export APP_HOME=$APP_HOME
echo $APP_HOME
#initial hadoop configuration path
HADOOP_CONF_DIR=/etc/hadoop/conf.my_cluste

# classpath initially contains $HADOOP_CONF_DIR
CLASSPATH="${HADOOP_CONF_DIR}"
libjars=''
for i in "$APP_HOME"/*.jar; do
   CLASSPATH="$CLASSPATH":"$i"
   libjars="$libjars","$i"
done
for i in "$APP_HOME"/lib/*.jar; do
   CLASSPATH="$CLASSPATH":"$i"
   libjars="$libjars","$i"
done
CLASSPATH=$CLASSPATH:logback.xml

export HADOOP_CLASSPATH=$CLASSPATH
hadoop jar gs-recommendation-biz.jar com.ctrip.gs.recommendation.itemcf.precompute.RecommenderJob -libjars=$libjars "$@"