#!/bin/sh
# Don't edit this file unless you know exactly what you're doing.

ROOT=$(cd $(dirname $0); pwd)
echo $ROOT

run () {
  if [ -f $RUN_PATH/$PID_FILE ]; then
    echo "$RUN_PATH/$PID_FILE already exists."
    echo "Now exiting ..."
    exit 1
  fi
  $@ > $LOG_PATH/$LOG_FILE 2>&1 &
  PID=$!
  echo $PID > "$RUN_PATH/$PID_FILE"
  wait $PID
  rm -f $RUN_PATH/$PID_FILE
}

usage="Usage:\n
$0 <ks | kds> \"<data>\"
ks \t pipline from kafka using kafka kafka streaming, data format: yyyyMMddHH\n
kds \t pipline from kafka using kafka direct streaming, data format: yyyyMMddHH\n"


if [ $# -lt 3 ]; then
  echo -e $usage
  exit 1
fi

LOG_PATH=$ROOT/logs
RUN_PATH=$ROOT/run

JAVA_OPTS="-Xmx2048m -Xmn256m "
SPARK_SUBMIT='/usr/bin/spark-submit'
if [ "$JAVA_HOME" != "" ] ; then
  JAVA=$JAVA_HOME/bin/java
else
  echo "Environment variable \$JAVA_HOME is not set."
  exit 1
fi

if [ ! -d $LOG_PATH ];then
  mkdir -p $LOG_PATH
fi

if [ ! -d $RUN_PATH ];then
  mkdir -p $RUN_PATH
fi 

case $1 in
   demo)
    CLASS="com.zqykj.bigdata.spark.streaming.SparkStreamingDemo01"
    CONF="$ROOT/conf/cstor-spark.properties"
    LOG_FILE="SparkStreamingDemo01.out"
    PID_FILE="SparkStreamingDemo01.pid"
    ;;
  *)
    echo -e $usage
    ;;
esac

CMD="
$SPARK_SUBMIT \
  --driver-class-path $CLASSPATH \
  --class $CLASS \
  --master yarn-cluster \
  --executor-memory 5G \
  --num-executors 5 \
  $ROOT/bigdata_dev-1.0-SNAPSHOT.jar \
  $3 "

echo -e "$CMD"
run "$CMD" &
