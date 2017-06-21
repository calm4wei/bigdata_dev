#!/bin/sh
# Don't edit this file unless you know exactly what you're doing.

Usage='''Usage: \n
$0 <demo | g> \n
g\tGatherWarning, gather waring of spark streaming process.\n
'''

ROOT="$(dirname $(cd $(dirname $0); pwd))"
echo "$ROOT"

#JAVA_OPTS="-Xmx2048m -Xmn256m "
SPARK_SUBMIT='/usr/bin/spark-submit'

run () {
  if [ -f "$RUN_PATH/$PID_FILE" ]; then
    echo "$RUN_PATH/$PID_FILE already exists."
    echo "Now exiting ..."
    exit 1
  fi
  $@ > $LOG_PATH/"$LOG_FILE" 2>&1 &
  PID=$!
  echo "$PID" > "$RUN_PATH/$PID_FILE"
  wait "$PID"
  rm -f "$RUN_PATH/$PID_FILE"
}

if [ $# -lt 1 ]; then
  echo -e "$Usage"
  exit 1
fi

LOG_PATH="$ROOT"/logs
RUN_PATH="$ROOT"/run

if [ "$JAVA_HOME" != "" ] ; then
  JAVA="$JAVA_HOME/bin/java"
else
  echo "Environment variable \$JAVA_HOME is not set."
  exit 1
fi

if [ ! -d "$LOG_PATH" ];then
  mkdir -p "$LOG_PATH"
fi

if [ ! -d "$RUN_PATH" ];then
  mkdir -p "$RUN_PATH"
fi

case $1 in
   demo)
    CLASS="com.zqykj.bigdata.spark.streaming.SparkStreamingDemo01"
    CONF="$ROOT/conf/zqy-app.properties"
    LOG_FILE="SparkStreamingDemo01.out"
    PID_FILE="SparkStreamingDemo01.pid"
    ;;
   g)
    CLASS="com.zqykj.bigdata.spark.alert.streaming.GatherWarning"
    CONF="$ROOT/conf/zqy-app.properties"
    LOG_FILE="GatherWarning.out"
    PID_FILE="GatherWarning.pid"
    ;;
  *)
    echo -e "$Usage"
    exit 1
    ;;
esac

CMD="
$SPARK_SUBMIT \
  --driver-class-path $CLASSPATH \
  --class $CLASS \
  --master yarn-cluster \
  --executor-memory 5G \
  --num-executors 2 \
  --executor-cores 2 \
  --properties-file $CONF \
  $ROOT/bigdata_dev-1.0-SNAPSHOT.jar"

echo -e "$CMD"
run "$CMD" &
