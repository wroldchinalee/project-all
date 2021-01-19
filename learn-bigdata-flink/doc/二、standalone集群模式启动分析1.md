### 二、standalone集群模式启动分析1-启动脚本

#### 一、start-cluster.sh脚本分析

##### 1.脚本代码

```shell
#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# 1.执行config.sh脚本
. "$bin"/config.sh

# 2.启动JobManager实例
# Start the JobManager instance(s)
# 开启nocasematch选项
shopt -s nocasematch
# 如果开启了高可用
if [[ $HIGH_AVAILABILITY == "zookeeper" ]]; then
    # HA Mode
    readMasters

    echo "Starting HA cluster with ${#MASTERS[@]} masters."

    for ((i=0;i<${#MASTERS[@]};++i)); do
        master=${MASTERS[i]}
        webuiport=${WEBUIPORTS[i]}

        if [ ${MASTERS_ALL_LOCALHOST} = true ] ; then
            "${FLINK_BIN_DIR}"/jobmanager.sh start "${master}" "${webuiport}"
        else
            ssh -n $FLINK_SSH_OPTS $master -- "nohup /bin/bash -l \"${FLINK_BIN_DIR}/jobmanager.sh\" start ${master} ${webuiport} &"
        fi
    done
# 没有开启高可用
else
    echo "Starting cluster."

    # Start single JobManager on this machine
    "$FLINK_BIN_DIR"/jobmanager.sh start
fi
# 关闭nocasematch选项
shopt -u nocasematch

# 3.启动TaskManager实例
# Start TaskManager instance(s)
TMSlaves start

```

##### 2.主要步骤

1. 执行config.sh脚本
2. 启动JobManager实例
3. 启动TaskManager实例

我们先不分析shell脚本里面的细节，主要看启动JobManager和TaskManager。

JobManager的启动的步骤是调用了jobmanager.sh这个脚本。

直接看一下jobmanager脚本的内容。



#### 二、jobmanager.sh脚本分析

##### 1.脚本代码

```shell
#!/usr/bin/env bash

# Start/stop a Flink JobManager.
USAGE="Usage: jobmanager.sh ((start|start-foreground) [host] [webui-port])|stop|stop-all"

# start或者stop参数
STARTSTOP=$1
HOST=$2 # optional when starting multiple instances
WEBUIPORT=$3 # optional when starting multiple instances

if [[ $STARTSTOP != "start" ]] && [[ $STARTSTOP != "start-foreground" ]] && [[ $STARTSTOP != "stop" ]] && [[ $STARTSTOP != "stop-all" ]]; then
  echo $USAGE
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

# entrypoint指向了standalone模式
ENTRYPOINT=standalonesession

# 加载一些flink配置
if [[ $STARTSTOP == "start" ]] || [[ $STARTSTOP == "start-foreground" ]]; then
    if [ ! -z "${FLINK_JM_HEAP_MB}" ] && [ "${FLINK_JM_HEAP}" == 0 ]; then
	    echo "used deprecated key \`${KEY_JOBM_MEM_MB}\`, please replace with key \`${KEY_JOBM_MEM_SIZE}\`"
    else
	    flink_jm_heap_bytes=$(parseBytes ${FLINK_JM_HEAP})
	    FLINK_JM_HEAP_MB=$(getMebiBytes ${flink_jm_heap_bytes})
    fi

    if [[ ! ${FLINK_JM_HEAP_MB} =~ $IS_NUMBER ]] || [[ "${FLINK_JM_HEAP_MB}" -lt "0" ]]; then
        echo "[ERROR] Configured JobManager memory size is not a valid value. Please set '${KEY_JOBM_MEM_SIZE}' in ${FLINK_CONF_FILE}."
        exit 1
    fi

    if [ "${FLINK_JM_HEAP_MB}" -gt "0" ]; then
        export JVM_ARGS="$JVM_ARGS -Xms"$FLINK_JM_HEAP_MB"m -Xmx"$FLINK_JM_HEAP_MB"m"
    fi

    # Add JobManager-specific JVM options
    export FLINK_ENV_JAVA_OPTS="${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_JM}"

    # Startup parameters
    args=("--configDir" "${FLINK_CONF_DIR}" "--executionMode" "cluster")
    if [ ! -z $HOST ]; then
        args+=("--host")
        args+=("${HOST}")
    fi

    if [ ! -z $WEBUIPORT ]; then
        args+=("--webui-port")
        args+=("${WEBUIPORT}")
    fi
fi

if [[ $STARTSTOP == "start-foreground" ]]; then
    exec "${FLINK_BIN_DIR}"/flink-console.sh $ENTRYPOINT "${args[@]}"
else
    "${FLINK_BIN_DIR}"/flink-daemon.sh $STARTSTOP $ENTRYPOINT "${args[@]}"
fi

```

简单分析一下脚本，主要做了以下几件事

1. 确定ENTRYPOINT参数
2. 加载一些flink配置，比如：jobmanager内存，jvm参数，主机名，webui端口
3. 根据STARTSTOP决定最终调用脚本

我这边主要分析flink-daemon.sh这个脚本。



#### 3.flink-daemon.sh脚本

```shell
#!/usr/bin/env bash

# Start/stop a Flink daemon.
USAGE="Usage: flink-daemon.sh (start|stop|stop-all) (taskexecutor|zookeeper|historyserver|standalonesession|standalonejob) [args]"

STARTSTOP=$1
DAEMON=$2
ARGS=("${@:3}") # get remaining arguments as array

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

case $DAEMON in
    (taskexecutor)
        CLASS_TO_RUN=org.apache.flink.runtime.taskexecutor.TaskManagerRunner
    ;;

    (zookeeper)
        CLASS_TO_RUN=org.apache.flink.runtime.zookeeper.FlinkZooKeeperQuorumPeer
    ;;

    (historyserver)
        CLASS_TO_RUN=org.apache.flink.runtime.webmonitor.history.HistoryServer
    ;;
	# jobmanager的启动走这个地方
    (standalonesession)
        CLASS_TO_RUN=org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
    ;;

    (standalonejob)
        CLASS_TO_RUN=org.apache.flink.container.entrypoint.StandaloneJobClusterEntryPoint
    ;;

    (*)
        echo "Unknown daemon '${DAEMON}'. $USAGE."
        exit 1
    ;;
esac

if [ "$FLINK_IDENT_STRING" = "" ]; then
    FLINK_IDENT_STRING="$USER"
fi

FLINK_TM_CLASSPATH=`constructFlinkClassPath`

pid=$FLINK_PID_DIR/flink-$FLINK_IDENT_STRING-$DAEMON.pid

mkdir -p "$FLINK_PID_DIR"

# Log files for daemons are indexed from the process ID's position in the PID
# file. The following lock prevents a race condition during daemon startup
# when multiple daemons read, index, and write to the PID file concurrently.
# The lock is created on the PID directory since a lock file cannot be safely
# removed. The daemon is started with the lock closed and the lock remains
# active in this script until the script exits.
command -v flock >/dev/null 2>&1
if [[ $? -eq 0 ]]; then
    exec 200<"$FLINK_PID_DIR"
    flock 200
fi

# Ascending ID depending on number of lines in pid file.
# This allows us to start multiple daemon of each type.
id=$([ -f "$pid" ] && echo $(wc -l < "$pid") || echo "0")

FLINK_LOG_PREFIX="${FLINK_LOG_DIR}/flink-${FLINK_IDENT_STRING}-${DAEMON}-${id}-${HOSTNAME}"
log="${FLINK_LOG_PREFIX}.log"
out="${FLINK_LOG_PREFIX}.out"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configuration=file:${FLINK_CONF_DIR}/log4j.properties" "-Dlogback.configurationFile=file:${FLINK_CONF_DIR}/logback.xml")

JAVA_VERSION=$(${JAVA_RUN} -version 2>&1 | sed 's/.*version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

# Only set JVM 8 arguments if we have correctly extracted the version
if [[ ${JAVA_VERSION} =~ ${IS_NUMBER} ]]; then
    if [ "$JAVA_VERSION" -lt 18 ]; then
        JVM_ARGS="$JVM_ARGS -XX:MaxPermSize=256m"
    fi
fi

case $STARTSTOP in

    (start)
        # Rotate log files
        rotateLogFilesWithPrefix "$FLINK_LOG_DIR" "$FLINK_LOG_PREFIX"

        # Print a warning if daemons are already running on host
        if [ -f "$pid" ]; then
          active=()
          while IFS='' read -r p || [[ -n "$p" ]]; do
            kill -0 $p >/dev/null 2>&1
            if [ $? -eq 0 ]; then
              active+=($p)
            fi
          done < "${pid}"

          count="${#active[@]}"

          if [ ${count} -gt 0 ]; then
            echo "[INFO] $count instance(s) of $DAEMON are already running on $HOSTNAME."
          fi
        fi

        # Evaluate user options for local variable expansion
        FLINK_ENV_JAVA_OPTS=$(eval echo ${FLINK_ENV_JAVA_OPTS})

        echo "Starting $DAEMON daemon on host $HOSTNAME."
        $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" ${CLASS_TO_RUN} "${ARGS[@]}" > "$out" 200<&- 2>&1 < /dev/null &

        mypid=$!

        # Add to pid file if successful start
        if [[ ${mypid} =~ ${IS_NUMBER} ]] && kill -0 $mypid > /dev/null 2>&1 ; then
            echo $mypid >> "$pid"
        else
            echo "Error starting $DAEMON daemon."
            exit 1
        fi
    ;;

    (stop)
        if [ -f "$pid" ]; then
            # Remove last in pid file
            to_stop=$(tail -n 1 "$pid")

            if [ -z $to_stop ]; then
                rm "$pid" # If all stopped, clean up pid file
                echo "No $DAEMON daemon to stop on host $HOSTNAME."
            else
                sed \$d "$pid" > "$pid.tmp" # all but last line

                # If all stopped, clean up pid file
                [ $(wc -l < "$pid.tmp") -eq 0 ] && rm "$pid" "$pid.tmp" || mv "$pid.tmp" "$pid"

                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    kill $to_stop
                else
                    echo "No $DAEMON daemon (pid: $to_stop) is running anymore on $HOSTNAME."
                fi
            fi
        else
            echo "No $DAEMON daemon to stop on host $HOSTNAME."
        fi
    ;;

    (stop-all)
        if [ -f "$pid" ]; then
            mv "$pid" "${pid}.tmp"

            while read to_stop; do
                if kill -0 $to_stop > /dev/null 2>&1; then
                    echo "Stopping $DAEMON daemon (pid: $to_stop) on host $HOSTNAME."
                    kill $to_stop
                else
                    echo "Skipping $DAEMON daemon (pid: $to_stop), because it is not running anymore on $HOSTNAME."
                fi
            done < "${pid}.tmp"
            rm "${pid}.tmp"
        fi
    ;;

    (*)
        echo "Unexpected argument '$STARTSTOP'. $USAGE."
        exit 1
    ;;

esac
```

这段脚本比较长，简单分析一下，主要关心两件事

传进来的参数$1是start，$2是standalonesession。

那么走到这句：

```shell
# jobmanager的启动走这个地方
    (standalonesession)
        CLASS_TO_RUN=org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint
    ;;
```

在这里设置了入口类。

第二个比较重要的是这里：

```shell
echo "Starting $DAEMON daemon on host $HOSTNAME."
        $JAVA_RUN $JVM_ARGS ${FLINK_ENV_JAVA_OPTS} "${log_setting[@]}" -classpath "`manglePathList "$FLINK_TM_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS"`" ${CLASS_TO_RUN} "${ARGS[@]}" > "$out" 200<&- 2>&1 < /dev/null &
```

这里调用了$JAVA_RUN命令，实际上就是$JAVA_HOME/bin/java命令

通过java命令运行前面的CLASS_TO_RUN的类。

下一章再分析JobManager启动类StandaloneSessionClusterEntrypoint的过程。