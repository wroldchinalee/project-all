### Flink on yarn的per-job模式源码分析

##### Flink版本为1.10.1

#### 一、启动日志

> 2021-01-25 20:55:25,530 INFO  org.apache.flink.client.cli.CliFrontend                       - --------------------------------------------------------------------------------
> 2021-01-25 20:55:25,548 INFO  org.apache.flink.client.cli.CliFrontend                       -  Starting Command Line Client (Version: 1.10.1, Rev:c5915cf, Date:07.05.2020 @ 13:58:51 CST)
> 2021-01-25 20:55:25,549 INFO  org.apache.flink.client.cli.CliFrontend                       -  OS current user: master
> 2021-01-25 20:55:27,383 INFO  org.apache.flink.client.cli.CliFrontend                       -  Current Hadoop/Kerberos user: master
> 2021-01-25 20:55:27,383 INFO  org.apache.flink.client.cli.CliFrontend                       -  JVM: Java HotSpot(TM) 64-Bit Server VM - Oracle Corporation - 1.8/25.181-b13
> 2021-01-25 20:55:27,384 INFO  org.apache.flink.client.cli.CliFrontend                       -  Maximum heap size: 633 MiBytes
> 2021-01-25 20:55:27,384 INFO  org.apache.flink.client.cli.CliFrontend                       -  JAVA_HOME: /home/master/software/jdk1.8.0_181
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -  Hadoop version: 2.7.5
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -  JVM Options:
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     -Dlog.file=/home/master/software/flink-1.10.1/log/flink-master-client-bigdata02.log
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     -Dlog4j.configuration=file:/home/master/software/flink-1.10.1/conf/log4j-cli.properties
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     -Dlogback.configurationFile=file:/home/master/software/flink-1.10.1/conf/logback.xml
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -  Program Arguments:
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     run
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -m
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     yarn-cluster
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -ys
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     1
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -yjm
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     1G
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -ytm
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     2G
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -d
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -c
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     examples/streaming/TopSpeedWindowing.jar
> 2021-01-25 20:55:27,396 INFO  org.apache.flink.client.cli.CliFrontend                       -  Classpath: /home/master/software/flink-1.10.1/lib/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar:/home/master/software/flink-1.10.1/lib/flink-table_2.11-1.10.1.jar:/home/master/software/flink-1.10.1/lib/flink-table-blink_2.11-1.10.1.jar:/home/master/software/flink-1.10.1/lib/log4j-1.2.17.jar:/home/master/software/flink-1.10.1/lib/slf4j-log4j12-1.7.15.jar:/home/master/software/flink-1.10.1/lib/flink-dist_2.11-1.10.1.jar:/home/master/software/hadoop-2.7.3/etc/hadoop:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/api-asn1-api-1.0.0-M20.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/mockito-all-1.8.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-collections-3.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/xz-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jsp-api-2.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/paranamer-2.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/java-xmlbuilder-0.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/junit-4.11.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/avro-1.7.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-cli-1.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jets3t-0.9.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/stax-api-1.0-2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-logging-1.1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/curator-framework-2.7.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jsch-0.1.42.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-codec-1.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/xmlenc-0.52.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/curator-client-2.7.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-net-3.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/snappy-java-1.0.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jaxb-api-2.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/apacheds-i18n-2.0.0-M15.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/slf4j-api-1.7.10.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/hadoop-auth-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/httpclient-4.2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/htrace-core-3.1.0-incubating.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/api-util-1.0.0-M20.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jsr305-3.0.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-compress-1.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-digester-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jettison-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/servlet-api-2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-beanutils-1.7.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-httpclient-3.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/hamcrest-core-1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-configuration-1.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/guava-11.0.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/activation-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/hadoop-annotations-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/zookeeper-3.4.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-math3-3.1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jersey-json-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jetty-util-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-lang-2.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/httpcore-4.2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jetty-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/gson-2.2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/curator-recipes-2.7.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/hadoop-common-2.7.3-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/hadoop-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/hadoop-nfs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/netty-all-4.0.23.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/xercesImpl-2.9.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-codec-1.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/xmlenc-0.52.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/xml-apis-1.3.04.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/htrace-core-3.1.0-incubating.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/servlet-api-2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/guava-11.0.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jetty-util-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jetty-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/hadoop-hdfs-nfs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/hadoop-hdfs-2.7.3-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/hadoop-hdfs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-collections-3.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/xz-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-cli-1.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/zookeeper-3.4.6-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/stax-api-1.0-2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-client-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-logging-1.1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-guice-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-codec-1.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/leveldbjni-all-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jaxb-api-2.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jsr305-3.0.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-compress-1.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jettison-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/servlet-api-2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-jaxrs-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/javax.inject-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/aopalliance-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/guice-servlet-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/guava-11.0.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/activation-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/guice-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/zookeeper-3.4.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-xc-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-json-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jaxb-impl-2.2.3-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jetty-util-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-lang-2.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jetty-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-nodemanager-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-api-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-client-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-registry-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-tests-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-web-proxy-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/xz-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/paranamer-2.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/junit-4.11.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/avro-1.7.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jersey-guice-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/leveldbjni-all-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/snappy-java-1.0.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/commons-compress-1.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/javax.inject-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/aopalliance-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/guice-servlet-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/guice-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.3.jar:/home/master/software/hadoop-2.7.3/contrib/capacity-scheduler/*.jar:/home/master/software/hadoop-2.7.3/etc/hadoop:
> 2021-01-25 20:55:27,396 INFO  org.apache.flink.client.cli.CliFrontend                       - --------------------------------------------------------------------------------
> 2021-01-25 20:55:27,427 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.address, bigdata02
> 2021-01-25 20:55:27,428 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.port, 6123
> 2021-01-25 20:55:27,428 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.heap.size, 864m
> 2021-01-25 20:55:27,428 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.memory.process.size, 1728m
> 2021-01-25 20:55:27,429 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.numberOfTaskSlots, 1
> 2021-01-25 20:55:27,429 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: parallelism.default, 1
> 2021-01-25 20:55:27,429 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.execution.failover-strategy, region
> 2021-01-25 20:55:28,382 INFO  org.apache.flink.runtime.security.modules.HadoopModule        - Hadoop user set to master (auth:SIMPLE), credentials check status: true
> 2021-01-25 20:55:28,447 INFO  org.apache.flink.runtime.security.modules.JaasModule          - Jaas file will be created as /tmp/jaas-6642237344757555961.conf.
> 2021-01-25 20:55:28,460 INFO  org.apache.flink.client.cli.CliFrontend                       - Running 'run' command.
> 2021-01-25 20:55:28,507 INFO  org.apache.flink.client.cli.CliFrontend                       - Building program from JAR file
> 2021-01-25 20:55:28,686 WARN  org.apache.flink.yarn.cli.FlinkYarnSessionCli                 - The configuration directory ('/home/master/software/flink-1.10.1/conf') already contains a LOG4J config file.If you want to use logback, then please delete or rename the log configuration file.
> 2021-01-25 20:55:28,730 INFO  org.apache.flink.client.ClientUtils                           - Starting program (detached: true)
> 2021-01-25 20:55:30,059 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.address, bigdata02
> 2021-01-25 20:55:30,060 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.port, 6123
> 2021-01-25 20:55:30,060 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.heap.size, 864m
> 2021-01-25 20:55:30,060 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.memory.process.size, 1728m
> 2021-01-25 20:55:30,063 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.numberOfTaskSlots, 1
> 2021-01-25 20:55:30,063 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: parallelism.default, 1
> 2021-01-25 20:55:30,065 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.execution.failover-strategy, region
> 2021-01-25 20:55:30,224 INFO  org.apache.hadoop.yarn.client.RMProxy                         - Connecting to ResourceManager at bigdata01/192.168.233.130:18040
> 2021-01-25 20:55:31,123 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - No path for the flink jar passed. Using the location of class org.apache.flink.yarn.YarnClusterDescriptor to locate the jar
> 2021-01-25 20:55:31,722 WARN  org.apache.flink.yarn.YarnClusterDescriptor                   - Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set. The Flink YARN Client needs one of these to be set to properly load the Hadoop configuration for accessing YARN.
> 2021-01-25 20:55:31,842 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - Cluster specification: ClusterSpecification{masterMemoryMB=1024, taskManagerMemoryMB=2048, slotsPerTaskManager=1}
> 2021-01-25 20:55:45,269 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - Submitting application master application_1611579253158_0001
> 2021-01-25 20:55:45,833 INFO  org.apache.hadoop.yarn.client.api.impl.YarnClientImpl         - Submitted application application_1611579253158_0001
> 2021-01-25 20:55:45,834 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - Waiting for the cluster to be allocated
> 2021-01-25 20:55:45,841 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - Deploying cluster, current state ACCEPTED
> 2021-01-25 20:56:10,387 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - YARN application has been deployed successfully.
> 2021-01-25 20:56:10,389 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - The Flink YARN session cluster has been started in detached mode. In order to stop Flink gracefully, use the following command:
> $ echo "stop" | ./bin/yarn-session.sh -id application_1611579253158_0001
> If this should not be possible, then you can also kill Flink via YARN's web interface or via:
> $ yarn application -kill application_1611579253158_0001
> Note that killing Flink might not clean up all job artifacts and temporary files.
> 2021-01-25 20:56:10,390 INFO  org.apache.flink.yarn.YarnClusterDescriptor                   - Found Web Interface bigdata02:56086 of application 'application_1611579253158_0001'.
> 2021-01-25 20:56:10,432 INFO  org.apache.flink.client.deployment.executors.AbstractJobClusterExecutor  - Job has been submitted with JobID 396d12058f05be4e01f15d41ef6683f0



#### 二、程序执行流程

```java
CliFrontend.main
	CliFrontend.parseParameters
		CliFrontend.run
			CliFrontend.buildProgram
			CliFrontend.getEffectiveConfiguration
				FlinkYarnSessionCli.applyCommandLineOptionsToConfiguration
					FlinkYarnSessionCli.applyDescriptorOptionToConfig
						FlinkYarnSessionCli.setLogConfigFileInConfig
							FlinkYarnSessionCli.discoverLogConfigFile
			CliFrontend.executeProgram
				ClientUtils.executeProgram
					PackagedProgram.invokeInteractiveModeForExecution
						用户类(TopSpeedWindowing).main
							StreamExecutionEnvironment.execute(String jobName)
								StreamExecutionEnvironment.execute(StreamGraph streamGraph)
									StreamExecutionEnvironment.executeAsync(StreamGraph streamGraph)
										YarnJobClusterExecutor.execute
											YarnClusterClientFactory.createClusterDescriptor
												YarnClusterClientFactory.getClusterDescriptor
```



#### 三、详细分析

##### 一、输出环境信息

第一段日志打印的主要是环境信息，包括系统用户，hadoop用户，JVM版本，堆内存，JAVA_HOME，JVM参数，程序参数，Classpath等信息。

代码为CliFrontend的main方法的这一句：

```
EnvironmentInformation.logEnvironmentInfo(LOG, "Command Line Client", args);
```

日志内容如下：

> 2021-01-25 20:55:25,530 INFO  org.apache.flink.client.cli.CliFrontend                       - --------------------------------------------------------------------------------
> 2021-01-25 20:55:25,548 INFO  org.apache.flink.client.cli.CliFrontend                       -  Starting Command Line Client (Version: 1.10.1, Rev:c5915cf, Date:07.05.2020 @ 13:58:51 CST)
> 2021-01-25 20:55:25,549 INFO  org.apache.flink.client.cli.CliFrontend                       -  OS current user: master
> 2021-01-25 20:55:27,383 INFO  org.apache.flink.client.cli.CliFrontend                       -  Current Hadoop/Kerberos user: master
> 2021-01-25 20:55:27,383 INFO  org.apache.flink.client.cli.CliFrontend                       -  JVM: Java HotSpot(TM) 64-Bit Server VM - Oracle Corporation - 1.8/25.181-b13
> 2021-01-25 20:55:27,384 INFO  org.apache.flink.client.cli.CliFrontend                       -  Maximum heap size: 633 MiBytes
> 2021-01-25 20:55:27,384 INFO  org.apache.flink.client.cli.CliFrontend                       -  JAVA_HOME: /home/master/software/jdk1.8.0_181
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -  Hadoop version: 2.7.5
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -  JVM Options:
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     -Dlog.file=/home/master/software/flink-1.10.1/log/flink-master-client-bigdata02.log
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     -Dlog4j.configuration=file:/home/master/software/flink-1.10.1/conf/log4j-cli.properties
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     -Dlogback.configurationFile=file:/home/master/software/flink-1.10.1/conf/logback.xml
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -  Program Arguments:
> 2021-01-25 20:55:27,394 INFO  org.apache.flink.client.cli.CliFrontend                       -     run
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -m
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     yarn-cluster
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -ys
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     1
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -yjm
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     1G
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -ytm
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     2G
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -d
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     -c
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     org.apache.flink.streaming.examples.windowing.TopSpeedWindowing
> 2021-01-25 20:55:27,395 INFO  org.apache.flink.client.cli.CliFrontend                       -     examples/streaming/TopSpeedWindowing.jar
> 2021-01-25 20:55:27,396 INFO  org.apache.flink.client.cli.CliFrontend                       -  Classpath: /home/master/software/flink-1.10.1/lib/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar:/home/master/software/flink-1.10.1/lib/flink-table_2.11-1.10.1.jar:/home/master/software/flink-1.10.1/lib/flink-table-blink_2.11-1.10.1.jar:/home/master/software/flink-1.10.1/lib/log4j-1.2.17.jar:/home/master/software/flink-1.10.1/lib/slf4j-log4j12-1.7.15.jar:/home/master/software/flink-1.10.1/lib/flink-dist_2.11-1.10.1.jar:/home/master/software/hadoop-2.7.3/etc/hadoop:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/api-asn1-api-1.0.0-M20.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/mockito-all-1.8.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-collections-3.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/xz-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jsp-api-2.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/paranamer-2.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/java-xmlbuilder-0.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/junit-4.11.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/avro-1.7.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-cli-1.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jets3t-0.9.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/stax-api-1.0-2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-logging-1.1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/curator-framework-2.7.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jsch-0.1.42.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-codec-1.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/xmlenc-0.52.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/curator-client-2.7.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-net-3.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/snappy-java-1.0.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jaxb-api-2.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/apacheds-i18n-2.0.0-M15.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/slf4j-api-1.7.10.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/hadoop-auth-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/httpclient-4.2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/htrace-core-3.1.0-incubating.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/api-util-1.0.0-M20.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jsr305-3.0.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-compress-1.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-digester-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jettison-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/servlet-api-2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-jaxrs-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/apacheds-kerberos-codec-2.0.0-M15.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-beanutils-1.7.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-httpclient-3.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/hamcrest-core-1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-configuration-1.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/guava-11.0.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/activation-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/hadoop-annotations-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/zookeeper-3.4.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jackson-xc-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-math3-3.1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jersey-json-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jaxb-impl-2.2.3-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jetty-util-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/commons-lang-2.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/httpcore-4.2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/jetty-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/gson-2.2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/curator-recipes-2.7.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/hadoop-common-2.7.3-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/hadoop-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/common/hadoop-nfs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/netty-all-4.0.23.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/xercesImpl-2.9.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-cli-1.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-logging-1.1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-codec-1.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/xmlenc-0.52.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/leveldbjni-all-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/xml-apis-1.3.04.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/htrace-core-3.1.0-incubating.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jsr305-3.0.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/servlet-api-2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-daemon-1.0.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/guava-11.0.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jetty-util-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/commons-lang-2.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/jetty-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/hadoop-hdfs-nfs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/hadoop-hdfs-2.7.3-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/hdfs/hadoop-hdfs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-collections-3.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/xz-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-cli-1.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/zookeeper-3.4.6-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/stax-api-1.0-2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-client-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-logging-1.1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-guice-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-codec-1.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/leveldbjni-all-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jaxb-api-2.2.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jsr305-3.0.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-compress-1.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jettison-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/servlet-api-2.5.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-jaxrs-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/javax.inject-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/aopalliance-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/guice-servlet-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/guava-11.0.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/activation-1.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/guice-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/zookeeper-3.4.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jackson-xc-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jersey-json-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jaxb-impl-2.2.3-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jetty-util-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/commons-lang-2.6.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/jetty-6.1.26.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-nodemanager-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-resourcemanager-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-api-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-client-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-registry-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-applications-distributedshell-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-applicationhistoryservice-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-tests-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-web-proxy-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/yarn/hadoop-yarn-server-sharedcachemanager-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/xz-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/protobuf-java-2.5.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/netty-3.6.2.Final.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/paranamer-2.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jackson-core-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jackson-mapper-asl-1.9.13.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/junit-4.11.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/avro-1.7.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jersey-guice-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/leveldbjni-all-1.8.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/snappy-java-1.0.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/log4j-1.2.17.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/commons-io-2.4.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/commons-compress-1.4.1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jersey-core-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/javax.inject-1.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/aopalliance-1.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/guice-servlet-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/hamcrest-core-1.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/jersey-server-1.9.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/guice-3.0.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/lib/asm-3.2.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-plugins-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-shuffle-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-app-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-hs-2.7.3.jar:/home/master/software/hadoop-2.7.3/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.3.jar:/home/master/software/hadoop-2.7.3/contrib/capacity-scheduler/*.jar:/home/master/software/hadoop-2.7.3/etc/hadoop:
> 2021-01-25 20:55:27,396 INFO  org.apache.flink.client.cli.CliFrontend                       - --------------------------------------------------------------------------------



##### 二、输出flink配置信息

第二段日志主要输出的是flink的配置信息，主要包括jobmanager地址，端口号，堆内存，taskmanager内存，slot数，并行度，失败策略等，其中jobmanager的地址和端口号因为是在yarn模式运行，所以是无效的。

代码如下：

```java
CliFrontend.main(final String[] args) 
	final Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
		loadConfiguration(configDir, null);
			Configuration configuration = loadYAMLResource(yamlConfigFile);
```

日志内容如下：

> 2021-01-25 20:55:27,427 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.address, bigdata02
> 2021-01-25 20:55:27,428 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.rpc.port, 6123
> 2021-01-25 20:55:27,428 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.heap.size, 864m
> 2021-01-25 20:55:27,428 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.memory.process.size, 1728m
> 2021-01-25 20:55:27,429 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: taskmanager.numberOfTaskSlots, 1
> 2021-01-25 20:55:27,429 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: parallelism.default, 1
> 2021-01-25 20:55:27,429 INFO  org.apache.flink.configuration.GlobalConfiguration            - Loading configuration property: jobmanager.execution.failover-strategy, region



##### 三、安全配置

第三段日志主要是关于安全配置的，主要是Hadoop和Jaas。

代码如下：

```java
CliFrontend.main(final String[] args) 
	SecurityUtils.install(new SecurityConfiguration(cli.configuration));
		SecurityModule module = moduleFactory.createModule(config);
		HadoopModule.install()
        JaasModule.install()
```

日志内容如下：

```java
2021-01-25 20:55:28,382 INFO  org.apache.flink.runtime.security.modules.HadoopModule        - Hadoop user set to master (auth:SIMPLE), credentials check status: true
2021-01-25 20:55:28,447 INFO  org.apache.flink.runtime.security.modules.JaasModule          - Jaas file will be created as /tmp/jaas-6642237344757555961.conf.
```



##### 四、执行action操作

第四段日志主要就是根据参数，执行具体的action操作。

代码流程如下：

```java
CliFrontend.main(final String[] args) 
	int retCode = SecurityUtils.getInstalledContext().runSecured(() -> cli.parseParameters(args));
		parseParameters(String[] args)
```

我们提交任务，执行的是run的动作。

所以走的是CliFrontend类里面这个分支：

```
protected void run(String[] args) throws Exception {}
```

日志：

> 2021-01-25 20:55:28,460 INFO  org.apache.flink.client.cli.CliFrontend                       - Running 'run' command.

后面会构建程序：

```
LOG.info("Building program from JAR file");
program = buildProgram(programOptions);
```



##### 五、处理启动参数

第五段日志主要是处理参数的。

代码流程如下：

```java
run(String[] args)
	getEffectiveConfiguration(commandLine, programOptions, jobJars);
		// customCommandLine实际上就是FlinkYarnSessionCli
		customCommandLine.applyCommandLineOptionsToConfiguration(commandLine);
			applyDescriptorOptionToConfig(commandLine, effectiveConfiguration);
				setLogConfigFileInConfig(configuration, configurationDirectory);
					FlinkYarnSessionCli.discoverLogConfigFile(configurationDirectory)
                        // 这个方法里面，如果存在logback文件，就会出现日志存在的log
```

日志如下：

> 2021-01-25 20:55:28,686 WARN  org.apache.flink.yarn.cli.FlinkYarnSessionCli                 - The configuration directory ('/home/master/software/flink-1.10.1/conf') already contains a LOG4J config file.If you want to use logback, then please delete or rename the log configuration file.



##### 六、执行程序

第六段是执行程序的方法，这个方法最终会调用我们自己写的程序类的main方法。

代码如下：

```java
CliFrontend.run(String[] args)
	executeProgram(effectiveConfiguration, program);
		ClientUtils.executeProgram(DefaultExecutorServiceLoader.INSTANCE, configuration, program);
			// 这个方法里面会输出starting program日志
```



##### 七、执行自己编写的代码程序

这里我的测试用例是官方的example中的TopSpeedWindowing。

代码最终会走到StreamExecutionEnvironment的execute方法：

```java
env.execute("CarTopSpeedWindowingExample");
```

之后的代码调用如下：

```java
execute(getStreamGraph(jobName)
	executeAsync(streamGraph)
        // 得到YarnJobClusterExecutorFactory
        executorServiceLoader.getExecutorFactory(configuration);
        	// 得到YarnJobClusterExecutor()，然后执行executor方法
        	executorFactory.getExecutor(configuration).execute(streamGraph, configuration);
        		clusterClientFactory.createClusterDescriptor(configuration)
        			getClusterDescriptor(configuration)
```

getClusterDescriptor(configuration)方法中的代码比较重要，代码如下：

```java
private YarnClusterDescriptor getClusterDescriptor(Configuration configuration) {
		final YarnClient yarnClient = YarnClient.createYarnClient();
		final YarnConfiguration yarnConfiguration = new YarnConfiguration();

		yarnClient.init(yarnConfiguration);
		yarnClient.start();

		return new YarnClusterDescriptor(
				configuration,
				yarnConfiguration,
				yarnClient,
				YarnClientYarnClusterInformationRetriever.create(yarnClient),
				false);
	}
```

这里面的代码创建了yarnClient，初始化，调用了start方法，然后返回YarnClusterDescriptor。

这里面的代码没有源码，所以没看到，但是根据日志分析，主要进行了如下操作。

1.加载flink配置

2.连接ResourceManager

3.根据hadoop的环境变量来读取配置的，HADOOP_CONF_DIR 或者 YARN_CONF_DIR 或者 HADOOP_HOME，这几个只要有一个配置了就可以。

3.部署应用

4.提交job



其中第二步的代码我猜测在yarnClinet相关代码里面，yarnClient这个对象应该是一个RPC客户端，因为代码不能看，所以没有具体证据。

第三步的代码步骤如下：

```java
YarnClusterDescriptor.getClusterDescriptor(Configuration configuration)
	new YarnClusterDescriptor(xxx)
		getLocalFlinkDistPath(flinkConfiguration)
```

第四步的代码就是部署了，代码步骤如下：

```java
AbstractJobClusterExecutor.execute(Pipeline pipeline,Configuration configuration)
	YarnClusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode())
      deployInternal(xxx)
```

部署的代码也比较重要，具体代码如下：

```java
private ClusterClientProvider<ApplicationId> deployInternal(
      ClusterSpecification clusterSpecification,
      String applicationName,
      String yarnClusterEntrypoint,
      @Nullable JobGraph jobGraph,
      boolean detached) throws Exception {

   if (UserGroupInformation.isSecurityEnabled()) {
      // note: UGI::hasKerberosCredentials inaccurately reports false
      // for logins based on a keytab (fixed in Hadoop 2.6.1, see HADOOP-10786),
      // so we check only in ticket cache scenario.
      boolean useTicketCache = flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);

      boolean isCredentialsConfigured = HadoopUtils.isCredentialsConfigured(
         UserGroupInformation.getCurrentUser(), useTicketCache);
      if (!isCredentialsConfigured) {
         throw new RuntimeException("Hadoop security with Kerberos is enabled but the login user " +
            "does not have Kerberos credentials or delegation tokens!");
      }
   }

   // 检查是否能部署，检查cpu core是否足够，hadoop环境变量配置等
   isReadyForDeployment(clusterSpecification);

   // ------------------ Check if the specified queue exists --------------------

   // 检查yarn队列是否存在
   checkYarnQueues(yarnClient);

   // ------------------ Check if the YARN ClusterClient has the requested resources --------------

    // 通过yarnClient创建yarn应用
   // Create application via yarnClient
   final YarnClientApplication yarnApplication = yarnClient.createApplication();
   final GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

   Resource maxRes = appResponse.getMaximumResourceCapability();

   final ClusterResourceDescription freeClusterMem;
   try {
      freeClusterMem = getCurrentFreeClusterResources(yarnClient);
   } catch (YarnException | IOException e) {
      failSessionDuringDeployment(yarnClient, yarnApplication);
      throw new YarnDeploymentException("Could not retrieve information about free cluster resources.", e);
   }

   final int yarnMinAllocationMB = yarnConfiguration.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 0);

   // 验证jobmanager和taskmanager需要的资源是否充足
   final ClusterSpecification validClusterSpecification;
   try {
      validClusterSpecification = validateClusterResources(
            clusterSpecification,
            yarnMinAllocationMB,
            maxRes,
            freeClusterMem);
   } catch (YarnDeploymentException yde) {
      failSessionDuringDeployment(yarnClient, yarnApplication);
      throw yde;
   }

   LOG.info("Cluster specification: {}", validClusterSpecification);

   final ClusterEntrypoint.ExecutionMode executionMode = detached ?
         ClusterEntrypoint.ExecutionMode.DETACHED
         : ClusterEntrypoint.ExecutionMode.NORMAL;

   flinkConfiguration.setString(ClusterEntrypoint.EXECUTION_MODE, executionMode.toString());

   // 启动applicationMaster
   ApplicationReport report = startAppMaster(
         flinkConfiguration,
         applicationName,
         yarnClusterEntrypoint,
         jobGraph,
         yarnClient,
         yarnApplication,
         validClusterSpecification);

   // print the application id for user to cancel themselves.
   if (detached) {
      final ApplicationId yarnApplicationId = report.getApplicationId();
      logDetachedClusterInformation(yarnApplicationId, LOG);
   }

   setClusterEntrypointInfoToConfig(report);

   return () -> {
      try {
         return new RestClusterClient<>(flinkConfiguration, report.getApplicationId());
      } catch (Exception e) {
         throw new RuntimeException("Error while creating RestClusterClient.", e);
      }
   };
}
```

```
startAppMaster(
      flinkConfiguration,
      applicationName,
      yarnClusterEntrypoint,
      jobGraph,
      yarnClient,
      yarnApplication,
      validClusterSpecification);
```