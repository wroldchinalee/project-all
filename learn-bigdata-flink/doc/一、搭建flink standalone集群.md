### 一、搭建flink standalone集群



**flink集群单独运行起来不需要其他组件，只需要jdk1.8或以上版本。**

#### 一、flink下载

安装包下载地址：http://flink.apache.org/downloads.html

![image-20210106215001338](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106215001338.png)

我下载的是Flink 1.10.1版本。

选择flink-1.10.1-bin-scala_2.11.tgz 

![image-20210106215059602](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106215059602.png)



#### 二、安装

##### 1.机器

我这边就安装了两个节点，如下：

| ip              | 主机名    | 用户   |
| --------------- | --------- | ------ |
| 192.168.233.131 | bigdata02 | master |
| 192.168.233.132 | bigdata03 | master |

##### 2.解压

将下载的安装包flink-1.10.1-bin-scala_2.11.tgz 解压到指定的安装目录/home/master/software

```shell
tar -xvf flink-1.10.1-bin-scala_2.11.tgz -C /home/master/software/
```



##### 3.修改配置文件

修改flink-conf.yaml文件

```shell
cd /home/master/software/flink-1.10.1/conf
vim flink-conf.yaml
```

修改配置：

```
将jobmanager的地址配置为bigdata02
jobmanager.rpc.address: bigdata02
```

修改slaves文件

```shell
vim slaves
```

将taskmanager所在的机器主机名配置进去，配置后如图：

```
[master@bigdata02 conf]$ more slaves
bigdata02
bigdata03
```



##### 4.将所有需要安装的节点重复上面解压和配置的步骤

使用scp命令即可。



##### 5.启动flink集群

```
cd /home/master/software/flink-1.10.1/bin
./start-cluster.sh
```

##### 6.查看webUI

如果启动成功可以通过webUI查看集群状态

地址：http://192.168.233.131:8081

如图：

![image-20210106221246873](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106221246873.png)

从图中可以看出来，我一共启动了两个TaskManager，每个TaskManager有一个Task Slot。



启动中遇到的问题：

![image-20210106221547537](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106221547537.png)

上面的图是flink-conf.yaml的配置文件。

默认情况下

> jobmanager.heap.size: 1024m
>
> taskmanager.memory.process.size: 1728m

我把这两个参数修改为

> jobmanager.heap.size: 512m
>
> taskmanager.memory.process.size: 864m

发现启动集群后，只有JobManager而没有TaskManager，猜测是分配给TaskManager的内存不够，

所以TaskManager启动应该是有一个最小的内存限制。

修改成默认值后启动成功。



##### 7.运行实例

先在一台机器上启动netcat

```shell
[master@bigdata02 conf]$ nc -l 9000
```

然后运行flink自带实例

```shell
cd /home/master/software/flink-1.10.1
./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9000
```

提交成功后webUI如图：

![image-20210106222646446](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106222646446.png)

我们输入数据

![image-20210106222851802](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106222851802.png)

我们进入到TaskManager的日志中可以看到输出结果

如下图：

![image-20210106223058477](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106223058477.png)

![image-20210106223123944](C:\Users\Administrator.USER-20190415PP\AppData\Roaming\Typora\typora-user-images\image-20210106223123944.png)



##### 8.进程情况

集群启动两台机器中运行的进程如下

*bigdata02*

```
[master@bigdata02 ~]$ jps | grep -v Jps
7481 TaskManagerRunner
8203 CliFrontend
7102 StandaloneSessionClusterEntrypoint
```

*bigdata03*

```
[master@bigdata03 conf]$ jps | grep -v Jps
3490 TaskManagerRunner
```

其中StandaloneSessionClusterEntrypoint是主节点，相当于集群的Master，

在Standalone模式下，包括了JobManager，ResourceManager，Dispather。

TaskManager相当于集群的Worker工作节点。

CliFrontend是客户端程序。