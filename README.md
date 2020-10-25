



# 概述：

mqproxy是基于 java客户端实现的一款kafka代理工具，它实现了生产、消费、延时队列等功能。目前我们只开源了生产者代理和延时队列代理，消费者代理我们会陆续进行开源。如果您正在正在被kafka不支持延时队列而困扰，mqproxy可能会为你提供一种新的思路。

# 一、设计思路：

## 1、生产者代理

​	设计思路：我们将生产的消息通过[mapdb](http://www.mapdb.org/)进行临时存储来保证消息传输的可靠性，同时利用[mapdb](http://www.mapdb.org/)的高吞吐量来保证代理的生产效率。

## 2、延时队列代理

​	设计思路：设计上，我们借鉴了多层时间轮的概念，将时间轮的等级和刻度设计为kafka的topic，通过不同延时级别的topic来传递消息，这样的设计避免了因代理意外宕机导致的数据难以恢复的问题和任意时延需求下kafka位移提交的问题。

举例：我们预先在kafka上创建4组topic（用户可以在代码中自定义组数），延时级别分别为

1s，2s，3s ... 9s, 

10s,20s,30s ... 90s ,

100s ,200s ,300s ...900s,

1000s,2000s,3000s ...9000s。

### 	  例1：当生产者代理收到一个延时级别为119s的延时请求：

​		1、**生产者代理**会把消息投入到延时级别为**100s的topic**中。

​		2、**延时队列代理**检测到100s 延时级别的请求，则将消息从队列拉取到**内存中**进行**100s倒计时**。

​		3、当100s倒计时结束后，发现还有19s的剩余时间，则会把消息继续向下投递到延时级别为**10s的topic**中，同时向延时级别为100s的topic**提交位移**。

​		4、**延时队列代理**检测到10s 延时级别的请求，则将消息从队列拉取到**内存中**进行**10s倒计时**。

​		5、当10s倒计时结束后，发现还有9s的剩余时间，则会把消息继续向下投递到延时级别为**9s的topic中**，同时向延时级别为10s的topic**提交位移**。

​		6、**延时队列代理**检测到9s 延时级别的请求，则将消息从队列拉取到内存中进行9s倒计时。

​		7、当9s倒计时结束后，发现延时时间已经被耗尽，则把消息投入到**原topic**，同时向延时级别为9s的topic**提交位移**，逐级传递的流程结束。

###      例2：当生产者代理收到一个延时级别为20001s的延时请求：

​		1、消息会在延时级别为**9000s**的延时队列中重复投递2次，直至剩余2001s，

​		2、重复“例1”中的逐级传递流程，直到投入到原topic为止



# 二、安装运行：

## 1、下载并安装jdk1.8

以cenos安装openjdk1.8为例

```
yum install -y java-1.8.0-openjdk
```

```
java -version
```

## 2、安装maven环境（略），编译打包：

```
mvn clean package -Dmaven.test.skip=true
```

## 3、运行jar包

生产者代理jar：kafka-producer-service-1.0.0，这里给出了一个启动命令，读者可以根据自身情况进行参数设定

```
nohup java -jar kafka-producer-service-1.0.0 --kafka.producer.server="127.0.0.1:9092" >/dev/null 2>&1 &
```

如果有延时消息请求，延时队列代理：kafka-delay-anytime-service-0.0.1-SNAPSHOT

```
nohup java -jar kafka-delay-anytime-service-1.0.0 --kafka.bootstrap-servers="127.0.0.1:9092" >/dev/null 2>&1 &
```



# 三、使用

1、发送普通消息

```
curl  --request POST '127.0.0.1:8080/v1/kafka/send' --header 'Content-Type: application/json' --data '{"headers": {"content": "goods","instant": "yes"},"topic": "topic-open","payload": "hello proxy"}'
```

2、发送延时时间为19秒的延时消息

```
curl  --request POST '127.0.0.1:8080/v1/kafka/send' --header 'Content-Type: application/json' --data '{"headers": {"content": "goods","instant": "yes","x-delay": "11"},"topic": "topic-open","payload": "hello delay"}'
```

**ps：生产者代理可以独立安装部署，延时队列代理需要借助生产者代理进行消息发送。**

