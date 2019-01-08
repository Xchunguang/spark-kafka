
# kafka和spark总结

## 本文涉及到的技术版本号：

- scala 2.11.8 
- kafka1.1.0 
- spark2.3.1

## kafka简介
kafka是一个分布式流平台，流媒体平台有三个功能
- 发布和订阅记录流
- 以容错的持久化的方式存储记录流
- 发生数据时对流进行处理

kafka通常用于两大类应用
- 构件在系统或应用程序之间可靠获取数据的实时数据管道
- 构件转换或响应数据流的实时流应用程序

kafka的几个概念
- kafka运行在集群上，或一个或多个能跨越数据中心的服务器上
- kafka集群上存储流记录的称为topic
- kafka的topic里，每一条记录包括一个key、一个value和一个时间戳timestamp

kafka有四个核心API

  - Producer API
    
    生产者api允许应用程序发布一个记录流到一个或多个kafka的topic
    
  - Consumer API 

    消费者api允许应用程序订阅一个或多个topic并且接受处理传送给消费者的数据流
    
  - Streams API

    流api允许应用程序作为一个流处理器，从一个或多个输入topic中消费输入流，并生产一个输出流到一个或多个输出topic中
    
  - Connector API

    连接器api允许构建和运行中的kafka的topic连接到现有的应用程序或数据系统中重用生产者或消费者。例如关系数据库的连接器可以捕获对表的每一个更改操作
  
kafka中的客户端和服务端之间是通过简单、高性能的语言无关的TCP协议完成的，该协议已经版本化并且高版本向低版本向后兼容。

## topics
topic为kafka为记录流提供的核心抽象，类似于数据通道，并且topic是发布记录和订阅的核心。

kafka的topic是多用户的，一个topic可以有0个、1个或多个消费者订阅记录

对于每一个topic，kafka集群都维护了一个如下所示的分区记录：

![topic](https://github.com/Xchunguang/spark-kafka/blob/master/img/log_anatomy.png)

其中每一个分区都是有序的不可变的记录序列，并且新数据是不断的追加到结构化的记录中。分区中的记录每个都分配了一个offset作为ID，它唯一标识分区中的每个记录。

kafka集群默认是保存所有记录，无论是否被消费过，但是可以通过配置保留时间策略。例如如果设置数据保留策略为两天，则超过两天的数据将被丢弃释放空间。kafka的性能受数据大小影响不大，因此长时间的存储数据并不是太大的问题。

其中，kafka 的消费者唯一对topic中的每一个分区都可以设置偏移量offset，标识当前消费者从哪个分区的哪一条数据开始消费，消费者通过对偏移量的设置可以灵活的对topic进行消费。如下图

![offset](https://github.com/Xchunguang/spark-kafka/blob/master/img/log_consumer.png)

消费者控制自己的偏移量就意味着kafka的消费者是轻量的，消费者之间互不影响。

topic记录中的分区有多种用途，首先它允许topic扩展到超出单台服务器适合的大小。每个分区都需要有适合托管分区的服务器，而topic可以有很多分区，因此一个topic可以处理任意数量的数据。另外这些分区作为并行的单位，效率很高，这也是相当重要的一点。

## 分配
记录分区分布在kafka集群服务器上，每个服务器共同处理数据并请求分区的共享。每个分区都可以在可用服务器的数量上进行复制，以此实现容错。

每一个分区都会有一个服务器作为leader，0个或多个服务器作为followers。leader处理分区的所有读取和写入请求，而follower被动的复制leader。如果leader出错，则其中一个follower会自动称为新的leader。集群中的每个服务器都充当某分区的leader和其他分区的follower，因此能在集群中达到负载均衡。

## 生产者
生产者将数据发布到所选择的分区，生产者在发布数据是需要选择将数据发送到哪个分区，分配分区可以通过循环方式完成也可以根据语义分区的功能实现。

## 消费者
消费者使用消费者组（consumer group）标记自己。发布到topic的每个记录会被发送到每个消费者组中的一个消费者实例。所以当一个消费者组中有多个消费者实例，则记录将在该消费者组中的所有消费者之间进行有效的负载均衡。

topic接受的每一条记录都会被广播发送到每个消费者组中。示意图如下：

![消费者](https://github.com/Xchunguang/spark-kafka/blob/master/img/consumer-groups.png)

上图有两个机器的kafka集群，某topic有四个分区p0-p3，有两个消费者组A/B订阅该topic，消费者组A有两个消费者实例，消费者组B有四个消费者实例。

kafka中实现消费的方式是通过在消费者实例上划分分区实现，保证实例在任何时间点都是公平分配的。消费者组中的成员划分分区是由kafka协议进行动态处理。如果新实例加入该组，那新加入的实例会从改组的成员中接管一些分区。如果消费者组中的某个实例死亡，则它所划分的分区分配给该消费组的其他实例。

kafka只能提供一个分区内的记录的顺序，但是不保证多个分区的记录顺序。如果用户想保证topic中的顺序，则使用一个分区的topic即可，但这样就意味着每个消费者组中只能有一个消费者实例。


## kafka提供的保证
- 同一个生产者实例发送到特定topic的特定分区的两条数据M1和M2，并且M1发送早于M2，则M1将拥有更低的偏移量，即可以保证插入顺序。
- 消费者可以按照记录存储的顺序消费记录
- 对于复制因子为N的topic，最多可以容忍N-1个服务器故障，而不会丢失提交到topic中的记录

## kafka常用命令

- 启动Zookeeper server

      bin/zookeeper-server-start.sh config/zookeeper.properties &
  
- 启动Kafka server

      nohup bin/kafka-server-start.sh config/server.properties &
  
- 停止Kafka server

      bin/kafka-server-stop.sh
  
- 停止Zookeeper server

      bin/zookeeper-server-stop.sh
  
- producer

      bin/kafka-console-producer.sh --broker-list 192.168.20.133:9092 --topic realtime
  
- consumer

      bin/kafka-console-consumer.sh --zookeeper 192.168.20.133:2181 --topic realtime --from-beginning

- 查看所有topic
  
      bin/kafka-topics.sh --list --zookeeper localhost:2181
  
- 创建一个topic

      bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic realtime0103
      
- 查看topic详情

      bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic realtime0103
      
- 删除topic

      bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic realtime0103
      
## java操作kafka

引入jar包

      <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka_2.11</artifactId>
        <version>1.1.0</version>
      </dependency>

#### Producer

    import java.util.Properties;
    import java.util.concurrent.ExecutionException;

    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.Producer;
    import org.apache.kafka.clients.producer.ProducerRecord;

    public class ProducerDemo {

      public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.20.133:9092,192.168.20.134:9092,192.168.20.135:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        
        String topic = "realtime0103";

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        String value = "{'name':'1','value':1}" ; 
        
        //设定分区规则，分区为0，1，2
        int partation = KafkaProducerClient.count.get() % 3;

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,partation, "key1",value );
          
        producer.send(record).get();
      }
    }

#### Customer


    import java.util.Collections;
    import java.util.HashMap;
    import java.util.Map;
    import java.util.Properties;

    import org.apache.kafka.clients.consumer.ConsumerRecord;
    import org.apache.kafka.clients.consumer.ConsumerRecords;
    import org.apache.kafka.clients.consumer.KafkaConsumer;
    import org.apache.kafka.clients.consumer.OffsetAndMetadata;
    import org.apache.kafka.common.TopicPartition;


    public class CustomerDemo {

      private static KafkaConsumer<String, String> consumer;
      private static String inputTopic;

      @SuppressWarnings({ "unchecked", "rawtypes" })
      public static void main(String[] args) {
        String groupId = "group1";
        inputTopic = "realtime0103";
        String brokers = "192.168.20.133:9092";

        consumer = new KafkaConsumer(createConsumerConfig(brokers, groupId));
        
        //分配topic 某分区的offset
        TopicPartition part0 = new TopicPartition(inputTopic, 0);
        TopicPartition part1 = new TopicPartition(inputTopic, 1);
        TopicPartition part2 = new TopicPartition(inputTopic, 2);
        OffsetAndMetadata offset0 = new OffsetAndMetadata(1);
        OffsetAndMetadata offset1 = new OffsetAndMetadata(2);
        OffsetAndMetadata offset2 = new OffsetAndMetadata(3);
        Map<TopicPartition,OffsetAndMetadata> offsetMap = new HashMap<>();
        offsetMap.put(part0,offset0);
        offsetMap.put(part1,offset1);
        offsetMap.put(part2,offset2);
        //提交offset信息
        consumer.commitSync(offsetMap);
        
        start();

      }

      private static Properties createConsumerConfig(String brokers, String groupId) {
            Properties props = new Properties();
            props.put("bootstrap.servers", brokers);
            props.put("group.id", groupId);
            props.put("auto.commit.enable", "false");
            props.put("auto.offset.reset", "earliest");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            return props;
        }

      private static void start() {
        consumer.subscribe(Collections.singletonList(inputTopic));

            System.out.println("Reading topic:" + inputTopic);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record: records) {
                    String ip = record.key();
                    String event = record.value();
                    System.out.println(event);
                }
                consumer.commitSync();
            }

      }
    }


## spark操作kafka

#### IDEA配置搭建spark scala开发环境(Windows)
- 安装jdk8并配置环境变量
- 安装scala2.11并配置环境变量(本文安装2.11.2)
- 安装IDEA
- IDEA安装SBT和Scala插件
- File->New->Project 创建新项目，选择Scala->sbt->next

![新建项目](https://github.com/Xchunguang/spark-kafka/blob/master/img/create.jpg)

- 选择项目名称、位置、Java版本号、sbt版本和Scala版本，Finish

![选择版本](https://github.com/Xchunguang/spark-kafka/blob/master/img/select.jpg)

- 打开build.sbt，添加相关依赖

      libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
      libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"
      libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
      libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"

- 刷新sbt项目，下载依赖：

![刷新](https://github.com/Xchunguang/spark-kafka/blob/master/img/refresh.png)

- 编写业务代码，可以使用以下的`使用spark Structured Streaming连接kafka处理流`部分代码
- 设置打包规则
  - File->Project Sturcture->Artifacts 点击绿色加号设置打jar包规则
  
    ![Artifacts](https://github.com/Xchunguang/spark-kafka/blob/master/img/artifact.png)
  
  - 选择Module和Main class，JAR file from libraries选择copy to output..即不将外部jar打包到jar文件中
    
    ![Artifacts](https://github.com/Xchunguang/spark-kafka/blob/master/img/artifact.jpg)
  
  - 导航栏 Build->Build Artifacts ，打包成jar，将jar包上传到spark集群
- 运行程序：
  - 以上配置打出jar包包含项目jar包和多个依赖jar包，提交spark作业时，可以使用--jars逗号隔开配置引用多个外部jar
  
        cd $SPARK_HOME
        ./bin/spark-submit --master spark://192.168.20.133:7077 --jars /root/interface-annotations-1.4.0.jar,/root/async-1.4.1.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 --class com.xuchg.app.Application /root/spark-kafka.jar

#### 使用spark Structured Streaming连接kafka处理流

    import org.apache.log4j.{Level, Logger}
    import org.apache.spark.SparkConf
    import org.apache.spark.sql._
    object Main extends App {

      //spark读取kafka示例
      Logger.getLogger("org").setLevel(Level.ERROR)
      val kafkaAddress = "192.168.20.133:9092"
      val zookeeper = "192.168.20.133:2181"
      val topic = "realtime0103"
      val topicOffset = "{\""+topic+"\":{\"0\":0,\"1\":0,\"2\":0}}"
      val sparkSession = SparkSession
        .builder()
        .config(new SparkConf()
          .setMaster("local[2]")
          .set("spark.streaming.stopGracefullyOnShutdown","true")//设置spark，关掉sparkstreaming程序，并不会立即停止，而是会把当前的批处理里面的数据处理完毕后 才会停掉，此间sparkstreaming不会再消费kafka的数据，这样以来就能保证结果不丢和重复。
          .set("spark.submit.deployMode","cluster")
          .set("spark.executor.memory","4g")//worker内存
          .set("spark.driver.memory","4g")
          .set("spark.cores.max","2")//设置最大核心数
        )
        .appName(getClass.getName)
        .getOrCreate()

      def createStreamDF(spark:SparkSession):DataFrame = {
        import spark.implicits._
        val df = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaAddress)
          .option("zookeeper.connect", zookeeper)
          .option("subscribe", topic)
          .option("startingOffsets", topicOffset)
          .option("enable.auto.commit", "false")
          .option("failOnDataLoss", false)
          .option("includeTimestamp", true)
          .load()
        df
      }

      var df = createStreamDF(sparkSession)

      val query = df.writeStream
        .format("console")
        .start()

      query.awaitTermination()
    }

#### 监控spark和kafka

此处根据实际应用情况使用两种监控方法，解决两个不同问题

- 解决spark启动和停止处理的动作，例如监听spark停止时处理或记录完所有计算

      //定义监听类继承SparkListener，并重写相关方法
      import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}
      class AppListener extends SparkListener{
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          //监控spark停止方法，可以处理spark结束的动作
          println("application 关闭")
        }

        override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
          println("application 启动")
        }
      }
      
      //在主类中注册监听类
      sparkSession.sparkContext.addSparkListener(new AppListener)

- 监控spark的查询，例如spark读取kafka流的偏移量offset，可以监听并记录下来，下次启动spark可以直接从该偏移量offset进行消费，不会消费相同的数据

      sparkSession.streams.addListener(new StreamingQueryListener() {
        override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
          println("Query started: " + queryStarted.id)
        }
        override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
          //服务出现问题而停止
          println("Query terminated: " + queryTerminated.id)
        }
        override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
          var progress = queryProgress.progress
          var sources = progress.sources
          if(sources.length>0){
            var a = 0
            for(a <- 0 to sources.length - 1){
              var offsetStr = sources.apply(a).startOffset
              if(offsetStr!=null){
                println("检测offset是否变化 -- " + offsetStr)
              }
            }
          }
        }
      })
      
  运行结果如下：可以看到对topic的每个分区的偏移量都可以获取到
  ![offset](https://github.com/Xchunguang/spark-kafka/blob/master/img/offset.png)
      
#### 管理和停止spark程序
在spark集群主节点配置并使用spark history server可以实现对spark作业进行管理和监控
- 配置spark history server
  - 修改$SPARK_HOME/conf/spark-defaults.conf，如果不存在则从模板复制一份
  
        cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
        vi $SPARK_HOME/conf/spark-defaults.conf
        
  - 修改配置文件如下：

        spark.eventLog.enabled           true
        spark.eventLog.dir               hdfs://192.168.20.133:9000/spark-history
        spark.eventLog.compress          true
        # 注意ip地址需要根据情况更改，9000为hdfs默认端口号，如hdfs端口号不是9000则需要更改

  - 创建hdfs目录
  
        $HADOOP_HOME/bin/hdfs dfs -mkdir /spark-history
        
  - 配置$SPARK_HOME/conf/spark-env.sh文件：
    - 如果不存在则从模板复制：
    
          cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
          
    - 编辑$SPARK_HOME/conf/spark-env.sh，结尾添加：
    
          export SPARK_HISTORY_OPTS="-Dspark.history.ui.port=18080 -Dspark.history.retainedApplications=3 -Dspark.history.fs.logDirectory=hdfs://192.168.20.133:9000/spark-history"
          # 18080为history server的访问端口号，192.168.20.133:9000为hdfs的ip和端口，根据实际情况修改
          
          
  - 打开防火墙18080端口
  - 执行命令打开history server服务
  
        $SPARK_HOME/sbin/start-history-server.sh
        
- 代码中sparkSession创建时添加配置：
  
      //设置spark，关掉sparkstreaming程序，并不会立即停止，而是会把当前的批处理里面的数据处理完毕后 才会停掉，此间sparkstreaming不会再消费kafka的数据，这样以来就能保证结果不丢和重复。
      new SparkConf().set("spark.streaming.stopGracefullyOnShutdown","true")
      
- 使用shell关掉某一个正在运行的spark作业：
  - spark作业关闭原理
    每一个spark作业都由一个appId唯一标识，而每一个作业包含多个Executors执行器，这些Executors中包含1个或几个id为driver的驱动程序，它是执行开发程序中的 main方法的进程。如果驱动程序停止，则当前spark作业就结束了。
    
  - spark关闭过程
    
    - 获取某appId的spark作业的driver节点的ip和端口，可以通过spark history server提供的页面或提供的api进行获取。此处介绍页面获取，后面会介绍api获取
      
      ![finddriver](https://github.com/Xchunguang/spark-kafka/blob/master/img/finddriver.png)
    
    - 根据获取的driver的端口号对spark作业发送停止命令，当然有时ctrl+c和在监控页面上都是可以直接停止的，但此处只提用shell停止，应用场景更广泛。
    
          centod7：ss -tanlp |  grep 60063|awk '{print $6}'|awk  -F, '{print $2}'|awk -F= '{print $2}'|xargs kill -15
          centos6：ss -tanlp |  grep 60063|awk '{print $6}'|awk  -F, '{print $2}'|xargs kill -15
    
      注意：centos6和centos7稍有不同，而且此处使用kill -15而不是kill -9，kill -9会直接杀死进程，可能会导致丢失数据。而kill -15是发送停止命令，不会直接杀死进程。
      
  - 通过以上内容可以实现在spark集群主节点部署web服务接收并远程调用执行shell语句来达到远程动态启动(可传参)和停止spark作业，大体如下：
    - 远程调用接口传参启动spark作业，此时记录下spark运行的appid
    - 通过调用spark history server提供的REST Api获取当前作业driver进程的端口号：
    
          http://192.168.20.133:18080/api/v1/applications/{appId}/allexecutors
          
      ![driver](https://github.com/Xchunguang/spark-kafka/blob/master/img/driver.png)
    
    - 通过获取到的端口号可以向spark集群主节点发送停止命令到该端口进程即可      
    