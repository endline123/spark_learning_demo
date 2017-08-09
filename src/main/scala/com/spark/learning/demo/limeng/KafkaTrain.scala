package com.spark.learning.demo.limeng

import java.util.Properties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by 李孟 on 2017/8/2.
  */

object KafkaTrain {

  private val brokers = "localhost:9092"
  // Zookeeper connection properties
  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  val kafkaConfig = new ProducerConfig(props)
  val producer = new Producer[String, String](kafkaConfig)
  val sourceTopic="mytest"
  val targetTopic="mytest2"

  def main(args: Array[String]) {

    // 初始化配置
    val sparkConf = new SparkConf()
      .setAppName("KafkaTrain")
      .setMaster("local[2]")
    // 初始化sparkContext
    val sc = new SparkContext(sparkConf)
    // 初始化streamingContext
    val ssc = new StreamingContext(sc, Seconds(10))
    // 设置参数
    val numThreads = 1
    val group = "testGroup"   // 自己另外设置group
    val zkQuorum = "localhost:2181"
    //val zkQuorum = "192.168.1.138:2181"
    val topicMap = sourceTopic.split(",").map((_, numThreads)).toMap
    //设置kafka的topic,kafka 里面的分组group，zkQuorum 指的是zookee集群
    // line: yuxia,18,female,2342342,chengdu
    val kafkaStream: ReceiverInputDStream[(String, String)]
    = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    //    kafkaStream.print()     // 需要替换成统计逻辑并且输出至文件

    val result = kafkaStream.flatMap(_._2.split(",")).map((_, 1))
      .reduceByKey(_+_)

    val time = System.currentTimeMillis()
    result.print()

    result.foreachRDD(rdd => {
      kafkaProducer(rdd.collect().mkString(","))
    })


    kafkaProducer(result.toString)
    // 结果输出至kafka另外一个topic
    //result.saveAsTextFiles(s"files/result/$time")

    ssc.start()
    ssc.awaitTermination()
  }

  def kafkaProducer(args: String) {
    if ((args != null)&&(args.trim.length>0)) {

      producer.send(new KeyedMessage[String, String](targetTopic, args))
      println("Message sent: " + args)

    }
  }

}
