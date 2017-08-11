package com.test

import java.util.{Date, Properties, UUID}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by XQ on 2017/7/26.
  *
  */
class  test_producer(brokerList : String, topic : String,massages:Array[String]) extends Runnable {
  private val BROKER_LIST = brokerList//"192.168.200.1:9092"//"master:9092,worker1:9092,worker2:9092"
  private val TARGET_TOPIC = topic//"test" //"new"
  /**
    * 1、配置属性
    * metadata.broker.list : kafka集群的broker，只需指定2个即可
    * serializer.class : 如何序列化发送消息
    * request.required.acks : 1代表需要broker接收到消息后acknowledgment,默认是0
    * producer.type : 默认就是同步sync
    */
  private val props = new Properties()
  props.put("metadata.broker.list", this.BROKER_LIST)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("request.required.acks", "1")
  props.put("producer.type", "async")
  /**
    * 2、创建Producer
    */
  private val config = new ProducerConfig(this.props)
  private val producer = new Producer[String, String](this.config)
  /**
    * 3、产生并发送消息
    * 搜索目录dir下的所有包含“transaction”的文件并将每行的记录以消息的形式发送到kafka
    *
    */
  def run( ) : Unit = {
  println("-------------chengxukaishi--------------")
    val message1 = new KeyedMessage[String, String](this.TARGET_TOPIC, "kaishiceshi")
    producer.send(message1)
    massages.foreach(line=>{

      val message = new KeyedMessage[String, String](this.TARGET_TOPIC, line)
      producer.send(message)
    }
    )
  }
}
object test_producer {
  def main (args:Array[String]):Unit = {
  val conf = new SparkConf().setMaster("local[2]").setAppName("kafkaProduceMassages")
    val sc =new SparkContext(conf)

    val lines =sc.textFile("data/testdata.txt")
    val words =lines.flatMap(line=>line.split(",")).take(3)

   val ipaddr= "localhost:9092"
    val topic = "test"
    new Thread(new test_producer(ipaddr,topic,words)).start()
  }

}
