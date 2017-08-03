package com.spark.learning.demo

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
 * 假设某论坛需要根据用户对站内网页的点击量，停留时间，以及是否点赞，来近实时的计算网页热度，进而动态的更新网站的今日热点模块，把最热话题的链接显示其中。
 * Created by Think on 2017/7/26.
 *第一项表示网页的 ID，第二项表示从进入网站到离开对该网页的点击次数，第三项表示停留时间，以分钟为单位，第四项是代表是否点赞，1 为赞，-1 表示踩，0 表示中立。
 *假设点击次数权重是 0.8，因为用户可能是由于没有其他更好的话题，所以再次浏览这个话题。停留时间权重是 0.8，因为用户可能同时打开多个 tab 页，但他真正关注的只是其中一个话题。是否点赞权重是 1，因为这一般表示用户对该网页的话题很有兴趣。
 *
 * 定义用下列公式计算某条行为数据对于该网页热度的贡献值。
 * f(x,y,z)=0.8x+0.8y+z
 * 那么对于上面的行为数据 (page001.html, 1, 0.5, 1)，利用公式可得：
 * H(page001)=f(x,y,z)= 0.8x+0.8y+z=0.8*1+0.8*0.5+1*1=2.2
 * 只关注它对于网页热度所做的贡献。
 *
 * 该程序每隔 5 秒钟会随机的向 user-behavior-topic 主题推送 0 到 50 条行为数据消息，扮演消息生产者的角色：
 * 网页 ID|点击次数|停留时间 (分钟)|是否点赞
 * 并假设该网站只有 100 个网页。
 *
 * page32|2|4.5292296|1
 * page5|5|2.185718|0
 * page64|5|0.9225978|1
 *
 *
 */
object UserBehaviorMsgProducerClient {
  def main(args: Array[String]) {
    new Thread(new UserBehaviorMsgProducer()).start()
  }

  class UserBehaviorMsgProducer() extends Runnable {
    private val brokerList = "30.3.8.162:9092"
    private val targetTopic = "user-behavior-topic"
    private val props = new Properties()
    props.put("metadata.broker.list", this.brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "async")
    private val config = new ProducerConfig(this.props)
    private val producer = new Producer[String, String](this.config)

    private val PAGE_NUM = 100
    private val MAX_MSG_NUM = 3
    private val MAX_CLICK_TIME = 5
    private val MAX_STAY_TIME = 10
    //Like,1;Dislike -1;No Feeling 0
    private val LIKE_OR_NOT = Array[Int](1, 0, -1)
    def run(): Unit = {
      val rand = new Random()
      while (true) {
        //how many user behavior messages will be produced
        val msgNum = rand.nextInt(MAX_MSG_NUM) + 1
        try {
          //generate the message with format like page1|2|7.123|1
          for (i <- 0 to msgNum) {
            var msg = new StringBuilder()
            msg.append("page" + (rand.nextInt(PAGE_NUM) + 1))
            msg.append("|")
            msg.append(rand.nextInt(MAX_CLICK_TIME) + 1)
            msg.append("|")
            msg.append(rand.nextInt(MAX_CLICK_TIME) + rand.nextFloat())
            msg.append("|")
            msg.append(LIKE_OR_NOT(rand.nextInt(3)))
            println(msg.toString())
            //send the generated message to broker
            sendMessage(msg.toString())
          }
          println("%d user behavior messages produced.".format(msgNum+1))
        } catch {
          case e: Exception => println(e)
        }
        try {
          //sleep for 5 seconds after send a micro batch of message
          Thread.sleep(5000)
        } catch {
          case e: Exception => println(e)
        }
      }
    }
    def sendMessage(message: String) = {
      try {
        val data = new KeyedMessage[String, String](this.targetTopic, message);
        producer.send(data);
      } catch {
        case e:Exception => println(e)
      }
    }
  }
}
