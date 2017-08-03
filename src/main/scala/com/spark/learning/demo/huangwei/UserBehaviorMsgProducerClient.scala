package com.spark.learning.demo

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.util.Random

/**
 * ����ĳ��̳��Ҫ�����û���վ����ҳ�ĵ������ͣ��ʱ�䣬�Լ��Ƿ���ޣ�����ʵʱ�ļ�����ҳ�ȶȣ�������̬�ĸ�����վ�Ľ����ȵ�ģ�飬�����Ȼ����������ʾ���С�
 * Created by Think on 2017/7/26.
 *��һ���ʾ��ҳ�� ID���ڶ����ʾ�ӽ�����վ���뿪�Ը���ҳ�ĵ���������������ʾͣ��ʱ�䣬�Է���Ϊ��λ���������Ǵ����Ƿ���ޣ�1 Ϊ�ޣ�-1 ��ʾ�ȣ�0 ��ʾ������
 *����������Ȩ���� 0.8����Ϊ�û�����������û���������õĻ��⣬�����ٴ����������⡣ͣ��ʱ��Ȩ���� 0.8����Ϊ�û�����ͬʱ�򿪶�� tab ҳ������������ע��ֻ������һ�����⡣�Ƿ����Ȩ���� 1����Ϊ��һ���ʾ�û��Ը���ҳ�Ļ��������Ȥ��
 *
 * ���������й�ʽ����ĳ����Ϊ���ݶ��ڸ���ҳ�ȶȵĹ���ֵ��
 * f(x,y,z)=0.8x+0.8y+z
 * ��ô�����������Ϊ���� (page001.html, 1, 0.5, 1)�����ù�ʽ�ɵã�
 * H(page001)=f(x,y,z)= 0.8x+0.8y+z=0.8*1+0.8*0.5+1*1=2.2
 * ֻ��ע��������ҳ�ȶ������Ĺ��ס�
 *
 * �ó���ÿ�� 5 ���ӻ�������� user-behavior-topic �������� 0 �� 50 ����Ϊ������Ϣ��������Ϣ�����ߵĽ�ɫ��
 * ��ҳ ID|�������|ͣ��ʱ�� (����)|�Ƿ����
 * ���������վֻ�� 100 ����ҳ��
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
