package com.xlh.rocketmq.demo;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * 普通消息消费者
 *
 * @author 胡晓磊
 * @company xxx
 * @date 2020年08月25日 22:34 胡晓磊 Exp $
 */
public class Consumer {
    public static final String NAME_SERVER_ADDR = "127.0.0.1:9876";

    public static void main(String[] args) throws MQClientException {
        // 创建消费者 push 对象
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("GROUP_TEST");

        // 设置NameServer的地址，如果设置了环境变量NAME_ADDR，可以省略此步
        consumer.setNamesrvAddr(NAME_SERVER_ADDR);

        // 订阅对应的主题和Tag
        consumer.subscribe("TopicTest", "*");

        // 注册消息接收到Broker消息后的处理接口
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            try {
                MessageExt messageExt = msgs.get(0);
                System.out.printf("线程：%-25s 接收到新消息 %s --- %s %n", Thread.currentThread().getName(), messageExt.getTags(), new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        // 启动消费者(必须在注册完消息监听器后启动，否则会报错)
        consumer.start();

        System.out.println("已启动消费者");

    }
}
