package com.rookiecoding.pulsar.client.springboot.client;

import org.apache.pulsar.client.api.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author mal
 * @date 2019/12/23
 */
@Configuration
public class ConsumerPulsarClient {

    @Value("${pulsarServiceUrl:1}")
    private String pulsarServiceUrl;

    private Client client;

    public Consumer createConsumer(String topic) throws PulsarClientException {
        client = new Client();
        return client.getPulsarClient().newConsumer()
                .consumerName(topic)//设置消费者名称
                .topic(topic)
                .subscriptionName(topic)//指定此使用者的订阅名称
                .ackTimeout(10, TimeUnit.SECONDS)//设置未确认消息的超时，将其截断为最接近的毫秒。超时时间必须大于10秒
                .maxTotalReceiverQueueSizeAcrossPartitions(10)//设置跨分区的最大总接收器队列大小
                .subscriptionType(SubscriptionType.Exclusive)//选择订阅主题时要使用的订阅类型 共享/私有/故障转移接收/密钥分享
                .subscribe();//subscribe方法将自动使消费者订阅指定的主题和订阅
    }

    /**
     * 获取一次，就关闭会话
     */
    public String getMessage(Consumer consumer) throws ExecutionException, InterruptedException, PulsarClientException {
        // Wait for a message
        System.out.printf("Start pulsar");
        CompletableFuture<Message> msg = consumer.receiveAsync();

         System.out.printf("Message received: %s", new String(msg.get().getData()));
        String result = "topic is: " + msg.get().getTopicName() + ",data is: " + new String(msg.get().getData());

        // Acknowledge the message so that it can be deleted by the message broker
        consumer.acknowledge(msg.get());
        consumer.close();
        client.Close();
        return result;
    }

    /**
     * 获取消息，保持回话
     * @param consumer
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws PulsarClientException
     */
    public void receiveMessage(Consumer consumer) throws ExecutionException, InterruptedException, PulsarClientException {
        while (true) {
            // Wait for a message
            Message msg = consumer.receive();
            try {
                // Do something with the message
                System.out.printf("Message received: %s", new String(msg.getData()));
                // Acknowledge the message so that it can be deleted by the message broker
                consumer.acknowledge(msg);
            } catch (Exception e) {
                // Message failed to process, redeliver later
                consumer.negativeAcknowledge(msg);
            }
        }
    }

    /**
     * 异步获取消息，保持回话
     * @param consumer
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws PulsarClientException
     */
    public void receiveMessageAsync(Consumer consumer) throws ExecutionException, InterruptedException, PulsarClientException {
        do {
            // Wait for a message
            CompletableFuture<Message> msg = consumer.receiveAsync();

            System.out.printf("Message received: %s", new String(msg.get().getData()));

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg.get());
        } while (true);
    }

}
