package com.rookiecoding.pulsar.client.consumer;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author mal
 * @date 2019/12/19.
 * 单主题消费
 */
public class MsgConsumerOnce {
    private static final String SERVER_URL = "pulsar://10.30.50.202:6650";

    public static void main(String[] args) throws Exception{
        // 构造Pulsar Client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVER_URL)
//                .enableTcpNoDelay(true)
                .build();
        Consumer consumer = client.newConsumer()
                .consumerName("maliang-topic123-con")
                .topic("maliang-topic123")
                .subscriptionName("maliang-topic123-subscription")
                .ackTimeout(10, TimeUnit.SECONDS)
                .maxTotalReceiverQueueSizeAcrossPartitions(10)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();
//        do {
//            // 接收消息有两种方式：异步和同步
////             CompletableFuture<Message<String>> message = consumer.receiveAsync();
//            Message message = consumer.receive();
//            log.info("get message from pulsar cluster,{}", message);
//        } while (true);

        do {
            // Wait for a message
            CompletableFuture<Message> msg = consumer.receiveAsync();

            System.out.printf("Message received: %s", new String(msg.get().getData()));

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg.get());
        } while (true);
    }
}
