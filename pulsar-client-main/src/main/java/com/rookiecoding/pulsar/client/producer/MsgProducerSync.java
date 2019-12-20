package com.rookiecoding.pulsar.client.producer;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author mal
 * @date 2019/12/19.
 * 生产者sync
 */
public class MsgProducerSync {
    private static final String SERVER_URL = "pulsar://10.30.50.202:6650";

    public static void main(String[] args) throws Exception {
        // 构造Pulsar Client
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(SERVER_URL)
                .build();

        // 构造生产者
        Producer<String> producer = client.newProducer(Schema.STRING)
                .producerName("my-producer")
                .topic("persistent://public/default/my-topic")
                .batchingMaxMessages(1024)
                .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                .enableBatching(true)
                .blockIfQueueFull(true)
                .maxPendingMessages(512)
                .sendTimeout(10, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();

        // 同步发送消息
        MessageId messageId = producer.send("Hello World");
//        log.info("message id is {}", messageId);
        CompletableFuture<MessageId> asyncMessageId = producer.sendAsync("This is a async message");
        // 阻塞线程，直到返回结果
//        log.info("async message id is {}",asyncMessageId.get());

        // 配置发送的消息元信息，同步发送
//        producer.newMessage()
//                .key("my-async-message-key")
//                .value("my-async-message")
//                .property("my-async-key", "my-async-value")
//                .property("my-async-other-key", "my-async-other-value")
//                .sendAsync();

        // 关闭producer的方式有两种：同步和异步
        producer.close();

        // 关闭licent的方式有两种，同步和异步
        client.close();

    }
}
