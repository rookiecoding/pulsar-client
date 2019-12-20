package com.rookiecoding.pulsar.client.consumer;

import com.rookiecoding.pulsar.client.client.Client;
import org.apache.pulsar.client.api.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author mal
 * @date 2019/12/19.
 * 单主题消费
 */
public class MsgConsumerSingle {
    private Client client;
    private Consumer consumer;

    public MsgConsumerSingle(String topic, String subscription) throws PulsarClientException {
        client = new Client();
        consumer = createConsumer(topic, subscription);
    }

    private Consumer createConsumer(String topic, String subscription) throws PulsarClientException {

        return client.getPulsarClient().newConsumer().topic(topic).subscriptionName(subscription)
                .ackTimeout(10, TimeUnit.SECONDS).subscriptionType(SubscriptionType.Exclusive).subscribe();
    }

    public String getMessage() throws ExecutionException, InterruptedException, PulsarClientException {
        /***
         * 获取一次，就关闭会话
         */
        // Wait for a message
        System.out.printf("Start pulsar");
        CompletableFuture<Message> msg = consumer.receiveAsync();

        // System.out.printf("Message received: %s", new String(msg.get().getData()));
        String result = "topic is: " + msg.get().getTopicName() + ",data is: " + new String(msg.get().getData());

        // Acknowledge the message so that it can be deleted by the message broker
        consumer.acknowledge(msg.get());
        consumer.close();
        client.Close();
        return result;
    }

    public void receiveMessage() throws ExecutionException, InterruptedException, PulsarClientException {
        /***
         * 用来异步获取，保持回话
         */
        do {
            // Wait for a message
            CompletableFuture<Message> msg = consumer.receiveAsync();

            System.out.printf("Message received: %s", new String(msg.get().getData()));

            // Acknowledge the message so that it can be deleted by the message broker
            consumer.acknowledge(msg.get());
        } while (true);
    }

    public static void main(String[] args) throws PulsarClientException, ExecutionException, InterruptedException {
        MsgConsumerSingle consumer = new MsgConsumerSingle("maliang-topic1", "topic1");
//        consumer.receiveMessage();
		String reString = consumer.getMessage();
		System.err.println(reString);
         consumer.client.Close();

    }
}
