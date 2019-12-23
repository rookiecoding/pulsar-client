//package com.rookiecoding.pulsar.client.springboot.service.impl;
//
//import com.rookiecoding.pulsar.client.springboot.init.PulsarClientInit;
//import com.rookiecoding.pulsar.client.springboot.service.PulsarService;
//import org.apache.pulsar.client.api.*;
//import org.apache.pulsar.client.impl.BatchMessageIdImpl;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.stereotype.Service;
//
//import java.util.concurrent.CompletableFuture;
//
//@Service
//public class PulsarServiceImpl implements PulsarService {
//
//    private static final Logger LOG = LoggerFactory.getLogger(PulsarServiceImpl.class);
//
//    @Value("${pulsarServiceUrl}")
//    private String pulsarServiceUrl;
//
//    /**
//     * produce
//     */
//    public String produce(String topic, String message) {
//        try {
//            Producer<String> producer = PulsarClientInit.pulsarClient.newProducer(Schema.STRING)
////                    .topic("persistent://public/default/" + topic)
//                    .topic(topic)
//                    .create();
//            MessageId messageId = producer.send(message);
//            producer.close();
//            LOG.info("topic={} message={} messageId={}", topic, message, messageId);
//            return "topic=" + topic + " message=" + message + " messageId=" + messageId;
//        } catch (Exception e) {
//            LOG.error("Pulsar produce failure!! error={}", e.getMessage());
//            return "Pulsar produce failure!! error=" + e.getMessage();
//        }
//    }
//
//    /**
//     * read
//     */
//    public void read(String topic, String offset) {
//        new Thread(() -> {
//            try {
//                String[] offsetSplit = offset.split(":");
//                MessageId msgId = new BatchMessageIdImpl(Long.parseLong(offsetSplit[0]), Long.parseLong(offsetSplit[1]), Integer.parseInt(offsetSplit[2]), Integer.parseInt(offsetSplit[3]));
//                Reader<String> reader = PulsarClientInit.pulsarClient.newReader(Schema.STRING)
//                        .topic(topic)
//                        .startMessageId(msgId)
//                        .create();
//                while (!Thread.currentThread().isInterrupted()) {
//                    Message message = reader.readNext();
//                    String data = new String(message.getData());
//                    MessageId messageId = message.getMessageId();
//                    LOG.info("topic={},message={},messageId={}", topic, data, messageId.toString());
//                    Thread.sleep(20);
//                }
//            } catch (Exception e) {
//                LOG.error("Pulsar read failure!! error={}", e.getMessage());
//            }
//        }).start();
//    }
//
//    /**
//     * consume
//     */
//    public void consume(String topic) {
//        new Thread(() -> {
//            try {
//                Consumer<String> consumer = PulsarClientInit.pulsarClient.newConsumer(Schema.STRING)
//                        .topic(topic)
//                        .subscriptionName(topic)
//                        .subscribe();
//            // 接收消息有两种方式：异步和同步
//                do {
//                    // 接收消息有两种方式：异步和同步
//                    // CompletableFuture<Message<String>> message = consumer.receiveAsync();
//                    Message message = consumer.receive();
//                    LOG.info("get message from pulsar cluster,{}", message);
//                } while (true);
//
////                do {
////                    // Wait for a message
////                    CompletableFuture<Message> msg = consumer.receiveAsync();
////
////                    System.out.printf("Message received: %s", new String(msg.get().getData()));
////
////                    // Acknowledge the message so that it can be deleted by the message broker
////                    consumer.acknowledge(msg.get());
////                } while (true);
//////             CompletableFuture<Message<String>> message = consumer.receiveAsync();
////            Message message = consumer.receive();
////                while (!Thread.currentThread().isInterrupted()) {
////                    Message<String> message = consumer.receive();
////                    String data = new String(message.getData());
////                    consumer.acknowledge(message);
////                    MessageId messageId = message.getMessageId();
////                    LOG.info("topic={},message={},messageId={}", topic, data, messageId.toString());
////                    Thread.sleep(20);
////                }
//            } catch (Exception e) {
//                LOG.error("Pulsar consume failure!! error={}", e.getMessage());
//            }
//        }).start();
//    }
//
//}
