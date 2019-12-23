package com.rookiecoding.pulsar.client.springboot.client;

import com.rookiecoding.pulsar.client.springboot.pojo.UserPojo;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author mal
 * @date 2019/12/23
 */
@Configuration
public class ProducerPulsarClient {

    private Client client;

    /**
     * 构建生产者(Config)
     * @param topic
     * @return
     * @throws PulsarClientException
     */
    public Producer<byte[]> createProducerConfig(String topic) throws PulsarClientException {
        Map<String, Object> config = new HashMap<>();
        config.put("producerName", "test-producer");
        config.put("sendTimeoutMs", 2000);
        ProducerBuilder<byte []> builder = null;
        builder = builder.loadConf(config);
        Producer <byte []> producer = builder.create();
        return producer;
    }

    /**
     * 构建生产者(Byte)
     * @param topic
     * @return
     * @throws PulsarClientException
     */
    public Producer<byte[]> createProducerByte(String topic) throws PulsarClientException {
        client = new Client();
        return client.getPulsarClient().newProducer()
                .topic(topic)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)//批量处理时间
                .sendTimeout(100, TimeUnit.SECONDS)//设置发送超时（默认值：30秒）
                .blockIfQueueFull(true)//设置当传出消息队列已满时，Producer.send(T)和Producer.sendAsync(T)操作是否应阻塞
                .create();
    }

    /**
     * 构建生产者(Bean)
     * @param topic
     * @return
     * @throws PulsarClientException
     */

    public Producer<UserPojo> createProducerBean(String topic) throws PulsarClientException {
        client = new Client();
        Schema<UserPojo> pojoSchema = JSONSchema.of(UserPojo.class);
        return client.getPulsarClient().newProducer(pojoSchema)
                .topic(topic)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)//批量处理时间
                .sendTimeout(100, TimeUnit.SECONDS)//设置发送超时（默认值：30秒）
                .blockIfQueueFull(true)//设置当传出消息队列已满时，Producer.send(T)和Producer.sendAsync(T)操作是否应阻塞
                .create();
    }

    /**
     * 构建生产者(String)
     * @param topic
     * @return
     * @throws PulsarClientException
     */
    public Producer<String> createProducerString(String topic) throws PulsarClientException {
        client = new Client();
        return client.getPulsarClient().newProducer(Schema.STRING)
                .topic(topic)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .sendTimeout(100, TimeUnit.SECONDS)
                .blockIfQueueFull(true)
                .create();
    }

    /**
     * 发送一次就关闭
     */
    public void sendOnce(Producer<byte[]> producer, String message) {
        try {
//            MessageId messageId = producer.send(message.getBytes());
            producer.send(message.getBytes());
            System.out.printf("Message with content %s successfully sent ", message);
            producer.close();
            client.Close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 异步调用
     * @param message
     */
    public void sendMessage(Producer<byte[]> producer, String message) {
        producer.sendAsync(message.getBytes()).thenAccept(msgId -> {
            System.out.printf("Message with ID %s successfully sent", msgId);
        });
    }

    /**
     * 异步关闭
     */
    public void close(Producer<byte[]> producer){
        producer.closeAsync()
                .thenRun(() -> System.out.println("Producer closed"));
    }
}
