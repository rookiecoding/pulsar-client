package com.rookiecoding.pulsar.client.springboot.service.impl;

import com.rookiecoding.pulsar.client.springboot.client.ConsumerPulsarClient;
import com.rookiecoding.pulsar.client.springboot.service.ConsumerService;
import org.apache.pulsar.client.api.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author mal
 * @date 2019/12/19.
 * 生产者
 */
@Service
public class ConsumerServiceImpl implements ConsumerService{

    private static final Logger LOG = LoggerFactory.getLogger(ProducerServiceImpl.class);

    @Autowired
    private ConsumerPulsarClient consumerPulsarClient;

    public void consume(String topic) throws Exception {
        Consumer consumer = consumerPulsarClient.createConsumer(topic);
        consumerPulsarClient.receiveMessage(consumer);
    }
}
