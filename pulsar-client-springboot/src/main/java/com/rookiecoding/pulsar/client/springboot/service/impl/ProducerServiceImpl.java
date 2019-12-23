package com.rookiecoding.pulsar.client.springboot.service.impl;

import com.rookiecoding.pulsar.client.springboot.client.ProducerPulsarClient;
import com.rookiecoding.pulsar.client.springboot.service.ProducerService;
import org.apache.pulsar.client.api.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author mal
 * @date 2019/12/19.
 * 生产者
 */
@Service
public class ProducerServiceImpl implements ProducerService{

    private static final Logger LOG = LoggerFactory.getLogger(ProducerServiceImpl.class);

    @Autowired
    private ProducerPulsarClient producerPulsarClient;

    @Value("${pulsarServiceUrl:1}")
    private String pulsarServiceUrl;

    public String produce(String topic, String message) throws Exception{
        Producer<byte[]> producer = producerPulsarClient.createProducerByte(topic);
        producerPulsarClient.sendOnce(producer, message);
        LOG.info("topic={} message={} messageId={}", topic, message);
        return "topic=" + topic + " message=" + message;
    }
}
