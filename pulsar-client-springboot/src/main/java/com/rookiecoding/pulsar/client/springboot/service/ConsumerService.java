package com.rookiecoding.pulsar.client.springboot.service;

import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Created by 13501143824 on 2019/12/23.
 */
public interface ConsumerService {

    void consume(String topic) throws Exception;
}
