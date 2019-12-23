package com.rookiecoding.pulsar.client.springboot.service;

/**
 * Created by 13501143824 on 2019/12/23.
 */
public interface ProducerService {

    /**
     * produce
     */
    String produce(String topic, String message) throws Exception;
}
