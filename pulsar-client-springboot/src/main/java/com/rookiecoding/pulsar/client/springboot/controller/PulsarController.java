package com.rookiecoding.pulsar.client.springboot.controller;

import com.rookiecoding.pulsar.client.springboot.service.ConsumerService;
import com.rookiecoding.pulsar.client.springboot.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PulsarController {

    @Autowired
    private ConsumerService consumerService;

    @Autowired
    private ProducerService producerService;

    @GetMapping(value = "/pulsar/produce")
    public String produce(String topic, String message) throws Exception {
        return producerService.produce(topic, message);
    }

    @GetMapping(value = "/pulsar/consume")
    public String consume(String topic) throws Exception {
        consumerService.consume(topic);
        return "SUCCESS";
    }
}
