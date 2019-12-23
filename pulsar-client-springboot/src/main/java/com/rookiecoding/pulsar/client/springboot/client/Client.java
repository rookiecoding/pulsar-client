package com.rookiecoding.pulsar.client.springboot.client;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.beans.factory.annotation.Value;

import java.util.HashMap;
import java.util.Map;

/**
 * @author mal
 * @date 2019/12/19.
 * 客户端构建
 */
public class Client {

//    @Value("${pulsarServiceUrl:1}")
//    private String pulsarServiceUrl;

    private PulsarClient client;

    public Client() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://10.30.50.202:6650")
                .build();
    }

//    public Client() throws PulsarClientException {
//        Map<String, Object> config = new HashMap<>();
//        config.put("serviceUrl", "pulsar://10.30.50.202:5550");
//        config.put("numIoThreads", 20);
//        ClientBuilder builder = null;
//        builder = builder.loadConf(config);
//        client = builder.build();
//    }

    public void Close() throws PulsarClientException {
        client.close();
    }
    public PulsarClient getPulsarClient(){
        return client;
    }

}
