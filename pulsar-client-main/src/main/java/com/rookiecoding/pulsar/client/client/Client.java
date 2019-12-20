package com.rookiecoding.pulsar.client.client;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * @author mal
 * @date 2019/12/19.
 * 客户端构建
 */
public class Client {
    private PulsarClient client;

    public Client() throws PulsarClientException {
        client = PulsarClient.builder()
                .serviceUrl("pulsar://10.30.50.202:6650")
                .build();
    }
    public void Close() throws PulsarClientException {
        client.close();
    }
    public PulsarClient getPulsarClient(){
        return client;
    }

}
