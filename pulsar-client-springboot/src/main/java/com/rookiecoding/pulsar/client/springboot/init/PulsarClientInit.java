//package com.rookiecoding.pulsar.client.springboot.init;
//
//import org.apache.pulsar.client.api.Client;
//import org.apache.pulsar.client.api.PulsarClientException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.CommandLineRunner;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//
///**
// * Created by 13501143824 on 2019/12/23.
// */
//@Component
//public class PulsarClientInit implements CommandLineRunner {
//    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientInit.class);
//    public static Client pulsarClient;
//
//    @Value("${pulsarServiceUrl:1}")
//    private String pulsarServiceUrl;
//
//    @PostConstruct
//    public void init() throws PulsarClientException{
//        pulsarClient = Client.builder()
//                    .serviceUrl(pulsarServiceUrl)
//                    .build();
//        try {
//            pulsarClient = Client.builder()
//                    .serviceUrl(pulsarServiceUrl)
//                    .build();
//        } catch (PulsarClientException e) {
//            LOG.error("Client build failure!! error={}", e.getMessage());
//        }
//    }
//
////    public Client getPulsarClient(){
////        return pulsarClient;
////    }
////
////    public void Close() throws PulsarClientException {
////        pulsarClient.close();
////    }
//
//    @Override
//    public void run(String... strings) throws Exception {
//        init();
//    }
//}
