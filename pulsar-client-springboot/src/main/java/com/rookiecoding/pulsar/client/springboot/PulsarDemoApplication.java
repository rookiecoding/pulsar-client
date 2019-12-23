package com.rookiecoding.pulsar.client.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = {"com.rookiecoding.pulsar.client.springboot"})
@SpringBootApplication
public class PulsarDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(PulsarDemoApplication.class, args);
    }
}
