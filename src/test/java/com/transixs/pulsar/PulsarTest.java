package com.transixs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.stream.IntStream;

@Disabled("Disabled - needs pulsar standalone to be running")
class PulsarTest {

  private static final Logger log = LogManager.getLogger(PulsarTest.class.getName());
  private static final String SERVICE_URL = "pulsar://localhost:6650";
  private static final String TOPIC_NAME = "test-topic";
  private static final String SUBSCRIPTION_NAME = "test-subscription";
  private static PulsarClient client;
  private static Producer<byte[]> producer;
  private static Consumer<byte[]> consumer;

  @BeforeAll
  static void initAll() {
    try {
      // Create a Pulsar client instance. A single instance can be shared across many
      // producers and consumer within the same application
      client = PulsarClient.builder()
        .serviceUrl(SERVICE_URL)
        .build();

      producer = client.newProducer()
        .producerName("test-producer")
        .topic(TOPIC_NAME)
        .compressionType(CompressionType.LZ4)
        .create();

      consumer = client.newConsumer()
        .consumerName("test-consumer")
        .topic(TOPIC_NAME)
        .subscriptionType(SubscriptionType.Shared) // Allow multiple consumers
        .subscriptionName(SUBSCRIPTION_NAME)
        .receiverQueueSize(1)
        .subscribe();

      // Once the producer is created, it can be used for the entire application life-cycle
      log.info("Created producer for the topic {}", TOPIC_NAME);

      // Once the consumer is created, it can be used for the entire application lifecycle
      log.info("Created consumer for the topic {}", TOPIC_NAME);
      
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  @AfterAll
  static void tearDownAll() {
    try {
      client.close();
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  private void sendMessage(int i) {
    try {
        String content = String.format("hello-pulsar-%d", i);
        MessageId msgId = producer.send(content.getBytes());
        log.info("Published message '{}' with the ID {}", content, msgId);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  private void receiveMessage(int i) {
    try {
      // Wait until a message is available
      Message<byte[]> msg = consumer.receive();
      // Extract the message as a printable string and then log
      String content = new String(msg.getData());
      log.info("Received message '{}' with ID {}", content, msg.getMessageId());
      // Acknowledge processing of the message so that it can be deleted
      consumer.acknowledge(msg);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  @Test
  void simple_produce_consume() {
    IntStream.range(1, 11).forEach(i -> sendMessage(i));
    IntStream.range(1, 11).forEach(i -> receiveMessage(i));
  }

}
