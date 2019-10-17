package com.transixs.mq;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum PulsarFactory {

  INSTANCE;

  private final Logger log = LogManager.getLogger(PulsarFactory.class.getName());

  protected PulsarClient getClient() {
    try {
        return PulsarClient.builder()
          .serviceUrl(ServiceConfig.PULSAR_SERVICE_URL.getValue())
          .build();
    } catch(Exception e){
      log.error("FAILED to initialize pulsar client", e);
      return null;
    }
  }

  // NOTE: there will be a single producer per TOPIC. In most cases a topic
  // will exist PER SERVICE for the WORKER QUEUE use case....but for PUB-SUB
  // the topic name is NOT related to a service, either way we will still only
  // have once producer per topic
  protected Producer getProducer(PulsarClient client, String serviceName) {
    try {
      return client.newProducer(Schema.BYTES)
        .topic(serviceName) // topicName is actually serviceName here
        .compressionType(CompressionType.LZ4)
        .create();
    } catch(Exception e){
      log.error("FAILED to initialize pulsar producer", e);
      return null;
    }
  }

  // The shared consumer is used to setup workers on a work queue
  // NOTE: should only need 1 of these per service
  protected Consumer getSharedConsumer(PulsarClient client) {
    try {
      return client.newConsumer()
        .topic(ServiceConfig.SERVICE_NAME.getValue()) // topicName is actually serviceName here
        .subscriptionType(SubscriptionType.Shared) // Allow multiple consumers
        .subscriptionName(ServiceConfig.SERVICE_NAME.getValue() + "-subscription")
        .receiverQueueSize(1)
        .subscribe();
    } catch(Exception e){
      log.error("FAILED to initialize shared pulsar consumer", e);
      return null;
    }
  }

  // The exclusive subscription is used as a REPLY-TO queue
  // Will have one of these for each service we talk to in SYNC mode
  protected Consumer getExclusiveConsumer(PulsarClient client) {
    try {
      String uuid = UUID.randomUUID().toString();
      return client.newConsumer()
        .topic(uuid)
        .subscriptionType(SubscriptionType.Exclusive) // Allow only one consumers
        .subscriptionName(uuid)
        .receiverQueueSize(1)
        .subscribe();
    } catch(Exception e){
      log.error("FAILED to initialize exclusive pulsar consumer", e);
      return null;
    }
  }

  // The exclusive subscription is used for PUB-SUB
  protected Consumer getExclusiveConsumerPubSub(PulsarClient client, String topic) {
    try {
      String uuid = UUID.randomUUID().toString();
      return client.newConsumer()
        .topic(topic)
        .subscriptionType(SubscriptionType.Exclusive) // Allow only one consumers
        .subscriptionName(uuid)
        .receiverQueueSize(1)
        .subscribe();
    } catch(Exception e){
      log.error("FAILED to initialize exclusive pulsar consumer PUB-SUB", e);
      return null;
    }
  }

  // for logging
  protected String getProducerName(String serviceName) {
    return "procuder-" + ServiceConfig.SERVICE_NAME.getValue() + "-" + serviceName;
  }

  protected String getSharedConsumerName() {
    return "consumer-" + ServiceConfig.SERVICE_NAME.getValue() + "-shared";
  }

  protected String getExclusiveConsumerName(Consumer consumer) {
    return "consumer-exclusive-" + ServiceConfig.SERVICE_NAME.getValue() + "-" + consumer.getSubscription();
  }

  protected String getExclusiveConsumerPubSubName(Consumer consumer) {
    return "consumer-exclusive-pub-sub-" + ServiceConfig.SERVICE_NAME.getValue() + "-" + consumer.getSubscription();
  }

}
