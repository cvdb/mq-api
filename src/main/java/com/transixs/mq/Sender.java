package com.transixs.mq;

import java.util.Map;
import java.util.HashMap;

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

public class Sender {

  private final Logger log = LogManager.getLogger(Sender.class.getName());
  private PulsarClient client;
  private Map<String, Producer> producers = new HashMap<>();
  private Map<String, ReceiverExclusive> receivers = new HashMap<>();

  public Sender(PulsarClient client) {
    this.client = client;
  }

  private synchronized Producer getProducer(String topicName) {
    if (producers.containsKey(topicName)) {
      return producers.get(topicName); 
    } else {
      Producer p = PulsarFactory.INSTANCE.getProducer(client, topicName);
      producers.put(topicName, p);
      return p;
    }
  }

  // This is a list of receivers for RESPONSE messages
  // only used for SEND_SYNC
  private synchronized ReceiverExclusive getReceiver(String topicName) {
    if (receivers.containsKey(topicName)) {
      return receivers.get(topicName); 
    } else {
      ReceiverExclusive r = new ReceiverExclusive(PulsarFactory.INSTANCE.getExclusiveConsumer(client));
      receivers.put(topicName, r);
      r.start();
      return r;
    }
  }

  public void sendAsyncPubSub(String topic, String message) {
    //TODO: don't log message here, need to add log maskimng to MQ API
    String requestDetails = "request : [topic:" + topic + "]";
    try {
      MessageId msgId = getProducer(topic).newMessage()
        .value(message.getBytes())
        .send();
      log.info("Published ASYNC-PUB-SUB message with the ID {}, {}", msgId, requestDetails);
    } catch(Exception e){
      throw new MessageQueueException("failed to send ASYNC-PUB-SUB" + requestDetails, e);
    }
  }

  public void sendAsync(String serviceName, String resource, String action, String message) {
    //TODO: don't log message here, need to add log maskimng to MQ API
    String requestDetails = "request : [service:" + serviceName +
      ", resource:" + resource + ", action:" + action + "]";
    try {
      MessageId msgId = getProducer(serviceName).newMessage()
        .value(message.getBytes())
        .property(MessageProperties.REQUEST_RESOURCE.name(), resource)
        .property(MessageProperties.REQUEST_ACTION.name(), action)
        .send();
      log.info("Published ASYNC message with the ID {}, {}", msgId, requestDetails);
    } catch(Exception e){
      throw new MessageQueueException("failed to send ASYNC" + requestDetails, e);
    }
  }

  public String sendSync(String serviceName, String resource, String action, String message) {
    //TODO: don't log message here, need to add log maskimng to MQ API
    String requestDetails = "request : [service:" + serviceName +
      ", resource:" + resource + ", action:" + action + "]";
    try {
      // NOTE: the receiver topic & subscription here are EXCLUSIVE
      // to the target SERVICE_NAME. The receiver topic will be contain a random GUID
      // to ensure it is enique.
      ReceiverExclusive receiver = getReceiver(serviceName);
      MessageId msgId = getProducer(serviceName).newMessage()
        .value(message.getBytes())
        .property(MessageProperties.REPLY_TO_TOPIC_URL.name(), receiver.getTopic())
        .property(MessageProperties.REQUEST_RESOURCE.name(), resource)
        .property(MessageProperties.REQUEST_ACTION.name(), action)
        .send();
      log.info("Published SYNC message with the ID {}, {}", msgId, requestDetails);
      return receiver.await(msgId);
    } catch(Exception e){
      throw new MessageQueueException("failed to send SYNC" + requestDetails, e);
    }
  }

  public void shutdown() {
    shutdownProducers();
    shutdownReceivers();
  }

  private void shutdownProducers() {
    try {
      for (Producer p : producers.values()) {
        p.flush();
        p.close();
      }
    } catch(Exception e){
      log.error("FAILED to shutdown producers", e);
    }
  }

  private void shutdownReceivers() {
    try {
      for (ReceiverExclusive r : receivers.values()) {
        r.stopit();
      }
    } catch(Exception e){
      log.error("FAILED to shutdown receivers", e);
    }
  }
}
