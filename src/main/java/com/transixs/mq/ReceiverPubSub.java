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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReceiverPubSub extends Thread {

  private final Logger log = LogManager.getLogger(ReceiverPubSub.class.getName());
  private IMessageProcessorFactory messageProcessorFactory;
  private PulsarClient client;
  private Consumer consumer;
  private String consumerName;
  private boolean doStop = false;

  public ReceiverPubSub(PulsarClient client, Consumer consumer, IMessageProcessorFactory messageProcessorFactory) {
    this.client = client;
    this.consumer = consumer;
    this.consumerName = PulsarFactory.INSTANCE.getExclusiveConsumerPubSubName(consumer);
    this.messageProcessorFactory = messageProcessorFactory;
  }

  public void run() {
    try {
      log.warn("starting for consumer : " + consumerName);
      int counter = 0; 
      do {
        counter++;
        if (counter > 11) { // 120 seconds
          log.info("waiting for messages for consumer : " + consumerName);
          counter = 0;
        }
        Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
        if (msg != null && !doStop) {
          processMessage(msg);
        }
      } while (!doStop);
      log.warn("shutdown for consumer : " + consumerName);
    } catch(Exception e){
      log.error("Error in consumer : " + consumerName, e);
      throw new MessageQueueException("Error in consumer : " + consumerName, e);
    }
  }

  private void processMessage(Message<byte[]> msg) {
    try {
      // NOTE: doStop may have been triggered WHILE the thread was blocked by consumer.receive() call.
      // so make sure we don't process the message if we should be stopping
      if (doStop) {
        return;
      }
      String content = new String(msg.getData());
      log.info("Received message '{}' with ID {} in consumer {}", content, msg.getMessageId(), consumerName);
      IMessageProcessor msgProcessor = messageProcessorFactory.getMessageProcessor();
      msgProcessor.process(content);
      log.info("Processed message '{}' with ID {} in consumer {}", content, msg.getMessageId(), consumerName);
      consumer.acknowledge(msg);
    } catch(Exception e){
      log.error("FAILED to process message with ID {} in consumer {}", msg.getMessageId(), consumerName);
      log.error("failed to process message for consumer : " + consumerName, e);
      //////////////////////////////////////////////////////////////////////////////////////////
      // DON'T THROW this exception. It would cause the processing thread to stop.
      //////////////////////////////////////////////////////////////////////////////////////////
    }
  }

  // We want to make sure all worker threads have completed their work
  // before we shutdown
  public synchronized void stopit() {
    doStop = true;
  }

}
