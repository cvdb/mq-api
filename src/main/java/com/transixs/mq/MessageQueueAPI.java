package com.transixs.mq;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.lang.Runtime;
import java.lang.Thread;

public enum MessageQueueAPI {

  INSTANCE;

  /*
   * NOTE
   * ====
   *
   * This API is intended to be used by any component that needs to either SEND or RECEIVE
   * message via Pulsar. Each component can do one or more of the following:
   *
   *  1. Send messages to another component ASYNC...fire and forget.
   *  2. Send messages to another component in a REQ-RES SYNC manner.
   *  3. Listen for any REQUEST messages (SYNC & ASYNC) sent from other components.
   *
   * Each component is identified by its SERVICE_NAME. Messages sent between components
   * are routed based on the target components SERVICE_NAME.
   *
   */

  private final Logger log = LogManager.getLogger(MessageQueueAPI.class.getName());
  private final PulsarClient client = PulsarFactory.INSTANCE.getClient();
  private final Sender sender = new Sender(client);
  private ReceiverShared receiver;
  private List<ReceiverPubSub> pubSubReceivers = new ArrayList<>();
  private CountDownLatch awaitShutdown = new CountDownLatch(1);

  public void sendAsync(String serviceName, String resource, String action, String message) {
    validateArgumentName(serviceName);
    validateArgumentName(resource);
    validateArgumentName(action);
    validateMessage(message);
    sender.sendAsync(serviceName, resource, action, message);
  }

  public void sendAsyncPubSub(String topic, String message) {
    validateArgumentName(topic);
    validateMessage(message);
    sender.sendAsyncPubSub(topic, message);
  }

  public String sendSync(String serviceName, String resource, String action, String message) {
    validateArgumentName(serviceName);
    validateArgumentName(resource);
    validateArgumentName(action);
    validateMessage(message);
    return sender.sendSync(serviceName, resource, action, message);
  }

  public synchronized void receive(IWorkerFactory factory) {
    if (receiver != null) {
      throw new MessageQueueException("Receiver can only be initialized once");
    }
    receiver = new ReceiverShared(client, PulsarFactory.INSTANCE.getSharedConsumer(client), factory);
    receiver.start();
    sleep();
    log.info("receiver initialized");
  }

  // NOTE: there is no danger here of having multiple
  // PUB-SUB concumers on the same topic
  public void receivePubSub(IMessageProcessorFactory factory, String topic) {
    ReceiverPubSub receiverPubSub = new ReceiverPubSub(client, PulsarFactory.INSTANCE.getExclusiveConsumerPubSub(client, topic), factory);
    receiverPubSub.start();
    pubSubReceivers.add(receiverPubSub);
    sleep();
    log.info("receiver PUB-SUB initialized");
  }

  public void validateArgumentName(String str) { 
    if ( (str != null) && (!str.equals("")) && (str.matches("^[A-Za-z0-9\\-\\_]+$")) ) { // only letters '^[a-z]*$'
      return; 
    }
    throw new MessageQueueException("Invalid argument name");
  } 

  public void validateMessage(String str) { 
    if ( (str != null) && (!str.equals("")) ) {
      return; 
    }
    throw new MessageQueueException("Empty message");
  } 

  {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        shutdown();
      }
    });
  }

  private void sleep() {
    try {
      Thread.sleep(2000);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  private void shutdownPubSubReceivers() {
    for(ReceiverPubSub rps : pubSubReceivers) {
      log.warn("shutting down receiver PUB-SUB");
      rps.stopit();
    }
  }

  public void shutdown() {
    try {
      if (receiver != null) {
        log.warn("shutting down sender");
        sender.shutdown();
        log.warn("shutting down receiver");
        receiver.stopit();
        log.warn("shutting down PUB-SUB receivers");
        shutdownPubSubReceivers();
        log.warn("shutting down client");
        client.close();
        log.warn("DONE - shutting down");
        receiver = null;
      }
    } catch(Exception e){
      log.error("FAILED to shutdown MQ API", e);
      throw new MessageQueueException("FAILED to shutdown MQ API", e);
    } finally {
      awaitShutdown.countDown();
    }
  }

  // This method call will block until
  // shutdown. Used in service MAIN methods
  public void waitUntilShutdown() {
    try {
      awaitShutdown.await();
    } catch(Exception e){
      log.error("FAILED to wait for shutdown of MQ API", e);
    }
  }

}


