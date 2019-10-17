package com.transixs.mq;

public class MqCli {

    public static void main(String[] args) {
      try {

        checkUsage(args);

        if (args[0].equals("--send-sync")) {
          sendSync(args);
        }

        if (args[0].equals("--send-async")) {
          sendAsync(args);
        }

        System.exit(0);

      } catch (Exception ex) {
        System.out.println("ERROR running MqCli");
        ex.printStackTrace();
      }
    }

    private static void sendSync(String[] args) throws Exception {
      System.out.println("sending message \n serviceName:" + args[1] + ", \n resource:" + args[2] + ", \n action:" + args[3] + ", \n message:" + args[4]);
      String response = MessageQueueAPI.INSTANCE.sendSync(args[1], args[2], args[3], args[4]);
      System.out.println("response: \n " + response);
    }

    private static void sendAsync(String[] args) throws Exception {
      System.out.println("sending message \n serviceName:" + args[1] + ", \n resource:" + args[2] + ", \n action:" + args[3] + ", \n message:" + args[4]);
      MessageQueueAPI.INSTANCE.sendAsync(args[1], args[2], args[3], args[4]);
      System.out.println("message sent");
    }

    private static void checkUsage(String[] args) throws Exception {
      if (args == null || args.length < 1 || args[0].equals("--help")) {
        printUsage();
        System.exit(1);
      }
      if (args[0].equals("--send-sync") && args.length != 5) {
        System.out.println("--send-sync command requires 4 more args: serviceName, resource, action, message");
        printUsage();
        System.exit(1);
      }
      if (args[0].equals("--send-async") && args.length != 5) {
        System.out.println("--send-async command requires 4 more args: serviceName, resource, action, message");
        printUsage();
        System.exit(1);
      }
    }

    private static void printUsage() {
      System.out.println("");
      System.out.println("MqCli usage: ");
      System.out.println("\t" + "requires at lest 1 argument, the command to run, one of: --help, --send-sync, --send-async");
      System.out.println("\t" + "--help command does not require any other args");
      System.out.println("\t" + "--send-sync command requires 4 more args: serviceName, resource, action, message");
      System.out.println("\t" + "--send-async command requires 4 more args: serviceName, resource, action, message");
    }
}
