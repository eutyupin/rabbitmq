package ru.gb.rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class PostsConsumer {
    private static final String EXCHANGE_NAME = "topic_exchanger";
    private static String[] commandRouting;
    private static Scanner scanner;
    private static Executor readThematicThread;

    public static void main(String[] argv) throws Exception {
        readThematicThread = Executors.newSingleThreadExecutor();
        scanner = new Scanner(System.in);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();
        readThematicFromConsole(readThematicThread, channel, queueName);
        readPostsFromQueue(channel, queueName);
    }

    private static void readThematicFromConsole(Executor readThematicThread, Channel channel, String queueName) {
        readThematicThread.execute(() -> {
            try {
                readRoutingKey(channel, queueName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void readRoutingKey(Channel channel, String queueName) throws IOException {
        commandMessage();
        commandRouting = scanner.nextLine().split(" ", 2);
        commandRoutingExecute(channel, queueName);
    }

    private static void commandMessage() {
        System.out.println("Введите команду, например: 'set_topic тематика' или 'del_topic тематика':");
    }

    private static void commandRoutingExecute(Channel channel, String queueName) throws IOException {
        String routingKey = commandRouting[1];
        if(commandRouting[0].equals("set_topic")) {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
        if (commandRouting[0].equals("del_topic")) {
            channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
        }
        readRoutingKey(channel, queueName);
    }

    private static void readPostsFromQueue(Channel channel, String queueName) throws IOException {
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            System.out.println("* Пост по тематике '" + delivery.getEnvelope().getRoutingKey() + "' *:");
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" -- " + message);
            commandMessage();
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
