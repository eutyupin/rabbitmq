package ru.gb.rabbitmq.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Scanner;

public class PostsConsumer {
    private static final String EXCHANGE_NAME = "topic_exchanger";
    private static String routingKey;
    private static Scanner scanner;

    public static void main(String[] argv) throws Exception {
        scanner = new Scanner(System.in);
        routingKey = "php";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        String queueName = channel.queueDeclare().getQueue();
        readPostsFromQueue(channel, queueName);

    }

    private static void readPostsFromQueue(Channel channel, String queueName) throws IOException {
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        System.out.println("* Пост по тематике '" + routingKey + "' *:");
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" -- " + message);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
