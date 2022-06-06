package ru.gb.rabbitmq.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;

public class PostsProducer {
    private static final String EXCHANGE_NAME = "topic_exchanger";
    private static String[] topicMessage;
    private static Scanner scanner;
    private static String routingKey;
    private static String message;


    public static void main(String[] args) throws Exception {
        scanner = new Scanner(System.in);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
                    readEnteredPosts(channel);
        }
    }

    private static void readEnteredPosts(Channel channel) throws IOException {
        System.out.println("Введите пост в формате: 'тематика'пробел'текст поста'");
        topicMessage = scanner.nextLine().split(" ", 2);
        String routingKey = topicMessage[0];
        String message = topicMessage[1];
        publishPost(routingKey, message, channel);
    }

    private static void publishPost(String routingKey, String message, Channel channel) throws IOException {
        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
        System.out.println(" Отправлено --> '" + routingKey + " -- " + message + "'");
        readEnteredPosts(channel);
    }
}
