/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package javatest;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 *
 * @author ivan.lee
 */
public class Main {

    static String ADDRESS = "tcp://localhost:61616";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        Producer.Enqueue("TEST.FOO", "Hello world! From: " + Thread.currentThread().getName() + " : ");
        Consumer.Dequeue("TEST.FOO");
    }

}

class Producer implements Runnable {

    final String queue, text;

    public Producer(String queue, String text) {
        this.queue = queue;
        this.text = text;
    }

    public static void Producer(String queue, String text) {
        Thread brokerThread = new Thread(new Producer(queue, text));
        brokerThread.setDaemon(false);
        brokerThread.start();
    }

    public void run() {
        Enqueue(queue, text);
    }

    public static void Enqueue(String queue, String text) {

        try {
            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(Main.ADDRESS);

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queue);

            // Create a MessageProducer from the Session to the Topic or Queue
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            // Create a messages
            TextMessage message = session.createTextMessage(text);

            // Tell the producer to send the message
            System.out.println("Sent message: " + message.hashCode() + " : " + Thread.currentThread().getName());
            producer.send(message);

            // Clean up
            session.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

}

class Consumer implements Runnable {

    final String queue;

    public Consumer(String queue) {
        this.queue = queue;
    }

    public static void Consumer(String queue) {
        Thread brokerThread = new Thread(new Consumer(queue));
        brokerThread.setDaemon(false);
        brokerThread.start();
    }

    public void run() {
        Dequeue(queue);
    }

    public static String Dequeue(String queue) {
        String text = null;
        try {

            // Create a ConnectionFactory
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(Main.ADDRESS);

            // Create a Connection
            Connection connection = connectionFactory.createConnection();
            connection.start();

            // Create a Session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create the destination (Topic or Queue)
            Destination destination = session.createQueue(queue);

            // Create a MessageConsumer from the Session to the Topic or Queue
            MessageConsumer consumer = session.createConsumer(destination);

            // Wait for a message
            Message message = consumer.receive(1000);

            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                text = textMessage.getText();
                System.out.println("Received: " + text);
            } else {
                System.out.println("Received: " + message);
            }

            consumer.close();
            session.close();
            connection.close();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
        return text;
    }

}
