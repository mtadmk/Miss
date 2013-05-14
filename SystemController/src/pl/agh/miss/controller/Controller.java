package pl.agh.miss.controller;

import java.io.IOException;

import pl.agh.miss.proto.GeneratorMessage.Plan;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class Controller {
	public final static String EXCHANGE_NAME = "EvaluatorBestTime";
	private final static String BIND_KEY = "EVALUATOR_STANDARD_BIND_KEY";

	public static void main(String[] args) throws InterruptedException {
		Controller man = new Controller();
		// lol
		man.run();
	}

	private void run() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "topic");
			String queueName = channel.queueDeclare().getQueue();

			channel.queueBind(queueName, EXCHANGE_NAME, BIND_KEY);

			System.out
					.println(" [Controller] Waiting for messages. To exit press CTRL+C");

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				Plan plan = Plan.parseFrom(delivery.getBody());
				System.out.println("[Controller] Received new plan: " + plan);

			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
