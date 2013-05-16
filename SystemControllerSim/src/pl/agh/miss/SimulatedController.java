package pl.agh.miss;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanAndTransitions;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class SimulatedController {
	private static final String EXCHANGE_NAME_AGENT = "AGENT_EXHANGE";
	private static final String TIME_TRANSITIONS_BIND_KEY = "TIME_TRANSITIONS_BIND_KEY";
	private static final String PLAN_BIND_KEY = "PLAN_BIND_KEY";
	private static final String RPC_QUEUE_NAME = "RPC_QUEUE";

	private Plan plan;
	private Map<Integer, List<PassTime>> timeTransitions = new HashMap<Integer, List<PassTime>>();
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer;

	public SimulatedController() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

			channel.basicQos(1);

			consumer = new QueueingConsumer(channel);
			channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		SimulatedController sc = new SimulatedController();
		sc.run();
	}

	public void run() {
		while (true) {
			try {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				this.getOnePlan(delivery);

				System.out.println("[SIMULATED CONTROLLER] Plan : " + plan);

				// have fun with this.plan and this.timeTransitions
				// job shop should return only result
				long result = getJobShopTime();
				this.sendResult(result, delivery);
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * TODO TOMEK
	 * 
	 * @return
	 */
	private long getJobShopTime() {
		Random rand = new Random();
		long result = 10000 + rand.nextInt(100000);
		return result;
	}

	private void sendResult(long result, Delivery delivery) throws UnsupportedEncodingException, IOException {
		BasicProperties props = delivery.getProperties();
		BasicProperties replyProps = new BasicProperties.Builder()
				.correlationId(props.getCorrelationId()).build();
		channel.basicPublish("", props.getReplyTo(), replyProps,
				String.valueOf(result).getBytes("UTF-8"));

		channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	}

	public void getOnePlan(Delivery delivery) throws InvalidProtocolBufferException {
		System.out
				.println(" [Simulation Controller] Waiting for messages. To exit press CTRL+C");
		String response = null;

//		QueueingConsumer.Delivery delivery = consumer.nextDelivery();


		PlanAndTransitions pat = PlanAndTransitions.parseFrom(delivery
				.getBody());
		this.plan = pat.getPlan();
		for (TimeTransitions tt : pat.getTimeTransitionsList()) {
			this.timeTransitions.put(tt.getJobId(), tt.getTimesList());
		}
//
//		try {
//			String message = new String(delivery.getBody(), "UTF-8");
//			int n = Integer.parseInt(message);
//
//			System.out.println(" [.] fib(" + message + ")");
//			response = "" + fib(n);
//		} catch (Exception e) {
//			System.out.println(" [.] " + e.toString());
//			response = "";
//		} finally {
//			channel.basicPublish("", props.getReplyTo(), replyProps,
//					response.getBytes("UTF-8"));
//
//			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
//		}
		// ConnectionFactory factory = new ConnectionFactory();
		// factory.setHost("localhost");
		// Connection connection;
		// try {
		// connection = factory.newConnection();
		// Channel channel = connection.createChannel();
		//
		// channel.exchangeDeclare(EXCHANGE_NAME_AGENT, "topic");
		// String queueName = channel.queueDeclare().getQueue();
		//
		// channel.queueBind(queueName, EXCHANGE_NAME_AGENT,
		// TIME_TRANSITIONS_BIND_KEY);
		// channel.queueBind(queueName, EXCHANGE_NAME_AGENT, PLAN_BIND_KEY);
		//
		// System.out
		// .println(" [Simulation Controller] Waiting for messages. To exit press CTRL+C");
		//
		// final QueueingConsumer consumer = new QueueingConsumer(channel);
		// channel.basicConsume(queueName, true, consumer);
		//
		//
		// while (true){
		// QueueingConsumer.Delivery delivery;
		// delivery = consumer.nextDelivery();
		//
		// String routingKey = delivery.getEnvelope().getRoutingKey();
		// if (routingKey.equals(TIME_TRANSITIONS_BIND_KEY)){
		// TimeTransitions transition = TimeTransitions
		// .parseFrom(delivery.getBody());
		// this.timeTransitions.put(transition.getJobId(),
		// transition.getTimesList());
		// } else if (routingKey.equals(PLAN_BIND_KEY)){
		// this.plan = Plan.parseFrom(delivery.getBody());
		// //end loop
		// break;
		// } else {
		// System.err.println("INVALID routing key");
		// }
		// }
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (ShutdownSignalException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (ConsumerCancelledException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}
}
