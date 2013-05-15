package pl.agh.miss;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class SimulatedController {
	private static final String EXCHANGE_NAME_AGENT = "AGENT_EXHANGE";
	private static final String TIME_TRANSITIONS_BIND_KEY = "TIME_TRANSITIONS_BIND_KEY";
	private static final String PLAN_BIND_KEY = "PLAN_BIND_KEY";

	private Plan plan;
	private Map<Integer, List<PassTime>> timeTransitions = new HashMap<Integer, List<PassTime>>();

	public static void main(String[] args) {
		SimulatedController sc = new SimulatedController();
		sc.run();
	}

	public void run() {
		while (true) {
			this.getOnePlan();
			
			System.out.println("[SIMULATED CONTROLLER] Plan : " + plan);

			// have fun with this.plan and this.timeTransitions
			// job shop should return only result
			long result = getJobShopTime();			
			this.sendResult(result);
		}
	}

	/**
	 * TODO TOMEK
	 * @return
	 */
	private long getJobShopTime() {
		Random rand = new Random();
		long result = 10000 + rand.nextInt(100000);
		return result;
	}

	private void sendResult(long result) {
		//TODO
	}

	public void getOnePlan() {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME_AGENT, "topic");
			String queueName = channel.queueDeclare().getQueue();

			channel.queueBind(queueName, EXCHANGE_NAME_AGENT,
					TIME_TRANSITIONS_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME_AGENT, PLAN_BIND_KEY);

			System.out
					.println(" [Simulation Controller] Waiting for messages. To exit press CTRL+C");

			final QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);
			
			
			while (true){
				QueueingConsumer.Delivery delivery;
				delivery = consumer.nextDelivery();

				String routingKey = delivery.getEnvelope().getRoutingKey();
				if (routingKey.equals(TIME_TRANSITIONS_BIND_KEY)){
					TimeTransitions transition = TimeTransitions
							.parseFrom(delivery.getBody());
					this.timeTransitions.put(transition.getJobId(),
							transition.getTimesList());
				} else if (routingKey.equals(PLAN_BIND_KEY)){
					this.plan = Plan.parseFrom(delivery.getBody());
					//end loop
					break;
				} else {
					System.err.println("INVALID routing key");
				}
			}			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
