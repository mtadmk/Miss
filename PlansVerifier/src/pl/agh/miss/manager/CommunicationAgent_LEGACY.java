package pl.agh.miss.manager;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanAndTransitions;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class CommunicationAgent_LEGACY implements Callable<ResultStruct> {

	private static final String EXCHANGE_NAME_AGENT = "AGENT_EXHANGE";
	private static final String TIME_TRANSITIONS_BIND_KEY = "TIME_TRANSITIONS_BIND_KEY";
	private static final String PLAN_BIND_KEY = "PLAN_BIND_KEY";
	private static final String RPC_QUEUE_NAME = "RPC_QUEUE";
	private Map<Integer, List<PassTime>> timeTranstitions;
	private Plan plan;
	private Connection connection;
	private Channel channel;
	private String replyQueueName;
	private QueueingConsumer consumer;
	private String corrId;

	public CommunicationAgent_LEGACY(Plan plan,
			Map<Integer, List<PassTime>> timeTransitions) {
		this.plan = plan;
		this.timeTranstitions = timeTransitions;

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			// replyQueueName = channel.queueDeclare().getQueue();
//			replyQueueName = plan.getPlanId() + "_"
//					+ System.currentTimeMillis();
			replyQueueName = channel.queueDeclare().getQueue();
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(replyQueueName, true, consumer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * run tests
	 */
	@Override
	public ResultStruct call() throws Exception {
		// send time transitions and plan to simulator
		sendPlan(plan, timeTranstitions);
		long time = waitAndRetrieveResults();
		Random rand = new Random();
		Thread.sleep(200 + rand.nextInt(10000));
		return new ResultStruct(time, this.plan);
	}

	private long waitAndRetrieveResults() throws ShutdownSignalException, ConsumerCancelledException, InterruptedException, NumberFormatException, UnsupportedEncodingException {
		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			if (delivery.getProperties().getCorrelationId().equals(corrId)) {
				return Long.parseLong(new String(delivery.getBody(), "UTF-8"));
			}
		}
	}

	private void sendPlan(Plan plan2,
			Map<Integer, List<PassTime>> timeTranstitions2) throws IOException {
		String response = null;
		corrId = UUID.randomUUID().toString();

		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrId).replyTo(replyQueueName).build();
		PlanAndTransitions.Builder pack = PlanAndTransitions.newBuilder();

		// it is assumed timetransitions are sent first and then plan
		addTimeTransitions(pack, timeTranstitions2);
		pack.setPlan(plan2);

		channel.basicPublish("", RPC_QUEUE_NAME, props, pack.build()
				.toByteArray());
		System.out.println(" [Communication Agent] Sent '" + plan + "'");

		// ConnectionFactory factory = new ConnectionFactory();
		// factory.setHost("localhost");
		// Connection connection;
		// try {
		// connection = factory.newConnection();
		// Channel channel = connection.createChannel();
		//
		// channel.exchangeDeclare(EXCHANGE_NAME_AGENT, "topic");
		//
		// PlanAndTransitions.Builder pack = PlanAndTransitions.newBuilder();
		//
		// // it is assumed timetransitions are sent first and then plan
		// addTimeTransitions(pack, timeTranstitions2);
		// pack.setPlan(plan2);
		//
		// channel.basicPublish(EXCHANGE_NAME_AGENT, PLAN_BIND_KEY, null, pack
		// .build().toByteArray());
		// System.out.println(" [Communication Agent] Sent '" + plan + "'");
		//
		// channel.close();
		// connection.close();
		// } catch (IOException e) {
		// e.printStackTrace();
		// }
	}

	private void addTimeTransitions(PlanAndTransitions.Builder pack,
			Map<Integer, List<PassTime>> timeTranstitions2) throws IOException {
		for (int id : timeTranstitions2.keySet()) {
			TimeTransitions.Builder trans = TimeTransitions.newBuilder();
			List<PassTime> passList = timeTranstitions2.get(id);
			for (PassTime pt : passList) {
				trans.addTimes(pt);
			}
			trans.setJobId(id);

			pack.addTimeTransitions(trans);
			// channel.basicPublish(EXCHANGE_NAME_AGENT,
			// TIME_TRANSITIONS_BIND_KEY, null, trans.build().toByteArray());
		}
	}

}
