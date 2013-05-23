package pl.agh.miss.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.SimulationState;
import pl.agh.miss.proto.GeneratorMessage.SimulationStateOrBuilder;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ShutdownSignalException;

public class Evaluator implements Runnable {
	private static final String REMOVE_ACTIVE_PLAN_MESSAGE = "removeActivePlan";
	private static final String GET_STATE_MESSAGE = "getState";
	private static final int GET_STATE_INTERVAL = 1000;
	private static final int CONTROLLER_SEND_INTERVAL = 10000;
	public final static String EXCHANGE_NAME = "EvaluatorBestTime";
	private final static String BIND_KEY = "EVALUATOR_STANDARD_BIND_KEY";
	private static final String RPC_EVALUATOR_QUEUE_NAME = "RPC_EVALUATOR_QUEUE";
	private QueueingConsumer consumer;

	private String controllerAddress = "localhost";

	// private PlanManager planManager;
	private long currentPlanDuration = Long.MAX_VALUE;
	private Plan currentPlan = null;
	//planid, queuename
	private Map<Integer, String> activeplans;
	private Map<Plan, Long> resultsMap = new ConcurrentHashMap<>();

	public Evaluator() {
		// this.planManager = pm;
	}

	public void setActivePlans(Map<Integer, String> properActivePlans) {
		this.activeplans = properActivePlans;
	}
	String replyQueueName;
	private String corrId;
	private Channel channel;
	private AtomicInteger currentActivePlans;
	private List<Integer> activePlansToRemove;
	private Map<Integer, Plan> allPlans;
	
	@Override
	public void run() {
		createSystemControllerThread();

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			replyQueueName = channel.queueDeclare().getQueue();
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(replyQueueName, true, consumer);
			corrId = UUID.randomUUID().toString();
			while (true){
				Thread.sleep(GET_STATE_INTERVAL);
				for (final Entry<Integer, String> entry : activeplans.entrySet()){
					SimulationState ss = getState(entry.getKey(), entry.getValue());
					System.out.println(ss);
					if (ss.getState() == 0){ //finished properly
						this.activeplans.remove(entry.getKey());
						currentActivePlans.decrementAndGet();
						resultsMap.put(getPlanById(entry.getKey()), ss.getPredictedExecutionTime());
						//TODO add execution time to map 
					}
					if (ss.getState() == -1) {
						//cancelled
						this.activeplans.remove(entry.getKey());
						currentActivePlans.decrementAndGet();						
					}
				}
			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private SimulationState getState(Integer planId, String queueName) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		BasicProperties props = new BasicProperties.Builder()
		.correlationId(corrId).replyTo(replyQueueName).build();
		
		String message = GET_STATE_MESSAGE;
		
		for (Integer planId1 : activePlansToRemove){
			if (planId1.equals(planId)){
				message = REMOVE_ACTIVE_PLAN_MESSAGE;
				break;
			}
		}
		
		channel.basicPublish("", queueName, props, planId.toString().getBytes());
		System.out.println(" [Evaluator] Sent '" + message + "' to " + planId);
		
		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
//		channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		System.out.println("[Evaluator] delivery received");
		return SimulationState.parseFrom(delivery.getBody());
	}

	private void createSystemControllerThread() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(CONTROLLER_SEND_INTERVAL);
					sendResults(resultsMap);

				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			public synchronized void sendResults(Map<Plan, Long> resultsMap) {
				// //TEST
				// System.out.println(resultsMap);
				// if (true ) return;

				// indicates if there was change beteen currently executed plan
				// and new plan
				boolean planChanged = false;
				for (Plan plan : resultsMap.keySet()) {
					long duration = resultsMap.get(plan);
					if (duration < currentPlanDuration) {
						currentPlanDuration = duration;
						currentPlan = plan;
						planChanged = true;
					}
				}
				if (planChanged) {
					sendNewPlan(currentPlan);
					//cleaning map for new plans
					resultsMap.clear();
				}
			}

			private void sendNewPlan(Plan plan) {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(controllerAddress);
				Connection connection;
				try {
					connection = factory.newConnection();
					Channel channel = connection.createChannel();

					channel.exchangeDeclare(EXCHANGE_NAME, "topic");

					channel.basicPublish(EXCHANGE_NAME, BIND_KEY, null,
							plan.toByteArray());

					System.out.println(" [Evaluator] Sent '" + plan + "'");

					channel.close();
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	public void setCurrentActivePlans(AtomicInteger currentActivePlans) {
		this.currentActivePlans=currentActivePlans;
	}

	public void setPlansToRemove(List<Integer> activePlansToRemove) {
		this.activePlansToRemove= activePlansToRemove;
	}

	public void setAllPlans(Map<Integer, Plan> allPlans) {
		this.allPlans = allPlans;
	}
	public Plan getPlanById(int planId){
		return allPlans.get(planId);
	}
}
