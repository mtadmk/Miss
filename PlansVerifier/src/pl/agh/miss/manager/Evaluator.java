package pl.agh.miss.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.SimulationState;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class Evaluator implements Runnable {
	private String brokerHost = "localhost";
	private static final String REMOVE_ACTIVE_PLAN_MESSAGE = "removeActivePlan";
	private static final String GET_STATE_MESSAGE = "getState";
	private static final int GET_STATE_INTERVAL = 300;
	private static final int CONTROLLER_SEND_INTERVAL = 10000;
	public final static String EXCHANGE_NAME = "EvaluatorBestTime";
	private final static String BIND_KEY = "EVALUATOR_STANDARD_BIND_KEY";
	private final static String FINAL_STATE_QUEUE = "FINAL_STATE_QUEUE";
	protected static final String EXCHANGE_NAME_FINISH = "EXCHANGE_NAME_FINISH";

	// private static final String RPC_EVALUATOR_QUEUE_NAME =
	// "RPC_EVALUATOR_QUEUE";
	private QueueingConsumer consumer;

	// ready simulate next plan
	protected final int notWorkingState = 0;
	// works but it is no worth to wait
	protected final int startingState = 1;
	protected final int pointlessToWaitState = 2;
	// its about to end simulation
	protected final int worthWaitingState = 3;
	protected final int justEndingState = 4;
	protected final int cancelled = 5;

	// private PlanManager planManager;
	private long currentPlanDuration = Long.MAX_VALUE;
	private Plan currentPlan = null;
	// planid, queuename
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
		createFinishedPlansThread();

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(brokerHost);
		Connection connection;
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			replyQueueName = channel.queueDeclare().getQueue();
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(replyQueueName, true, consumer);
			corrId = UUID.randomUUID().toString();
			while (true) {
				Thread.sleep(GET_STATE_INTERVAL);

				// polling msgs from clients
				for (final Entry<Integer, String> entry : activeplans
						.entrySet()) {
					handleStatesFromPlan(entry);
				}

			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void handleStatesFromPlan(Entry<Integer, String> activePlanEntry)
			throws ShutdownSignalException, ConsumerCancelledException,
			IOException, InterruptedException {
		SimulationState ss = getState(activePlanEntry.getKey(),
				activePlanEntry.getValue());
		// System.out.println(ss);
		if (ss.getState() == notWorkingState) { // finished properly
			//for this purpose new queue's been created
//			this.activeplans.remove(activePlanEntry.getKey());
//			currentActivePlans.decrementAndGet();
//			resultsMap.put(getPlanById(activePlanEntry.getKey()),
//					ss.getPredictedExecutionTime());
		} else if (ss.getState() == startingState) {
			// TODO unused now
		} else if (ss.getState() == pointlessToWaitState) {
			// cancel simulation inside simulator
		} else if (ss.getState() == worthWaitingState) {
			// simulation will proceed
		} else if (ss.getState() == justEndingState) {
			// pass
		} else if (ss.getState() == cancelled) {
			// cancelled
			this.activeplans.remove(activePlanEntry.getKey());
			currentActivePlans.decrementAndGet();
		}
	}

	private SimulationState getState(Integer planId, String queueName)
			throws IOException, ShutdownSignalException,
			ConsumerCancelledException, InterruptedException {
		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrId).replyTo(replyQueueName).build();

		String message = GET_STATE_MESSAGE;

		for (Integer planId1 : activePlansToRemove) {
			if (planId1.equals(planId)) {
				message = REMOVE_ACTIVE_PLAN_MESSAGE;
				activePlansToRemove.remove(planId);
				break;
			}
		}

		channel.basicPublish("", queueName, props, message.getBytes());
//		System.out.println(" [Evaluator] Sent '" + message + "' to " + planId);

		QueueingConsumer.Delivery delivery = consumer.nextDelivery();
//		System.out.println("[Evaluator] delivery received");
		return SimulationState.parseFrom(delivery.getBody());
	}

	private void createSystemControllerThread() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					try {
						Thread.sleep(CONTROLLER_SEND_INTERVAL);
						sendResults(resultsMap);

					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

			public synchronized void sendResults(Map<Plan, Long> resultsMap) {
				// indicates if there was change between currently executed plan
				// and new plan
//				boolean planChanged = false;
				for (Plan plan : resultsMap.keySet()) {
					long duration = resultsMap.get(plan);
					if (duration < getCurrentPlanDuration()) {
						setCurrentPlanDuration(duration);
						currentPlan = plan;
//						planChanged = true;
					}
				}

				
				sendNewPlan(currentPlan);
//				System.out.println("plansMap: ");
//				for (Plan p : Evaluator.this.resultsMap.keySet()){
//					System.out.println(p.getPlanId());
//				}
				
				
				// if (planChanged) {
				// cleaning map for new plans
				resultsMap.clear();
				// }

			}

			private void sendNewPlan(Plan plan) {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(brokerHost);
				Connection connection;
				try {
					connection = factory.newConnection();
					Channel channel = connection.createChannel();

					channel.exchangeDeclare(EXCHANGE_NAME, "topic");

					if (plan != null) {
						channel.basicPublish(EXCHANGE_NAME, BIND_KEY, null,
								plan.toByteArray());
						System.out.println(" [Evaluator] Sent new plan '"
								+ plan.getPlanId() + "'");
					}

					channel.close();
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}
	private void createFinishedPlansThread() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(brokerHost);
				Connection connection;
				try {
					connection = factory.newConnection();
					Channel channel = connection.createChannel();

					channel.exchangeDeclare(EXCHANGE_NAME_FINISH, "topic");
					String queueName = channel.queueDeclare().getQueue();

					channel.queueBind(queueName, EXCHANGE_NAME_FINISH, BIND_KEY);

					QueueingConsumer consumer = new QueueingConsumer(channel);
					channel.basicConsume(queueName, true, consumer);
					
					while (true) {						
						QueueingConsumer.Delivery delivery = consumer.nextDelivery();
						SimulationState ss = SimulationState.parseFrom(delivery.getBody());
						activeplans.remove(ss.getPlan());
						currentActivePlans.decrementAndGet();
						resultsMap.put(ss.getPlan(),
								ss.getPredictedExecutionTime());
						System.out.println("[Evaluator] Received simulated plan: " + ss.getPlan().getPlanId());
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
		}).start();
	}

	public void setCurrentActivePlans(AtomicInteger currentActivePlans) {
		this.currentActivePlans = currentActivePlans;
	}

	public void setPlansToRemove(List<Integer> activePlansToRemove) {
		this.activePlansToRemove = activePlansToRemove;
	}

	public void setAllPlans(Map<Integer, Plan> allPlans) {
		this.allPlans = allPlans;
	}

	public Plan getPlanById(int planId) {
		return allPlans.get(planId);
	}

	public String getBrokerHost() {
		return brokerHost;
	}

	public void setBrokerHost(String brokerHost) {
		this.brokerHost = brokerHost;
	}

	private long getCurrentPlanDuration() {
		return currentPlanDuration;
	}

	private synchronized void setCurrentPlanDuration(long currentPlanDuration) {
		this.currentPlanDuration = currentPlanDuration;
	}
	
	public void plansChanged(){
		this.setCurrentPlanDuration(Long.MAX_VALUE);
	}
}
