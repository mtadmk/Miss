package pl.agh.miss.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanAndTransitions;
import pl.agh.miss.proto.GeneratorMessage.PlanQueueInfo;
import pl.agh.miss.proto.GeneratorMessage.PlanRemoval;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class PlanManager {

	private static String brokerHost = "localhost";

	QueueingConsumer consumer;
	
	// NOTE: each of these static fields must be the same as in PlansGenerator
	// class
	private static final String ADD_PLAN_BIND_KEY = "addplanbindkey";
//	private static final String ADD_PLANS_BIND_KEY = "addplansbindkey";
	private static final String REMOVE_PLAN_BIND_KEY = "removeplanbindkey";
	private static final String REMOVE_PLAN_ALL_BIND_KEY = "removeplanallbindkey";
	private static final String ADD_TRANSITION_BIND_KEY = "addtransitionbindkey";
	private static final String ADD_ALL_TRANSITION_BIND_KEY = "addalltransitionbindkey";
//	private static final String REMOVE_TRANSITION_BIND_KEY = "removetransitionbindkey";
//	private static final String REMOVE_TRANSITION_ALL_BIND_KEY = "removetransitionallbindkey";
//	private static final String REGISTER_SIMULATOR = "registersimulator";
	public final static String EXCHANGE_NAME = "GeneratorQueue";
//	private static final String TASK_QUEUE_NAME = "taskqueue";
	private static final String RPC_QUEUE_NAME = "RPC_QUEUE";
	/**
	 * how many machines - how many simulators we have at least 1
	 */
//	private static final int MACHINES_COUNT = 3;
	private static final int MAX_PLANS_IN_QUEUE = 20;
	private AtomicInteger currentActivePlans = new AtomicInteger(0);

	private Map<Integer, Plan> plans = new ConcurrentHashMap<>();
//	private Map<Integer, Plan> activePlans = new ConcurrentHashMap<>();
	private Map<Integer, String> properActivePlans = new ConcurrentHashMap<>();
//	private Map<Integer, CommunicationAgent_LEGACY> activeAgents = new ConcurrentHashMap<>();
	private Map<Integer, List<PassTime>> timeTransitions = new ConcurrentHashMap<>();
	private Map<Integer, Plan> allPlans = new ConcurrentHashMap<>();

//	private boolean enoughPlans = false;
	private Evaluator evaluator;
//	private SimulationManager_LEGACY simulationManager;
//	private Map<Integer, Future<ResultStruct>> futureMap;
	
	Channel channel1; 
	String replyQueueName;

	private List<Integer> activePlansToRemove = new CopyOnWriteArrayList<>();

	public static void main(String[] args) throws InterruptedException, IOException {
		if (args.length > 0){
			brokerHost = args[0];
		}
		PlanManager man = new PlanManager();
		man.run(new Evaluator());
	}
	
	public PlanManager() throws IOException{
	    ConnectionFactory factory1 = new ConnectionFactory();
	    factory1.setHost(brokerHost);
	    Connection connection1 = factory1.newConnection();
	    channel1 = connection1.createChannel();
	    replyQueueName = channel1.queueDeclare().getQueue(); 
	    consumer = new QueueingConsumer(channel1);
	    channel1.basicConsume(replyQueueName, true, consumer);
	}

	private void run(Evaluator evaluator) {
		this.evaluator = evaluator;
		this.evaluator.setActivePlans(this.properActivePlans);
		this.evaluator.setCurrentActivePlans(currentActivePlans);
		this.evaluator.setPlansToRemove(activePlansToRemove);
		this.evaluator.setAllPlans(this.allPlans);
		this.evaluator.setBrokerHost(brokerHost);
		new Thread(evaluator).start();

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(brokerHost);
		Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
			runReceiverThread();
			runSimulationThread();

			channel.exchangeDeclare(EXCHANGE_NAME, "topic");
			String queueName = channel.queueDeclare().getQueue();

			channel.queueBind(queueName, EXCHANGE_NAME, ADD_PLAN_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, REMOVE_PLAN_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME,
					REMOVE_PLAN_ALL_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, ADD_ALL_TRANSITION_BIND_KEY);
			
			System.out
					.println(" [Manager] Waiting for messages. To exit press CTRL+C");

			final QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);
			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							QueueingConsumer.Delivery delivery;
							delivery = consumer.nextDelivery();

							switch (delivery.getEnvelope().getRoutingKey()) {
							case ADD_PLAN_BIND_KEY:
								// dodawanie joba do wykonania
								Plan plan = Plan.parseFrom(delivery.getBody());
								addPlan(plan);
								System.out
										.println("[PlanManager] Received new plan");
								break;
							case REMOVE_PLAN_BIND_KEY:
								// usuwanie jobow lub ich zatrzymywanie po id
								PlanRemoval pr = PlanRemoval.parseFrom(delivery
										.getBody());
								System.out
										.println("[PlanManager] Received plan to remove: "
												+ pr.getPlanId());
								removeById(pr.getPlanId());
								break;
							case REMOVE_PLAN_ALL_BIND_KEY:
								// usuwanie wszystkich jobow i zatrzymywanie
								// tych ktore sa
								// symulowane
								System.out
										.println("[PlanManager] Received remove all plans message");
								removeAll();
								break;
							case ADD_ALL_TRANSITION_BIND_KEY:
								// dodawanie jednego rekordu do tablcy przejsc
								PlanAndTransitions transitions = PlanAndTransitions
										.parseFrom(delivery.getBody());
								addAllTimeTransition(transitions.getTimeTransitionsList());
								System.out
										.println("[PlanManager] Received all new transistions");
								break;
							case ADD_TRANSITION_BIND_KEY:
								// dodawanie jednego rekordu do tablcy przejsc
								TimeTransitions transitions1 = TimeTransitions
										.parseFrom(delivery.getBody());
								addTimeTransition(transitions1);
								System.out
										.println("[PlanManager] Received new transition");
								break;
							default:
								System.err.println("invalid message");
								break;
							}
						} catch (ShutdownSignalException
								| ConsumerCancelledException
								| InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InvalidProtocolBufferException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

			}).start();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ShutdownSignalException e) {
			e.printStackTrace();
		} catch (ConsumerCancelledException e) {
			e.printStackTrace();
		}
	}

	private void addAllTimeTransition(
			List<TimeTransitions> list) {
//		this.timeTransitions.clear();
		for (TimeTransitions transition : list){
			addTimeTransition(transition);
//			this.timeTransitions.put(transition.getJobId(),
//					transition.getTimesList());
		}
	}
	private void addTimeTransition(
			TimeTransitions transition) {
		this.timeTransitions.put(transition.getJobId(),
				transition.getTimesList());
	}
	
	private void removeById(int jobId) {
		if (plans.containsKey(jobId)){
			plans.remove(jobId);
		} else if (properActivePlans.containsKey(jobId)){
			//shared resource
			activePlansToRemove.add(jobId);
		} else {
			System.err.println("Plan with this id doesn't exits: " + jobId);
		}
	}

	private synchronized void removeAll() throws IOException{
		// removing unactive plans
		plans.clear();
		// stopnig active plans
		for (Integer id : properActivePlans.keySet()) {
			removeById(id);
		}
		if (channel1 != null){
			channel1.queuePurge(RPC_QUEUE_NAME);
		}
		evaluator.plansChanged();
	}

	private synchronized void addPlan(Plan plan) throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
		plans.put(plan.getPlanId(), plan);
		allPlans.put(plan.getPlanId(), plan);
	}


	/**
	 * method for informing planmanager that all simulations are done and new
	 * ones should be executed
	 */
//	public synchronized void afterAllSimulationsAreDone() {
//		activeAgents.clear();
//		if (isEnoughPlans()) {
////			runSimulation();
//		}
//	}

//	private synchronized boolean isEnoughPlans() {
//		return enoughPlans;
//	}

	private boolean canAddPlanToQueue(){
		return currentActivePlans.get() < MAX_PLANS_IN_QUEUE;
	}

	private synchronized void sendPlanToQueue() throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {


	    String corrId = UUID.randomUUID().toString();
	    
	    for (int planId : plans.keySet()){
	    	Plan plan = plans.remove(planId);
			PlanAndTransitions.Builder pack = PlanAndTransitions.newBuilder();

			addTimeTransitions(pack, timeTransitions);
			pack.setPlan(plan);
			
			BasicProperties props = new BasicProperties.Builder()
			.correlationId(corrId).replyTo(replyQueueName).build();
			

			channel1.basicPublish("", RPC_QUEUE_NAME, props, pack.build().toByteArray());
//			System.out.println(" [PlanManager] Sent '" + plan.getPlanId() + "'");
			
			
			//actualize current active plans
			currentActivePlans.incrementAndGet();
			if (!canAddPlanToQueue()){
				break;
			}
	    }
	    
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
		}
	}

	/**
	 * actualize activeplans map
	 * gets info from agents
	 */
	private void runReceiverThread(){
		new Thread(new Runnable() {			
			@Override
			public void run() {
				while(true){
					try {
						QueueingConsumer.Delivery delivery = consumer.nextDelivery();
						PlanQueueInfo pqi = PlanQueueInfo.parseFrom(delivery.getBody());
//						System.out.println("[PLANMANAGER] received: " + pqi.getPlanId());
						properActivePlans.put(pqi.getPlanId(), pqi.getQueueName());	
					} catch (ShutdownSignalException
							| ConsumerCancelledException | InterruptedException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}				
			}
		}).start();
	}
	
	/**
	 * in order to be ablo to control how many plans in queue
	 */
	private void runSimulationThread(){
		new Thread(new Runnable() {			
			@Override
			public void run() {
				while(true){
					try {
						Thread.sleep(1000);
						if (currentActivePlans.get() < MAX_PLANS_IN_QUEUE){
//							System.out.println("current active plans no " + currentActivePlans.get() + "planssize: " +plans.keySet().size());
							sendPlanToQueue();
						}						
					} catch (InterruptedException | ShutdownSignalException | ConsumerCancelledException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}				
			}
		}).start();
	}
}
