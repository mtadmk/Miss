package pl.agh.miss.manager;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanRemoval;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitionsRemoval;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class PlanManager {
	//NOTE: each of these static fields must be the same as in PlansGenerator class
	private static final String ADD_PLAN_BIND_KEY = "addplanbindkey";
	private static final String REMOVE_PLAN_BIND_KEY = "removeplanbindkey";
	private static final String REMOVE_PLAN_ALL_BIND_KEY = "removeplanallbindkey";
	private static final String ADD_TRANSITION_BIND_KEY = "addtransitionbindkey";
	private static final String REMOVE_TRANSITION_BIND_KEY = "removetransitionbindkey";
	private static final String REMOVE_TRANSITION_ALL_BIND_KEY = "removetransitionallbindkey";
	public final static String EXCHANGE_NAME = "GeneratorQueue";
	/**
	 * how many machines - how many simulators we have
	 * at least 1
	 */
	private static final int MACHINES_COUNT = 3;
	
	private Map<Integer, Plan> plans = new HashMap<>();
	private Map<Integer, Plan> activePlans = new HashMap<>();
	private boolean enoughPlans = false;
	private Map<Integer, CommunicationAgent> activeAgents = new HashMap<>();
	private Evaluator evaluator;
	
	private Map<Integer, List<PassTime>> timeTransitions = new HashMap<>();

	public static void main(String[] args) throws InterruptedException {
		PlanManager man = new PlanManager();
		//lol
		man.run(new Evaluator());
	}

	private void run(Evaluator evaluator) {
		this.evaluator = evaluator;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();
			
			channel.exchangeDeclare(EXCHANGE_NAME, "topic");
			String queueName = channel.queueDeclare().getQueue();
			
			channel.queueBind(queueName, EXCHANGE_NAME, ADD_PLAN_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, REMOVE_PLAN_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, REMOVE_PLAN_ALL_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, ADD_TRANSITION_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, REMOVE_TRANSITION_BIND_KEY);
			channel.queueBind(queueName, EXCHANGE_NAME, REMOVE_TRANSITION_ALL_BIND_KEY);
			
			
			System.out
					.println(" [Manager] Waiting for messages. To exit press CTRL+C");			

			QueueingConsumer consumer = new QueueingConsumer(channel);
			channel.basicConsume(queueName, true, consumer);

			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				switch (delivery.getEnvelope().getRoutingKey()) {
				case ADD_PLAN_BIND_KEY:
					//dodawanie joba do wykonania
					Plan plan = Plan.parseFrom(delivery.getBody());
					addPlan(plan);
					System.out.println("[PlanManager] Received new plan");
					break;
				case REMOVE_PLAN_BIND_KEY:
					//usuwanie jobow lub ich zatrzymywanie po id
					PlanRemoval pr = PlanRemoval.parseFrom(delivery.getBody());
					System.out.println("[PlanManager] Received plan to remove: " + pr.getTaskId());
					removeById(pr.getTaskId());
					break;
				case REMOVE_PLAN_ALL_BIND_KEY:
					//usuwanie wszystkich jobow i zatrzymywanie tych co sa symulowane
					System.out.println("[PlanManager] Received remove all plans message");
					removeAll();
					break;
				case ADD_TRANSITION_BIND_KEY:
					//dodawanie jednego rekordu do tablcy przejsc
					TimeTransitions transition = TimeTransitions.parseFrom(delivery.getBody());
					addTimeTransition(transition);
					System.out.println("[PlanManager] Received new transition");
					break;
				case REMOVE_TRANSITION_BIND_KEY:
					//usuwanie tranxzycji po id
					TimeTransitionsRemoval tr = TimeTransitionsRemoval.parseFrom(delivery.getBody());
					System.out.println("[PlanManager] Received transition to remove: " + tr.getJobId());
					removeTransitionById(tr.getJobId());
					break;
				case REMOVE_TRANSITION_ALL_BIND_KEY:
					//usuwanie wszystkich rekordow tablicy tranzycji
					System.out.println("[PlanManager] Received remove all transitions message");
					removeAllTransitions();
					break;
				default: 
					System.err.println("invalid message");
					break;
				}
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

	private void removeAllTransitions() {
		this.timeTransitions.clear();
	}

	private void removeTransitionById(int jobId) {
		this.timeTransitions.remove(jobId);
	}

	private void addTimeTransition(TimeTransitions transition) {
		this.timeTransitions.put(transition.getJobId(), transition.getTimesList());
	}

	private void removeById(int jobId) {
		if (activePlans.containsKey(jobId)){
			stopSimulationByid(jobId);
		} else if (plans.containsKey(jobId)){
			plans.remove(jobId);
		} else { 
			System.err.println("Plan with this id doesn't exits: " + jobId);
		}
		if (plans.size() < MACHINES_COUNT){
			setEnoughPlans(false);
		}
	}

	private void stopSimulationByid(int jobId) {
		activeAgents.get(jobId).stopSimulation();
	}

	private void removeAll() {
		//removing unactive plans
		plans.clear();
		//stopnig active plans
		for (Integer id : activePlans.keySet()){
			stopSimulationByid(id);
		}
		activePlans.clear();
		setEnoughPlans(false);
	}

	private void addPlan(Plan plan) {
		plans.put(plan.getTaskId(), plan);
		if (isEnoughPlans() && plans.size() >= MACHINES_COUNT){
			setEnoughPlans(true);
			runSimulation();
		}
	}
	
	private void runSimulation() {
		
		//preparing agents to run
		int iteration = 1;
		for (Integer planId : plans.keySet()){
			Plan plan = plans.get(planId);
			plans.remove(planId);
			activePlans.put(planId, plan);
			activeAgents.put(plan.getTaskId(), new CommunicationAgent(plan, timeTransitions));
//			runSimulationforPlan(plan);
			
			//there should be exacly MACHINES_COUNT active simulations - 
			if (iteration == MACHINES_COUNT){
				break;
			}
			iteration++;
		}
		if (plans.size() < MACHINES_COUNT){
			setEnoughPlans(false);
		}		
		
		//actual run 
		
		//running agent threads and waiting for finishing
		ExecutorService pool = Executors.newFixedThreadPool(MACHINES_COUNT);
	    Set<Future<ResultStruct>> resultSet = new HashSet<Future<ResultStruct>>();
	    for (Integer id : activeAgents.keySet()){
	    	CommunicationAgent ca  = activeAgents.get(id);
	        Future<ResultStruct> future = pool.submit(ca);
	        resultSet.add(future);
	    }
	    
	    //getting results
	    Map<Plan, Long> resultsMap = new HashMap<>();
	    for (Future<ResultStruct> result : resultSet) {
	    	//ensure if anything bad to thread happend that this value won't be chosen by Evaluator
	    	long durationTime = Long.MAX_VALUE;
	    	Plan plan = null;
			try {
				durationTime = result.get().durationTime;
		    	plan = result.get().plan;
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
	    	resultsMap.put(plan, durationTime);
	    }
	    
	    //sending results
	    this.evaluator.sendResults(resultsMap);
	    
	    //run new simulation
	    afterAllSimulationsAreDone();
	}

	/**
	 * LEGACY
	 * should run a real thread with call method returning time result of simulation
	 * @param plan
	 */
//	private void runSimulationforPlan(Plan plan) {
//		CommunicationAgent ca = new CommunicationAgent(plan, timeTransitions);
//		activeAgents.put(plan.getTaskId(), ca);
//		ca.runSimulation();
//	}

	/**
	 * method for informing planmanager that all simulations are done and new ones should be executed
	 */
	public void afterAllSimulationsAreDone(){
		activeAgents.clear();
		if (isEnoughPlans()){
			runSimulation();
		}
	}

	private boolean isEnoughPlans() {
		return enoughPlans;
	}

	private void setEnoughPlans(boolean enoughPlans) {
		this.enoughPlans = enoughPlans;
	}
}
