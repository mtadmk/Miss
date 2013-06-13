package pl.agh.miss.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanAndTransitions;
import pl.agh.miss.proto.GeneratorMessage.PlanRemoval;
import pl.agh.miss.proto.GeneratorMessage.Task;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class PlansGenerator {
	private static String brokerHost = "localhost";
	Random r = new Random();
	private Plan currentPlan;

	private ArrayList<Integer> globalJobsIds = new ArrayList<>();
	ReentrantLock lock = new ReentrantLock();
	/**
	 * jobid, timetransition
	 */
	private Map<Integer, TimeTransitions> transitionsMap = new ConcurrentHashMap<>();

	private static final String ADD_PLAN_BIND_KEY = "addplanbindkey";
//	private static final String REMOVE_PLAN_BIND_KEY = "removeplanbindkey";
	private static final String REMOVE_PLAN_ALL_BIND_KEY = "removeplanallbindkey";
	private static final String ADD_ALL_TRANSITION_BIND_KEY = "addalltransitionbindkey";
	private static final String ADD_TRANSITION_BIND_KEY = "addtransitionbindkey";
//	private static final String REMOVE_TRANSITION_BIND_KEY = "removetransitionbindkey";
	private static final String REMOVE_TRANSITION_ALL_BIND_KEY = "removetransitionallbindkey";
	public final static String EXCHANGE_NAME = "GeneratorQueue";
	public final static String EXCHANGE_NAME_EVALUATOR = "EvaluatorBestTime";
	private final static String BIND_KEY = "EVALUATOR_STANDARD_BIND_KEY";

	private static Plan tmpPlan;
	List<Task> firstTasks = new LinkedList<>();
	private static final int MACHINES_COUNT = 3;
	private static final int JOBS_COUNT = 3;
	AtomicBoolean isNewPlan = new AtomicBoolean(false);
	/**
	 * on this task controller must wait for new plan
	 */
	Task waitOnTask;

	public static void main(String[] args) throws InterruptedException {
		PlansGenerator gen = new PlansGenerator();
		try {
			if (args.length > 0){
				brokerHost = args[0];	
			}
			gen.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	Connection connection = null;
	Channel channel = null;

	public void run() throws InterruptedException, IOException {
		tmpPlan = buildTmpPlan();
		newControllerThread();
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(brokerHost);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "topic");

			int i = JOBS_COUNT;
			while (i-- > 0) {
				globalJobsIds.add(r.nextInt());
			}

			// send transitions
			PlanAndTransitions.Builder pt = PlanAndTransitions.newBuilder();
			pt.setPlan(tmpPlan);
			for (Integer jobid : globalJobsIds) {
				TimeTransitions transition = buildSimpleTransition(jobid);
				pt.addTimeTransitions(transition);
//				channel.basicPublish(EXCHANGE_NAME, ADD_TRANSITION_BIND_KEY,
//						null, transition.toByteArray());
				transitionsMap.put(jobid, transition);
				System.out.println(" [Generator] Sent trans'" + transition.getJobId() + "'");
			}
			channel.basicPublish(EXCHANGE_NAME, ADD_ALL_TRANSITION_BIND_KEY,
					null, pt.build().toByteArray());
			
			
			//mam poczatkowe tranzycje - i sa wyslane na maszyne

			System.out.print(" [Generator] Sent plans ");
			// insert jobs or other operations
			int ii = 3;
			while (ii-->0) {
				Plan plan = buildSimplePlan(r.nextInt());
				if (ii == 0){
					waitOnTask = plan.getTasks(r.nextInt(plan.getTasksCount()));
				}
				channel.basicPublish(EXCHANGE_NAME, ADD_PLAN_BIND_KEY, null,
						plan.toByteArray());

				 System.out.print(plan.getPlanId() + ", ");
			}	
			System.out.println("'");
			
			startSimulationThread();

			
			while (true){
				if (r.nextInt() % 3 == 1){
					// once on while add new job
					int jobid = r.nextInt();
					globalJobsIds.add(jobid);
					TimeTransitions transition = buildSimpleTransition(jobid);
					
//PO wyslaniu tranzycji powinny sie raczej wszystkie plany uaktualnic - 					
					
					channel.basicPublish(EXCHANGE_NAME, ADD_TRANSITION_BIND_KEY,
							null, transition.toByteArray());
					transitionsMap.put(jobid, transition);
					channel.basicPublish(EXCHANGE_NAME, REMOVE_TRANSITION_ALL_BIND_KEY,
							null, transition.toByteArray());
					System.out.println(" [Generator] Sent trans'" + transition.getJobId() + "'");
				} 
				
				System.out.print(" [Generator] Sent plans ");
				ii = 3;
				while (ii-->0) {
					Plan plan = buildSimplePlan(r.nextInt());
					if (ii == 0){
						waitOnTask = plan.getTasks(r.nextInt(plan.getTasksCount()));
					}
					channel.basicPublish(EXCHANGE_NAME, ADD_PLAN_BIND_KEY, null,
							plan.toByteArray());
					 System.out.print(plan.getPlanId() + ", ");
				}	
				System.out.println("'");
				
//TRZEBA usuwac stare plany jak sie dodaje nowe joby!
				
				Thread.sleep(6000);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			channel.close();
			connection.close();
		}
	}



	private List<Task> getFirstTasks() {
		return firstTasks;
	}

	private TimeTransitions buildSimpleTransition(int jobId) {
		TimeTransitions.Builder trans = TimeTransitions.newBuilder();

		PassTime pt;
		for (int i = 0; i < MACHINES_COUNT; i++) {
			pt = createPassTime(i);
			trans.addTimes(pt);
		}

		trans.setJobId(jobId);

		return trans.build();
	}

	private PassTime createPassTime(int id) {
		PassTime.Builder pt = PassTime.newBuilder();

		pt.setMachineId(id);
		pt.setTime(r.nextInt(500));
		return pt.build();
	}

	private PlanRemoval buildSimplePlanRemoval() {
		PlanRemoval.Builder rplan = PlanRemoval.newBuilder();

		rplan.setPlanId(1);

		return rplan.build();
	}
	private Plan buildTmpPlan(){
		Plan.Builder plan = Plan.newBuilder();

		plan.setPlanId(-1);
		Task.Builder t = Task.newBuilder();
		t.setJobId(-1);
		t.setMachineId(-1);
		plan.addTasks(t);
		
		return plan.build();
	}

	private Plan buildSimplePlan(int id) {
		Plan.Builder plan = Plan.newBuilder();

		plan.setPlanId(id);

		ArrayList<Integer> machinesList = new ArrayList<>();
		ArrayList<Integer> jobsIdList = new ArrayList<>(globalJobsIds);

		ArrayList<Task> tasksList = new ArrayList<>();
		Task pt = null;
		for (int i = 0; i < JOBS_COUNT; i++) {
			for (int ii = 0; ii < MACHINES_COUNT; ii++) {
				machinesList.add(ii);
			}

			int jobid = jobsIdList.remove(r.nextInt(jobsIdList.size()));
			for (int j = 0; j < MACHINES_COUNT; j++) {
				int machineid = machinesList
						.remove(r.nextInt(machinesList.size()));
				pt = createTask(jobid, machineid);
				tasksList.add(pt);
//				plan.addTasks(pt);
			}
		}
		
		//taskss list is sorted by id - must unsort it
		ArrayList<Task> unsortedTasksList = new ArrayList<>();
		int size = tasksList.size();
		for (int i =0; i < size; i++){
			unsortedTasksList.add(tasksList.remove(r.nextInt(tasksList.size())));
		}		
		
		plan.addAllTasks(unsortedTasksList);

		return plan.build();
	}

	private Task createTask(int jobId, int machineId) {
		Task.Builder task = Task.newBuilder();
		task.setJobId(jobId);
		task.setMachineId(machineId);
		return task.build();
	}

	private void newControllerThread() {
		new Thread(new Runnable() {			
			@Override
			public void run() {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(brokerHost);
				Connection connection;
				try {
					connection = factory.newConnection();
					Channel channel = connection.createChannel();

					channel.exchangeDeclare(EXCHANGE_NAME_EVALUATOR, "topic");
					String queueName = channel.queueDeclare().getQueue();

					channel.queueBind(queueName, EXCHANGE_NAME_EVALUATOR, BIND_KEY);

					System.out
							.println(" [Controller] Waiting for messages. To exit press CTRL+C");

					QueueingConsumer consumer = new QueueingConsumer(channel);
					channel.basicConsume(queueName, true, consumer);

					while (true) {
						
						QueueingConsumer.Delivery delivery = consumer.nextDelivery();
						setCurrentPlan(Plan.parseFrom(delivery.getBody()));
						isNewPlan.set(true);
						System.out.println("[Controller] Received new plan: " + getCurrentPlan().getPlanId());

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

	synchronized Plan getCurrentPlan() {
		return currentPlan;
	}

	synchronized void setCurrentPlan(Plan currentPlan) {
		this.currentPlan = currentPlan;
	}
	
	ConcurrentLinkedQueue<Task> nextTasks = new ConcurrentLinkedQueue<>();
	private void startSimulationThread() {
		new Thread(new Runnable() {			
			@Override
			public void run() {
				while (true){
					while(!isNewPlan.get()){
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}						
					}
//					System.out.println("CONTROLLER: new plan");
					boolean areNextTask = false;
					for (Task task : getCurrentPlan().getTasksList()){
						if (!areNextTask){
							//actualize timetransitions table
							//get this job transitions
							TimeTransitions transitions = transitionsMap.get(task.getJobId());
							TimeTransitions.Builder newTaskTransitions = TimeTransitions.newBuilder();
							newTaskTransitions.setJobId(task.getJobId());
							for (PassTime pt : transitions.getTimesList()){
								if (pt.getMachineId() == task.getMachineId()){
									if (pt.getTime()!= 0){
										//obejscie jobow ktore sie wykonuja
//										System.out.println("simulating jobid: " + task.getJobId() + ", machine: " + task.getMachineId());
									}
									PassTime.Builder newPassTime = PassTime.newBuilder();
									newPassTime.setMachineId(pt.getMachineId());
									newPassTime.setTime(0);
									newTaskTransitions.addTimes(newPassTime);
									//pass time odpowiadajacy danej tranzycji founded
									try {
										Thread.sleep(pt.getTime());
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							}
							transitionsMap.put(task.getJobId(), newTaskTransitions.build());
						} else {
//							isNewPlan.set(false);
							nextTasks.add(task);
						}
						if (task.getJobId() == waitOnTask.getJobId() && task.getMachineId() == waitOnTask.getMachineId()){
							//if we are in wait for task
							areNextTask=true;
							//update transitions
							//firstly remove all actual tasks
							try {
								channel.basicPublish(EXCHANGE_NAME, REMOVE_PLAN_ALL_BIND_KEY,
										null, "".getBytes());
								PlanAndTransitions.Builder pt = PlanAndTransitions.newBuilder();
								pt.setPlan(tmpPlan);
								for (Integer id : transitionsMap.keySet()){
									pt.addTimeTransitions(transitionsMap.get(id));									
								}
								channel.basicPublish(EXCHANGE_NAME, ADD_ALL_TRANSITION_BIND_KEY,
										null, pt.build().toByteArray());
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
					isNewPlan.set(false);
				}
			}
		}).start();
	}
}
