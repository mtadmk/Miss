package pl.agh.miss.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanRemoval;
import pl.agh.miss.proto.GeneratorMessage.Task;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PlansGenerator {
	Random r = new Random();

	private ArrayList<Integer> globalJobsIds = new ArrayList<>();

	private static final String ADD_PLAN_BIND_KEY = "addplanbindkey";
	private static final String REMOVE_PLAN_BIND_KEY = "removeplanbindkey";
	private static final String REMOVE_PLAN_ALL_BIND_KEY = "removeplanallbindkey";
	private static final String ADD_TRANSITION_BIND_KEY = "addtransitionbindkey";
	private static final String REMOVE_TRANSITION_BIND_KEY = "removetransitionbindkey";
	private static final String REMOVE_TRANSITION_ALL_BIND_KEY = "removetransitionallbindkey";
	public final static String EXCHANGE_NAME = "GeneratorQueue";

	private static final int MACHINES_COUNT = 3;
	private static final int JOBS_COUNT = 4;

	public static void main(String[] args) throws InterruptedException {
		PlansGenerator gen = new PlansGenerator();
		try {
			gen.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void run() throws InterruptedException, IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = null;
		Channel channel = null;
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME, "topic");

			int i = JOBS_COUNT;
			while (i-- > 0) {
				globalJobsIds.add(r.nextInt());
			}

			// send transitions
			for (Integer jobid : globalJobsIds) {
				TimeTransitions transition = buildSimpleTransition(jobid);
				channel.basicPublish(EXCHANGE_NAME, ADD_TRANSITION_BIND_KEY,
						null, transition.toByteArray());
				System.out.println(" [Generator] Sent '" + transition + "'");
			}

			// insert jobs or other operations
			int ii = 10;
			while (ii-->0) {
//			while (true) {
				Plan plan = buildSimplePlan(r.nextInt());
				channel.basicPublish(EXCHANGE_NAME, ADD_PLAN_BIND_KEY, null,
						plan.toByteArray());
				System.out.println(" [Generator] Sent '" + plan + "'");
//				Thread.sleep(200 + r.nextInt(500));
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			channel.close();
			connection.close();
		}
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

}
