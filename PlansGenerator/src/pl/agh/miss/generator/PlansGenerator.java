package pl.agh.miss.generator;

import java.io.IOException;
import java.util.Random;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanRemoval;
import pl.agh.miss.proto.GeneratorMessage.Task;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PlansGenerator {
	private static final String ADD_PLAN_BIND_KEY = "addplanbindkey";
	private static final String REMOVE_PLAN_BIND_KEY = "removeplanbindkey";
	private static final String REMOVE_PLAN_ALL_BIND_KEY = "removeplanallbindkey";
	private static final String ADD_TRANSITION_BIND_KEY = "addtransitionbindkey";
	private static final String REMOVE_TRANSITION_BIND_KEY = "removetransitionbindkey";
	private static final String REMOVE_TRANSITION_ALL_BIND_KEY = "removetransitionallbindkey";
	public final static String EXCHANGE_NAME = "GeneratorQueue";

	public static void main(String[] args) throws InterruptedException {
		PlansGenerator gen = new PlansGenerator();
		gen.run();
	}

	public void run() throws InterruptedException {
		while (true) {
			Thread.sleep(300);
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost("localhost");
			Connection connection;
			try {
				connection = factory.newConnection();
				Channel channel = connection.createChannel();

				channel.exchangeDeclare(EXCHANGE_NAME, "topic");

				Plan plan = buildSimplePlan();
				PlanRemoval rplan = buildSimplePlanRemoval();

				channel.basicPublish(EXCHANGE_NAME, ADD_PLAN_BIND_KEY, null,
						plan.toByteArray());
				channel.basicPublish(EXCHANGE_NAME, REMOVE_PLAN_BIND_KEY, null,
						rplan.toByteArray());
				channel.basicPublish(EXCHANGE_NAME, REMOVE_PLAN_ALL_BIND_KEY, null,
						"lol".getBytes());
				

				System.out.println(" [Generator] Sent '" + plan + "'");

				channel.close();
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			break;
		}
	}

	private PlanRemoval buildSimplePlanRemoval() {
		PlanRemoval.Builder rplan = PlanRemoval.newBuilder();

		rplan.setTaskId(1);

		return rplan.build();
	}

	private Plan buildSimplePlan() {
		Plan.Builder plan = Plan.newBuilder();

		Task pt = createTask(1, 10);
		plan.addTasks(pt);
		pt = createTask(2, 11);
		plan.addTasks(pt);

		Random rand = new Random();
		plan.setTaskId(rand.nextInt());

		return plan.build();
	}

	private Task createTask(int jobId, int time) {
		Task.Builder task = Task.newBuilder();
		task.setJobId(1);
		task.setMachineId(1);
		return task.build();
	}

}
