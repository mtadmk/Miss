package pl.agh.miss.manager;

import java.io.IOException;
import java.util.Map;

import pl.agh.miss.proto.GeneratorMessage.Plan;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Evaluator {
	public final static String EXCHANGE_NAME = "EvaluatorBestTime";
	private final static String BIND_KEY = "EVALUATOR_STANDARD_BIND_KEY";
	
	private String controllerAddress = "localhost";
	

//	private PlanManager planManager;
	private long currentPlanDuration = Long.MAX_VALUE;
	private Plan currentPlan = null;

	public Evaluator() {
//		this.planManager = pm;
	}

	public synchronized void sendResults(Map<Plan, Long> resultsMap) {
//		//TEST
//		System.out.println(resultsMap);
//		if (true ) return;
		
		//indicates if there was change beteen currently executed plan and new plan
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
	
}
