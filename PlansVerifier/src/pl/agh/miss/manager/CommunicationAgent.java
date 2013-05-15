package pl.agh.miss.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanRemoval;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

public class CommunicationAgent implements Callable<ResultStruct> {

	private static final String EXCHANGE_NAME_AGENT = "AGENT_EXHANGE";
	private static final String TIME_TRANSITIONS_BIND_KEY = "TIME_TRANSITIONS_BIND_KEY";
	private static final String PLAN_BIND_KEY = "PLAN_BIND_KEY";
	private Map<Integer, List<PassTime>> timeTranstitions;
	private Plan plan;

	public CommunicationAgent(Plan plan, Map<Integer, List<PassTime>> timeTransitions) {
		this.plan = plan;
		this.timeTranstitions = timeTransitions;
	}

//	public void stopSimulation() {
//		// TODO Auto-generated method stub
//		
//	}

	/**
	 * run tests 
	 */
	@Override
	public ResultStruct call() throws Exception {
		//send time transitions and plan to simulator
		sendPlan(plan, timeTranstitions);
		long time = waitAndRetrieveResults();
		Random rand = new Random();
		Thread.sleep(200 + rand.nextInt(10000));
		return new ResultStruct(rand.nextInt(100), this.plan);
	}

	private long waitAndRetrieveResults() {
		// TODO Auto-generated method stub
		//method must retrieve time of simulated execution from simulator 
		return 0;
	}

	private void sendPlan(Plan plan2,
			Map<Integer, List<PassTime>> timeTranstitions2) {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection;
		try {
			connection = factory.newConnection();
			Channel channel = connection.createChannel();

			channel.exchangeDeclare(EXCHANGE_NAME_AGENT, "topic");
			//it is assumed timetransitions are sent first and then plan
			sendTimeTransitions(channel, timeTranstitions2);

			channel.basicPublish(EXCHANGE_NAME_AGENT, PLAN_BIND_KEY, null, plan2.toByteArray());
			System.out.println(" [Communication Agent] Sent '" + plan + "'");

			channel.close();
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void sendTimeTransitions(
			Channel channel, Map<Integer, List<PassTime>> timeTranstitions2) throws IOException {
		for (int id : timeTranstitions2.keySet()){
			TimeTransitions.Builder trans = TimeTransitions.newBuilder();
			List<PassTime> passList = timeTranstitions2.get(id);
			for (PassTime pt : passList){
				trans.addTimes(pt);
			}	
			trans.setJobId(id);
			
			channel.basicPublish(EXCHANGE_NAME_AGENT, TIME_TRANSITIONS_BIND_KEY, null, trans.build().toByteArray());
		}		
	}

}
