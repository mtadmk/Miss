package pl.agh.miss.manager;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;

public class CommunicationAgent implements Callable<ResultStruct> {

	private Map<Integer, List<PassTime>> timeTranstitions;
	private Plan plan;

	public CommunicationAgent(Plan plan, Map<Integer, List<PassTime>> timeTransitions) {
		this.plan = plan;
		this.timeTranstitions = timeTransitions;
	}

	public void stopSimulation() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ResultStruct call() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}


	
//	public void runSimulation() {
//		// TODO Auto-generated method stub
//		
//	}
}
