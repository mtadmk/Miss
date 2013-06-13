//package pl.agh.miss.manager;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.concurrent.CompletionService;
//import java.util.concurrent.ConcurrentLinkedDeque;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorCompletionService;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
//import pl.agh.miss.proto.GeneratorMessage.Plan;
//
//public class SimulationManager_LEGACY implements Runnable {
//
//	private Map<Integer, CommunicationAgent_LEGACY> activeAgents;
//	private Map<Integer, Future<ResultStruct>> futureMap = new HashMap<>();
//	private Evaluator evaluator;
//	private PlanManager planManager;
//	private int machinesCount;
//
//	public SimulationManager_LEGACY(Map<Integer, CommunicationAgent_LEGACY> activeAgents2,
//			Evaluator evaluator2, PlanManager planManager, int machinesCount) {
//		this.activeAgents = activeAgents2;
//		this.evaluator = evaluator2;
//		this.planManager = planManager;
//		this.machinesCount = machinesCount;
//	}
//
//	@Override
//	public void run() {
//		// running agent threads and waiting for finishing
//		ExecutorService pool = Executors.newFixedThreadPool(machinesCount);
//		CompletionService<ResultStruct> ecs = new ExecutorCompletionService<ResultStruct>(pool);
//		
//		
//		for (Integer id : activeAgents.keySet()) {
//			CommunicationAgent_LEGACY ca = activeAgents.get(id);
//			Future<ResultStruct> future = ecs.submit(ca);
//			futureMap.put(id, future);
//		}
//
////		planManager.setFuturesMap(futureMap);
//		
//		// getting results
//		Map<Plan, Long> resultsMap = new HashMap<>();
//		int n = futureMap.size();
//		for (int i = 0; i < n; i++){
//			// ensure if anything bad to thread happend that this value won't be
//			// chosen by Evaluator
//			long durationTime = Long.MAX_VALUE;
//			Plan plan = null;
//			try {
//				ResultStruct resultStruct = ecs.take().get();
//				durationTime = resultStruct.durationTime;
//				plan = resultStruct.plan;
//				resultsMap.put(plan, durationTime);
//			} catch (InterruptedException | ExecutionException |NullPointerException e) {
//				//ignore as there may be interrupted threads
//				//as for nullpointerexception - resultStruct culd be as it may be interrupted by other
////				e.printStackTrace();
//			}
//		}
//
//		// sending results
////		this.evaluator.sendResults(resultsMap);
//
//		// run new simulation
//		planManager.afterAllSimulationsAreDone();
//	}
//
////	public void stopSimulationById(int jobId) {
////		futureMap.get(jobId).cancel(true);
////	}
//
//}
