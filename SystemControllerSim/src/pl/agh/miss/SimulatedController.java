package pl.agh.miss;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import pl.agh.miss.proto.GeneratorMessage.PassTime;
import pl.agh.miss.proto.GeneratorMessage.Plan;
import pl.agh.miss.proto.GeneratorMessage.PlanAndTransitions;
import pl.agh.miss.proto.GeneratorMessage.PlanQueueInfo;
import pl.agh.miss.proto.GeneratorMessage.SimulationState;
import pl.agh.miss.proto.GeneratorMessage.Task;
import pl.agh.miss.proto.GeneratorMessage.TimeTransitions;

import com.google.protobuf.InvalidProtocolBufferException;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

public class SimulatedController {
	private static final String EXCHANGE_NAME_AGENT = "AGENT_EXHANGE";
	private static final String TIME_TRANSITIONS_BIND_KEY = "TIME_TRANSITIONS_BIND_KEY";
	private static final String PLAN_BIND_KEY = "PLAN_BIND_KEY";
	private static final String RPC_QUEUE_NAME = "RPC_QUEUE";
	private static final String RPC_EVALUATOR_QUEUE_NAME = "RPC_EVALUATOR_QUEUE";
	private static final String REMOVE_ACTIVE_PLAN_MESSAGE = "removeActivePlan";
	private static final String GET_STATE_MESSAGE = "getState";

	private AtomicBoolean canFinish = new AtomicBoolean(false);

	private Plan plan;
	private Map<Integer, List<PassTime>> timeTransitions = new HashMap<Integer, List<PassTime>>();
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer;

	private String queueName = UUID.randomUUID().toString().replace("-", "");

	/**
	 * important zobacz createState metode
	 */
	private SimulationState simulationState;

	/**
	 * important jak zmieni sie na false (ja to zmieniam) to masz przerwac
	 * symulacje
	 */
	private AtomicBoolean isCancelled = new AtomicBoolean(false);

	// TODO Tomek te 2 powyzsze zmienne masz utrzymywac aktualne i tyle

	public SimulatedController() {
		// MOCK
		this.simulationState = createState(11, 111);
		//

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");

		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

			channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);

			channel.basicQos(1);

			consumer = new QueueingConsumer(channel);
			channel.basicConsume(RPC_QUEUE_NAME, true, consumer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		SimulatedController sc = new SimulatedController();
		sc.run();
	}

	public void run() {
		while (true) {
			try {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				this.getOnePlan(delivery);

				this.startCommunicationThread();
				this.sendCommunicationQueue(delivery);

				System.out.println("[SIMULATED CONTROLLER] Plan : " + plan);

				simulationState = createState(1, 111);

				// MOCK there should be method getJobShopTime Executed
				long result = 1000;
				Thread.sleep(10000);

				// zawsze
				// long result = getJobShopTime();
				simulationState = createState(0, result);
				while (!canFinish.get()) {
					Thread.sleep(500);
				}
				System.out
						.println("[SIMULATED CONTROLLER] Simulation finished");

				// have fun with this.plan and this.timeTransitions
				// job shop should return only result
				// long result = getJobShopTime();
				// this.sendResult(result, delivery);
			} catch (ShutdownSignalException | ConsumerCancelledException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * 
	 * @return
	 */
	private long getJobShopTime() {
		List<Task> taskList = plan.getTasksList();
		// key: job, value: pos on jobsTimeList
		Map<Integer, Integer> jobsListPosMap = new HashMap<>();
		// list of (stop start) times of each job
		List<List<Long>> jobsTimesList = new ArrayList<>();
		// list of (stop start) times of jobs on each machine
		List<List<Long>> machineSlotsList = new ArrayList<>();
		int i = 0;
		int randJobId = 0;
		for (int jobId : timeTransitions.keySet()) {
			jobsListPosMap.put(jobId, i);
			ArrayList<Long> tmpList = new ArrayList<>();
			jobsTimesList.add(i, tmpList);
			++i;
			randJobId = jobId;
		}
		// do all list have this same size?
		int machineCount = timeTransitions.get(randJobId).size();
		// do machines number starts from 0 i numeracja jest ciagla?
		for (int it = 0; it < machineCount; ++it) {
			ArrayList<Long> tmpList = new ArrayList<>();
			tmpList.add((long) 0);
			tmpList.add((long) 0);
			machineSlotsList.add(tmpList);
		}

		for (Task task : taskList) {
			int jobId = task.getJobId();
			int machineId = task.getMachineId();
			long time = 0;
			for (PassTime passTime : timeTransitions.get(jobId)) {
				if (passTime.getMachineId() == machineId) {
					time = passTime.getTime();
					break;
				}
			}
			if (time == 0) {
				System.out.println("0 time break");
				continue;
			}
			List<Long> currentJobTimes = jobsTimesList.get(jobsListPosMap
					.get(jobId));
			List<Long> currentMachineSlots = machineSlotsList.get(machineId);
			// try to match gap before checked slot (starts from 2nd slot)
			int insertSlot = -1;
			long startTime = 0;
			for (int slotIt = 2; slotIt < currentMachineSlots.size(); slotIt += 2) {
				int prevStart = slotIt - 2;
				int prevStop = slotIt - 1;
				int start = slotIt;
				int stop = slotIt + 1;
				if (currentMachineSlots.get(start)
						- currentMachineSlots.get(prevStop) >= time) {
					long tmpStartTime = findNoConflictStartTime(
							currentJobTimes, currentMachineSlots.get(prevStop),
							currentMachineSlots.get(start), time);
					if (tmpStartTime != -1) {
						startTime = tmpStartTime;
						insertSlot = slotIt;
						break;
					}
				}
			}
			// all slots are free (0 and 1 - empty 0)
			System.out.printf("job: %d, machine:  %d", jobId, machineId);
			if (currentMachineSlots.size() == 2 && insertSlot == -1) {

				startTime = findFirstAvaiableStartTime(currentJobTimes, 0, time);
				currentMachineSlots.add(2, startTime);
				currentMachineSlots.add(3, startTime + time);
				insertBeforeFirstSmallerValue(currentJobTimes, startTime, time);
				System.out.println(" ,start: " + startTime + ", stop:  "
						+ (startTime + time));
				System.out.println("empty");
			} else {
				if (insertSlot == -1) {
					int curMachSlotsSizeBeforeInsert = currentMachineSlots
							.size();
					startTime = findFirstAvaiableStartTime(currentJobTimes,
							currentMachineSlots
									.get(curMachSlotsSizeBeforeInsert - 1),
							time);
					currentMachineSlots.add(curMachSlotsSizeBeforeInsert,
							startTime);
					currentMachineSlots.add(curMachSlotsSizeBeforeInsert + 1,
							startTime + time);
					insertBeforeFirstSmallerValue(currentJobTimes, startTime,
							time);
					System.out.println(" ,start: " + startTime + ", stop:  "
							+ (startTime + time));
					System.out.println("no match - end");
				}
				// proper free slot
				else {

					currentMachineSlots.add(insertSlot, startTime);
					currentMachineSlots.add(insertSlot + 1, startTime + time);
					insertBeforeFirstSmallerValue(currentJobTimes, startTime,
							time);
					System.out.println(" ,start: " + startTime + ", stop:  "
							+ (startTime + time));
					System.out.println("proper");
				}
			}

		}
		long max = 0;
		for (List<Long> tmpList : machineSlotsList) {
			long tmpMax = tmpList.get(tmpList.size() - 1);
			max = Math.max(max, tmpMax);
		}
		// Random rand = new Random();
		// long result = 10000 + rand.nextInt(100000);
		return max;
	}

	/**
	 * Find no conflict slot.
	 * 
	 * @return -1 is slot is not matched
	 */
	private long findNoConflictStartTime(List<Long> list, long start,
			long stop, long time) {
		long tmpStart = findFirstAvaiableStartTime(list, start, time);
		if (tmpStart + time <= stop) {
			return tmpStart;
		}
		return -1;
	}

	private long findFirstAvaiableStartTime(List<Long> list, long start,
			long time) {
		if (list.size() == 0) {
			return start;
		}
		long startResult = list.get(list.size() - 1);
		// if last stop from list is before start return start
		if (startResult <= start) {
			return start;
		}
		int it = 0;
		// pass all task which ends before given start
		while (list.get(it + 1) < start) {
			it += 2;
		}
		// it fits before first
		if (it == 0 && list.get(0) >= time) {
			return start;
		}

		// there is one task which ends after given start time so -2
		// search for slot starting from last slot which ends before given time
		for (; it < list.size() - 2; it += 2) {
			if (list.get(it + 2) - list.get(it + 1) >= time) {
				startResult = list.get(it + 1);
				// TODO - no conflict?
				break;
			}
		}

		return startResult;
	}

	private void insertBeforeFirstSmallerValue(List<Long> currentJobTimes,
			long startTime, long time) {
		int insertJobSlot = 0;
		for (int it = 0; it < currentJobTimes.size(); it += 2) {
			insertJobSlot = it;
			if (currentJobTimes.get(it) > startTime) {
				break;
			}
			if (it + 2 == currentJobTimes.size()) {
				insertJobSlot = currentJobTimes.size();
			}
		}
		currentJobTimes.add(insertJobSlot, startTime);
		currentJobTimes.add(insertJobSlot + 1, startTime + time);
	}

	private void sendResult(long result, Delivery delivery)
			throws UnsupportedEncodingException, IOException {
		BasicProperties props = delivery.getProperties();
		BasicProperties replyProps = new BasicProperties.Builder()
				.correlationId(props.getCorrelationId()).build();
		channel.basicPublish("", props.getReplyTo(), replyProps, String
				.valueOf(result).getBytes("UTF-8"));

		// channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	}

	public void getOnePlan(Delivery delivery) throws IOException {
		System.out
				.println(" [Simulation Controller] Waiting for messages. To exit press CTRL+C");
		String response = null;

		// QueueingConsumer.Delivery delivery = consumer.nextDelivery();

		PlanAndTransitions pat = PlanAndTransitions.parseFrom(delivery
				.getBody());
		// channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		this.plan = pat.getPlan();
		for (TimeTransitions tt : pat.getTimeTransitionsList()) {
			this.timeTransitions.put(tt.getJobId(), tt.getTimesList());
		}
	}

	private void sendCommunicationQueue(Delivery delivery) throws IOException {
		BasicProperties props = delivery.getProperties();
		BasicProperties replyProps = new BasicProperties.Builder()
				.correlationId(props.getCorrelationId()).build();
		PlanQueueInfo.Builder b = PlanQueueInfo.newBuilder();
		b.setPlanId(plan.getPlanId());
		b.setQueueName(this.queueName);
		PlanQueueInfo pq = b.build();
		channel.basicPublish("", props.getReplyTo(), replyProps,
				pq.toByteArray());

		// channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	}

	/**
	 * important
	 * 
	 * 
	 * @param state
	 *            : wymysl sobie jakies stany posrednie miedzy 0 -zakonczono
	 *            symulacje 1 - symulacja trwa np 2 - warto czekac na koniec
	 *            symulacji 3 - nie warto czekac na koniec symulacji
	 * @param predictionTime
	 * @return
	 */
	private synchronized SimulationState createState(int state,
			long predictionTime) {
		SimulationState.Builder b = SimulationState.newBuilder();
		b.setState(state);
		b.setPredictedExecutionTime(predictionTime);
		return b.build();
	}

	private void startCommunicationThread() {
		new Thread(new Runnable() {
			ConnectionFactory factory = new ConnectionFactory();
			Connection connection = null;
			Channel channel = null;

			@Override
			public void run() {
				try {
					factory.setHost("localhost");

					connection = factory.newConnection();
					channel = connection.createChannel();

					channel.queueDeclare(queueName, false, false, false, null);

					channel.basicQos(1);

					QueueingConsumer consumer1 = new QueueingConsumer(channel);
					channel.basicConsume(queueName, true, consumer1);

					while (true) {
						QueueingConsumer.Delivery delivery = consumer1
								.nextDelivery();
						// unused
						String msg = new String(delivery.getBody());
						System.out.println(" [SimulatedController] Received '"
								+ msg + "'");
						if (msg.equals(REMOVE_ACTIVE_PLAN_MESSAGE)) {
							isCancelled.set(true);
							sendState(delivery);
						} else if (msg.equals(GET_STATE_MESSAGE)) {
							sendState(delivery);
						}
					}
				} catch (IOException | ShutdownSignalException
						| ConsumerCancelledException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					if (connection != null) {
						try {
							connection.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}

			private void sendState(Delivery delivery) throws IOException {
				BasicProperties props = delivery.getProperties();
				BasicProperties replyProps = new BasicProperties.Builder()
						.correlationId(props.getCorrelationId()).build();

				if (simulationState.getState() == 0) {
					canFinish.set(true);
				} else {
					canFinish.set(false);
				}
				channel.basicPublish("", props.getReplyTo(), replyProps,
						simulationState.toByteArray());

			}
		}).start();
	}
}
