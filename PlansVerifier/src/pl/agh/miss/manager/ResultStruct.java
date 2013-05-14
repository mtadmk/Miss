package pl.agh.miss.manager;

import pl.agh.miss.proto.GeneratorMessage.Plan;

public class ResultStruct {
	public long durationTime;
	public Plan plan;
	
	public ResultStruct(long d, Plan p){
		this.durationTime = d;
		this.plan = p;
	}
}
