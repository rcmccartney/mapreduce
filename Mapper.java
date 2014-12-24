package mapreduce;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public abstract class Mapper<IK extends Serializable, 
							 IV extends Serializable,
							 OV extends Serializable> {
	
	private Job<IK, IV, OV> job;
	
	public abstract HashMap<IK, IV> map(File resource);
	
	public abstract OV reduce(IK key, List<IV> listOfValues);

	public void emit(IK key, IV value) {
		job.emit(key, value);
	}
	
	@SuppressWarnings("unchecked")
	public void setJob(Job<?, ?, ?> job) {
		this.job = (Job<IK, IV, OV>) job;
	}
}
