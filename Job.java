package mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Job<key, value> {

	protected ExecutorService exec;
	protected Worker worker;
	protected Mapper<key, value> mr;
	protected HashMap<key, List<value>> output;
	protected String[] files;
	
	public Job(Worker worker, String...strings) {
		this.worker = worker;
		exec = Executors.newWorkStealingPool();
		files = strings;
	}
	
	public void begin() {
		
		for (final String filename : files) {
			exec.execute(new Runnable() {				
				@Override
				public void run() {
					File f = new File(filename);
					mr.map(f);					
				}
			});
		}
		// now the output map has been populated, so it needs to be shuffled and sorted 
		// first notify Master of the keys you have at this node, and their sizes
		for(key k: output.keySet()) {
			worker.writeMaster("k" + " " + k + " " + output.get(k).size());
		}
	}
	
	public void emit(key k, value v) {
		if (output.containsKey(k)) 
			output.get(k).add(v);
		else {
			List<value> l = new ArrayList<>();
			l.add(v);
			output.put(k, l);
		}
	}
	
	public void receiveKeyAssignment(String k, String ipaddr, String port) {
		key ky = mr.parse(k);
		
	}
	
	public void stopExecution() {
		exec.shutdown();
		
	}
}
