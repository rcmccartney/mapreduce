package mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Job<K, V> {

	protected ExecutorService exec;
	protected Worker worker;
	protected Mapper<K, V> mr;
	protected HashMap<K, List<V>> output;
	protected String[] files;
	
	public Job(Worker worker, Mapper<K, V> mr, String...strings) {
		this.worker = worker;
		this.mr = mr;
		this.mr.setJob(this);
		exec = Executors.newCachedThreadPool();
		files = strings;
		output = new HashMap<>();
		begin();
	}
	
	public void begin() {
		
		// since this is reading from a file don't do it in parallel,
		// speed limit is reading from disk
		for (final String filename : files) 
			// convenience function provided if user doesn't want to call 'emit'
			emit(mr.map(new File(filename)));

		// now the output map has been populated, so it needs to be shuffled and sorted 
		// first notify Master of the keys you have at this node, and their sizes
		for(K key: output.keySet()) {
			//worker.writeMaster("k" + " " + key + " " + output.get(key).size());
			System.out.println("HAVE THESE KEYS: " + key + " Value: " + output.get(key));
		}
	}
	
	public void emit(K key, V value) {
		if (output.containsKey(key)) 
			output.get(key).add(value);
		else {
			List<V> l = new ArrayList<>();
			l.add(value);
			output.put(key, l);
		}
	}
	
	public void emit(HashMap<K, V> tmp) {
		if (tmp == null)
			return;
		
		for(K key: tmp.keySet()) 
			emit(key, tmp.get(key));
	}
	
	public void receiveKeyAssignment() {
		// need key, ip address, and port from Master to fwd your values there
		// K key = mr.parse("F");
		
	}
	
	public void stopExecution() {
		exec.shutdown();
		
	}
}
