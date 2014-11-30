package mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Job<K, V> {

	protected Worker worker;
	protected Mapper<K, V> mr;
	protected HashMap<K, List<V>> mapOutput;
	protected HashMap<K, V> finalOut;
	protected String[] files;
	
	public Job(Worker worker, Mapper<K, V> mr, String...strings) {
		this.worker = worker;
		this.mr = mr;
		this.mr.setJob(this);
		files = strings;
		mapOutput = new HashMap<>();
		finalOut = new HashMap<>();
	}
	
	public void begin() {
		
		// since this is reading from a file don't do it in parallel,
		// speed limit is reading from disk
		for (final String filename : files) 
			// convenience function provided if user doesn't want to call 'emit'
			emit(mr.map(new File(filename)));

		// now the output map has been populated, so it needs to be shuffled and sorted 
		// first notify Master of the keys you have at this node, and their sizes
		sendAllKeysToMaster();
	}
	
	public void reduce() {
		ArrayList<Thread> thrs = new ArrayList<>();
		for(final K key: mapOutput.keySet()) {
			thrs.add(new Thread(new Runnable() {
				public void run() {
					finalOut.put(key, mr.reduce(key, mapOutput.get(key)));
				}
			}));
			thrs.get(thrs.size()-1).start();
		}
		// wait for all the threads to finish
		for(Thread t: thrs){
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// TODO this is testing code to be removed,
		for( K key : finalOut.keySet() ) {
			System.out.println("key: " + key + " result: " + finalOut.get(key));
		}
		
		//finalOut holds the results of this MR job, send it to Master
		worker.writeMaster(Utils.W2M_RESULTS);
		sendResults();
	}
	
	public void emit(K key, V value) {
		if (mapOutput.containsKey(key)) 
			mapOutput.get(key).add(value);
		else {
			List<V> l = new ArrayList<>();
			l.add(value);
			mapOutput.put(key, l);
		}
	}
	
	public void emit(HashMap<K, V> tmp) {
		if (tmp == null)
			return;
		for(K key: tmp.keySet()) 
			emit(key, tmp.get(key));
	}
	
	public void receiveKeyAssignments() {
		// need key, ip address, and port from Master to fwd your values there
		// K key = mr.parse("F");
		// TODO
		
		// Then send all the P2P traffic...
		// P2P traffic should remove values from output as it is sent and add to it as it
		// is received,
		
		//then once P2P is fished call this.reduce()
		// so output has all the key - list of Values
		
	}
	
	public void sendAllKeysToMaster() {
		byte[] data;
		for (K key: mapOutput.keySet()) {
			worker.writeMaster(Utils.W2M_KEY);
			data = Utils.concat(mr.getBytes(key), 
					Utils.intToByteArray(mapOutput.get(key).size()));
			worker.writeMaster(data);
		}
		worker.writeMaster(Utils.W2M_KEY_COMPLETE);
		System.out.println("Keys transferred to Master");
	}
	
	public void sendResults() {
		// TODO send results to Master
	}
	
	public void stopExecution() {
		
	}
}
