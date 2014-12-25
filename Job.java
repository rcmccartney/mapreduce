package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Job<K extends Serializable, 
				 IV extends Serializable,
				 OV extends Serializable> {

	protected Worker worker;
	protected Mapper<K, IV, OV> mr;
	protected Map<K, List<IV>> mapOutput;
	protected Map<K, OV> finalOut;
	protected List<String> files;
	protected int jobID;
	
	public Job(int jobID, Worker worker, Mapper<K, IV, OV> mr, List<String> data) {
		this.jobID = jobID;
		this.worker = worker;
		this.mr = mr;
		this.mr.setJob(this);  // allows user to call emit
		this.files = data;
		mapOutput = new ConcurrentHashMap<>();
		finalOut = new ConcurrentHashMap<>();
	}
	
	////////////////////////////////////////////////
	//
	// Map phase of map-reduce on this worker node
	//
	///////////////////////////////////////////////
	
	public void begin(String basePath) throws IOException {
		
		// since this is reading from a file don't do it in parallel,
		// speed limit is reading from disk
		for (final String filename : files) {
			// convenience function provided if user doesn't want to call 'emit'
			emit(mr.map(new File(basePath + File.separator + filename)));
		}
		// now the output map has been populated, so it needs to be shuffled and sorted 
		// first notify Master of the keys you have at this node, and their sizes
		sendKeysToMaster();
	}
	
	public void emit(K key, IV value) {
		if (mapOutput.containsKey(key)) 
			mapOutput.get(key).add(value);
		else {
			List<IV> l = new ArrayList<>();
			l.add(value);
			mapOutput.put(key, l);
		}
	}
	
	public void emit(HashMap<K, IV> tmp) {
		if (tmp == null)
			return;
		for(K key: tmp.keySet()) 
			emit(key, tmp.get(key));
	}
	
	public void sendKeysToMaster() throws IOException {
		for (K key: mapOutput.keySet()) {
			Utils.writeCommand(worker.out, Utils.W2M_KEY, jobID);
			Utils.writeObject(worker.out, new Object[]{key, mapOutput.get(key).size()});
			worker.in.read();  // wait for ACK
		}
		Utils.writeCommand(worker.out, Utils.W2M_KEY_COMPLETE, jobID);
	}
	
	////////////////////////////////////////////////
	//
	// Shuffle and sort phase of map-reduce on this worker node
	//
	///////////////////////////////////////////////

	@SuppressWarnings("unchecked")
	public void receiveKeyAssignments() {

		try {
			ObjectInputStream objInStream = new ObjectInputStream(worker.in);
			List<Object[]> keyTransferMsg = (List<Object[]>) objInStream.readObject();
			for (Object[] o : keyTransferMsg) {  // each object is { K, ipaddr, port} for where to send K
				K k = (K) o[0];
				String peerAddress = (String) o[1]; 
				Integer peerPort = (Integer) o[2]; 
				//so that only keys assigned to this worker are left in mapOutput
				List<IV> v = mapOutput.remove(k); 
				worker.wP2P.send(k, v, jobID, peerAddress, peerPort); //sends key and its value list as object[]
			}
			//A worker sends this message, so that master can keep track of workers who are ready for reduce
			Utils.writeCommand(worker.out, Utils.W2M_KEYSHUFFLED, jobID);   
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void receiveKV(Object k, Object v){
		K key = (K) k; 
		List<IV> valList = (List<IV>) v;
		if (mapOutput.containsKey(key))
			mapOutput.get(key).addAll(valList);
		else
			mapOutput.put(key, valList);
	}
	
	////////////////////////////////////////////////
	//
	// Reduce phase of map-reduce on this worker node
	//
	///////////////////////////////////////////////

	public void reduce() throws IOException {
		List<Thread> thrs = new ArrayList<>();
		for(final K key: mapOutput.keySet()) {
			thrs.add(new Thread(new Runnable() {
				public void run() {
					finalOut.put(key, mr.reduce(key, mapOutput.get(key)));
				}
			}));
			thrs.get(thrs.size()-1).start();
		}
		// wait for all the threads to finish
		for(Thread t: thrs) {
			try { 
				t.join(); 
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//finalOut holds the results of this MR job, send it to Master
		sendResults();
	}
	
	public void sendResults() throws IOException {
		for (Map.Entry<K, OV> e : finalOut.entrySet()) {
			Utils.writeCommand(worker.out, Utils.W2M_RESULTS, jobID);
			Utils.writeObject(worker.out, new Object[]{e.getKey(), e.getValue()});
			worker.in.read();  // wait for ACK
		}
		Utils.writeCommand(worker.out, Utils.W2M_JOBDONE, jobID);
	}
	
	public void stopExecution() {
		
	}
}
