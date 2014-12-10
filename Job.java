package mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Job<K extends Serializable, V extends Serializable> {

	protected Worker worker;
	protected Mapper<K, V> mr;
	protected HashMap<K, List<V>> mapOutput;
	protected HashMap<K, V> finalOut;
	protected List<String> files;
	
	public Job(Worker worker, Mapper<K, V> mr, List<String> data) {
		this.worker = worker;
		this.mr = mr;
		this.mr.setJob(this);
		files = data;
		mapOutput = new HashMap<>();
		finalOut = new HashMap<>();
	}
	
	public void begin() throws IOException {
		
		// since this is reading from a file don't do it in parallel,
		// speed limit is reading from disk
		for (final String filename : files) {
			// convenience function provided if user doesn't want to call 'emit'
			emit(mr.map(new File(worker.basePath + File.separator + filename)));
		}
		// now the output map has been populated, so it needs to be shuffled and sorted 
		// first notify Master of the keys you have at this node, and their sizes
		sendAllKeysToMaster();
	}
	
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
	
	@SuppressWarnings("unchecked")
	public void receiveKVAndAggregate(Object k, Object v){
		//wMinusOneCount++;
		K key = (K)k; 
		List<V> valList = (List<V>) v;
		if(mapOutput.containsKey(key)){
			mapOutput.get(key).addAll(valList);
		} else {
			mapOutput.put(key, valList);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void receiveKeyAssignments(InputStream in) {
		// TODO bug that the socket hangs if it is given zero files to read
		// since the worker is not assigned any Keys by the master
		try {
			ObjectInputStream objInStream = new ObjectInputStream(in);
			List<Object[]> keyTransferMsgs = (List<Object[]>) objInStream.readObject();
			for (Object[] o : keyTransferMsgs) {
				K k = (K) o[0];
				String peerAddress = (String) o[1]; 
				Integer peerPort = (Integer) o[2]; 
				if (!(worker.wP2P.equals(peerAddress, peerPort))) {
					List<V> v = mapOutput.get(k);
					mapOutput.remove(k); //so that only keys assigned to this worker are left in mapOutput
					worker.wP2P.send(k, v, peerAddress, peerPort); //sends key and its value list as object[]
				}
			}
			//A worker sends this message, so that master can keep track of workers who are ready for reduce
			Utils.write(worker.out, Utils.W2M_KEYSHUFFLED);   
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void sendAllKeysToMaster() throws IOException {
		//byte[] data;
		for (K key: mapOutput.keySet()) {
			Utils.write(worker.out, Utils.W2M_KEY, new Object[]{key, mapOutput.get(key).size()});
			worker.in.read();  // wait for AWK
			//data = Utils.concat(mr.getBytes(key), 
			//		Utils.intToByteArray(mapOutput.get(key).size()));
			//worker.writeMaster(data);
			// otherwise data buffer reads into next message
			//try { Thread.sleep(10); } catch (Exception e) {}
		}
		Utils.write(worker.out, Utils.W2M_KEY_COMPLETE);
	}
	
	public void sendResults() throws IOException {
		for (Map.Entry<K, V> e : finalOut.entrySet()) {
			Utils.write(worker.out, Utils.W2M_RESULTS, new Object[]{e.getKey(), e.getValue()});
			worker.in.read();  // wait for AWK
		}
		Utils.write(worker.out, Utils.W2M_JOBDONE);
	}
	
	public void stopExecution() {
		
	}
}
