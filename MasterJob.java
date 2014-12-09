package mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MasterJob<K extends Serializable, V extends Serializable> {

	protected Mapper<K, V> currentJob;
	protected HashMap<K, V> results;
	protected HashMap<K, Integer> keyCounts; 
	protected Master master;
	//Map b/w key and list of Worker Ids it came from
	protected Map<K, List<Integer>> key_workers_map; 
	protected int wCount = 0, wDones = 0; //to keep track of number of workers who have sent keys
	protected int wShuffleCount = 0; //keeps track of # of workers who finished mapping
	// Map b/w WorkerId (Integer) and List of Transfer Messages for this workerId 
	// i.e List<<Key, AddressOfWorkerPeer>>
	protected Map<Integer, List<Object[]>> worker_messages_map; 
	
	
	public MasterJob(Mapper<K, V> mr, Master master) {
		this.master = master;
		currentJob = mr;
		keyCounts = new HashMap<>();
		key_workers_map = new HashMap<>();
		worker_messages_map = new HashMap<>();
		results = new HashMap<>();
	}

	public void storeKeyToWorker(K key, int workerID) {
		//aggregate key_workers_map
		if(key_workers_map.containsKey(key))
			key_workers_map.get(key).add(workerID); // append worker ID its corresponding key mapping
		else {
			List<Integer> l = new ArrayList<>();
			l.add(workerID);
			key_workers_map.put(key, l);
		}
	}
	
	public void aggregateKeyCounts(K key, int count) {
		if (keyCounts.containsKey(key)) 
			keyCounts.put(key, keyCounts.get(key)+count);
		else
			keyCounts.put(key, count);
	}

	@SuppressWarnings("unchecked")
	public synchronized void receiveWorkerKey(InputStream in, int id) {
		/*
		byte[] bInt = new byte[4]; 
		byte[] keyArr = new byte[barr.length-4]; //subtract last 4 for integer
		System.arraycopy(barr, 0, keyArr, 0, barr.length-4);
		System.arraycopy(barr, barr.length-4, bInt, 0, 4);
		K key = currentJob.readBytes(keyArr);
		aggregate(key, Utils.byteArrayToInt(bInt)); 
		storeKeyToWorker(key, id);
		*/
		try {
			ObjectInputStream objInStream = new ObjectInputStream(in);
			Object[] o = (Object[]) objInStream.readObject();		
			K key = (K) o[0];
			Integer size = (Integer) o[1];
			aggregateKeyCounts(key, size); 
			storeKeyToWorker(key, id);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public void receiveWorkerResults(InputStream in) {
		try {
			ObjectInputStream objInStream = new ObjectInputStream(in);
			Object[] o = (Object[]) objInStream.readObject();		
			K k = (K) o[0];
			V v = (V) o[1];
			results.put(k, v);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized void coordinateKeysOnWorkers(){
		int numOfKs = key_workers_map.keySet().size();
		int numOfWs = master.workerQueue.size();
		int incr = 1;
		if (numOfKs > numOfWs) {
			incr = (numOfKs % numOfWs == 0) ? (numOfKs / numOfWs) : ((numOfKs / numOfWs)+1); 
		}
		int kIdx = 0, wQIdx = 0;// WorkerConnection currWoker = master.workerQueue.get(0); 
		for (K key : key_workers_map.keySet()){
			Object[] transferMessage = new Object[]{key,  //contains key, ipaddress and port to send 
					master.workerQueue.get(wQIdx).clientSocket.getInetAddress().getHostAddress(),
					master.workerIDToPort.get(master.workerQueue.get(wQIdx).id)}; 
			for (Integer wId : key_workers_map.get(key)){
				if (worker_messages_map.containsKey(wId)) {
					worker_messages_map.get(wId).add(transferMessage);
				} 
				else {
					List<Object[]> messages = new ArrayList<Object[]>();
					messages.add(transferMessage);
					worker_messages_map.put(wId, messages);
				}
			}
			
			if ((++kIdx) % incr == 0){
				++wQIdx;
			}
		} //worker_messages_map populated
		/*
		 * for (WorkerConnection wc : master.workerQueue){
			wc.writeWorker(Utils.M2W_COORD_KEYS);
			if (worker_messages_map.get(wc.id) == null) 
				worker_messages_map.put(wc.id, new ArrayList<Object[]>());
			else 
				wc.writeObjToWorker(worker_messages_map.get(wc.id));
		}
		 */
		for (Map.Entry<Integer, List<Object[]>> entry : worker_messages_map.entrySet()) {
			WorkerConnection wc = master.getWCwithId(entry.getKey());
			Utils.write(wc.out, Utils.M2W_COORD_KEYS, entry.getValue());
		}
	}

	public void printResults() {
		System.out.println("***Final Results***");
		for (K key: results.keySet()) {
			System.out.println("Key: " + key + " Value: " + results.get(key));
		}
		System.out.print("> ");
	}
}
