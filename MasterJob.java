package mapreduce;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MasterJob<K extends Serializable, V> {

	protected Mapper<K, V> currentJob;
	protected HashMap<K, Integer> keyCounts; 
	protected Master master;
	//Map b/w key and list of Worker Ids it came from
	protected Map<K, List<Integer>> key_workers_map; 
	protected int wCount = 0; //to keep track of number of workers who have sent keys
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
	}

	public synchronized void receiveWorkerKey(byte[] barr, int id) {
		byte[] bInt = new byte[4];
		byte[] keyArr = new byte[barr.length-4]; //subtract last 4 for integer
		System.arraycopy(barr, 0, keyArr, 0, barr.length-4);
		System.arraycopy(barr, barr.length-4, bInt, 0, 4);
		K key = currentJob.readBytes(keyArr);
		aggregate(key, Utils.byteArrayToInt(bInt)); 
		
		//aggregate key_workers_map
		if(key_workers_map.containsKey(key))
			key_workers_map.get(key).add(id); // append worker ID its corresponding key mapping
		else 
			key_workers_map.put(key, Arrays.asList(id));
	}
	
	public void aggregate(K key, int count) {
		if (keyCounts.containsKey(key)) 
			keyCounts.put(key, keyCounts.get(key)+count);
		else
			keyCounts.put(key, count);
		System.out.println("AGGREGATE: ");
		for (K a : keyCounts.keySet())
			System.out.println("key: " + a + " Value " + keyCounts.get(a));
	}

	public void setKeyTransferComplete(int id) {
		
		//TODO: change wCount to compare with current actual # of workers on this job 
		++wCount;
		if (wCount == master.workerQueue.size()) { // master now has all the keys from the workers
			coordinateKeysOnWorkers();
		}
	}

	public void receiveWorkerResults(byte[] barr) {
		// TODO Auto-generated method stub
		
	}
	
	public synchronized void coordinateKeysOnWorkers(){
		System.out.println("Coodinate Keys called");
		int numOfKs = key_workers_map.keySet().size();
		int numOfWs = master.workerQueue.size();
		int incr = 1;
		if (numOfKs > numOfWs) {
			incr = (numOfKs % numOfWs == 0) ? (numOfKs / numOfWs) : ((numOfKs / numOfWs)+1); 
		}
		int kIdx = 0, wQIdx = 0;// WorkerConnection currWoker = master.workerQueue.get(0); 
		for (K key : key_workers_map.keySet()){
			Object[] transferMessage = new Object[]
					{key,  //contains key, ipaddress and port to send the key to
					master.workerQueue.get(wQIdx).clientSocket.getInetAddress().getHostAddress(),
					master.workerQueue.get(wQIdx).clientSocket.getPort()}; //TODO: change to wp2p port
			for (Integer wId : key_workers_map.get(key)){
				if(wId == master.workerQueue.get(wQIdx).id)//if its the worker to which the key's assigned, then skip
					continue;
				if(worker_messages_map.containsKey(wId)){
					worker_messages_map.get(wId).add(transferMessage);
				} else {
					List<Object[]> messages = new ArrayList<Object[]>();
					messages.add(transferMessage);
					worker_messages_map.put(wId, messages);
				}
			}
			
			if ((++kIdx) % incr == 0){
				++wQIdx;
			}
		} //worker_messages_map populated
		for (Map.Entry<Integer, List<Object[]>> entry : worker_messages_map.entrySet()){
			WorkerConnection wc = master.getWCwithId(entry.getKey());
			wc.writeWorker(Utils.M2W_COORD_KEYS);
			wc.writeObjToWorker(entry.getValue());
		}
	}

}
