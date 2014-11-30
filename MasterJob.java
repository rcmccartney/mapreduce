package mapreduce;

import java.util.HashMap;

public class MasterJob<K, V> {

	protected Mapper<K, V> currentJob;
	protected HashMap<K, Integer> keyCounts; 
	protected HashMap<Integer, K> keyWorker; 

	
	public MasterJob(Mapper<K, V> mr) {
		currentJob = mr;
		keyCounts = new HashMap<>();
		keyWorker = new HashMap<>();
	}

	public synchronized void receiveWorkerKey(byte[] barr, int id) {
		byte[] bInt = new byte[4];
		byte[] keyArr = new byte[barr.length-4]; //subtract last 4 for integer
		System.arraycopy(barr, 0, keyArr, 0, barr.length-4);
		System.arraycopy(barr, barr.length-4, bInt, 0, 4);
		K key = currentJob.readBytes(keyArr);
		aggregate(key, Utils.byteArrayToInt(bInt)); 
		keyWorker.put(id, key);
	}
	
	public void aggregate(K key, int count) {
		if (keyCounts.containsKey(key)) 
			keyCounts.put(key, keyCounts.get(key)+count);
		else
			keyCounts.put(key, count);
	}

	public void setKeyTransferComplete(int id) {
		for(K key: keyCounts.keySet()) 
			System.out.println("Key: " + key + " Value: " + keyCounts.get(key));
	}

	public void receiveWorkerResults(byte[] barr) {
		// TODO Auto-generated method stub
		
	}
}
