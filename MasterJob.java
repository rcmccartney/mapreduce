package mapreduce;

import java.util.HashMap;

public class MasterJob<K, V> {

	protected Mapper<K, V> currentJob;
	protected HashMap<K, Integer> keyCounts; 
	protected HashMap<Integer, K> keyWorker; 

	
	public MasterJob(Mapper<K, V> mr) {
		keyCounts = new HashMap<>();
		keyWorker = new HashMap<>();
		System.out.println("HERE2 CONSTRUCT");
	}

	public void receiveWorkerKey(byte[] barr, int id) {
		System.out.println("HERE2");
		byte[] bInt = new byte[4];
		byte[] keyArr = new byte[barr.length-4]; //subtract last 4 for integer
		System.arraycopy(barr, 0, keyArr, 0, barr.length-4);
		System.arraycopy(barr, barr.length-4, bInt, 0, 4);
		K key = (K) currentJob.readBytes(keyArr);
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
		System.out.println(keyWorker.get(id));
		for( K key: keyCounts.keySet()) 
			System.out.println(key + ": " + keyCounts.get(key));
	}

	public void receiveWorkerResults(byte[] barr) {
		// TODO Auto-generated method stub
		
	}
}
