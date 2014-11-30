package mapreduce;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public abstract class Mapper<K extends Serializable, V> {
	
	private Job<K, V> job;
	
	public abstract HashMap<K, V> map(File resource);
	
	public abstract V reduce(K key, List<V> listOfValues);
	
	public abstract byte[] getBytes(K key);
	
	public abstract K readBytes(byte[] barr);

	public void emit(K key, V value) {
		job.emit(key, value);
	}
	
	@SuppressWarnings("unchecked")
	public void setJob(Job<?, ?> job) {
		this.job = (Job<K, V>) job;
	}
}
