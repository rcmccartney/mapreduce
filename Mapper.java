package mapreduce;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public interface Mapper<K, V> {
	
	public HashMap<K, V> map(File resource);
	
	public V reduce(K key, List<V> listOfValues);

	public K parse(String rep);
	
}
