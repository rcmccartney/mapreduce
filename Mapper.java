package mapreduce;

import java.io.File;
import java.util.List;

public interface Mapper<key extends Comparable<?>, value> {
	
	public void reduce(key k, List<value> listOfValues);

	public void map(File resource);
	
	public key parse(String rep);
	
}
