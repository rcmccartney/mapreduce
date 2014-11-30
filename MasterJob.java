package mapreduce;

public class MasterJob<K, V> {

	public MasterJob(Mapper<K, V> mr) {
		System.out.println("here");
	}

}
