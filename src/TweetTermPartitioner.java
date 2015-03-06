import org.apache.hadoop.mapreduce.Partitioner;


public class TweetTermPartitioner extends Partitioner<Tweet,Term> {
	@Override
	public int getPartition(Tweet key, Term value, int numReduceTasks) {
		//long id = Long.parseLong(key.getKey().getId());
		String id = key.getKey().getId();
		String lastChar = id.substring(id.length()-1);
		return (int)(Integer.parseInt(lastChar) % numReduceTasks);
	}

}