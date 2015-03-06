
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3 {
	
	public static class Mapper3 extends Mapper<Term, Tweet, Term, Tweet>{
		public void map(Term key, Tweet value, Context context
	            ) throws IOException, InterruptedException {
			
			context.write(key, value);
	
		}
	}
		

	public static class Reducer3 extends Reducer<Term, Tweet, Tweet, Term> {
	
		public void reduce(Term key, Iterable<Tweet> values,
		               Context context
		               ) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			ArrayList<Tweet> tweetsArray = new ArrayList<Tweet>();
			
			Iterator<Tweet> it = values.iterator();
			Term term = WritableUtils.clone(key, conf);
			//term.getNumOfTweetsContainTerm()
			int counter=0;
	    	while(it.hasNext()){
	    		counter ++;
	    		Tweet tweet = WritableUtils.clone(it.next(), conf);
	    		String id = tweet.getKey().getId();
	    		term.putTweet(id, tweet);
				tweetsArray.add(tweet); 		
				
				if (counter >= 11){
					break;
				}
	    	}
			for (Tweet tweet : tweetsArray){

			    context.write(tweet, term);	
			}
			
		}
	
	}
	
	
}
