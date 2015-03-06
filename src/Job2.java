
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2 {
	
	public static class Mapper2 extends Mapper<Tweet, Term, Tweet, Term>{
		public void map(Tweet key, Term value, Context context
	            ) throws IOException, InterruptedException {
			
			
			
			context.write(key, value);
	
		}
	}
		

	public static class Reducer2 extends Reducer<Tweet, Term, Term, Tweet> {
	
		public void reduce(Tweet key, Iterable<Term> values,
		               Context context
		               ) throws IOException, InterruptedException {

			
			ArrayList<Term> terms = new ArrayList<Term>();
			
			Configuration conf = context.getConfiguration();
			long totalTweets = conf.getLong("numOfTweets", 1);
			//calculate vector length
			double vectorLength = 0;
			for (Term term : values){
				double termFreq = ((double)term.getNumOfTweetsContainTerm()/(double)totalTweets)* 100;
				double idf;
				double tfidf = 0;
				if(termFreq<1){
					idf = Math.log((double)totalTweets/(double)term.getNumOfTweetsContainTerm());
					tfidf = idf * key.getTermTf(term.getWord());
					
					terms.add(WritableUtils.clone(term, conf));
				}	 
				vectorLength += tfidf * tfidf;
				
				
			}
			vectorLength = Math.sqrt(vectorLength);
			key.setVectorLength(vectorLength);
			
			for (Term term : terms){
				//term.updateVectorLength(key.getKey().getId(), vectorLength);
				context.write(term, key);
			}
			
			
		}
	
	}
	
}
