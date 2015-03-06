
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

//import org.apache.logging.log4j.core.util.FileUtils;


public class Job1 {

	public static class Mapper1 extends Mapper<TweetKey, TweetValue, Text, Tweet>{
		
		public static final String HDFS_STOPWORD_LIST = "/data/StopWords.txt";
		protected HashSet<String> stopWords;
		
		@Override
		protected void setup(Context context) throws IOException,
        InterruptedException {
			 
		/*	
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				 URI mappingFileUri = context.getCacheFiles()[0];
				 loadStopWords(mappingFileUri);
			}
			 */
		}
		
		

		  /*void loadStopWords(URI cachePath) throws IOException {
		    // note use of regular java.io methods here - this is a local file now
		    BufferedReader wordReader = new BufferedReader(
		        new FileReader(cachePath.toString()));
		    try {
		      String line;
		      this.stopWords = new HashSet<String>();
		      while ((line = wordReader.readLine()) != null) {
		        this.stopWords.add(line);
		      }
		    } finally {
		      wordReader.close();
		    }
		  }*/
		

		
	    public void map(TweetKey key, TweetValue value, Context context
	                    ) throws IOException, InterruptedException {
	   
	      StringTokenizer itr = new StringTokenizer(value.getText());
	      
	      HashMap<String, Integer> countTerms = new HashMap<String, Integer>();
	      ArrayList<String> listOfTerms = new ArrayList<String>();

	      Tweet tweet = new Tweet(key, value);
	      
	      //count how many time term appears in the tweet
	      while (itr.hasMoreTokens()) {
	    	String w = itr.nextToken();
	    	if(!StopWords.contains(w)){
		    	if(countTerms.containsKey(w)){
		    		countTerms.put(w, new Integer(countTerms.get(w).intValue()+1));
		    	}else{
		    		//Text word = new Text(w);
		    		listOfTerms.add(w);
		    		countTerms.put(w, new Integer(1));
		    	}       
	    	}
	      }
	      
	      int numOfTerms = listOfTerms.size();
	      //calculate tf and add to list of terms in tweet
	      for (String word : listOfTerms) {
	    	Text w = new Text(word);
	    	int termCount = countTerms.get(w.toString());
	      	tweet.putTerm(w, new DoubleWritable((double)termCount/(double)numOfTerms));	      	
	      }
	      for (String word : listOfTerms) {
	    	  Text w = new Text(word);
	    	  context.write(w, tweet);
	      }
	      context.getCounter(TweetCount.NUM_OF_TWEETS).increment(1);
	    }
	}
	
	public static class Reducer1 extends Reducer<Text, Tweet, Tweet, Term> {
		
	    public void reduce(Text key, Iterable<Tweet> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	
	    	ArrayList<Tweet> tweetsArray = new ArrayList<Tweet>();
	    	Configuration conf = context.getConfiguration();
	    	
	    	Term term = new Term(key);
	    	Iterator<Tweet> it = values.iterator();
	    	long sum = 0;
	    	while(it.hasNext()){
	    		sum ++;
	    		Tweet tweet = WritableUtils.clone(it.next(), conf);
		    	//term.putTweet(tweet);
		    	tweetsArray.add(tweet);    	 
		    }
	    	term.setNumOfTweetsContainTerm(sum);
	    	
		    for (Tweet tweet : tweetsArray){
		    	context.write(tweet, term);	
		    }

	    }

	}
	
}
