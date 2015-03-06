
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4 {
	
	public static class Mapper4 extends Mapper<Tweet, Term, Tweet, Term>{
		public void map(Tweet key, Term value, Context context
	            ) throws IOException, InterruptedException {
			
			context.write(key, value);
	
		}
	}
		

	public static class Reducer4 extends Reducer<Tweet, Term, Text, Text> {
	
		public void reduce(Tweet key, Iterable<Term> values,
		               Context context
		               ) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			long totalTweets = conf.getLong("numOfTweets", 1);
			
			Tweet tweet1 = WritableUtils.clone(key, conf);
			
			ArrayList<Term> terms = new ArrayList<Term>();
			HashMap<String, Tweet> tweets = new HashMap<String, Tweet>();
			
			Collection<Writable> tempTweets;
			//go over the terms in tweet1
			for (Term t : values){
				Term term = WritableUtils.clone(t, conf);
				
				double idf = Math.log((double)totalTweets/(double)term.getNumOfTweetsContainTerm());
				if(idf!=0){
					tempTweets = term.getTweetsMap().values();
					
					//context.write(new Text("term  "+ term.toString()), new Text("tweets:  "+ tempTweets.toString()));
					//find all tweets that have at least 1 term in common with tweet1
					for (Writable tweet : tempTweets){
						
						Tweet tweet2 =	WritableUtils.clone((Tweet)tweet, conf);
						
						if(tweet1.compareTo(tweet2)!= 0){
							tweets.put(tweet2.getKey().getId(), tweet2);
						}
					}
	
					terms.add(WritableUtils.clone(term, conf));
				}
			}
			
			
			
			CosineComparator comp = new CosineComparator();
			PriorityQueue<Cosine> cosineQueue = new PriorityQueue<Cosine>(10, comp);
			Collection<Tweet> tweetsFromMap = tweets.values();
			//go over tweets that have common term with tweet1
			double AxA = tweet1.getVectorLength();
			for (Tweet tweet2 : tweetsFromMap){	
				double AxB = 0;				
				double BxB = tweet2.getVectorLength();
				//find common terms and calculate tfidf
				for (Term t : terms){
					Term term = WritableUtils.clone(t, conf);
					double idf = Math.log((double)totalTweets/(double)term.getNumOfTweetsContainTerm());
					double tfidf1 = idf * tweet1.getTermTf(term.getWord());
					double tf2 = tweet2.getTermTf(term.getWord());
					
					//if the term is in tweet2
					if(tf2 != -1){
						double tfidf2 = idf * tf2;
						AxB += tfidf1 * tfidf2;	
						//context.write(new Text("tweet1: "+tweet1.getValue().getText() + " tweet2: "+tweet2.getValue().getText()), new Text("tfidf2 : " +Double.toString(tfidf2) + "  AxB : " +Double.toString(AxB) + "  AxA : " +Double.toString(AxA)+ " BxB : " +Double.toString(BxB) ) );
					}
				}
				//cosine similarity between tweet1 and tweet2
				double cosine = AxB/(AxA*BxB);
				
				Cosine cosineObj = new Cosine(new Double(cosine), tweet2);
				cosineQueue.add(cosineObj);
			}
			
			int N = conf.getInt("N", 10);
			ArrayList<Cosine> topN = new ArrayList<Cosine>();
			for (int i = 0; i < N ; i++) {
				Cosine topTweet = cosineQueue.poll();
				if(topTweet == null){
					break;
				}
				topN.add(topTweet);
			}
			
			context.write(new Text("Tweet id: " + tweet1.getKey().getId() +" created_at: "+tweet1.getKey().getCreatedAt().toString()+" favorited: "+Boolean.toString(tweet1.getValue().favorited)+" retweeted: "+Boolean.toString(tweet1.getValue().retweeted)+" Text: " + tweet1.getValue().getText()), new Text(topN.toString()));
			
		}
	
	}
	
	public static class CosineComparator implements Comparator<Cosine>
	{
	    @Override
	    public int compare(Cosine x, Cosine y)
	    { 	
	        if (x.getCosine().doubleValue() > y.getCosine().doubleValue())
	        {
	            return -1;
	        }
	        if (x.getCosine().doubleValue() < y.getCosine().doubleValue())
	        {
	            return 1;
	        }
	        return 0;
	    }
	}
}
