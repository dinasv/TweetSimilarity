import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class Term implements WritableComparable<Term>{
	protected Text word;
	protected LongWritable numOfTweetsContainTerm;
	protected MapWritable tweetsMap;

	public Term() {
		word = null;
		numOfTweetsContainTerm=null;
		tweetsMap=null;
    }
	
	public Term(Text word){
		this.word = word;
		this.numOfTweetsContainTerm=new LongWritable(0);
		tweetsMap = new MapWritable();
	}
	
	public Term(Term other){
		word = new Text(other.getWord().toString());
		numOfTweetsContainTerm = new LongWritable(other.getNumOfTweetsContainTerm());
		tweetsMap = new MapWritable(other.getTweetsMap());
	}
	/*public void updateVectorLength(LongWritable id, double num){
		if(tweetsMap.containsKey(id)){
			Tweet t = (Tweet) tweetsMap.get(id);
			t.setVectorLength(num);
			tweetsMap.put(id, t);
		}
	}*/
	public void setNumOfTweetsContainTerm(long num){
		numOfTweetsContainTerm.set(num);
	}
	public void putTweet(String id, Tweet tweet){
		//if(!tweetsMap.containsKey(tweet.getKey().getId())){
			//numOfTweetsContainTerm.set(numOfTweetsContainTerm.get()+1);
		//Tweet t = new Tweet(tweet);
		tweetsMap.put(new Text(id), tweet);
		//}
	}
	public Tweet getTweet(long id){
		return (Tweet)tweetsMap.get(new LongWritable(id));
	}
	public Text getWord(){
		return word;
	}
	public MapWritable getTweetsMap(){
		return tweetsMap;
	}
	public long getNumOfTweetsContainTerm(){
		return numOfTweetsContainTerm.get();
	}

	@Override
    public void readFields(DataInput in) throws IOException {
		word = new Text();
		//tf = new DoubleWritable();
		word.readFields(in);
		numOfTweetsContainTerm = new LongWritable();
		numOfTweetsContainTerm.readFields(in);
		tweetsMap = new MapWritable();
		tweetsMap.readFields(in);
        
    }
    @Override
    public void write(DataOutput out) throws IOException {
    	word.write(out);
    	numOfTweetsContainTerm.write(out);
    	tweetsMap.write(out);

    }
    
    @Override
    public int compareTo(Term other) {
        return (int) (word.toString().compareTo(other.word.toString()));
    }
    
    @Override
    public String toString() {
        return word.toString();
    }
}
