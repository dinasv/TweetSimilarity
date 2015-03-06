import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;


public class Tweet implements WritableComparable<Tweet>{
	protected TweetKey key;
	protected TweetValue value;
	protected MapWritable termsTf;
	protected DoubleWritable vectorLength;
	
	public Tweet() {
		key = null;
		value = null;
		termsTf = new MapWritable();
		vectorLength = new DoubleWritable();
    }
	public Tweet(Tweet other){
		key = new TweetKey(other.getKey());
		value = new TweetValue(other.getValue());
		termsTf = new MapWritable();
		termsTf.putAll(other.getTermsTf());
		vectorLength = new DoubleWritable(other.getVectorLength());
	}
	public Tweet(TweetKey key, TweetValue value){
		this.key = key;
		this.value = value;
		termsTf = new MapWritable();
		vectorLength = new DoubleWritable();
	}
	public void setVectorLength(double num){
		vectorLength.set(num);
	}
	public double getVectorLength(){
		return vectorLength.get();
	}

	public MapWritable getTermsTf(){
		return termsTf;
	}
	public void putTerm(Text term, DoubleWritable tf){
		termsTf.put(term, tf);
	}
	
	public TweetKey getKey(){
		return key;
	}
	
	public TweetValue getValue(){
		return value;
	}
	
	public double getTermTf(Text term){
		DoubleWritable tf = (DoubleWritable) termsTf.get(term);
		if(tf!=null){
			return tf.get();
		}
		return -1;
	}
	
    @Override
    public void readFields(DataInput in) throws IOException {
    	key = new TweetKey();
    	key.readFields(in);
    	value = new TweetValue();
    	value.readFields(in);
    	termsTf = new MapWritable();
    	termsTf.readFields(in);
    	vectorLength = new DoubleWritable();
    	vectorLength.readFields(in);
        
    }
    @Override
    public void write(DataOutput out) throws IOException {
    	key.write(out);
    	value.write(out);
    	termsTf.write(out);
    	vectorLength.write(out);
    }
    
    @Override
    public int compareTo(Tweet other) {
    	return key.compareTo(other.getKey());
        //return (key.getId().get() - other.key.id.get());
    }
    
    @Override
    public String toString() {
        return value.text;
    }
}
