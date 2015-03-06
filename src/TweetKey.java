import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TweetKey implements WritableComparable<TweetKey>{
	protected String id;
    protected Date created_at;
    
    public TweetKey() {
        id = new String();
        created_at = null;
    }
    
    public TweetKey(String id, Date created_at) {
        this.id = new String(id);
        this.created_at = created_at;
    }
    public TweetKey(TweetKey other){
    	id = new String(other.getId());
    	created_at = other.getCreatedAt();
    }

 
    public String getId() { return id; }
    public Date getCreatedAt() { return created_at; }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        created_at = new Date(in.readLong());
        
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeLong(created_at.getTime());
    }
    
    @Override
    public int compareTo(TweetKey other) {
    	return id.compareTo(other.getId());
    }
	
	
}
