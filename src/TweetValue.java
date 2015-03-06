import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TweetValue implements Writable {
	
	protected String user;
	protected String text;
	protected boolean favorited;
	protected boolean retweeted;
    
    
    public TweetValue() {
    	user = "";
    	text = "";
    	favorited=false;
    	retweeted =false;
    }
    
    public TweetValue(String user, String text, boolean favorited, boolean retweeted) {
       this.user = user;
       this.text = text;
       this.favorited = favorited; 
       this.retweeted = retweeted;
    }
    public TweetValue(TweetValue other){
    	user = other.getUser();
    	text = other.getText();
    	favorited = other.favorited;
    	retweeted = other.retweeted;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    	
    	user = in.readUTF();
    	text = in.readUTF();
    	favorited = in.readBoolean();
    	retweeted = in.readBoolean();

    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(user);
    	out.writeUTF(text);
        out.writeBoolean(favorited);
        out.writeBoolean(retweeted);

    }
    
    public String getUser() {
        return user;
    }
    
    public String getText() {
        return text;
    }
    
    public boolean getFavorited() {
        return favorited;
    }
    
    public boolean getRetweeted() {
        return retweeted;
    }
}
