import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.json.JSONException;
import org.json.JSONObject;

public class TweetsRecordReader extends RecordReader<TweetKey,TweetValue>{
	 LineRecordReader reader;
	    
	 TweetsRecordReader() {
        reader = new LineRecordReader(); 
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        reader.initialize(split, context);
    }
 
 
    @Override
    public void close() throws IOException {
        reader.close();        
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
    }
    
    @Override
    public TweetKey getCurrentKey() throws IOException, InterruptedException {
    	JSONObject tweetJson = new JSONObject(reader.getCurrentValue().toString());
    	String id = tweetJson.getString("id_str");
    	Date date = null;
		try {
			date = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy").parse(tweetJson.getString("created_at"));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return new TweetKey(id, date);

    }
    
    @Override
    public TweetValue getCurrentValue() throws IOException, InterruptedException {
      
    	JSONObject tweetJson = new JSONObject(reader.getCurrentValue().toString());
    	JSONObject user = (JSONObject) tweetJson.get("user");
	    String userName = user.getString("name");
    	String text = tweetJson.getString("text");
    	boolean favorited = tweetJson.getBoolean("favorited");
    	boolean retweeted = tweetJson.getBoolean("retweeted");
    	
        return new TweetValue(userName, text, favorited, retweeted);
     
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

}
