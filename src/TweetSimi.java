

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



	

public class TweetSimi extends Configured implements Tool {
	//public static final String LOCAL_STOPWORD_LIST = "/home/dina/workspace/dsps2/StopWords.txt";

	//public static final String HDFS_STOPWORD_LIST = "/data/StopWords.txt";
	
	//String CORPUS = "https://s3-us-west-1.amazonaws.com/dsp151/tweetCorpus.txt";
	static String CORPUS;
	static String folderName;
	String OUTPUT1 = "s3n://dinartur/"+folderName+"/output1";
	String OUTPUT2 = "s3n://dinartur/"+folderName+"/output2";
	String OUTPUT3 = "s3n://dinartur/"+folderName+"/output3";
	String OUTPUT4 = "s3n://dinartur/"+folderName+"/output4";

		  
	
	
	
		@Override
		 public int run(String[] args) throws Exception {
			
			int N = Integer.parseInt(args[3]);
			//JOB 1
			Configuration conf1 = getConf();//new Configuration();
			
			conf1.set("mapreduce.job.maps","10");
	        conf1.set("mapreduce.job.reduces","10");
			
			Job job1 = Job.getInstance(conf1, "job1");
			
			job1.setNumReduceTasks(10);
			//job1.addCacheFile(new Path("s3n://dinartur/dsps2/StopWords.txt").toUri());
			// Set the outputs for the Map
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Tweet.class);
			
			
		    job1.setJarByClass(TweetSimi.class);
		    job1.setMapperClass(Job1.Mapper1.class);
		    job1.setReducerClass(Job1.Reducer1.class);
		    
		    //set output for job
		    job1.setOutputKeyClass(Tweet.class);
		    job1.setOutputValueClass(Term.class);
		    
		    
		    job1.setInputFormatClass(TweetInputFormat.class);
		    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		    TweetInputFormat.addInputPath(job1, new Path(CORPUS));
		    
		    
		    FileOutputFormat.setOutputPath(job1, new Path(OUTPUT1));
		    
		    job1.waitForCompletion(true);
		    
		    System.out.println("JOB 1 completed");
		    
		    //JOB 2
		    Configuration conf2 = new Configuration();
		    conf2.set("mapreduce.job.maps","10");
	        conf2.set("mapreduce.job.reduces","10");
		    
		    long numOfTweets = job1.getCounters().findCounter(TweetCount.NUM_OF_TWEETS).getValue();
		    conf2.setLong("numOfTweets", numOfTweets);
			Job job2 = Job.getInstance(conf2, "job2");
			job2.setNumReduceTasks(10);
			
			job2.setJarByClass(TweetSimi.class);
		    job2.setMapperClass(Job2.Mapper2.class);
		    job2.setReducerClass(Job2.Reducer2.class);
			job2.setPartitionerClass(TweetTermPartitioner.class);
		    
		    job2.setMapOutputKeyClass(Tweet.class);
		    job2.setMapOutputValueClass(Term.class);
			// Set the outputs
			job2.setOutputKeyClass(Term.class);
			job2.setOutputValueClass(Tweet.class);
		    
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			
			FileInputFormat.addInputPath(job2, new Path(OUTPUT1));
		    
			FileOutputFormat.setOutputPath(job2, new Path(OUTPUT2));
			
			job2.waitForCompletion(true);
			
			System.out.println("Job 2 finished");
			//JOB 3////////////////////////////////////////
		    Configuration conf3 = new Configuration();
		    conf3.set("mapreduce.job.maps","10");
	        conf3.set("mapreduce.job.reduces","10");
		    
			Job job3 = Job.getInstance(conf3, "job3");
			job3.setNumReduceTasks(10);			
			
			job3.setJarByClass(TweetSimi.class);
			job3.setMapperClass(Job3.Mapper3.class);
			job3.setReducerClass(Job3.Reducer3.class);
			
			job3.setMapOutputKeyClass(Term.class);
			job3.setMapOutputValueClass(Tweet.class);
			// Set the outputs
			job3.setOutputKeyClass(Tweet.class);
			job3.setOutputValueClass(Term.class);

			job3.setInputFormatClass(SequenceFileInputFormat.class);
			job3.setOutputFormatClass(SequenceFileOutputFormat.class);
			//
			FileInputFormat.addInputPath(job3, new Path(OUTPUT2));
			
			FileOutputFormat.setOutputPath(job3,  new Path(OUTPUT3));
			
			job3.waitForCompletion(true);

			//JOB 4/////////////////////////////////////////////////////////////
		    Configuration conf4 = new Configuration();
		    conf4.set("mapreduce.job.maps","10");
	        conf4.set("mapreduce.job.reduces","10");
		    
		    conf4.setLong("numOfTweets", numOfTweets);
		    conf4.setInt("N", N);
		    
			Job job4 = Job.getInstance(conf4, "job4");
			job4.setNumReduceTasks(10);
			
			job4.setJarByClass(TweetSimi.class);
			job4.setMapperClass(Job4.Mapper4.class);
			job4.setReducerClass(Job4.Reducer4.class);
			
			job4.setMapOutputKeyClass(Tweet.class);
			job4.setMapOutputValueClass(Term.class);
			job4.setPartitionerClass(TweetTermPartitioner.class);
			// Set the outputs
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);
		    
			job4.setInputFormatClass(SequenceFileInputFormat.class);
			
			
			FileInputFormat.addInputPath(job4, new Path(OUTPUT3));
   
			FileOutputFormat.setOutputPath(job4, new Path(OUTPUT4));
			
		    if(job4.waitForCompletion(true)){	
		    	return 0;
		    }else{
		    	return 1;
		    }
		}
	
	
	public static void main(String[] args) throws Exception {
		CORPUS = args[1];
		folderName = args[2];
		
		 ToolRunner.run(new Configuration(), new TweetSimi(), args);	
	    
	    
	}
}
