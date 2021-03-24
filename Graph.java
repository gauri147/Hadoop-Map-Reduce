import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Graph {
	public static class CountMapper extends Mapper<Object,Text,IntWritable,IntWritable> {
		@Override
		public void map( Object key, Text value, Context context) throws IOException, InterruptedException {
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int val1 = s.nextInt();
			int val2 =s.nextInt();
			context.write(new IntWritable(val1), new IntWritable(val2));
			s.close();
		}
		
	}
    public static class CountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	   @Override
	   public void reduce(IntWritable key,Iterable<IntWritable> values,Context context)
	        throws IOException,InterruptedException {
		   int count = 0;
		   for (IntWritable v: values)
		   {
			   count++;
		   };
		   context.write(key,new IntWritable(count));
	        }
    }
	public static class GroupMapper extends Mapper<IntWritable,IntWritable,IntWritable,IntWritable> {
	    @Override
	    public void map(IntWritable key,IntWritable value,Context context) throws IOException, InterruptedException
	    {
		 	Scanner s = new Scanner(value.toString()).useDelimiter(",");
			int neighbours = s.nextInt();
			context.write(new IntWritable(neighbours),new IntWritable(1));
			s.close();

	    }
    }
    public static class GroupReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	    @Override
	    public void reduce(IntWritable key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
	    {
		   int count = 0;
		   for(IntWritable v:values)
		   {
			   count++;
		   };
		   context.write(key,new IntWritable(count));
	    }
	}  
    public static void main ( String[] args ) throws Exception {
    	String tempDirectory = "/output/temp";
    	
    	//Job1 - To count the neighbour for every node.
    	Job Countjob = Job.getInstance();
    	Countjob.setJobName("Countjob");
    	Countjob.setJarByClass(Graph.class);
	    
    	Countjob.setOutputKeyClass(IntWritable.class);
    	Countjob.setOutputValueClass(IntWritable.class);
	    
    	Countjob.setMapOutputKeyClass(IntWritable.class);
    	Countjob.setMapOutputValueClass(IntWritable.class);
	    
    	Countjob.setMapperClass(CountMapper.class);
    	Countjob.setReducerClass(CountReducer.class);
	    
    	Countjob.setInputFormatClass(TextInputFormat.class);
    	Countjob.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(Countjob,new Path(args[0]));
	    FileOutputFormat.setOutputPath(Countjob,new Path("temp"));
	    boolean jobsuccess=Countjob.waitForCompletion(true);
	    System.out.println("Count Job Completed.....Starting Second Job");
	    
	    if(jobsuccess)
	    {
	    	//Job2 - Group by the number of neighbours
		    Job GroupByjob = Job.getInstance();
		    GroupByjob.setJobName("GroupByjob");
		    GroupByjob.setJarByClass(Graph.class);
		    
		    GroupByjob.setOutputKeyClass(IntWritable.class);
		    GroupByjob.setOutputValueClass(IntWritable.class);
		    
		    GroupByjob.setMapOutputKeyClass(IntWritable.class);
		    GroupByjob.setMapOutputValueClass(IntWritable.class);
		    
		    GroupByjob.setMapperClass(GroupMapper.class);
		    GroupByjob.setReducerClass(GroupReducer.class);
		    
		    GroupByjob.setInputFormatClass(SequenceFileInputFormat.class);
		    GroupByjob.setOutputFormatClass(TextOutputFormat.class);
		    
		    FileInputFormat.setInputPaths(GroupByjob,new Path("temp/part-r-00000"));
		    FileOutputFormat.setOutputPath(GroupByjob,new Path(args[1]));
		    GroupByjob.waitForCompletion(true);
	    }
    }
}
