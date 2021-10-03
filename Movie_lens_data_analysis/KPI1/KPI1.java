import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class KPI1
{
	public static class movieDataMapper extends Mapper <LongWritable,Text,LongWritable,Text>
	{
		
		//data format => MovieID::Title::Genres
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			String []tokens = value.toString().split("::");		
			long movie_id = Long.parseLong(tokens[0]);
			String name_gen = tokens[1]+":"+tokens[2];
			context.write(new LongWritable(movie_id), new Text(name_gen));
		}
	}
	
	public static class ratingDataMapper extends Mapper<LongWritable,Text,LongWritable,Text> 
	{
		//data format => UserID::MovieID::Rating::Timestamp
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{			
			String []tokens = value.toString().split("::");	
			long movie_id = Long.parseLong(tokens[1]);
			String count = "1";
			context.write(new LongWritable(movie_id), new Text(count));			
		}
	}
	
	public static class dataReducer extends Reducer<LongWritable,Text,Text,LongWritable>
	{
		// Reducer for movieDataMapper and ratingsDataMapper
		public void reduce(LongWritable key, Iterable<Text>values,Context context)throws IOException,InterruptedException
		{ 
			/*
			key(movie_id)            values
			    1          [ 1,1,.,Moviename:genre,1,1,1.....]
			*/
			long count = 0;
			String name_gen = null;
			for(Text val:values)
			{
				String token = val.toString();
				if(token.equals("1"))   //which means the token is from ratingsDataMapper
				{
					count++;	
				}
				else
				{
					name_gen = token;   //which means the token is from movieDataMapper;
				}
			}
			context.write(new Text(name_gen), new LongWritable(count));
		}
	}
	
	
	public static class topTenMapper extends Mapper<LongWritable,Text,Text,LongWritable> 
	{
		private TreeMap<Long,String> top10;
		String name_gen=null;
		long count=0;
		
		public void setup(Context context)throws IOException, InterruptedException
		{
			top10 = new TreeMap<Long,String>();            	
		}

		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			//data format => name_gen   count
			String []tokens =  value.toString().split("\t");
			count = Long.parseLong(tokens[1]);
			name_gen = tokens[0].trim();

			top10.put(count, name_gen);
			if(top10.size() >10)                    //if size crosses 10 we will remove the topmost key-value pair.
			{ 
				top10.remove(top10.firstKey());    
			}
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			
			for(Map.Entry<Long,String> entry : top10.entrySet()) 
			{
				Long key = entry.getKey();            //count
				String value = entry.getValue();      //name_gen
				
				context.write(new Text(value),new LongWritable(key));
			}
		}
	}
	
	public static class topTenReducer extends Reducer <Text,LongWritable,LongWritable,Text> 
	{
		private TreeMap<Long,String> top10;
		String name_gen=null;
		long count=0;

		public void setup(Context context)throws IOException, InterruptedException
		{
			top10 = new TreeMap<Long,String>();            	
		}

		public void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException,InterruptedException
		{
			//data format => name_gen     count			
			for(LongWritable val:values)
			{
				count = val.get();
		
			}
		
			name_gen = key.toString().trim();
			top10.put(count,name_gen);
		
			if(top10.size()>10)
			{
				top10.remove(top10.firstKey());     
			}		
	
		
		}

		public void cleanup(Context context) throws IOException,InterruptedException
		{		
			for(Map.Entry<Long,String> entry : top10.entrySet()) 
			{
		     		Long key = entry.getKey();            //count
			  	String value = entry.getValue();      //name_gen
			  
			  	context.write(new LongWritable(key),new Text(value));
			}
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		Path firstPath = new Path(args[0]);
		Path sencondPath = new Path(args[1]);
		
		Path outputPath_1 = new Path(args[2]);
		Path outputPath_2 = new Path(args[3]);
		 
		Configuration conf = new Configuration();
		
		// job 0
		Job job = Job.getInstance(conf, "Most Viewed Movies");
		job.setJarByClass(KPI1.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, movieDataMapper.class);
		MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, ratingDataMapper.class);
		
		job.setReducerClass(dataReducer.class);

		FileOutputFormat.setOutputPath(job, outputPath_1);

		job.waitForCompletion(true);	
		

		// job 1
		Job job1 = Job.getInstance(conf, "10 Most Viewed Movies2");	
		job1.setJarByClass(KPI1.class);
		
		job1.setMapperClass(topTenMapper.class);
		job1.setReducerClass(topTenReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);
       
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, outputPath_1);
		FileOutputFormat.setOutputPath(job1, outputPath_2);

		job1.waitForCompletion(true);
	}
}
