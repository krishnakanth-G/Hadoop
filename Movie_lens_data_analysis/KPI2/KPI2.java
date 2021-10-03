import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
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

public class KPI2
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
			String rating = tokens[2]+":"+"rating";
			context.write(new LongWritable(movie_id), new Text(rating));			
		}
	}
	
	public static class dataReducer extends Reducer<LongWritable,Text,Text,FloatWritable>
	{
		// Reducer for movieDataMapper and ratingsDataMapper
		public void reduce(LongWritable key, Iterable<Text>values,Context context)throws IOException,InterruptedException
		{ 
			/*
			key(movie_id)                                values
			    1              [4:rating,5:rating..,Moviename:genre,1:rating,2:rating.....]
			*/
			float avg_rating = 0;
			float count = 0;
			float sum = 0;
			String movie_name = null;
			String genre = null;
			for(Text val:values)
			{
				String line = val.toString();
				String []token = line.split(":");
				if(token[1].equals("rating"))   //which means the token is from ratingsDataMapper
				{
					sum = sum+Float.parseFloat(token[0]);
					count++;	
				}
				else
				{
					movie_name = token[0];   //which means the token is from movieDataMapper
					genre = token[1];
				}
			}
			if(count >= 40)  // Condition to check atleast 40 users rated a movie
			{
				avg_rating = sum/count;
				context.write(new Text(movie_name+"::"+genre), new FloatWritable(avg_rating));
			}
		}
	}
	
	public static class topTwentyMapper extends Mapper<LongWritable,Text,FloatWritable,Text>
	{
		private TreeMap<Float,String> top20;
		String name_gen = null;
		float average_rating;
	
		public void setup(Context context)throws IOException,InterruptedException
		{
			top20 = new TreeMap<Float,String>();
		}	
	
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			//data format => movie_name::genre   average_rating    
			String []tokens = value.toString().split("\t");
		
		 	name_gen = tokens[0];
			average_rating = Float.parseFloat(tokens[1]);

			top20.put(average_rating, name_gen);
		 
			if(top20.size()>20)
			{
				top20.remove(top20.firstKey());
			}
		}
		
		public void cleanup(Context context)throws IOException,InterruptedException
		{
			for(Map.Entry<Float,String> entry : top20.entrySet()) 
			{
			
				float key = entry.getKey();             //average rating
				String value = entry.getValue();         //movie_name::genre
			  
				context.write(new FloatWritable(key), new Text(value));
			}	
		}
	}

	
	public static class topTwentyReducer extends Reducer<FloatWritable,Text,FloatWritable,Text> 
	{
		private TreeMap<Float,String> top20;
			
		public void setup(Context context)throws IOException,InterruptedException
		{
			top20 = new TreeMap<Float,String>();            	
		}
		
		public void reduce(FloatWritable key,Iterable<Text>values,Context context)throws IOException,InterruptedException
		{
			// data format => average_rating    movie_name::genre
			Float average_rating = key.get();
			String name_gen = null;
		
			for(Text val:values)
			{
				name_gen = val.toString().trim(); 
			}	
		
			top20.put(average_rating, name_gen);
		
			if(top20.size()>20)
			{
				top20.remove(top20.firstKey());
			}
		}
	
		public void cleanup(Context context)throws IOException,InterruptedException
		{
			for(Map.Entry<Float,String> entry : top20.entrySet()) 
			{
				Float key = entry.getKey();             //average rating
				String value = entry.getValue();         //movie_name::genre
			  
			 	context.write(new FloatWritable(key), new Text(value));
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
		Job job = Job.getInstance(conf, "Average Rated Movies");
		job.setJarByClass(KPI2.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		MultipleInputs.addInputPath(job, firstPath, TextInputFormat.class, movieDataMapper.class);
		MultipleInputs.addInputPath(job, sencondPath, TextInputFormat.class, ratingDataMapper.class);
		
		job.setReducerClass(dataReducer.class);
		FileOutputFormat.setOutputPath(job, outputPath_1);

		job.waitForCompletion(true);	
		

		// job 1
		Job job1 = Job.getInstance(conf, "Top 20 Average rated movies");
		job1.setJarByClass(KPI2.class);
		job1.setMapperClass(topTwentyMapper.class);		
		job1.setReducerClass(topTwentyReducer.class);
		
		job1.setMapOutputKeyClass(FloatWritable.class);
		job1.setMapOutputValueClass(Text.class);
		        
		job1.setOutputKeyClass(FloatWritable.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, outputPath_1);
		FileOutputFormat.setOutputPath(job1, outputPath_2);

		job1.waitForCompletion(true);
	}
}
