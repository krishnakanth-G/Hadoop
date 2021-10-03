import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KPI4 
{	
    /*** 
    	--------------------------------First MapReduce---------------------------- 
    ***/
    
    public static class movieDataMapper extends Mapper <LongWritable,Text,Text,Text>
    {        
        //data format => MovieID::Title::Genres
        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
        {
            String []tokens = value.toString().split("::");
            String movie_id = tokens[0];
            String genre = tokens[2].trim();
            context.write(new Text(movie_id), new Text(genre+"_movie"));  
        }
    }

    public static class ratingDataMapper extends Mapper<LongWritable,Text,Text,Text>
    {        
        //data format => UserID::MovieID::Rating::Timestamp
        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
        {
                
            String []tokens = value.toString().split("::");      
            String movie_id = tokens[1];
            String star_rating = tokens[2];
                
            context.write(new Text(movie_id), new Text(star_rating+"_ratings"));                
        }        
    }
    
    public static class dataReducer extends Reducer<Text,Text,Text,FloatWritable>
    {
        //we are getting input from  ***movieDataMapper*** and ***ratingDataMapper***
        public void reduce(Text key, Iterable<Text>values,Context context)throws IOException,InterruptedException
        { 
            /***
                key                     value
                 1          [ genre_movies,4_ratings,3_ratings .... ]
            ***/
            String genre = null;
            ArrayList<String> arr = new ArrayList<String>();
            float sum =0;
            float count =0;
            for(Text val:values)
            {
                String []tokens = val.toString().split("_");
                if(tokens[1].equals("movie"))    //from movieDataMapper
                {
                    genre = tokens[0];
                }
            
                else if(tokens[1].equals("ratings"))  //from ratingDataMapper
                {
                    sum = sum+Float.parseFloat(tokens[0]);
		    count++;
                }
            }
            if(count >= 40)
            {       
            	float avg = sum/count;       
                context.write(new Text(genre), new FloatWritable(avg));
            }
        }
    }

    /*** 
    	--------------------------------Second MapReduce---------------------------- 
    ***/
    public static class genreRatingMapper extends Mapper<LongWritable,Text,Text,FloatWritable> 
    {
        //data format => genre  rating    
        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
        {
            String []tokens = value.toString().split("\t");    
            String genre  = tokens[0];
            float rating = Float.parseFloat(tokens[1]);
            
            context.write(new Text(genre), new FloatWritable(rating));
        }
    }

    // data from ***genreRatingMapper
    public static class genreRatingReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> 
    {
        /***
        	key           values
               genre       [4.3,2.3,...]
        ***/
        
        public void reduce(Text key,Iterable<FloatWritable> values,Context context)throws IOException,InterruptedException
        {
            float count =0;
            float rating=0;
            for(FloatWritable val:values)
            {
                rating = Float.parseFloat(val.toString());
                count++;
	    }
	    float avg = rating/count;  
            context.write(new Text(key), new FloatWritable(avg));
        }
    }

    /*** 
    	--------------------------------Third MapReduce---------------------------- 
    ***/
    public static class topTwentyMapper extends Mapper<LongWritable,Text,FloatWritable,Text>
	{
		private TreeMap<Float,String> top20;
		String genre = null;
		float average_rating;
	
		public void setup(Context context)throws IOException,InterruptedException
		{
			top20 = new TreeMap<Float,String>();
		}	
	
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			//data format => genre   average_rating    
			String []tokens = value.toString().split("\t");
		
		 	genre = tokens[0];
			average_rating = Float.parseFloat(tokens[1]);

			top20.put(average_rating, genre);
		 
			if(top20.size()>20)
			{
				top20.remove(top20.firstKey());
			}
		}
		
		public void cleanup(Context context)throws IOException,InterruptedException
		{
			for(Map.Entry<Float,String> entry : top20.entrySet()) 
			{
			
				float key = entry.getKey();              //average rating
				String value = entry.getValue();         //genre
			  
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
			// data format => average_rating    genre
			Float average_rating = key.get();
			String genre = null;
		
			for(Text val:values)
			{
				genre = val.toString().trim(); 
			}	
		
			top20.put(average_rating, genre);
		
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
				String value = entry.getValue();        //genre
			  
			 	context.write(new FloatWritable(key), new Text(value));
			}	
		}
	}
    

    public static void main(String[] args) throws Exception 
    {
	Path inputPath_1 = new Path(args[0]);
	Path inputPath_2 = new Path(args[1]);
	
	Path outputPath_1 = new Path(args[2]);
	Path outputPath_2 = new Path(args[3]);
	Path finaloutput = new Path(args[4]);
	 
	Configuration conf = new Configuration();
		
        //job0
	Job job = Job.getInstance(conf, "Average movie ratings");
	job.setJarByClass(KPI4.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(FloatWritable.class);

	MultipleInputs.addInputPath(job, inputPath_1, TextInputFormat.class, movieDataMapper.class);
	MultipleInputs.addInputPath(job, inputPath_2, TextInputFormat.class, ratingDataMapper.class);

	job.setReducerClass(dataReducer.class);
	FileOutputFormat.setOutputPath(job, outputPath_1);
	job.waitForCompletion(true);


        //job1
	Job job1 = Job.getInstance(conf, "Average genre ratings");
	job1.setJarByClass(KPI4.class);
	
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(FloatWritable.class);
		        
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(FloatWritable.class);			

	job1.setMapperClass(genreRatingMapper.class);
	job1.setReducerClass(genreRatingReducer.class);
	FileInputFormat.addInputPath(job1, outputPath_1);
	FileOutputFormat.setOutputPath(job1, outputPath_2);
	job1.waitForCompletion(true);

        //job2
	Job job2 = Job.getInstance(conf, "Top 20 Average rated genre");
	job2.setJarByClass(KPI4.class);
	job2.setMapperClass(topTwentyMapper.class);		
	job2.setReducerClass(topTwentyReducer.class);
	
	job2.setMapOutputKeyClass(FloatWritable.class);
	job2.setMapOutputValueClass(Text.class);
	        
	job2.setOutputKeyClass(FloatWritable.class);
	job2.setOutputValueClass(Text.class);
	
	FileInputFormat.addInputPath(job2, outputPath_2);
	FileOutputFormat.setOutputPath(job2, finaloutput);
	job2.waitForCompletion(true);
    }
}
