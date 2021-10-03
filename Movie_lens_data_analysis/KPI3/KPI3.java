import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class KPI3 
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
            String user_id = tokens[0];    
            String movie_id = tokens[1];
            String star_rating = tokens[2];
                
            context.write(new Text(movie_id), new Text(user_id+":"+star_rating+"_ratings"));                
        }        
    }
    
    public static class dataReducer extends Reducer<Text,Text,Text,Text>
    {
        //we are getting input from  ***movieDataMapper*** and ***ratingDataMapper***
        public void reduce(Text key, Iterable<Text>values,Context context)throws IOException,InterruptedException
        { 
            /***
                key                     value
                 1          [ genre_movies , 222:4_ratings .... ]
            ***/
            String genre = null;
            ArrayList<String> arr = new ArrayList<String>();
            
            for(Text val:values)
            {
                String []tokens = val.toString().split("_");
                if(tokens[1].equals("movie"))    //from movieDataMapper
                {
                    genre = tokens[0];
                }
            
                else if(tokens[1].equals("ratings"))  //from ratingDataMapper
                {
                    arr.add(tokens[0]);
                }
            }
            
            for(String val:arr)
            {
                String []splitAgain = val.split(":");
                String user_id = splitAgain[0];
                String rating = splitAgain[1];
                
                context.write(new Text(user_id), new Text(genre+"::"+rating));
            }
        }
    }

    /*** 
    	--------------------------------Second MapReduce---------------------------- 
    ***/
    public static class userGenreRatingMapper extends Mapper<LongWritable,Text,Text,Text> 
    {
        //data format => user_id     genre::rating    
        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
        {
            String []tokens = value.toString().split("\t");    
            String user_id  = tokens[0];
            
            context.write(new Text(user_id), new Text(tokens[1]+"_file2"));
        }
    }

    public static class userDataMapper extends Mapper<LongWritable,Text,Text,Text> 
    { 
        //data format => UserID::Gender::Age::Occupation::Zip-code 
        public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
        {
            String []tokens = value.toString().split("::");
            String user_id = tokens[0];
            String age = tokens[2];
            String occupation = tokens[3];
            
            context.write(new Text(user_id), new Text(age+"::"+occupation+"_file1"));
        }
    }

    // data from ***userGenreRatingMapper  and ***userDataMapper
    public static class userAgeOccupationGenreRatingReducer extends Reducer<Text,Text,Text,Text> 
    {
        /***
        	key                             values
        	223       [genre::rating_file2,age::occupation_file1 .....]
        ***/
        
        public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
        {
            String age = null;
            String occupation = null;
            ArrayList<String> arr2 = new ArrayList<String>();
            
            for(Text val:values)
            {
                String []tokens = val.toString().split("_");
                String file = tokens[1];

                if(file.equals("file1"))  //means data from userDataMapper
                {
                    String []splitAgain = tokens[0].split("::");
                    age = splitAgain[0];       
                    occupation = splitAgain[1];
                }
                else if(file.equals("file2"))  //means data from userGenreRatingMapper
                {  
                    arr2.add(tokens[0]);                  
                } 
            }

            for(String val:arr2)
            {   
                String []splitAgain2 = val.toString().split("::");
                String genre = splitAgain2[0];
                String rating = splitAgain2[1];

                context.write(new Text(age+"::"+occupation+"::"+genre), new Text(rating));
            }
        }
    }

    /*** 
    	--------------------------------Third MapReduce---------------------------- 
    ***/
    // input data from   ****userAgeOccupationGenreRatingReducer
    public static class averageRatingMapper extends Mapper<LongWritable,Text,Text,Text> 
    {
	    //data format => age::occupation::genre      rating
	    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
	    {
		    String []tokens = value.toString().split("\t");
		    String age_occupation_genre = tokens[0];
		    String rating = tokens[1];
		    String splitAgain[] = tokens[0].split("::");
		    long age = Long.parseLong(splitAgain[0]);
		
		    if(age >=18)                                 //18+ age group is only considered
		    {
		        context.write(new Text(age_occupation_genre), new Text(rating));
		    }
	    }	
    }
    
    public static class averageRatingReducer extends Reducer <Text,Text,Text,Text>
    {
        public void reduce(Text key,Iterable<Text>values,Context context)throws IOException,InterruptedException
        {
            /***
            	     key                  value (ratings)
            age::occupation::genre    [ 1,4,2,3,5,5,5 .......]
            ***/

            double avg = 0.0;
            double sum = 0.0;
            long count = 0;
            
            for(Text val:values)
            {
                String temp = val.toString();
                long rating = Long.parseLong(temp);
                sum+=rating; 
                count++;      
            }
            avg = sum/count;
            String average_rating = String.valueOf(avg);
            
            context.write(new Text(average_rating), new Text(key));  
        }
    }
    

    public static void main(String[] args) throws Exception 
    {
	Path inputPath_1 = new Path(args[0]);
	Path inputPath_2 = new Path(args[1]);
	
	Path outputPath_1 = new Path(args[2]);
	Path inputPath_3   = new Path(args[3]);
	
	Path outputPath_2 = new Path(args[4]);
	Path finalOutput = new Path(args[5]);
	 
	Configuration conf = new Configuration();
		
        //job0
	Job job = Job.getInstance(conf, "first job");
	job.setJarByClass(KPI3.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	MultipleInputs.addInputPath(job, inputPath_1, TextInputFormat.class, movieDataMapper.class);
	MultipleInputs.addInputPath(job, inputPath_2, TextInputFormat.class, ratingDataMapper.class);

	job.setReducerClass(dataReducer.class);
	FileOutputFormat.setOutputPath(job, outputPath_1);
	job.waitForCompletion(true);


        //job1
	Job job1 = Job.getInstance(conf, "Second job");
	job1.setJarByClass(KPI3.class);

	MultipleInputs.addInputPath(job1,outputPath_1,TextInputFormat.class,userGenreRatingMapper.class);
	MultipleInputs.addInputPath(job1,inputPath_3,TextInputFormat.class,userDataMapper.class);

	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(Text.class);
		        
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(Text.class);			

	job1.setReducerClass(userAgeOccupationGenreRatingReducer.class);
	FileOutputFormat.setOutputPath(job1, outputPath_2);
	job1.waitForCompletion(true);

        //job2
	Job job2 = Job.getInstance(conf,"Third job");
	job2.setJarByClass(KPI3.class);
			
	job2.setMapperClass(averageRatingMapper.class);
	job2.setReducerClass(averageRatingReducer.class);			
			
        job2.setMapOutputKeyClass(Text.class);
	job2.setMapOutputValueClass(Text.class);

	job2.setOutputKeyClass(Text.class);
	job2.setOutputValueClass(Text.class);
						
	FileInputFormat.addInputPath(job2, outputPath_2);
	FileOutputFormat.setOutputPath(job2,finalOutput);
						
	job2.waitForCompletion(true);
    }
}
