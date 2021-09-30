import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job; 
   
import org.apache.hadoop.io.IntWritable;  
import org.apache.hadoop.io.LongWritable;   
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
     


public class FrequentChar 
{
  public static class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
  {
    private final static IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
       String line = value.toString();    
       String [] chars = line.split("");    
       for(String ch:chars)
       {
         if(!Character.isWhitespace(ch.charAt(0)))
         {
            Text outputkey = new Text(ch.toUpperCase());
            context.write(outputkey, one);
         }
       }
    }
  }

  public static class CharCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
      int sum = 0; 
      for(IntWritable val : values)
        sum += val.get();
      
      result.set(sum);
      context.write(key, result);
    }
  }
  
  public static class CharCountMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> 
  {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
       IntWritable outputkey = new IntWritable(1);
       context.write(outputkey, value);
    }
  }

  public static class CharCountReducer1 extends Reducer<IntWritable,Text,IntWritable,Text> 
  {
	  public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
      int max = 0; 
      String line;
      String [] pair;
      String ch = "";
      int count = 0;
      ArrayList<String> li = new ArrayList<String>();
      for(Text val : values)
      {
        line = val.toString();    
        pair = line.split("\t");
        if(count == Integer.parseInt(pair[1]))
        {
          li.add(pair[0]);
        }
        else if(Integer.parseInt(pair[1])> count)
        {
          li.clear();
          count = Integer.parseInt(pair[1]);
          li.add(pair[0]);
        }
      }
      Iterator itr=li.iterator();
      while(itr.hasNext())
      {
        ch=(String)itr.next();  
        context.write(new IntWritable(count), new Text(ch));
      }
    }
  }


  public static void main(String[] args) throws Exception 
  {
    Path inputPath = new Path(args[0]);
    Path outputPath_1 = new Path(args[1]);
    Path outputPath_2 = new Path(args[2]);
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(FrequentChar.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    job.setMapperClass(CharCountMapper.class);
    job.setReducerClass(CharCountReducer.class);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath_1);
    
    job.waitForCompletion(true);
    
    
    Job job1 = Job.getInstance(conf, "Most Frequent word");
    job1.setJarByClass(FrequentChar.class);
    
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Text.class);
    
    job1.setMapperClass(CharCountMapper1.class);
    job1.setReducerClass(CharCountReducer1.class);
    
    FileInputFormat.addInputPath(job1, outputPath_1);
    FileOutputFormat.setOutputPath(job1, outputPath_2);
    
    job1.waitForCompletion(true);
  }
}

