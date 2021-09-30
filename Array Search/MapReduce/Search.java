import java.io.IOException;   

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

public class Search
{
  public static class SearchMapper extends Mapper<LongWritable, Text, IntWritable, Text> 
  {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {    
       String line = value.toString();    
       String [] words = line.split(",");    
       int val = Integer.parseInt(words[1]);
       IntWritable outputkey = new IntWritable(val);
       Text outputval = new Text(words[0]+","+words[2]);
       context.write(outputkey, outputval);    
     }   
  }

  public static class SearchReducer extends Reducer<IntWritable,Text,IntWritable,Text> 
  {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
       for(Text value : values)
       {
         String line = value.toString();    
         String [] words = line.split(",");    
         int val = Integer.parseInt(words[1]);
         if(val == key.get())
         {
           Text outputval = new Text(words[0]);
           context.write(key, outputval);
         }
       }  
    }
  }


  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Search");
    job.setJarByClass(Search.class);
    job.setMapperClass(SearchMapper.class);
    job.setReducerClass(SearchReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
