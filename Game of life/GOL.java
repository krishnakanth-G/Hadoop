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

public class GOL 
{
  public static class GOLMapper extends Mapper<LongWritable, Text, Text, Text> 
  {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {    
       String line = value.toString();    
       String [] words = line.split(",");    
       Text outputkey = new Text(words[0]);
       Text outputval = new Text(words[1]+","+words[2]);
       context.write(outputkey, outputval);
     }  
  }

  public static class GOLReducer extends Reducer<Text,Text,Text,Text> 
  {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
      for(Text value : values)
      {
         String line = value.toString();    
         String [] words = line.split(",");
         int alive = Integer.parseInt(words[1]);
         int val = Integer.parseInt(words[0]);
         Text outputVal = new Text();
         if(val == 1)
         {
             if(alive == 2 || alive == 3)
               outputVal.set("1");
             else
               outputVal.set("0");
         }
         else
         {
             if(alive == 3)
               outputVal.set("1");
             else
               outputVal.set("0");
         }
         context.write(key, outputVal);
      }
    }
  }

  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(GOL.class);
    job.setMapperClass(GOLMapper.class);
    job.setReducerClass(GOLReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

