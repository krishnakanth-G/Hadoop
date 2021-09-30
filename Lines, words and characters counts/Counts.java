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
     


public class Counts 
{
  public static class CountsMapper extends Mapper<LongWritable, Text, Text, Text> 
  {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    {
       String line = value.toString();    
       String [] words = line.split(" ");
       String [] chars = line.split("");
       Text outputkey = new Text("Counts: Lines,Words,Characters");
       int count1 = 0;
       int count2 = 0;
       
       for(String word:words)
       {    
            count1 = count1 + 1;    
       }
       for(String cha:chars)
       {    
            count2 = count2 + 1;    
       }
       Text outputval = new Text("1"+","+count1+","+count2);
       context.write(outputkey, outputval);
    }
  }

  public static class CountsReducer extends Reducer<Text,Text,Text,Text> 
  {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
      int count0 = 0;
      int count1 = 0;
      int count2 = 0; 
      for(Text val : values)
      {
        String listVal = val.toString();
        String []count = listVal.split(",");
        count0 = count0+Integer.parseInt(count[0]);
        count1 = count1+Integer.parseInt(count[1]);
        count2 = count2+Integer.parseInt(count[2]);      
      } 
      Text outputval = new Text(count0+","+count1+","+count2);
      context.write(key, outputval);
    }
  }


  public static void main(String[] args) throws Exception 
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "count : lines,words,characters");
    job.setJarByClass(Counts.class);
    job.setMapperClass(CountsMapper.class);
    job.setReducerClass(CountsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

