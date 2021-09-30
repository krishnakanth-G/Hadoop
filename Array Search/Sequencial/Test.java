//import the required libraries
import java.util.Random;
import java.io.FileWriter;
import java.io.IOException;

public class Test
{
  //Function for creating input file with random number of random integers
  public static void testing(String fileName) throws IOException
  {
    FileWriter writer = new FileWriter("in");
    Random num = new Random();
    int size = num.nextInt(1000);
    int [] arr = new int [size];
    int target = num.nextInt(1000);
    for(int i=0;i<arr.length;i++)
    {
      arr[i] = num.nextInt(1000);
      writer.write(i+1+","+arr[i]+","+target);
      if(arr.length-1 != i)
      	writer.write("\n");
    }
    writer.close();
  }
  // Main function
  public static void main(String[] args) throws IOException
  {
    testing("in");
  }
}

