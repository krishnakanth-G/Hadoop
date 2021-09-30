//import the required libraries
import java.util.Random;
import java.io.FileWriter;
import java.io.IOException;

public class Test
{
  //Function for creating input file with random number of random integers
  public static void testing(String fileName) throws IOException
  {
    FileWriter writer = new FileWriter(fileName	);
    Random num = new Random();
    int x_shape = num.nextInt(50);
    int y_shape = num.nextInt(50);
    int [][] arr = new int [x_shape][y_shape];
    for(int x=0;x<x_shape;x++)
    {
      for(int y=0;y<y_shape;y++)
      {
        arr[x][y] = num.nextInt(2);
      }
    }
    for(int x=0;x<x_shape;x++)
    {
      for(int y=0;y<y_shape;y++)
      {
        int alive = 0;
    	if(x+1 < x_shape)
    	{
         if(y+1 < y_shape)
           alive = alive+arr[x+1][y+1];
         if(y < y_shape)
            alive = alive+arr[x+1][y];
         if(y-1 >= 0)
            alive = alive+arr[x+1][y-1];
         }
    	if(y+1 < y_shape)
    	{
         if(x < x_shape)
            alive = alive+arr[x][y+1];
         if(x-1 >= 0)
            alive = alive+arr[x-1][y+1];
        }
    	if(x-1 >= 0)
    	{
         if(y-1 >= 0)
            alive = alive+arr[x-1][y-1];
         if(y < y_shape)
            alive = alive+arr[x-1][y];
         }
    	if(y-1 >= 0)
         if(x < x_shape)
            alive = alive+arr[x][y-1];
        writer.write(x+""+y+","+arr[x][y]+","+alive+"\n");
      }
    }
    writer.close();
  }
  // Main function
  public static void main(String[] args) throws IOException
  {
    testing("input.txt");
  }
}

