//import the required libraries
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class Search
{
  //Function for counting number of lines in a input file
  public static int no_of_lines(String fileName) throws IOException
  {
    int count = 0;
    Scanner sc = new Scanner(new File(fileName));
    while(sc.hasNextLine()) 
    {
      sc.nextLine();
      count++;
    }
    sc.close();
    return count;
  }
  //Function for searching a number in the given array and output the indicies when find the number
  public static void search(String fileName) throws IOException
  {
    Scanner sc = new Scanner(new File(fileName));
    FileWriter writer = new FileWriter("out");
    int size = no_of_lines(fileName);
    int [] arr = new int [size];
    int i = 0;
    int target=0;
    while(sc.hasNextLine())
    {
      String line = sc.nextLine();
      String[] words = line.split(",");
      if(i == 0)
      	target = Integer.parseInt(words[2]); 
      arr[i++] = Integer.parseInt(words[1]);
    }
    sc.close();
    
    for(i=0;i<size;i++)
    {
       if(arr[i] == target)
    	 writer.write((i+1)+"\n");
    }
    writer.close();
  }
  // main function
  public static void main(String[] args) throws IOException
  {
    search("in");
  }
}
