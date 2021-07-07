import java.io.*;
import java.util.Arrays;
// import java.util.SortedMap;
// import java.util.TreeMap;

public class Check {
  public static void main(String[] args) {
    String fileName = args[0];
    String line;
    String check = "[CHECK]";

    try {
      BufferedReader reader = new BufferedReader(new FileReader(fileName));
      
      while((line = reader.readLine()) != null) {
        String[] l = line.split(" ");
        if(l[0].equals(check)){
          System.out.println(l[0]);
        }
        
      }
      reader.close();
    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
