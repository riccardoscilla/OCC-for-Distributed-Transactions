import java.io.*;
import java.util.*;

public class Check {
  public static void main(String[] args) {
    String fileName = args[0];
    String line;
    String check = "[CHECK]";
    Map<String, Map<String, Integer>> results = new LinkedHashMap<>(); // preserve insertion order
    String lastTxnId = "";

    try {
      BufferedReader reader = new BufferedReader(new FileReader(fileName));
      
      while((line = reader.readLine()) != null) {
        line = line.replace("\0", "");
        String[] l = line.split(" ");

        if(l[0].contains(check)){
          System.out.println(line);
          
          String txnId = l[1];
          String serverId = l[2];
          int dataStoreValue = Integer.parseInt(l[3]);
          
          if(results.isEmpty()){
            results.put(txnId, new HashMap<>());
            lastTxnId = txnId;
          }

          if(!txnId.equals(lastTxnId)){
            // copy last txn values in new one
            Map<String, Integer> copy = new HashMap<>();
            copy.putAll(results.get(lastTxnId));
            results.put(txnId, copy);
            lastTxnId = txnId;
          }

          if(txnId.equals(lastTxnId)){
            if(results.get(txnId).containsKey(serverId)){
              results.get(txnId).replace(serverId,dataStoreValue);
            }else{
              results.get(txnId).put(serverId,dataStoreValue);
            }
          }

        }
        
      }
      reader.close();

      System.out.println();
      for(String key : results.keySet()){
        int sum = results.get(key).values().stream().mapToInt(Integer::intValue).sum();
        if(sum % 1000 == 0){
          System.out.println(key + " " + sum + " OK");
        }else{
          System.out.println(key + " " + sum + " WRONG");
        }
        
      }

    }
    catch(Exception ex) {
      ex.printStackTrace();
    }
  }
}
