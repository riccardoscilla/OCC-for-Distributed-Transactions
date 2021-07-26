import java.io.*;
import java.util.*;

public class Check{
  static String fileName;
  static ArrayList<String> order = new ArrayList<String>();
  static Map<String, Map<String, Integer>> results = new LinkedHashMap<>();
  static String lastTxnId;
  
  /*---------------------------------------------------------- */
  public static void getOrder(){  
    String line;

    try {

      BufferedReader reader = new BufferedReader(new FileReader(fileName));

      while((line = reader.readLine()) != null) {
        line = line.replace("\0", "");
        String[] l = line.split(" ");
   
        if(Arrays.stream(l).anyMatch("END"::equals)){
          // System.out.println(line);
          order.add(l[1]);
        }
      }

      reader.close();

    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }

  /*---------------------------------------------------------- */
  public static void matchOrder(String o){
    String line;
    String toMatch = "";
    try {

      BufferedReader reader = new BufferedReader(new FileReader(fileName));

      while((line = reader.readLine()) != null) {
        line = line.replace("\0", "");
        String[] l = line.split(" ");
  
        if(Arrays.stream(l).anyMatch("[CHECK]"::equals)){
          String txnId = l[1];
          String serverId = l[2];
          int dataStoreValue = Integer.parseInt(l[3]);

          if(txnId.substring((txnId.length() - 1)).equals(o) && toMatch.equals("")){
            
            if(!results.containsKey(txnId)){
              toMatch = txnId;
              if(results.isEmpty()){
                results.put(txnId, new HashMap<>());
                lastTxnId = txnId;
              }else{
                // String lastTxnId = results.lastKey();
                Map<String, Integer> copy = new HashMap<>();
                copy.putAll(results.get(lastTxnId));
                results.put(txnId, copy);
                lastTxnId = txnId;
              }
            }
          }

          if(txnId.substring((txnId.length() - 1)).equals(o) && toMatch.equals(txnId)){
            if(results.get(txnId).containsKey(serverId)){
              results.get(txnId).replace(serverId,dataStoreValue);
            }else{
              results.get(txnId).put(serverId,dataStoreValue);
            }
          }

        }
      }

      reader.close();

    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }

  /*---------------------------------------------------------- */
  public static void main(String[] args) {
    fileName = args[0];

    getOrder();  
    System.out.println("Client Order "+order);
    System.out.println();

    for(String o : order){
      matchOrder(o);
    }
    
    results.entrySet().forEach(entry -> {
      System.out.println(entry.getKey() + " " + entry.getValue());
    });
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
}
