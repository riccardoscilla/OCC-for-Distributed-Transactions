import java.io.*;
import java.util.*;

public class Check{
  static String fileName;
  static ArrayList<String> order = new ArrayList<String>();
  static Map<String, ArrayList<Integer>> changes = new LinkedHashMap<>();
  static Map<String, Map<String, Integer>> results = new LinkedHashMap<>();
  static Map<String, Integer> diffs = new LinkedHashMap<>();
  static String lastTxnId;
  
  /*---------------------------------------------------------- */
  public static void getOrder(){  
    String line;

    try {

      BufferedReader reader = new BufferedReader(new FileReader(fileName));

      while((line = reader.readLine()) != null) {
        line = line.replace("\0", "");
        String[] l = line.split(" ");
   
        if(Arrays.stream(l).anyMatch("BEGIN"::equals)){
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
  public static void printKeyword(String keyword){
    String line;
    try {

      BufferedReader reader = new BufferedReader(new FileReader(fileName));

      while((line = reader.readLine()) != null) {
        line = line.replace("\0", "");
        String[] l = line.split(" ");
  
        if(Arrays.stream(l).anyMatch(keyword::equals)){
          System.out.println(line);
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

          if(changes.containsKey(serverId)){
            changes.get(serverId).add(dataStoreValue);
          }else{
            changes.put(serverId ,new ArrayList<>());
            changes.get(serverId).add(1000);
            changes.get(serverId).add(dataStoreValue);
          }


          if(txnId.substring((txnId.length() - 1)).equals(o) && toMatch.equals("")){
            
            if(!diffs.containsKey(txnId)){
              diffs.put(txnId, 0);
            }
            int d = diffs.get(txnId) + changes.get(serverId).get(changes.get(serverId).size()-1) - dataStoreValue;
            diffs.put(txnId, d);


            if(!results.containsKey(txnId)){
              toMatch = txnId;
              if(results.isEmpty()){
                results.put(txnId, new HashMap<>());
                lastTxnId = txnId;
              }else{
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
  public static void printOKFAIL(){
    String line;
    try {
      
      Map<String, String> print = new LinkedHashMap<>();
      BufferedReader reader = new BufferedReader(new FileReader(fileName));

      while((line = reader.readLine()) != null) {
        line = line.replace("\0", "");
        String[] l = line.split(" ");

        if(Arrays.stream(l).anyMatch("BEGIN"::equals)){
          System.out.println(line);
        }

        if(Arrays.stream(l).anyMatch("TIMEOUT"::equals)){
          System.out.println(line);
        }
  
        if(Arrays.stream(l).anyMatch("OK"::equals) || Arrays.stream(l).anyMatch("FAIL"::equals)
          && Arrays.stream(l).anyMatch("COMMIT"::equals)){
          System.out.println(line);
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

    printKeyword("Seed:");
    printKeyword("Info:");

    getOrder();  
    System.out.println("Client Order "+order);
    System.out.println();

    // printKeyword("OK");
    // printKeyword("FAIL");
    // System.out.println();
    
    printOKFAIL();
    System.out.println();

    // printKeyword("[CHECK]");
    // System.out.println();

    for(String o : order){
      matchOrder(o);
    }
    
    results.entrySet().forEach(entry -> {
      System.out.println(entry.getKey() + " " + entry.getValue());
    });
    System.out.println();

    for(String key : diffs.keySet()){
      if(diffs.get(key) == 0){
        System.out.println(key + " " + diffs.get(key) + " OK");
      }else{
        System.out.println(key + " " + diffs.get(key) + " WRONG");
      }
    }
    System.out.println();

    int finalSum = 0;
    for(String key : changes.keySet()){
      finalSum = finalSum + changes.get(key).get(changes.get(key).size()-1);
    }
    if(finalSum % 1000 == 0){
      System.out.println("Final sum = " + finalSum + " OK");
    }else{
      System.out.println("Final sum = " + finalSum + " WRONG");
    }
  }
}
