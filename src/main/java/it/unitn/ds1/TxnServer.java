package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

public class TxnServer extends AbstractActor {
  private final Integer serverId;
  private final Map<Integer, int[]> dataStore;
  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnServer(int serverId) {
    this.serverId = serverId;
    this.dataStore = new HashMap<>();
    initDataStore();
  }

  static public Props props(int serverId) {
    return Props.create(TxnServer.class, () -> new TxnServer(serverId));
  }

  /*-- Actor start logic ---------------------------------------------------- */

  private void initDataStore(){
    for (int i=10*this.serverId; i<=10*this.serverId+9; i++) {
      this.dataStore.put(i, new int[] {0,100});
    }
     
    if (this.serverId.equals(0)){
        this.dataStore.entrySet().forEach(entry -> {
        System.out.println(entry.getKey() + " " + Arrays.toString(entry.getValue()));
      });
    }
    
  }  

  /*-- Message classes ------------------------------------------------------ */

  /*-- Actor methods -------------------------------------------------------- */

  /*-- Message handlers ----------------------------------------------------- */


  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .build();
  }
}
