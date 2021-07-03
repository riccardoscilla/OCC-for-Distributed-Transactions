package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import it.unitn.ds1.TxnCoordinator.TxnId;

import it.unitn.ds1.TxnCoordinator.FwdReadMsg;
import it.unitn.ds1.TxnCoordinator.FwdWriteMsg;
// import it.unitn.ds1.TxnCoordinator.FwdWriteResultMsg;

public class TxnServer extends AbstractActor {
  private final Integer serverId;
  private final Map<Integer, int[]> dataStore;
  // private final Map<TxnId, int[]> workSpace;

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnServer(int serverId) {
    this.serverId = serverId;
    this.dataStore = new HashMap<>();
    initDataStore();
    // System.out.println("[INFO] SERVER "+serverId+" DataStore Init");
  }

  static public Props props(int serverId) {
    return Props.create(TxnServer.class, () -> new TxnServer(serverId));
  }

  /*-- Actor start logic ---------------------------------------------------- */

  private void initDataStore(){
    for (int i=10*this.serverId; i<=10*this.serverId+9; i++) {
      this.dataStore.put(i, new int[] {0,100});
    }
    
    // if (this.serverId.equals(0)){
    //     this.dataStore.entrySet().forEach(entry -> {
    //     System.out.println(entry.getKey() + " " + Arrays.toString(entry.getValue()));
    //   });
    // }
    
  }  

  /*-- Message classes ------------------------------------------------------ */

  // reply from the server when requested a READ on a given key
  public static class FwdReadResultMsg implements Serializable {
    public final Integer key; // the key associated to the requested item
    public final Integer value; // the value found in the data store for that item
    public final TxnId txn;
    public FwdReadResultMsg(int key, int value, TxnId txn) {
      this.key = key;
      this.value = value;
      this.txn = txn;
    }
  }

  /*-- Actor methods -------------------------------------------------------- */

  // get value for a given key
  private Integer getValueFromKey(Integer key){
    Integer version = dataStore.get(key)[0];
    Integer value = dataStore.get(key)[1];
    return value;
  }

  /*-- Message handlers ----------------------------------------------------- */

  private void onFwdReadMsg(FwdReadMsg msg) {
    System.out.println("[INFO] SERVER " + serverId + " Received Read from " + getSender().path().name());

    Integer value = getValueFromKey(msg.key);
    getSender().tell(new FwdReadResultMsg(msg.key, value, msg.txn), getSelf());

  }

  private void onFwdWriteMsg(FwdWriteMsg msg) {
    System.out.println("[INFO] SERVER " + serverId + " Received Write from " + getSender().path().name());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(FwdReadMsg.class,  this::onFwdReadMsg)
            .match(FwdWriteMsg.class,  this::onFwdWriteMsg)
            .build();
  }
}
