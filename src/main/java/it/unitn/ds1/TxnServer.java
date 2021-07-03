package it.unitn.ds1;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import it.unitn.ds1.TxnCoordinator.TxnId;

import it.unitn.ds1.TxnCoordinator.FwdReadMsg;
import it.unitn.ds1.TxnCoordinator.FwdWriteMsg;

import it.unitn.ds1.TxnCoordinator.CanCommitMsg;
import it.unitn.ds1.TxnCoordinator.FinalDecisionMsg;

public class TxnServer extends AbstractActor {
  private final Integer serverId;
  private final Map<Integer, Integer[]> dataStore;
  private final Map<TxnId, Set<Integer[]>> workSpace;

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnServer(int serverId) {
    this.serverId = serverId;
    this.dataStore = new TreeMap<>();
    this.workSpace = new HashMap<>();
    initDataStore();
    // System.out.println("\t\tSERVER "+serverId+" DataStore Init");
  }

  static public Props props(int serverId) {
    return Props.create(TxnServer.class, () -> new TxnServer(serverId));
  }

  /*-- Actor start logic ---------------------------------------------------- */

  private void initDataStore(){
    for (int i=10*this.serverId; i<=10*this.serverId+9; i++) {
      this.dataStore.put(i, new Integer[] {0,100});
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

  // reply with commit decision
  public static class ServerDecisionMsg implements Serializable {
    public final boolean commit;
    public final TxnId txn;
    public ServerDecisionMsg(boolean commit, TxnId txn) {
      this.commit = commit;
      this.txn = txn;
    }
  }

  /*-- Actor methods -------------------------------------------------------- */

  // get value for a given key
  private Integer getValueFromKey(Integer key){
    return dataStore.get(key)[1];
  }

  private Integer getVersionFromKey(Integer key){
    return dataStore.get(key)[0];
  }

  private String printWorkspace(Set<Integer[]> ws){
    String res = "";
    for(Integer[] i : ws){
      res = res + Arrays.toString(i) + " ";
    }
    return res;
  }

  // loop in the workspace for that txn and compare the version
  // can change if all the versions are +1 
  // TODO: lock objects so that other clients cannot commit in the meantime
  private Boolean checkIfCanChange(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value}
      if( !dataStore.get(c[0])[0].equals(c[1]-1) ) return false;
    }
    return true;
  }

  private void ApplyChanges(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value}
      dataStore.replace(c[0],new Integer[] {c[1],c[2]});
    }
  }

  private void printDataStore(){
    System.out.println(getSelf().path().name());
    dataStore.entrySet().forEach(entry -> {
      System.out.println(entry.getKey() + " " + Arrays.toString(entry.getValue()));
    });
  }


  /*-- Message handlers ----------------------------------------------------- */

  private void onFwdReadMsg(FwdReadMsg msg) {
    System.out.println("\t\tSERVER " + serverId + " Received Read from " + getSender().path().name());

    workSpace.putIfAbsent(msg.txn, new HashSet<>());    

    Integer value = getValueFromKey(msg.key);
    getSender().tell(new FwdReadResultMsg(msg.key, value, msg.txn), getSelf());

  }

  private void onFwdWriteMsg(FwdWriteMsg msg) {
    
    Integer version = getVersionFromKey(msg.key);
    Integer[] writeResult = new Integer[] {msg.key,version+1,msg.value};
    workSpace.get(msg.txn).add(writeResult);

    System.out.println("\t\tSERVER " + serverId + " Received Write from " + getSender().path().name()
                      + " - WS "+ printWorkspace(workSpace.get(msg.txn)));
  
  }

  private void onCanCommitMsg(CanCommitMsg msg){
    System.out.println("\t\tSERVER " + serverId + " Received Commit Request "
                      + " - WS "+printWorkspace(workSpace.get(msg.txn)));

    Boolean canChange = checkIfCanChange(workSpace.get(msg.txn));

    if(canChange){ System.out.println("\t\tSERVER " + serverId + " Can Change");}
    else{ System.out.println("\t\tSERVER " + serverId + " Can't Change");}

    getSender().tell(new ServerDecisionMsg(canChange, msg.txn), getSelf());

  }

  private void onFinalDecisionMsg(FinalDecisionMsg msg){
    System.out.println("\t\tSERVER " + serverId + " Received Final Decision ");

    if( msg.decision ) ApplyChanges(workSpace.get(msg.txn));      
    workSpace.remove(msg.txn);

    printDataStore();

  }



  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(FwdReadMsg.class,  this::onFwdReadMsg)
            .match(FwdWriteMsg.class,  this::onFwdWriteMsg)
            .match(CanCommitMsg.class,  this::onCanCommitMsg)
            .match(FinalDecisionMsg.class,  this::onFinalDecisionMsg)
            .build();
  }
}
