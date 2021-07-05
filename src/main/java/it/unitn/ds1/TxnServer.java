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
  private final Map<TxnId, Set<ActorRef>> txnPartecipants;
  private final Map<TxnId, Boolean> txnHistory;
  private final Map<TxnId, Cancellable> decisionTimeout;

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnServer(int serverId) {
    this.serverId = serverId;
    this.dataStore = new TreeMap<>();
    this.workSpace = new HashMap<>();
    this.txnPartecipants = new HashMap<>();
    this.txnHistory = new HashMap<>();
    this.decisionTimeout = new HashMap<>();
    initDataStore();
    // System.out.println("\t\tSERVER "+serverId+" DataStore Init");
  }

  static public Props props(int serverId) {
    return Props.create(TxnServer.class, () -> new TxnServer(serverId));
  }

  /*-- Actor start logic ---------------------------------------------------- */

  private void initDataStore(){
    for (int i=10*this.serverId; i<=10*this.serverId+9; i++) {
      this.dataStore.put(i, new Integer[] {0,100,0});
    }    
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

  // the server may timeout waiting for the decision of the transaction
  public static class TxnDecisionTimeoutMsg implements Serializable {
    public final TxnId txn;
    public TxnDecisionTimeoutMsg(TxnId txn) {
      this.txn = txn;
    }
  }

  // ask to other partecipants the commit decision
  public static class PartecipantsDecisionMsg implements Serializable {
    public final TxnId txn;
    public PartecipantsDecisionMsg(TxnId txn) {
      this.txn = txn;
    }
  }

  // reply with commit decision to the other partecipant
  public static class FwdPartecipantsDecisionMsg implements Serializable {
    public final boolean decision;
    public final TxnId txn;
    public FwdPartecipantsDecisionMsg(boolean decision, TxnId txn) {
      this.decision = decision;
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
  // lock objects so that other clients cannot commit in the meantime
  private Boolean checkIfCanChange(Set<Integer[]> changes){
    for(Integer[] c : changes){ 
      // c = {key, version, value}
      // dataStore.get(c[0]) = {version, value, lock}
      
      // if the lock on the key is already acquired (set to 1)
      // or the version of the change is not the next one
      // return false; else acquire the lock on the key
      if( dataStore.get(c[0])[2].equals(1) ||
        !dataStore.get(c[0])[0].equals(c[1]-1) ){
        return false;
      }
      dataStore.get(c[0])[2] = 1;
    }
    LockChanges(changes);
    return true;
  }

  private void ApplyChanges(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value}
      dataStore.replace(c[0],new Integer[] {c[1],c[2],0});
    }
  }

  private void FreeLocks(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value}
      dataStore.get(c[0])[2] = 0;
    }
  }

  private void LockChanges(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value}
      dataStore.get(c[0])[2] = 1;
    }
  }

  // TODO: rewrite + check output
  private void printDataStore(){
    System.out.println(getSelf().path().name());
    dataStore.entrySet().forEach(entry -> {
      System.out.println(entry.getKey() + " " + Arrays.toString(entry.getValue()));
    });
  }

  // start the termination protocol asking all the partecifants 
  //if they have received a decision
  private void terminationProtocol(TxnId txn){
    for(ActorRef i : txnPartecipants.get(txn)){
      i.tell(new PartecipantsDecisionMsg(txn), getSelf());
    }
    setTimeout(txn, 500);
  }

  //set a decision timeout with delay t
  private void setTimeout(TxnId txn, int t){
    Cancellable timeout = getContext().system().scheduler().scheduleOnce(
            Duration.create(t, TimeUnit.MILLISECONDS),
            getSelf(),
            new TxnDecisionTimeoutMsg(txn), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    decisionTimeout.put(txn, timeout);
  }

  //cancel a certain timeout
  private void cancelTimeout(TxnId txn){
    if(decisionTimeout.get(txn) != null) decisionTimeout.get(txn).cancel();
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

    if(canChange){ 
      System.out.println("\t\tSERVER " + serverId + " Can Change");
      setTimeout(msg.txn, 500);
      txnPartecipants.put(msg.txn, msg.partecipants);
      }
    else{ 
      System.out.println("\t\tSERVER " + serverId + " Can't Change");
      workSpace.remove(msg.txn);
      txnHistory.put(msg.txn, canChange);
      }

    getSender().tell(new ServerDecisionMsg(canChange, msg.txn), getSelf());

  }

  private void onFinalDecisionMsg(FinalDecisionMsg msg){
    if(workSpace.get(msg.txn) == null) return;  // if already aborted do nothing

    System.out.println("\t\tSERVER " + serverId + " Received Final Decision ");

    if( msg.decision ) ApplyChanges(workSpace.get(msg.txn));
    else FreeLocks(workSpace.get(msg.txn)); // free the locks that may have been acquired

    workSpace.remove(msg.txn);
    txnHistory.put(msg.txn, msg.decision);
    txnPartecipants.remove(msg.txn);
    cancelTimeout(msg.txn);

    // printDataStore();

  }

  private void onTxnDecisionTimeoutMsg(TxnDecisionTimeoutMsg msg) throws InterruptedException {
    if(txnHistory.get(msg.txn) == null) terminationProtocol(msg.txn);
  }

  private void onPartecipantsDecisionMsg(PartecipantsDecisionMsg msg) throws InterruptedException {
    if(txnHistory.get(msg.txn) != null){
      getSender().tell(new FwdPartecipantsDecisionMsg(txnHistory.get(msg.txn), msg.txn), getSelf());
    }
  }

  private void onFwdPartecipantsDecisionMsg(FwdPartecipantsDecisionMsg msg) throws InterruptedException {
    if( msg.decision ) ApplyChanges(workSpace.get(msg.txn));
    else FreeLocks(workSpace.get(msg.txn)); // free the locks that may have been acquired

    workSpace.remove(msg.txn);
    txnHistory.put(msg.txn, msg.decision);
    txnPartecipants.remove(msg.txn);
    cancelTimeout(msg.txn);
  }



  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(FwdReadMsg.class,  this::onFwdReadMsg)
            .match(FwdWriteMsg.class,  this::onFwdWriteMsg)
            .match(CanCommitMsg.class,  this::onCanCommitMsg)
            .match(FinalDecisionMsg.class,  this::onFinalDecisionMsg)
            .match(TxnDecisionTimeoutMsg.class,  this::onTxnDecisionTimeoutMsg)
            .match(PartecipantsDecisionMsg.class,  this::onPartecipantsDecisionMsg)
            .match(FwdPartecipantsDecisionMsg.class,  this::onFwdPartecipantsDecisionMsg)
            .build();
  }
}
