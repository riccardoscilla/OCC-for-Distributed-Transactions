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
import it.unitn.ds1.TxnCoordinator.AbortMsg;
import it.unitn.ds1.TxnCoordinator.FinalDecisionMsg;

import it.unitn.ds1.TxnSystem;
import it.unitn.ds1.TxnSystem.CrashMsg;
import it.unitn.ds1.TxnSystem.RecoveryMsg;

public class TxnServer extends AbstractActor {
  private final Integer serverId;
  private final Map<Integer, Integer[]> dataStore;
  private String logMode = "Verbose";
  private final Map<TxnId, Set<Integer[]>> workSpace;
  private final Map<TxnId, Set<ActorRef>> txnPartecipants;  // map transactions with all its partecipants
  private final Map<TxnId, Boolean> txnHistory;             // save an history of all the past transactions
  private final Map<TxnId, Cancellable> decisionTimeout;    // contain a timeout for every transaction waiting for a decision
  private Cancellable crash;    //crash timeout
  private final Random r;

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnServer(int serverId) {
    this.serverId = serverId;
    this.dataStore = new TreeMap<>();
    this.workSpace = new HashMap<>();
    this.txnPartecipants = new HashMap<>();
    this.txnHistory = new HashMap<>();
    this.decisionTimeout = new HashMap<>();
    this.r = new Random();
    this.r.setSeed(TxnSystem.seed*(serverId+1));
    initDataStore();
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
  // print log 
  private void printLog(String logString, String mode){
    Set<String> logModeAllowed = new HashSet<>();
    if(logMode.equals("Verbose")){
      logModeAllowed.add("Verbose"); logModeAllowed.add("Check"); 
    }   
    if(logMode.equals("Check")){
      logModeAllowed.add("Check"); 
    } 

    if(logModeAllowed.contains(mode)){
      System.out.println(logString);
    }
  }
  
  // send messages with simulated network delays
  private void sendReal(Object msg, ActorRef sender, ActorRef receiver){
    try{
      Thread.sleep((int)((r.nextDouble())*(TxnSystem.maxDelay - TxnSystem.minDelay)) + TxnSystem.minDelay);
    }catch (InterruptedException e){
      System.err.println(e);
    }
    receiver.tell(msg, sender);
  }

  // get value for a given key
  // if the key has already been touched in this transaction,
  // return the value from the workspace (otw there is inconsistency)
  // else return the value from the datastore
  private Integer getValueFromKey(Integer key, Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value, r/w}
      if(c[0].equals(key)){
        return c[2];
      }
    }
    return dataStore.get(key)[1]; // dataStore.get(key) = {version, value, lock}
  }

  // get version for a given key (datastore only)
  private Integer getVersionFromKey(Integer key){
    return dataStore.get(key)[0];
  }
  
  // add a new read operation in the txn workspace
  // if the key is not already in the txn workspace
  // put a read operation (last value = 0) with the current version found
  // else do nothing
  private void addWorkspace(Integer key, Integer version, Integer value, Set<Integer[]> changes){
    Boolean inWorkspace = false;
    for(Integer[] c : changes){
      if(c[0].equals(key)){
        inWorkspace = true;
        break;
      }
    }
    if(!inWorkspace){ 
      changes.add(new Integer[] {key, version, value, 0});
    }    
  }

  // update the workspace of a given transaction
  // search for the read operation put before
  // change the version (only in the first change from read/write), 
  //        the value 
  //        set write
  private void updateWorkspace(Integer key, Integer value, Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value, r/w}
      if(c[0].equals(key)){
        if(c[3] == 0){
          c[1] = c[1] + 1;
        }
        c[2] = value;
        c[3] = 1;
        break;
      }
    }
  }

  // print workspace for debugging
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
    for(Integer[] c : changes){ // c = {key, version, value, r/w}
                                // dataStore.get(c[0]) = {version, value, lock}
      
      // if the lock on the key is already acquired (set to 1)
      // or the version of the change is not the next one
      // return false; else continue
      if( dataStore.get(c[0])[2].equals(1) ||
        !dataStore.get(c[0])[0].equals(c[1]-1) ){
        return false;
      }

    }
    // lock only after being sure it can commit
    LockChanges(changes);
    return true;
  }

  // apply changes in the workspace only for writes operations
  private void ApplyChanges(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value, r/w}
      if(c[3].equals(1)){ 
        // dataStore.replace(c[0],new Integer[] {c[1],c[2],0});
        dataStore.put(c[0],new Integer[] {c[1],c[2],0});
      }
    }
  }

  private void FreeLocks(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value, r/w}
      dataStore.get(c[0])[2] = 0;
    }
  }

  private void LockChanges(Set<Integer[]> changes){
    for(Integer[] c : changes){ // c = {key, version, value, r/w}
      dataStore.get(c[0])[2] = 1;
    }
  }

  // print the sum of the values of the datastore
  // used to check the correctness
  private String printCheck(TxnId txnId){
    String res = "[CHECK] ";
    res = res + txnId.name + " " + getSelf().path().name() + " ";
    Integer sum = 0;
    for(Integer key : dataStore.keySet()){
      sum = sum + dataStore.get(key)[1];
    }
    res = res + sum;
    return res;
  }

  // start the termination protocol asking all the partecifants 
  // if they have received a decision
  private void terminationProtocol(TxnId txn){
    for(ActorRef i : txnPartecipants.get(txn)){
      sendReal(new PartecipantsDecisionMsg(txn), getSelf(), i);
    }
    setTimeout(txn, 500);
  }

  // set a decision timeout with delay t
  private void setTimeout(TxnId txn, int t){
    Cancellable timeout = getContext().system().scheduler().scheduleOnce(
            Duration.create(t, TimeUnit.MILLISECONDS),
            getSelf(),
            new TxnDecisionTimeoutMsg(txn), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    decisionTimeout.put(txn, timeout);
  }

  // cancel a certain timeout
  private void cancelTimeout(TxnId txn){
    if(decisionTimeout.get(txn) != null) decisionTimeout.get(txn).cancel();
  }

  private void crash(int time){
    for(TxnId txn : decisionTimeout.keySet()){    //delete all pending timeouts
      cancelTimeout(txn);
    }
    //set a time to wake up from crash
    crash = getContext().system().scheduler().scheduleOnce(
            Duration.create(time, TimeUnit.MILLISECONDS),
            getSelf(),
            new RecoveryMsg(), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    getContext().become(crashed()); //ignore following messages
  }

  /*-- Message handlers ----------------------------------------------------- */

  private void onFwdReadMsg(FwdReadMsg msg) {

    workSpace.putIfAbsent(msg.txn, new HashSet<>());
    Integer value = getValueFromKey(msg.key, workSpace.get(msg.txn));
    Integer version = getVersionFromKey(msg.key);

    addWorkspace(msg.key, version, value, workSpace.get(msg.txn));   

    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Received Read from " + getSender().path().name()
             + " - WS "+ printWorkspace(workSpace.get(msg.txn)), "Verbose");

    sendReal(new FwdReadResultMsg(msg.key, value, msg.txn), getSelf(), getSender());

  }

  private void onFwdWriteMsg(FwdWriteMsg msg) {

    updateWorkspace(msg.key, msg.value, workSpace.get(msg.txn));

    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Received Write from " + getSender().path().name() 
             + " - WS "+ printWorkspace(workSpace.get(msg.txn)), "Verbose");

  }

  private void onCanCommitMsg(CanCommitMsg msg){
    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Received Commit Request "
             + " - WS "+printWorkspace(workSpace.get(msg.txn)), "Verbose");

    Boolean canChange = checkIfCanChange(workSpace.get(msg.txn));

    if(canChange){ 
      printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Can Change", "Verbose");
      setTimeout(msg.txn, 500); // start a timeout waiting for a decision
      txnPartecipants.put(msg.txn, msg.partecipants); // save the set of partecipants to the transaction (for termination protocol)
    } 
    else{   // if the server send an abort vote it can immediatly abort (coordinator decision will be abort)
      printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Can't Change", "Verbose");
      workSpace.remove(msg.txn);    // clear the workspace
      txnHistory.put(msg.txn, canChange); // save the decision in the history
    }

    sendReal(new ServerDecisionMsg(canChange, msg.txn), getSelf(), getSender());   // send the vote

  }

  private void onAbortMsg(AbortMsg msg){
    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Received ABORT", "Verbose");

    FreeLocks(workSpace.get(msg.txn)); // free the locks that may have been acquired
    
    // clear workspace and other transaction info
    workSpace.remove(msg.txn);
    txnPartecipants.remove(msg.txn);
    cancelTimeout(msg.txn);
    txnHistory.put(msg.txn, false);  // add the decision to the history (always abort)

    printLog(printCheck(msg.txn),"Check");
  }

  private void onFinalDecisionMsg(FinalDecisionMsg msg){
    if(workSpace.get(msg.txn) == null) { // if already aborted do nothing
      printLog(printCheck(msg.txn),"Check");
      return; 
    } 
    
    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Received Final Decision ", "Verbose");

    if( msg.decision ) ApplyChanges(workSpace.get(msg.txn));
    else FreeLocks(workSpace.get(msg.txn)); // free the locks that may have been acquired

    // clear workspace and other transaction info
    workSpace.remove(msg.txn);
    txnPartecipants.remove(msg.txn);
    cancelTimeout(msg.txn);
    txnHistory.put(msg.txn, msg.decision);  // add the decision to the history

    printLog(printCheck(msg.txn),"Check");
  }

  private void onTxnDecisionTimeoutMsg(TxnDecisionTimeoutMsg msg) throws InterruptedException {
    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Timeout on Final Decision ", "Verbose");
    if(txnHistory.get(msg.txn) == null) terminationProtocol(msg.txn);   // when the decision message timeouts the server start the termination protocol
  }

  private void onPartecipantsDecisionMsg(PartecipantsDecisionMsg msg) throws InterruptedException {
    if(txnHistory.get(msg.txn) != null){  // if the server knows the decision for a certain transaction
      printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Forwardinf Final Decision (termination protocol) to server " + getSender().path().name(), "Verbose");
      sendReal(new FwdPartecipantsDecisionMsg(txnHistory.get(msg.txn), msg.txn), getSelf(), getSender());    // comunicate it to the asking server (termination protocol)
    }
  }

  private void onFwdPartecipantsDecisionMsg(FwdPartecipantsDecisionMsg msg) throws InterruptedException {
    if(workSpace.get(msg.txn) == null) return;  // if already decided, do nothing

    printLog("\t\t" + msg.txn.name + " SERVER " + serverId + " Received Final Decision (termination protocol)", "Verbose");
    
    if( msg.decision ) ApplyChanges(workSpace.get(msg.txn));
    else FreeLocks(workSpace.get(msg.txn)); // free the locks that may have been acquired

    // clear workspace and other transaction info
    workSpace.remove(msg.txn);
    txnPartecipants.remove(msg.txn);
    cancelTimeout(msg.txn);
    txnHistory.put(msg.txn, msg.decision);  // add the decision to the history
  }

  private void onCrashMsg(CrashMsg msg) throws InterruptedException {
    crash(msg.time);
  }

  private void onRecoveryMsg(RecoveryMsg msg) throws InterruptedException{
    getContext().become(createReceive());   //restart to handle messages

    //Handle crash
  }




  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(FwdReadMsg.class,  this::onFwdReadMsg)
            .match(FwdWriteMsg.class,  this::onFwdWriteMsg)
            .match(CanCommitMsg.class,  this::onCanCommitMsg)
            .match(AbortMsg.class, this::onAbortMsg)
            .match(FinalDecisionMsg.class,  this::onFinalDecisionMsg)
            .match(TxnDecisionTimeoutMsg.class,  this::onTxnDecisionTimeoutMsg)
            .match(PartecipantsDecisionMsg.class,  this::onPartecipantsDecisionMsg)
            .match(FwdPartecipantsDecisionMsg.class,  this::onFwdPartecipantsDecisionMsg)
            .match(CrashMsg.class,  this::onCrashMsg)
            .build();
  }

  public Receive crashed(){   //The only message handled while in crash
    return receiveBuilder()
              .match(RecoveryMsg.class, this::onRecoveryMsg)
              .matchAny(msg -> {})
              .build();
  }
}

  