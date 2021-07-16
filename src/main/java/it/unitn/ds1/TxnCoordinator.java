package it.unitn.ds1;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

import it.unitn.ds1.TxnClient.TxnBeginMsg;
import it.unitn.ds1.TxnClient.TxnAcceptMsg;
import it.unitn.ds1.TxnClient.ReadMsg;
import it.unitn.ds1.TxnClient.ReadResultMsg;
import it.unitn.ds1.TxnClient.WriteMsg;
import it.unitn.ds1.TxnClient.TxnEndMsg;
import it.unitn.ds1.TxnClient.TxnResultMsg;

import it.unitn.ds1.TxnServer.FwdReadResultMsg;
import it.unitn.ds1.TxnServer.ServerDecisionMsg;

import it.unitn.ds1.TxnSystem;
import it.unitn.ds1.TxnSystem.CrashMsg;
import it.unitn.ds1.TxnSystem.RecoveryMsg;


public class TxnCoordinator extends AbstractActor {
  private final Integer coordinatorId;
  private List<ActorRef> servers;
  private int globID;
  private String logMode = "Check";
  private final Map<TxnId,Set<ActorRef>> OngoingTxn; // custom objects as key of Map
  private final Map<TxnId,List<Boolean>> ServerDecisions;
  private final Map<TxnId, Cancellable> voteTimeout;    // contain a timeout for every transaction waiting for server votes
  private Cancellable crash;      //crash timeout

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnCoordinator(int coordinatorId) {
    this.coordinatorId = coordinatorId;
    this.globID = 0;
    this.OngoingTxn = new HashMap<>(); 
    this.ServerDecisions = new HashMap<>(); 
    this.voteTimeout = new HashMap<>();
  }

  static public Props props(int coordinatorId) {
    return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(coordinatorId));
  }


  /*-- Message classes ------------------------------------------------------ */

  // send this message to the coordinator at startup to inform it about the servers
  public static class WelcomeMsg2 implements  Serializable {
    public final List<ActorRef> servers;
    public WelcomeMsg2(List<ActorRef> servers) {
      this.servers = Collections.unmodifiableList(new ArrayList<>(servers));
    }
  }

  // READ request from the coordinator to the server
  public static class FwdReadMsg implements Serializable {
    public final Integer key; // the key of the value to read
    public final TxnId txn;
    public FwdReadMsg(int key, TxnId txn) {
      this.key = key;
      this.txn = txn;
    }
  }

  // WRITE request from the coordinator to the server
  public static class FwdWriteMsg implements Serializable {
    public final Integer key; // the key of the value to write
    public final Integer value; // the new value to write
    public final TxnId txn;
    public FwdWriteMsg(int key, int value, TxnId txn) {
      this.key = key;
      this.value = value;
      this.txn = txn;
    }
  }

  // COMMIT request from the coordinator to the server
  public static class CanCommitMsg implements Serializable {
    public final TxnId txn;
    public final Set<ActorRef> partecipants;
    public CanCommitMsg(TxnId txn, Set<ActorRef> partecipants) {
      this.txn = txn;
      this.partecipants = partecipants;
    }
  }

  public static class FinalDecisionMsg implements Serializable {
    public final Boolean decision;
    public final TxnId txn;
    public FinalDecisionMsg(Boolean decision, TxnId txn) {
      this.decision = decision;
      this.txn = txn;
    }
  }

  // the coordinator may timeout waiting for the votes of the servers
  public static class TxnVoteTimeoutMsg implements Serializable {
    public final TxnId txn;
    public TxnVoteTimeoutMsg(TxnId txn) {
      this.txn = txn;
    }
  }

  /*-- Actor methods -------------------------------------------------------- */

  private void sendReal(Object msg, ActorRef sender, ActorRef receiver){
    try{
      Thread.sleep((int)((Math.random())*(TxnSystem.maxDelay - TxnSystem.minDelay)) + TxnSystem.minDelay);
    }catch (InterruptedException e){
      System.err.println(e);
    }
    receiver.tell(msg, sender);
  }

  public static class TxnId{
    public final ActorRef client;
    public int id;
    public int coordinator;

    public TxnId(ActorRef client, int id, int coordinator){
      this.client = client;
      this.id = id;
      this.coordinator = coordinator;
    }

    public boolean matches(Object obj){
      if(obj == null || obj.getClass() != this.getClass()) return false;
      TxnId txn = (TxnId) obj;
      return( txn.client.equals(this.client) );
    }

    public String getName(){
      return "TxnId@" + coordinator + "." + id;
    }

    @Override
    public boolean equals(Object obj){
      if(this == obj) return true;
      if(obj == null || obj.getClass() != this.getClass()) return false;
      TxnId txn = (TxnId) obj;
      return( txn.client.equals(this.client) && txn.coordinator == this.coordinator && txn.id == this.id );
    }

    @Override
    public int hashCode(){ // map 2 int into a single one
      int a = coordinator;
      int b = id;
      // Cantor pairing function
      // return ((a+b) * (a + b + 1) / 2 + a);

      // Szudzik's function
      return (a >= b ? a * a + a + b : a + b * b);
    }
  }

  // print log 
  private void printLog(String logString, String mode){
    Set<String> logModeAllowed = new HashSet<>();
    if(logMode.equals("Verbose")){
      logModeAllowed.add("Verbose"); logModeAllowed.add("Check"); 
    }    

    if(logModeAllowed.contains(mode)){
      System.out.println(logString);
    }
  }

  // get Server that is in charged of the given key
  private ActorRef getServerFromKey(Integer key){
    return servers.get(key/10);
  }

  private TxnId bindRequestOngoing(ActorRef sender){
    TxnId candidateTxn = new TxnId(sender,0,coordinatorId);
    for(TxnId key : OngoingTxn.keySet()){
      if(key.matches(candidateTxn)){
        return key;
      }
    }
    return null;
  }

  private String printOngoing(Set<ActorRef> s){
    String res = "";
    for(ActorRef i : s){
      res = res + i.path().name() + " ";
    }
    return res;
  }

  private void printFullOngoing(){
    OngoingTxn.entrySet().forEach(entry -> {
      System.out.println(entry.getKey() + " " );
    });
  }

  private String printServerDecisions(List<Boolean> s){
    String res = "[";
    for(Boolean i : s){
      if(i) res = res + "True ";
      else res = res + "False ";
    }
    res = res.substring(0,res.length()-1)+"]";
    return res;
  }

  private Boolean getfinalDecision(List<Boolean> decisions){
    for(Boolean d : decisions){
      if(!d) return false;
    }
    return true;
  }

  //set a decision timeout with delay t
  private void setTimeout(TxnId txn, int t){
    Cancellable timeout = getContext().system().scheduler().scheduleOnce(
            Duration.create(t, TimeUnit.MILLISECONDS),
            getSelf(),
            new TxnVoteTimeoutMsg(txn), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    voteTimeout.put(txn, timeout);
  }

  //cancel a certain timeout
  private void cancelTimeout(TxnId txn){
    if(voteTimeout.get(txn) != null) voteTimeout.get(txn).cancel();
  }

  private void crash(int time){
    for(TxnId txn : voteTimeout.keySet()){    //delete all pending timeouts
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

  private void onWelcomeMsg2(WelcomeMsg2 msg) {
    this.servers = msg.servers;
  }

  private void onTxnBeginMsg(TxnBeginMsg msg) {
    
    printLog("\tCOORDI " + coordinatorId + " Received txnBegin from " + getSender().path().name(), "Verbose");
    
    OngoingTxn.put(new TxnId(getSender(),globID,coordinatorId),new HashSet<>()); // add new transaction in Ongoing
    ServerDecisions.put(new TxnId(getSender(),globID,coordinatorId),new ArrayList<>()); // add new transaction in ServerDecisions
    globID = globID + 1;

    // printFullOngoing();
    
    sendReal(new TxnAcceptMsg(), getSelf(), getSender()); // send accept txn to client
  }

  /* --------------------------------------------------------------------*/
  // receive Read request from Client, forward to Server
  private void onReadMsg(ReadMsg msg) {
   
    ActorRef server = getServerFromKey(msg.key);
    
    printLog("\tCOORDI " + coordinatorId + " Received Read from " + getSender().path().name() 
             + " - Ask to " + server.path().name(), "Verbose");

    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    if(txn != null){
      sendReal(new FwdReadMsg(msg.key, txn), getSelf(), server); // forward the read to the right server
    } else{
      printLog("\tNO TXN WITH THIS ID", "Verbose");
    }
    
  }

  // receive Read result from Server, forward to Client
  private void onFwdReadResultMsg(FwdReadResultMsg msg) {
    
    printLog("\tCOORDI " + coordinatorId + " Received value from " + getSender().path().name(), "Verbose");

    sendReal(new ReadResultMsg(msg.key,msg.value), getSelf(), msg.txn.client);
  
  }

  /* --------------------------------------------------------------------*/
  // receive Write request from Client, forward to Server
  private void onWriteMsg(WriteMsg msg) {
  
    ActorRef server = getServerFromKey(msg.key);

    printLog("\tCOORDI " + coordinatorId + " Received Write from " + getSender().path().name() 
             + " - Ask to " + server.path().name(), "Verbose");

    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    if(txn != null){
      OngoingTxn.get(txn).add(server);
      sendReal(new FwdWriteMsg(msg.key, msg.value, txn), getSelf(), server); // forward the write to the right server
    } else{
      printLog("\tNO TXN WITH THIS ID", "Verbose");
    }

  }

  /* --------------------------------------------------------------------*/
  private void onTxnEndMsg(TxnEndMsg msg) {
    
    printLog("\tCOORDI " + coordinatorId + " Received TxnEnd from " + getSender().path().name(), "Verbose");
    
    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    if(txn != null){
      // Set<ActorRef> serverToCommit = OngoingTxn.get(txn);
      printLog("\tCOORDI "+ coordinatorId + " - Validation with " + printOngoing(OngoingTxn.get(txn)), "Verbose");

      setTimeout(txn, 500);    // set a timeout waiting for votes
      for(ActorRef server : OngoingTxn.get(txn)){
        sendReal(new CanCommitMsg(txn, OngoingTxn.get(txn)), getSelf(), server); // ask to commit
      }
    } else{
      printLog("\tNO TXN WITH THIS ID", "Verbose");
    }
  }

  private void onServerDecisionMsg(ServerDecisionMsg msg){
    if(OngoingTxn.get(msg.txn) == null) return;   // decision to abort already taken

    printLog("\tCOORDI " + coordinatorId + " Received Decision from " + getSender().path().name(), "Verbose");

    ServerDecisions.get(msg.txn).add(msg.commit);

    if( Integer.valueOf(ServerDecisions.get(msg.txn).size())
        .equals(Integer.valueOf(OngoingTxn.get(msg.txn).size())) || !msg.commit){ // if arrives an abort OR all votes are commits then decide
      
      printLog("\tCOORDI " + coordinatorId + " Decisions "+ printServerDecisions(ServerDecisions.get(msg.txn)), "Verbose");
      
      Boolean finalDecision = getfinalDecision(ServerDecisions.get(msg.txn));
      for(ActorRef server : OngoingTxn.get(msg.txn)){
        sendReal(new FinalDecisionMsg(finalDecision, msg.txn), getSelf(), server); // send final Decision to all servers
      }

      sendReal(new TxnResultMsg(finalDecision), getSelf(), msg.txn.client); // send final Decision 

      // remove transaction
      OngoingTxn.remove(msg.txn);
      ServerDecisions.remove(msg.txn);
      cancelTimeout(msg.txn);

    }
    
  }

  private void onTxnVoteTimeoutMsg(TxnVoteTimeoutMsg msg) throws InterruptedException {
    if(OngoingTxn.get(msg.txn) == null) return;   // decision to abort already taken

    printLog("\tCOORDI " + coordinatorId + " Timeout while waiting for votes", "Verbose");

    Boolean finalDecision = false;
    for(ActorRef server : OngoingTxn.get(msg.txn)){
      sendReal(new FinalDecisionMsg(finalDecision, msg.txn), getSelf(), server); // send final Decision to all servers
    }

    sendReal(new TxnResultMsg(finalDecision), getSelf(), msg.txn.client); // send final Decision 

    // remove transaction
    OngoingTxn.remove(msg.txn);
    ServerDecisions.remove(msg.txn);
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
            .match(WelcomeMsg2.class,  this::onWelcomeMsg2)
            .match(TxnBeginMsg.class,  this::onTxnBeginMsg)
            .match(ReadMsg.class,  this::onReadMsg)
            .match(FwdReadResultMsg.class,  this::onFwdReadResultMsg)
            .match(WriteMsg.class,  this::onWriteMsg)
            .match(TxnEndMsg.class,  this::onTxnEndMsg)
            .match(ServerDecisionMsg.class, this::onServerDecisionMsg)
            .match(TxnVoteTimeoutMsg.class,  this::onTxnVoteTimeoutMsg)
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
