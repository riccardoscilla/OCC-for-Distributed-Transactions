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
import it.unitn.ds1.TxnServer.FwdParticipantsDecisionMsg;
import it.unitn.ds1.TxnServer.ParticipantsDecisionMsg;

import it.unitn.ds1.TxnSystem;
import it.unitn.ds1.TxnSystem.CrashCoordMsg;
import it.unitn.ds1.TxnSystem.RecoveryMsg;


public class TxnCoordinator extends AbstractActor {
  private final Integer coordinatorId;
  private List<ActorRef> servers;
  private int globID;
  private final Map<TxnId,Set<ActorRef>> OngoingTxn; // custom objects as key of Map
  private final Map<TxnId,List<Boolean>> ServerDecisions;
  private final Map<TxnId,Boolean> txnHistory;
  private final Map<TxnId,Map<Integer,Cancellable>> readTimeout;    // contain a timeout for every transaction waiting for reads
  private final Map<TxnId,Cancellable> voteTimeout;    // contain a timeout for every transaction waiting for server votes
  private final Map<TxnId, String> txnState;           // follow the steps of a transaction (not decided, decided)

  private final Random r;

  private Cancellable crash;    // crash timeout
  enum CrashCoordType {  // type of the next simulated crash
    NONE,
    BeforeDecide,
    AfterDecide
  }
  private CrashCoordType nextCrash;
  private int timeCrashed;

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnCoordinator(int coordinatorId) {
    this.coordinatorId = coordinatorId;
    this.globID = 0;
    this.OngoingTxn = new HashMap<>(); 
    this.ServerDecisions = new HashMap<>();
    this.txnHistory = new HashMap<>();
    this.readTimeout = new HashMap<>();
    this.voteTimeout = new HashMap<>();
    this.txnState = new HashMap<>();
    this.r = new Random();
    this.r.setSeed(TxnSystem.seed*(coordinatorId+1));
    this.nextCrash = CrashCoordType.NONE;
  }

  static public Props props(int coordinatorId) {
    return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(coordinatorId));
  }


  /*-- Message classes ------------------------------------------------------ */

  // send this message to the coordinator at startup to inform it about the servers
  public static class WelcomeCoordMsg implements  Serializable {
    public final List<ActorRef> servers;
    public WelcomeCoordMsg(List<ActorRef> servers) {
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
    public final Set<ActorRef> participants;
    public CanCommitMsg(TxnId txn, Set<ActorRef> participants) {
      this.txn = txn;
      this.participants = participants;
    }
  }

  // ABORT from the coordinator to the server
  public static class AbortMsg implements Serializable {
    public final TxnId txn;
    public final Set<ActorRef> participants;
    public AbortMsg(TxnId txn, Set<ActorRef> participants) {
      this.txn = txn;
      this.participants = participants;
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

  // the coordinator may timeout waiting for the reads of the servers
  public static class TxnReadTimeoutMsg implements Serializable {
    public final TxnId txn;
    public final ActorRef server;
    public TxnReadTimeoutMsg(TxnId txn, ActorRef server) {
      this.txn = txn;
      this.server = server;
    }
  }

  // the coordinator may timeout waiting for the votes of the servers
  public static class TxnVoteTimeoutMsg implements Serializable {
    public final TxnId txn;
    public TxnVoteTimeoutMsg(TxnId txn) {
      this.txn = txn;
    }
  }

  /*-- TxnId class -------------------------------------------------------- */

  public static class TxnId{
    public final ActorRef client;
    public final ActorRef coordinator;
    public int id;
    public int clientId;
    public int coordId;
    public String name;

    public TxnId(ActorRef client, ActorRef coordinator, int id){
      this.client = client;
      this.coordinator = coordinator;
      this.id = id;
      this.clientId = Integer.parseInt(client.path().name().substring(9));
      this.coordId = Integer.parseInt(coordinator.path().name().substring(14));
      this.name = "TxnId@" + coordId + "." + id + "/" + clientId;
    }

    public boolean matches(Object obj){
      if(obj == null || obj.getClass() != this.getClass()) return false;
      TxnId txn = (TxnId) obj;
      return( txn.client.equals(this.client) );
    }

    @Override
    public boolean equals(Object obj){
      if(this == obj) return true;
      if(obj == null || obj.getClass() != this.getClass()) return false;
      TxnId txn = (TxnId) obj;
      return( txn.client.equals(this.client) && txn.coordinator.equals(this.coordinator) && txn.id == this.id );
    }

    @Override
    public int hashCode(){ // map 2 int into a single one
      int a = coordId;
      int b = id;
      // Cantor pairing function
      // return ((a+b) * (a + b + 1) / 2 + a);

      // Szudzik's function
      return (a >= b ? a * a + a + b : a + b * b);
    }
  }

  /*-- Actor methods -------------------------------------------------------- */

  private void sendReal(Object msg, ActorRef sender, ActorRef receiver){
    // getContext().system().scheduler().scheduleOnce(
    //         Duration.create((int)((r.nextDouble())*(TxnSystem.maxDelay - TxnSystem.minDelay)) + TxnSystem.minDelay, TimeUnit.MILLISECONDS),
    //         receiver,
    //         msg, // message sent to myself
    //         getContext().system().dispatcher(),
    //         sender
    // );

    try{
      Thread.sleep((int)((r.nextDouble())*(TxnSystem.maxDelay - TxnSystem.minDelay)) + TxnSystem.minDelay);
    }catch (InterruptedException e){
      System.err.println(e);
    }
    receiver.tell(msg, sender);
  }

  /*---------------------------------------------------------- */
  private void printLog(String logString, String mode){
    Set<String> logModeAllowed = new HashSet<>();
    if(TxnSystem.logMode.equals("Verbose")){
      logModeAllowed.add("Verbose"); logModeAllowed.add("Termination"); logModeAllowed.add("Crash"); logModeAllowed.add("Check"); 
    }   
    if(TxnSystem.logMode.equals("Termination")){
      logModeAllowed.add("Termination"); logModeAllowed.add("Check"); 
    }    
    if(TxnSystem.logMode.equals("Check")){
      logModeAllowed.add("Check"); 
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
    TxnId candidateTxn = new TxnId(sender,getSelf(),0);
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

  //set a decision timeout with delay t for reads 
  private void setReadTimeout(TxnId txn, ActorRef server, Integer key, int t){
    Cancellable timeout = getContext().system().scheduler().scheduleOnce(
            Duration.create(t, TimeUnit.MILLISECONDS),
            getSelf(),
            new TxnReadTimeoutMsg(txn,server), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    readTimeout.putIfAbsent(txn, new HashMap<>());
    /*if(readTimeout.get(txn).get(key) != null){
      if(!readTimeout.get(txn).get(key).isCancelled()){
        readTimeout.get(txn).get(key).cancel();
      }
    }*/
    readTimeout.get(txn).put(key,timeout);

    // printLog("\t"+txn.name+" COORDI " + coordinatorId + " insert timeout " +server.path().name(), "Verbose");

  }

  //set a decision timeout with delay t for votes
  private void setVoteTimeout(TxnId txn, int t){
    Cancellable timeout = getContext().system().scheduler().scheduleOnce(
            Duration.create(t, TimeUnit.MILLISECONDS),
            getSelf(),
            new TxnVoteTimeoutMsg(txn), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    voteTimeout.put(txn, timeout);
  }

  //cancel a certain timeout
  private void cancelReadTimeout(TxnId txn, ActorRef server, Integer key){
    if(readTimeout.get(txn).get(key) != null){
      

      if(!readTimeout.get(txn).get(key).isCancelled()){
        readTimeout.get(txn).get(key).cancel();
        // printLog("\t"+txn.name+" COORDI " + coordinatorId + " cancel timeout of " +server.path().name(), "Verbose");
      }

    }
    // else{
    //   printLog("\t"+txn.name+" COORDI " + coordinatorId +" "+server.path().name() +" Not present ", "Verbose");
    // }
  }

  private void cancelVoteTimeout(TxnId txn){
    if(voteTimeout.get(txn) != null) voteTimeout.get(txn).cancel();
  }

  private void crash(){
    for(TxnId txn : voteTimeout.keySet()){    //delete all pending timeouts
      // cancelReadTimeout(txn);
      cancelVoteTimeout(txn);
    }
    //set a time to wake up from crash
    crash = getContext().system().scheduler().scheduleOnce(
            Duration.create(timeCrashed, TimeUnit.MILLISECONDS),
            getSelf(),
            new RecoveryMsg(), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    getContext().become(crashed()); //ignore following messages
  }

  /*-- Message handlers ----------------------------------------------------- */

  private void onWelcomeCoordMsg(WelcomeCoordMsg msg) {
    this.servers = msg.servers;
  }

  private void onTxnBeginMsg(TxnBeginMsg msg) {
    
    printLog("\tCOORDI " + coordinatorId + " Received txnBegin from " + getSender().path().name(), "Verbose");
    TxnId txn = new TxnId(getSender(),getSelf(),globID);
    OngoingTxn.put(txn,new HashSet<>()); // add new transaction in Ongoing
    ServerDecisions.put(txn,new ArrayList<>()); // add new transaction in ServerDecisions
    txnState.put(txn,CrashCoordType.BeforeDecide.name());
  
    globID = globID + 1;

    sendReal(new TxnAcceptMsg(), getSelf(), getSender()); // send accept txn to client
  }

  /* --------------------------------------------------------------------*/
  // receive Read request from Client, forward to Server
  private void onReadMsg(ReadMsg msg) {
   
    ActorRef server = getServerFromKey(msg.key);
    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    
    printLog("\t" + txn.name + " COORDI " + coordinatorId + " Received Read from " + getSender().path().name() 
             + " - Ask to " + server.path().name(), "Verbose");

    setReadTimeout(txn,server,msg.key,500);
    if(txn != null){
      OngoingTxn.get(txn).add(server);
      sendReal(new FwdReadMsg(msg.key, txn), getSelf(), server); // forward the read to the right server
    } else{
      printLog("\tNO TXN WITH THIS ID", "Verbose");
    }
    
  }

  // receive Read result from Server, forward to Client
  private void onFwdReadResultMsg(FwdReadResultMsg msg) {
    
    printLog("\t" + msg.txn.name + " COORDI " + coordinatorId + " Received value from " + getSender().path().name(), "Verbose");

    cancelReadTimeout(msg.txn,getSender(),msg.key);
    sendReal(new ReadResultMsg(msg.key,msg.value), getSelf(), msg.txn.client);
  
  }

  /* --------------------------------------------------------------------*/
  // receive Write request from Client, forward to Server
  private void onWriteMsg(WriteMsg msg) {
  
    ActorRef server = getServerFromKey(msg.key);

    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());

    printLog("\t" + txn.name + " COORDI " + coordinatorId + " Received Write from " + getSender().path().name() 
             + " - Ask to " + server.path().name(), "Verbose");

    
    if(txn != null){
      sendReal(new FwdWriteMsg(msg.key, msg.value, txn), getSelf(), server); // forward the write to the right server
    } else{
      printLog("\tNO TXN WITH THIS ID", "Verbose");
    }

  }

  /* --------------------------------------------------------------------*/
  private void onTxnEndMsg(TxnEndMsg msg) { 
    
    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender()); 
  
    if(msg.commit) printLog("\t" + txn.name + " COORDI " + coordinatorId + " Received TxnEnd COMMIT from " + getSender().path().name(), "Verbose");
    else printLog("\t" + txn.name + " COORDI " + coordinatorId + " Received TxnEnd ABORT from " + getSender().path().name(), "Verbose");

    if(txn != null){
      Set<ActorRef> participants = new HashSet<ActorRef>(OngoingTxn.get(txn));
      participants.add(getSelf());

      if(msg.commit){ // if received commit, do validation procedure
        printLog("\t" + txn.name + " COORDI "+ coordinatorId + " - Validation with " + printOngoing(OngoingTxn.get(txn)), "Verbose");

        setVoteTimeout(txn, 500); // set a timeout waiting for votes
        for(ActorRef server : OngoingTxn.get(txn)){
          sendReal(new CanCommitMsg(txn, participants), getSelf(), server); // ask to commit
        }

        // check if coordinator should crash (before sending decision)
        if(nextCrash.name().equals(txnState.get(txn))) {
          printLog("\t" + "COORDI " + coordinatorId + " Crashing - " + nextCrash.name(), "Crash");
          crash();
          return;
        }

      } else{ // if received abort, send abort to servers TODO: add false in decision history
        printLog("\t" + txn.name + " COORDI "+ coordinatorId + " - Abort to " + printOngoing(OngoingTxn.get(txn)), "Verbose");
        
        for(ActorRef server : OngoingTxn.get(txn)){
          sendReal(new AbortMsg(txn, participants), getSelf(), server); // tell to abort TODO: send FinalDecisionMsg instead?
        }

        // remove transaction (do not expect a response back to servers)
        OngoingTxn.remove(txn);
        ServerDecisions.remove(txn);
        cancelVoteTimeout(txn);
      }
    } else{
      printLog("\tNO TXN WITH THIS ID", "Verbose");
    }
  }

  private void onServerDecisionMsg(ServerDecisionMsg msg){
    if(OngoingTxn.get(msg.txn) == null) return;   // decision to abort already taken

    printLog("\t" + msg.txn.name + " COORDI " + coordinatorId + " Received Decision from " + getSender().path().name(), "Verbose");

    ServerDecisions.get(msg.txn).add(msg.commit);

    if( Integer.valueOf(ServerDecisions.get(msg.txn).size()).equals(Integer.valueOf(OngoingTxn.get(msg.txn).size())) 
        || !msg.commit){ // if arrives an abort OR all votes are commits then decide
      
      printLog("\t" + msg.txn.name + " COORDI " + coordinatorId + " Decisions "+ printServerDecisions(ServerDecisions.get(msg.txn)), "Verbose");
      
      Boolean finalDecision = getfinalDecision(ServerDecisions.get(msg.txn));
      txnHistory.put(msg.txn, finalDecision);

      txnState.put(msg.txn,CrashCoordType.AfterDecide.name());
      // check if coordinator should crash (after sending decision to one server)
      if(nextCrash.name().equals(txnState.get(msg.txn))) {
        printLog("\t" + "COORDI " + coordinatorId + " Crashing - " + nextCrash.name(), "Crash");
        ActorRef serverToSend = OngoingTxn.get(msg.txn).iterator().next(); // chose a random server to send the result
        sendReal(new FinalDecisionMsg(finalDecision, msg.txn), getSelf(), serverToSend); // send final Decision to only one server
        crash();
        return;
      }

      for(ActorRef server : OngoingTxn.get(msg.txn)){
        sendReal(new FinalDecisionMsg(finalDecision, msg.txn), getSelf(), server); // send final Decision to all servers
      }

      sendReal(new TxnResultMsg(finalDecision), getSelf(), msg.txn.client); // send final Decision 

      // remove transaction
      OngoingTxn.remove(msg.txn);
      ServerDecisions.remove(msg.txn);
      cancelVoteTimeout(msg.txn);

    }
    
  }

  /* --------------------------------------------------------------------*/
  private void onTxnReadTimeoutMsg(TxnReadTimeoutMsg msg) throws InterruptedException {
    if(OngoingTxn.get(msg.txn) == null) return;   // decision to abort already taken

    printLog("\t" + msg.txn.name + " COORDI " + coordinatorId + " Timeout while waiting for read result from " + msg.server.path().name(), "Verbose");

    Boolean finalDecision = false;
    txnHistory.put(msg.txn, finalDecision);
    for(ActorRef server : OngoingTxn.get(msg.txn)){
      sendReal(new FinalDecisionMsg(finalDecision, msg.txn), getSelf(), server); // send final Decision to all servers
    }

    sendReal(new TxnResultMsg(finalDecision), getSelf(), msg.txn.client); // send final Decision 

    // remove transaction
    OngoingTxn.remove(msg.txn);
    ServerDecisions.remove(msg.txn);
  }

  /* --------------------------------------------------------------------*/
  private void onTxnVoteTimeoutMsg(TxnVoteTimeoutMsg msg) throws InterruptedException {
    if(OngoingTxn.get(msg.txn) == null) return;   // decision to abort already taken

    printLog("\t" + msg.txn.name + " COORDI " + coordinatorId + " Timeout while waiting for votes", "Verbose");

    Boolean finalDecision = false;
    txnHistory.put(msg.txn, finalDecision);
    for(ActorRef server : OngoingTxn.get(msg.txn)){
      sendReal(new FinalDecisionMsg(finalDecision, msg.txn), getSelf(), server); // send final Decision to all servers
    }

    sendReal(new TxnResultMsg(finalDecision), getSelf(), msg.txn.client); // send final Decision 

    // remove transaction
    OngoingTxn.remove(msg.txn);
    ServerDecisions.remove(msg.txn);
  }

  /* --------------------------------------------------------------------*/
  private void onCrashCoordMsg(CrashCoordMsg msg) throws InterruptedException {
    printLog("\t" + "COORDI " + coordinatorId + " Received crash msg "+msg.nextCrash.name()+" "+msg.timeCrashed, "Termination");
    nextCrash = msg.nextCrash;
    timeCrashed = msg.timeCrashed;
    // crash(msg.time);
  }

  private void onRecoveryMsg(RecoveryMsg msg) throws InterruptedException{
    printLog("\t" + "COORDI " + coordinatorId + " Recovered after crash", "Crash");
    getContext().become(createReceive());   //restart to handle messages
    nextCrash = CrashCoordType.NONE;

    // Handle crash
    // Depending on the state that the coordinator was in each transaction,
    // do the steps of 2PC cohort recovery
    for(TxnId txn : OngoingTxn.keySet()){

      if(txnState.get(txn).equals(CrashCoordType.BeforeDecide.name())){
        printLog("\t" + txn.name + " COORDI " + coordinatorId + " Sending abort after recovery", "Crash");
        
        Boolean finalDecision = false;
        txnHistory.put(txn, finalDecision);
        
        for(ActorRef server : OngoingTxn.get(txn)){
          sendReal(new FinalDecisionMsg(finalDecision, txn), getSelf(), server); // send final Decision to all servers
        }
        sendReal(new TxnResultMsg(finalDecision), getSelf(), txn.client); // send final Decision 

        // remove transaction
        OngoingTxn.remove(txn);
        ServerDecisions.remove(txn);
      }

      if(txnState.get(txn).equals(CrashCoordType.AfterDecide.name())){
        printLog("\t" + txn.name + " COORDI " + coordinatorId + " Sending decision after recovery", "Crash");
        
        Boolean finalDecision = txnHistory.get(txn);
        for(ActorRef server : OngoingTxn.get(txn)){
          sendReal(new FinalDecisionMsg(finalDecision, txn), getSelf(), server); // send final Decision to all servers
        }
        sendReal(new TxnResultMsg(finalDecision), getSelf(), txn.client); // send final Decision 

        // remove transaction
        OngoingTxn.remove(txn);
        ServerDecisions.remove(txn);
      }
      
    }
  }

  private void onParticipantsDecisionMsg(ParticipantsDecisionMsg msg) throws InterruptedException {
    if(txnHistory.get(msg.txn) != null){  // if the server knows the decision for a certain transaction
      printLog("\t" + msg.txn.name + " COORDI " + coordinatorId + " Forwarding Final Decision (termination protocol) to server " + getSender().path().name(), "Termination");
      sendReal(new FwdParticipantsDecisionMsg(txnHistory.get(msg.txn), msg.txn), getSelf(), getSender());    // comunicate it to the asking server (termination protocol)
    }
  }

  private void onFwdParticipantsDecisionMsg(FwdParticipantsDecisionMsg msg) throws InterruptedException {
    
  }



  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(WelcomeCoordMsg.class,  this::onWelcomeCoordMsg)
            .match(TxnBeginMsg.class,  this::onTxnBeginMsg)
            .match(ReadMsg.class,  this::onReadMsg)
            .match(FwdReadResultMsg.class,  this::onFwdReadResultMsg)
            .match(WriteMsg.class,  this::onWriteMsg)
            .match(TxnEndMsg.class,  this::onTxnEndMsg)
            .match(ServerDecisionMsg.class, this::onServerDecisionMsg)
            .match(TxnReadTimeoutMsg.class,  this::onTxnReadTimeoutMsg)
            .match(TxnVoteTimeoutMsg.class,  this::onTxnVoteTimeoutMsg)
            .match(CrashCoordMsg.class,  this::onCrashCoordMsg)
            .match(ParticipantsDecisionMsg.class,  this::onParticipantsDecisionMsg)
            //.match(FwdParticipantsDecisionMsg.class,  this::onFwdParticipantsDecisionMsg)
            .build();
  }

  public Receive crashed(){   //The only message handled while in crash
    return receiveBuilder()
              .match(RecoveryMsg.class, this::onRecoveryMsg)
              .matchAny(msg -> {})
              .build();
  }
}
