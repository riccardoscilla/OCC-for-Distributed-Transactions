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
import it.unitn.ds1.TxnServer.FwdReadResultMsg;
import it.unitn.ds1.TxnClient.WriteMsg;
import it.unitn.ds1.TxnClient.TxnEndMsg;
import it.unitn.ds1.TxnServer.ServerDecisionMsg;
import it.unitn.ds1.TxnClient.TxnResultMsg;


public class TxnCoordinator extends AbstractActor {
  private final Integer coordinatorId;
  private List<ActorRef> servers;
  private int globID;
  private final Map<TxnId,Set<ActorRef>> OngoingTxn; // custom objects as key of Map
  private final Map<TxnId,List<Boolean>> ServerDecisions;

  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnCoordinator(int coordinatorId) {
    this.coordinatorId = coordinatorId;
    this.globID = 0;
    this.OngoingTxn = new HashMap<>(); 
    this.ServerDecisions = new HashMap<>(); 
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

  /*-- Actor methods -------------------------------------------------------- */

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

    @Override
    public boolean equals(Object obj){
      if(this == obj) return true;
      if(obj == null || obj.getClass() != this.getClass()) return false;
      TxnId txn = (TxnId) obj;
      return( txn.client.equals(this.client) && txn.coordinator == this.coordinator && txn.id == this.id );
    }

    @Override
    public int hashCode(){
      return this.id;
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

  /*-- Message handlers ----------------------------------------------------- */

  private void onWelcomeMsg2(WelcomeMsg2 msg) {
    this.servers = msg.servers;
  }

  private void onTxnBeginMsg(TxnBeginMsg msg) {
    
    System.out.println("\tCOORDI " + coordinatorId + " Received txnBegin from " + getSender().path().name());
    
    OngoingTxn.put(new TxnId(getSender(),globID,coordinatorId),new HashSet<>()); // add new transaction in Ongoing
    ServerDecisions.put(new TxnId(getSender(),globID,coordinatorId),new ArrayList<>()); // add new transaction in ServerDecisions
    globID = globID + 1;

    // printFullOngoing();
    
    getSender().tell(new TxnAcceptMsg(), getSelf()); // send accept txn to client
  }

  /* --------------------------------------------------------------------*/
  // receive Read request from Client, forward to Server
  private void onReadMsg(ReadMsg msg) {
   
    ActorRef server = getServerFromKey(msg.key);
    
    System.out.println("\tCOORDI " + coordinatorId + " Received Read from " + getSender().path().name() 
                      + " - Ask to " + server.path().name());

    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    if(txn != null){
      server.tell(new FwdReadMsg(msg.key, txn), getSelf()); // forward the read to the right server
    } else{
      System.out.println("\tNO TXN WITH THIS ID");
    }
    
  }

  // receive Read result from Server, forward to Client
  private void onFwdReadResultMsg(FwdReadResultMsg msg) {
    
    System.out.println("\tCOORDI " + coordinatorId + " Received value from " + getSender().path().name());

    msg.txn.client.tell(new ReadResultMsg(msg.key,msg.value), getSelf());
  
  }

  /* --------------------------------------------------------------------*/
  // receive Write request from Client, forward to Server
  private void onWriteMsg(WriteMsg msg) {
  
    ActorRef server = getServerFromKey(msg.key);

    System.out.println("\tCOORDI " + coordinatorId + " Received Write from " + getSender().path().name() 
                      + " - Ask to " + server.path().name());

    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    if(txn != null){
      OngoingTxn.get(txn).add(server);
      server.tell(new FwdWriteMsg(msg.key, msg.value, txn), getSelf()); // forward the write to the right server
    } else{
      System.out.println("\tNO TXN WITH THIS ID");
    }

  }

  /* --------------------------------------------------------------------*/
  private void onTxnEndMsg(TxnEndMsg msg) {
    
    System.out.println("\tCOORDI " + coordinatorId + " Received TxnEnd from " + getSender().path().name());
    
    // bind the current request to the OngoingTxn
    TxnId txn = bindRequestOngoing(getSender());
    if(txn != null){
      // Set<ActorRef> serverToCommit = OngoingTxn.get(txn);
      System.out.println("\tCOORDI "+ coordinatorId 
                        + " - Validation with " + printOngoing(OngoingTxn.get(txn)));
      for(ActorRef server : OngoingTxn.get(txn)){
        server.tell(new CanCommitMsg(txn, OngoingTxn.get(txn)), getSelf()); // ask to commit
      }
    } else{
      System.out.println("\tNO TXN WITH THIS ID");
    }
  }

  private void onServerDecisionMsg(ServerDecisionMsg msg){
    if(OngoingTxn.get(msg.txn) == null) return;   // decision to abort already taken

    System.out.println("\tCOORDI " + coordinatorId + " Received Decision from " + getSender().path().name());

    ServerDecisions.get(msg.txn).add(msg.commit);

    if( Integer.valueOf(ServerDecisions.get(msg.txn).size())
        .equals(Integer.valueOf(OngoingTxn.get(msg.txn).size())) || !msg.commit){
      System.out.println("\tCOORDI " + coordinatorId 
                        + " Decisions "+ printServerDecisions(ServerDecisions.get(msg.txn)));
      
      Boolean finalDecision = getfinalDecision(ServerDecisions.get(msg.txn));
      for(ActorRef server : OngoingTxn.get(msg.txn)){
        server.tell(new FinalDecisionMsg(finalDecision, msg.txn), getSelf()); // send final Decision
      }

      msg.txn.client.tell(new TxnResultMsg(msg.commit), getSelf()); // send final Decision 

      // remove transaction
      OngoingTxn.remove(msg.txn);
      ServerDecisions.remove(msg.txn);

    }
    
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
            .build();
  }
}
