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

import it.unitn.ds1.TxnServer.FwdReadResultMsg;

public class TxnCoordinator extends AbstractActor {
  private final Integer coordinatorId;

  private List<ActorRef> servers;
  private int globID;
  private final Map<TxnId,Set<ActorRef>> OngoingTxn; // custom objects as key of Map


  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnCoordinator(int coordinatorId) {
    this.coordinatorId = coordinatorId;
    this.globID = 0;
    this.OngoingTxn = new HashMap<>(); 
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

  /*-- Actor methods -------------------------------------------------------- */

  public static class TxnId{
    public final ActorRef client;
    public int id;

    public TxnId(ActorRef client, int id){
      this.client = client;
      this.id = id;
    }

    @Override
    public boolean equals(Object obj){
      if(this == obj) return true;
      if(obj == null || obj.getClass() != this.getClass()) return false;
      TxnId txn = (TxnId) obj;
      return( txn.client.equals(this.client) );
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

  /*-- Message handlers ----------------------------------------------------- */

  private void onWelcomeMsg2(WelcomeMsg2 msg) {
    this.servers = msg.servers;
    // System.out.println(servers);
  }

  private void onTxnBeginMsg(TxnBeginMsg msg) {
    System.out.println("[INFO] COORDI " + coordinatorId + " Received txnBegin from " + getSender().path().name());
    
    OngoingTxn.put(new TxnId(getSender(),globID),new HashSet<>()); // add new transaction in Ongoing
    globID = globID + 1;
    
    getSender().tell(new TxnAcceptMsg(), getSelf()); // send accept txn to client
  }

  /* --------------------------------------------------------------------*/
  // receive Read request from Client, forward to Server
  private void onReadMsg(ReadMsg msg) {
    System.out.println("[INFO] COORDI " + coordinatorId + " Received Read from " + getSender().path().name());
    
    ActorRef server = getServerFromKey(msg.key);
    System.out.println("[INFO] COORDI " + coordinatorId + " Server to ask " + server.path().name());

    // bind the current request to the OngoingTxn
    TxnId candidateTxn = new TxnId(getSender(),0);
    boolean ok = false;
    for(TxnId key : OngoingTxn.keySet()){
      if(key.equals(candidateTxn)){
        server.tell(new FwdReadMsg(msg.key, key), getSelf()); // forward the read to the right server
        ok = true;
      }
    }
    if(!ok){
      System.out.println("[INFO] NO TXN WITH THIS ID");
    }
    
  }

  // receive Read result from Server, forward to Client
  private void onFwdReadResultMsg(FwdReadResultMsg msg) {
    System.out.println("[INFO] COORDI " + coordinatorId + " Received value from " + getSender().path().name());

    msg.txn.client.tell(new ReadResultMsg(msg.key,msg.value), getSelf());
  }

  /* --------------------------------------------------------------------*/
  // receive Write request from Client, forward to Server
  // private void onWriteMsg(WriteMsg msg) {
  //   System.out.println("[INFO] COORDI " + coordinatorId + " Received Write from " + getSender().path().name());

  //   ActorRef server = getServerFromKey(msg.key);
  //   System.out.println("[INFO] COORDI " + coordinatorId + " Server to ask " + server.path().name());

  //   server.tell(new FwdWriteMsg(msg.key, msg.value, getSender()), getSelf());
  // }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(WelcomeMsg2.class,  this::onWelcomeMsg2)
            .match(TxnBeginMsg.class,  this::onTxnBeginMsg)
            .match(ReadMsg.class,  this::onReadMsg)
            .match(FwdReadResultMsg.class,  this::onFwdReadResultMsg)
            // .match(WriteMsg.class,  this::onWriteMsg)
            .build();
  }
}
