package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

public class TxnClient extends AbstractActor {
  private static final double COMMIT_PROBABILITY = 0.8;
  private static final double WRITE_PROBABILITY = 0.5;
  private static final int MIN_TXN_LENGTH = 20;
  private static final int MAX_TXN_LENGTH = 40;
  private static final int RAND_LENGTH_RANGE = MAX_TXN_LENGTH - MIN_TXN_LENGTH + 1;

  private final Integer clientId;
  private List<ActorRef> coordinators;

  // the maximum key associated to items of the store
  private Integer maxKey;

  // keep track of the number of TXNs (attempted, successfully committed)
  private Integer numAttemptedTxn;
  private Integer numCommittedTxn;

  // TXN operation (move some amount from a value to another)
  private Boolean acceptedTxn;
  private ActorRef currentCoordinator;
  private Integer firstKey, secondKey;
  private Integer firstValue, secondValue;
  private Integer numOpTotal;
  private Integer numOpDone;
  private Cancellable acceptTimeout;
  private final Random r;

  /*-- Actor constructor ---------------------------------------------------- */

  public TxnClient(int clientId) {
    this.clientId = clientId;
    this.numAttemptedTxn = 0;
    this.numCommittedTxn = 0;
    this.r = new Random();
  }

  static public Props props(int clientId) {
    return Props.create(TxnClient.class, () -> new TxnClient(clientId));
  }

  /*-- Message classes ------------------------------------------------------ */

  // send this message to the client at startup to inform it about the coordinators and the keys
  public static class WelcomeMsg implements  Serializable {
    public final Integer maxKey;
    public final List<ActorRef> coordinators;
    public WelcomeMsg(int maxKey, List<ActorRef> coordinators) {
      this.maxKey = maxKey;
      this.coordinators = Collections.unmodifiableList(new ArrayList<>(coordinators));
    }
  }

  // stop the client
  public static class StopMsg implements Serializable {}

  // message the client sends to a coordinator to begin the TXN
  public static class TxnBeginMsg implements Serializable {
    public final Integer clientId;
    public TxnBeginMsg(int clientId) {
      this.clientId = clientId;
    }
  }

  // reply from the coordinator receiving TxnBeginMsg
  public static class TxnAcceptMsg implements Serializable {}

  // the client may timeout waiting for TXN begin confirmation (TxnAcceptMsg)
  public static class TxnAcceptTimeoutMsg implements Serializable {}

  // message the client sends to a coordinator to end the TXN;
  // it may ask for commit (with probability COMMIT_PROBABILITY), or abort
  public static class TxnEndMsg implements Serializable {
    public final Integer clientId;
    public final Boolean commit; // if false, the transaction should abort
    public TxnEndMsg(int clientId, boolean commit) {
      this.clientId = clientId;
      this.commit = commit;
    }
  }

  // READ request from the client to the coordinator
  public static class ReadMsg implements Serializable {
    public final Integer clientId;
    public final Integer key; // the key of the value to read
    public ReadMsg(int clientId, int key) {
      this.clientId = clientId;
      this.key = key;
    }
  }

  // WRITE request from the client to the coordinator
  public static class WriteMsg implements Serializable {
    public final Integer clientId;
    public final Integer key; // the key of the value to write
    public final Integer value; // the new value to write
    public WriteMsg(int clientId, int key, int value) {
      this.clientId = clientId;
      this.key = key;
      this.value = value;
    }
  }

  // reply from the coordinator when requested a READ on a given key
  public static class ReadResultMsg implements Serializable {
    public final Integer key; // the key associated to the requested item
    public final Integer value; // the value found in the data store for that item
    public ReadResultMsg(int key, int value) {
      this.key = key;
      this.value = value;
    }
  }

  // message from the coordinator to the client with the outcome of the TXN
  public static class TxnResultMsg implements Serializable {
    public final Boolean commit; // if false, the transaction was aborted
    public TxnResultMsg(boolean commit) {
      this.commit = commit;
    }
  }

  /*-- Actor methods -------------------------------------------------------- */

  // start a new TXN: choose a random coordinator, send TxnBeginMsg and set timeout
  void beginTxn() {

    // some delay between transactions from the same client
    try { Thread.sleep(10); }
    catch (InterruptedException e) { e.printStackTrace(); }

    acceptedTxn = false;
    numAttemptedTxn++;

    // contact a random coordinator and begin TXN
    currentCoordinator = coordinators.get(r.nextInt(coordinators.size()));
    currentCoordinator.tell(new TxnBeginMsg(clientId), getSelf());

    // how many operations (taking some amount and adding it somewhere else)?
    int numExtraOp = RAND_LENGTH_RANGE > 0 ? r.nextInt(RAND_LENGTH_RANGE) : 0;
    numOpTotal = MIN_TXN_LENGTH + numExtraOp;
    numOpDone = 0;

    // timeout for confirmation of TXN by the coordinator (sent to self)
    acceptTimeout = getContext().system().scheduler().scheduleOnce(
            Duration.create(500, TimeUnit.MILLISECONDS),
            getSelf(),
            new TxnAcceptTimeoutMsg(), // message sent to myself
            getContext().system().dispatcher(), getSelf()
    );
    System.out.println("CLIENT " + clientId + " BEGIN");
  }

  // end the current TXN sending TxnEndMsg to the coordinator
  void endTxn() {
    boolean doCommit = r.nextDouble() < COMMIT_PROBABILITY;
    currentCoordinator.tell(new TxnEndMsg(clientId, doCommit), getSelf());
    firstValue = null;
    secondValue = null;
    System.out.println("CLIENT " + clientId + " END");
  }

  // READ two items (will move some amount from the value of the first to the second)
  void readTwo() {

    // read two different keys
    firstKey = r.nextInt(maxKey + 1);
    int randKeyOffset = 1 + r.nextInt(maxKey - 1);
    secondKey = (firstKey + randKeyOffset) % (maxKey + 1);

    // READ requests
    currentCoordinator.tell(new ReadMsg(clientId, firstKey), getSelf());
    currentCoordinator.tell(new ReadMsg(clientId, secondKey), getSelf());

    // delete the current read values
    firstValue = null;
    secondValue = null;

    System.out.println("CLIENT " + clientId + " READ #"+ numOpDone + " (" + firstKey + "), (" + secondKey + ")");
  }

  // WRITE two items (called with probability WRITE_PROBABILITY after readTwo() values are returned)
  void writeTwo() {

    // take some amount from one value and pass it to the other, then request writes
    Integer amountTaken = 0;
    if(firstValue >= 1) amountTaken = 1 + r.nextInt(firstValue);
    currentCoordinator.tell(new WriteMsg(clientId, firstKey, firstValue - amountTaken), getSelf());
    currentCoordinator.tell(new WriteMsg(clientId, secondKey, secondValue + amountTaken), getSelf());
    System.out.println("CLIENT " + clientId + " WRITE #"+ numOpDone
            + " taken " + amountTaken
            + " (" + firstKey + ", " + (firstValue - amountTaken) + "), ("
            + secondKey + ", " + (secondValue + amountTaken) + ")");
  }

  /*-- Message handlers ----------------------------------------------------- */

  private void onWelcomeMsg(WelcomeMsg msg) {
    this.coordinators = msg.coordinators;
    System.out.println(coordinators);
    this.maxKey = msg.maxKey;
    beginTxn();
  }

  private void onStopMsg(StopMsg msg) {
    getContext().stop(getSelf());
  }

  private void onTxnAcceptMsg(TxnAcceptMsg msg) {
    acceptedTxn = true;
    acceptTimeout.cancel();
    readTwo();
  }

  private void onTxnAcceptTimeoutMsg(TxnAcceptTimeoutMsg msg) throws InterruptedException {
    if(!acceptedTxn) beginTxn();
  }

  private void onReadResultMsg(ReadResultMsg msg) {
    System.out.println("CLIENT " + clientId + " READ RESULT (" + msg.key + ", " + msg.value + ")");

    // save the read value(s)
    if(msg.key.equals(firstKey)) firstValue = msg.value;
    if(msg.key.equals(secondKey)) secondValue = msg.value;

    boolean opDone = (firstValue != null && secondValue != null);

    // do we only read or also write?
    double writeRandom = r.nextDouble();
    boolean doWrite = writeRandom < WRITE_PROBABILITY;
    if(doWrite && opDone) writeTwo();
    
    // check if the transaction should end;
    // otherwise, read two again
    if(opDone) numOpDone++;
    if(numOpDone >= numOpTotal) {
      endTxn();
    }
    else if(opDone) {
      readTwo();
    }
  }

  private void onTxnResultMsg(TxnResultMsg msg) throws InterruptedException {
    if(msg.commit) {
      numCommittedTxn++;
      System.out.println("CLIENT " + clientId + " COMMIT OK ("+numCommittedTxn+"/"+numAttemptedTxn+")");
    }
    else {
      System.out.println("CLIENT " + clientId + " COMMIT FAIL ("+(numAttemptedTxn - numCommittedTxn)+"/"+numAttemptedTxn+")");
    }
    beginTxn();
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .match(WelcomeMsg.class,  this::onWelcomeMsg)
            .match(TxnAcceptMsg.class,  this::onTxnAcceptMsg)
            .match(TxnAcceptTimeoutMsg.class,  this::onTxnAcceptTimeoutMsg)
            .match(ReadResultMsg.class,  this::onReadResultMsg)
            .match(TxnResultMsg.class,  this::onTxnResultMsg)
            .match(StopMsg.class,  this::onStopMsg)
            .build();
  }
}
