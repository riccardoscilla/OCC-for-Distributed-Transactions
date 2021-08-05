package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.*;
import java.io.IOException;
import java.io.Serializable;

import java.time.Duration;
import akka.actor.*;

import it.unitn.ds1.TxnClient.WelcomeMsg;
import it.unitn.ds1.TxnCoordinator.WelcomeMsg2;

import it.unitn.ds1.TxnServer.CrashType;

public class TxnSystem {
  final static int N_CLIENTS = 1;
  final static int N_COORDINATORS = 1;
  final static int N_SERVERS = 3;
  final static int maxKey = N_SERVERS*10-1;
  final static int maxDelay = 50;
  final static int minDelay = 10;
  static final String logMode = "Verbose";
  static int seed = 405556085;
  // 54154073

  static Cancellable serverCrash;

  //Start a crash simulation
  public static class CrashMsg implements Serializable {
    public final CrashType nextCrash;
    public final int timeCrashed;
    public CrashMsg(CrashType nextCrash, int timeCrashed){
      this.nextCrash = nextCrash;
      this.timeCrashed = timeCrashed;
    }
  }

  //Recover from the simulated crash
  public static class RecoveryMsg implements Serializable {
    public RecoveryMsg(){
      super();
    }
  }

  public static void main(String[] args) {
    Random r = new Random();
    if (seed == 0){
      int max = Math.max(N_CLIENTS,Math.max(N_COORDINATORS,N_SERVERS));
      seed = r.nextInt(Integer.MAX_VALUE/max);
    }

    // Create the actor system
    final ActorSystem system = ActorSystem.create("txnSystem");

    // Create client nodes and put them to a list
    List<ActorRef> clients = new ArrayList<>();
    for (int i=0; i<N_CLIENTS; i++) {
      clients.add(system.actorOf(TxnClient.props(i), "txnClient" + i));
    }

    // Create coordinator nodes and put them to a list
    List<ActorRef> coordinators = new ArrayList<>();
    for (int i=0; i<N_COORDINATORS; i++) {
      coordinators.add(system.actorOf(TxnCoordinator.props(i), "txnCoordinator" + i));
    }

    // Create coordinator nodes and put them to a list
    List<ActorRef> servers = new ArrayList<>();
    for (int i=0; i<N_SERVERS; i++) {
      servers.add(system.actorOf(TxnServer.props(i), "txnServer" + i));
    }

    // Send Welcome message to all Clients to make known the Coordinators
    WelcomeMsg welcomeClient = new WelcomeMsg(maxKey,coordinators);
    for (ActorRef client: clients) {
      client.tell(welcomeClient, ActorRef.noSender());
    }

    // Send Welcome message to all Coordinators to make known the Servers
    WelcomeMsg2 welcomeCoordi = new WelcomeMsg2(servers);
    for (ActorRef client: coordinators) {
      client.tell(welcomeCoordi, ActorRef.noSender());
    }


    inputContinue();
    servers.get(r.nextInt(servers.size())).tell(new CrashMsg(CrashType.BeforeVote, 600), ActorRef.noSender());
    inputContinue();

    inputTerminate(system);
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    }
    catch (IOException ioe) {}
  }

  public static void inputTerminate(ActorSystem system) {
    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
      system.terminate();
    }
    catch (IOException ioe) {}
    
  }

}
