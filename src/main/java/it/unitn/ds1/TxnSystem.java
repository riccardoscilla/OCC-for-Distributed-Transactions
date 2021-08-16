package it.unitn.ds1;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.*;
import java.io.IOException;
import java.io.Serializable;

import java.time.Duration;
import akka.actor.*;

import it.unitn.ds1.TxnClient.StopMsg;

import it.unitn.ds1.TxnClient.WelcomeClientMsg;
import it.unitn.ds1.TxnCoordinator.WelcomeCoordMsg;

import it.unitn.ds1.TxnServer.CrashServerType;
import it.unitn.ds1.TxnCoordinator.CrashCoordType;

public class TxnSystem {
  final static int N_CLIENTS = 1;
  final static int N_COORDINATORS = 1;
  final static int N_SERVERS = 3;
  final static int maxKey = N_SERVERS*10-1;

  final static int maxDelay = 50;
  final static int minDelay = 10;

  final static int maxCrash = 800;
  final static int minCrash = 300;

  static final String logMode = "Verbose";
  static int seed = 405556085;
  // 54154073

  static Cancellable serverCrash;


  //Start a crash simulation
  public static class CrashServerMsg implements Serializable {
    public final CrashServerType nextCrash;
    public final int timeCrashed;
    public CrashServerMsg(CrashServerType nextCrash, int timeCrashed){
      this.nextCrash = nextCrash;
      this.timeCrashed = timeCrashed;
    }
  }

  public static class CrashCoordMsg implements Serializable {
    public final CrashCoordType nextCrash;
    public final int timeCrashed;
    public CrashCoordMsg(CrashCoordType nextCrash, int timeCrashed){
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
    System.out.println("Seed: " + seed);

    Config myConfig = ConfigFactory.parseString("akka.log-dead-letters = off");

    // Create the actor system
    final ActorSystem system = ActorSystem.create("txnSystem", myConfig.withFallback(ConfigFactory.load()));

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
    WelcomeClientMsg welcomeClient = new WelcomeClientMsg(maxKey,coordinators);
    for (ActorRef client: clients) {
      client.tell(welcomeClient, ActorRef.noSender());
    }

    // Send Welcome message to all Coordinators to make known the Servers
    WelcomeCoordMsg welcomeCoord = new WelcomeCoordMsg(servers);
    for (ActorRef client: coordinators) {
      client.tell(welcomeCoord, ActorRef.noSender());
    }
    
    
    // automated crash simulator
    // Cancellable cancellable = system.scheduler().scheduleWithFixedDelay(
    //         Duration.ofMillis(1000), // initial wait 
    //         Duration.ofMillis(1000), // fixed delay
    //         new Runnable() {
    //           @Override
    //           public void run() {
    //             if (r.nextDouble() < 0){
    //               ActorRef serverToCrash = servers.get(r.nextInt(servers.size()));
    //               // CrashServerType nextCrash = CrashServerType.values()[r.nextInt(CrashServerType.values().length)];
    //               int timeToCrash = (int)(((r.nextDouble())*(maxCrash - minCrash)) + minCrash);
    //               serverToCrash.tell(new CrashServerMsg(CrashServerType.BeforeVote, timeToCrash), ActorRef.noSender());
    //             }
    //             else{
    //               ActorRef coordToCrash = coordinators.get(r.nextInt(coordinators.size()));
    //               // CrashCoordType nextCrash = ...
    //               int timeToCrash = (int)(((r.nextDouble())*(maxCrash - minCrash)) + minCrash);
    //               coordToCrash.tell(new CrashCoordMsg(CrashCoordType.BeforeDecide, timeToCrash), ActorRef.noSender());
    //             }
                
    //           }
    //         },
    //         system.dispatcher()
    // );

    // manual crash simulator
    inputContinue();
    ActorRef coordToCrash = coordinators.get(r.nextInt(coordinators.size()));
    int timeToCrash = 5000;
    coordToCrash.tell(new CrashCoordMsg(CrashCoordType.BeforeDecide, timeToCrash), ActorRef.noSender());
    inputContinue();

    inputTerminate(system, clients);
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    }
    catch (IOException ioe) {}
  }

  public static void inputTerminate(ActorSystem system, List<ActorRef> clients) {
    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();      
    }
    catch (IOException ioe) {}

    for (ActorRef client: clients) {
      client.tell(new StopMsg(), ActorRef.noSender());
    }

    system.scheduler().scheduleOnce(
            Duration.ofMillis(2000),
            new Runnable() {
              @Override
              public void run() {
                system.terminate();
              }
            },
            system.dispatcher()
    );
    
  }


}
