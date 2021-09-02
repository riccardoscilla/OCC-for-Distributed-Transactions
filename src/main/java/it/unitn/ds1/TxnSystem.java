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
  // final static int N_CLIENTS = 3;
  // final static int N_COORDINATORS = 3;
  // final static int N_SERVERS = 6;
  // final static int maxKey = N_SERVERS*10-1;

  static int N_CLIENTS;
  static int N_COORDINATORS;
  static int N_SERVERS;
  static int maxKey;

  final static int processTime = 70;

  final static int maxDelay = 50;
  final static int minDelay = 10;

  static int maxCrash;
  static int minCrash;

  final static int simDuration = 90*1000; // sec*1000

  static final String logMode = "Verbose";
  static int seed = 0; // set 0 to generate randomly
  // 405556085

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

    // Set seed
    if (seed == 0){
      // int max = Math.max(N_CLIENTS,Math.max(N_COORDINATORS,N_SERVERS));
      seed = r.nextInt(Integer.MAX_VALUE/100); 
    }
    r.setSeed(seed);

    // Set actor quantity
    N_CLIENTS = (int)(((r.nextDouble())*(10 - 2)) + 2);
    N_COORDINATORS = (int)(((r.nextDouble())*(10 - 2)) + 2);
    N_SERVERS = (int)(((r.nextDouble())*(N_CLIENTS*10 - N_CLIENTS*8)) + N_CLIENTS*8);
    maxKey = N_SERVERS*10-1;

    maxCrash = (int)(N_SERVERS*processTime*1.5);
    minCrash = (int)(N_SERVERS*processTime*0.7);

    System.out.println("Seed: " + seed);
    System.out.println("Actor Info: clients:"+N_CLIENTS+" coordinators:"+N_COORDINATORS+" servers:"+N_SERVERS);

    // Create the actor system
    Config myConfig = ConfigFactory.parseString("akka.log-dead-letters = off");
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
    WelcomeClientMsg welcomeClient = new WelcomeClientMsg(maxKey,new ArrayList<>(coordinators));
    for (ActorRef client: clients) {
      client.tell(welcomeClient, ActorRef.noSender());
    }

    // Send Welcome message to all Coordinators to make known the Servers
    WelcomeCoordMsg welcomeCoord = new WelcomeCoordMsg(new ArrayList<>(servers));
    for (ActorRef client: coordinators) {
      client.tell(welcomeCoord, ActorRef.noSender());
    }
    
    // ------------------ Automated crash simulator ------------------
    Cancellable cancellable = system.scheduler().scheduleWithFixedDelay(
            Duration.ofMillis(20000), // initial wait 
            Duration.ofMillis(20000), // fixed delay
            new Runnable() {
              @Override
              public void run() {
                if (r.nextDouble() < 0.5){
                  ActorRef serverToCrash = servers.get(r.nextInt(servers.size()));
                  CrashServerType nextCrash = CrashServerType.values()[r.nextInt(CrashServerType.values().length)];
                  int timeToCrash = (int)(((r.nextDouble())*(maxCrash - minCrash)) + minCrash);
                  serverToCrash.tell(new CrashServerMsg(nextCrash, timeToCrash), ActorRef.noSender());
                }
                else{
                  ActorRef coordToCrash = coordinators.get(r.nextInt(coordinators.size()));
                  CrashCoordType nextCrash =  CrashCoordType.values()[r.nextInt(CrashCoordType.values().length)];
                  int timeToCrash = (int)(((r.nextDouble())*(maxCrash - minCrash)) + minCrash);
                  coordToCrash.tell(new CrashCoordMsg(nextCrash, timeToCrash), ActorRef.noSender());
                }
                
              }
            },
            system.dispatcher()
    );

    // ------------------ Manual crash simulator ------------------
    // inputContinue();

    // ActorRef serverToCrash = servers.get(0);
    // int timeToCrash = 200;
    // CrashServerType nextCrash = CrashServerType.AfterVote;
    // serverToCrash.tell(new CrashServerMsg(nextCrash, timeToCrash), ActorRef.noSender());

    // ActorRef coordToCrash = coordinators.get(0);
    // int timeToCrash = 500;
    // CrashCoordType nextCrash = CrashCoordType.AfterDecide;
    // coordToCrash.tell(new CrashCoordMsg(nextCrash, timeToCrash), ActorRef.noSender());

    // inputContinue();

    // ------------------ Automated termination ------------------
    system.scheduler().scheduleOnce(
      Duration.ofMillis(simDuration),
      new Runnable() {
        @Override
        public void run() {
          terminate(system, clients);
        }
      },
      system.dispatcher()
    );

    // ------------------ Manual termination ------------------
    // inputTerminate(system, clients);


  }

  public static void terminate(ActorSystem system, List<ActorRef> clients) {

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
