package it.unitn.ds1;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.*;
import java.io.IOException;

import it.unitn.ds1.TxnClient.WelcomeMsg;
import it.unitn.ds1.TxnCoordinator.WelcomeMsg2;

public class TxnSystem {
  final static int N_CLIENTS = 5;
  final static int N_COORDINATORS = 3;
  final static int N_SERVERS = 2;
  final static int maxKey = N_SERVERS*10-1;

  public static void main(String[] args) {

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
    
    // Terminate
    try {
      System.out.println(">>> Press ENTER to exit <<<\n");
      System.in.read();
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}
