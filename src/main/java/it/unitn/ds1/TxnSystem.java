package it.unitn.ds1;

import java.util.*;
import java.io.IOException;

import akka.actor.*;

public class TxnSystem {
  final static int N_CLIENTS = 3;
  final static int N_COORDINATORS = 3;
  final static int N_SERVERS = 3;

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
    for (int i=0; i<N_COORDINATORS; i++) {
      servers.add(system.actorOf(TxnServer.props(i), "txnServer" + i));
    }
    
    // Terminate
    try {
      System.out.println(">>> Press ENTER to exit <<<");
      System.in.read();
    } 
    catch (IOException ioe) {}
    system.terminate();
  }
}
