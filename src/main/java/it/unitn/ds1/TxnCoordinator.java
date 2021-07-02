package it.unitn.ds1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

import akka.actor.*;
import scala.concurrent.duration.Duration;

public class TxnCoordinator extends AbstractActor {
  private final Integer coordinatorId;
  /*-- Actor constructor ---------------------------------------------------- */
  
  public TxnCoordinator(int coordinatorId) {
    this.coordinatorId = coordinatorId;
  }

  static public Props props(int coordinatorId) {
    return Props.create(TxnCoordinator.class, () -> new TxnCoordinator(coordinatorId));
  }

  /*-- Message classes ------------------------------------------------------ */

  /*-- Actor methods -------------------------------------------------------- */

  /*-- Message handlers ----------------------------------------------------- */


  @Override
  public Receive createReceive() {
    return receiveBuilder()
            .build();
  }
}
