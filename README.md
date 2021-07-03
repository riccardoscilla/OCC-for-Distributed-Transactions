# Distributed Systems 1 Project
 
## Implementation choices (attempt)
* **DataStore**: Map<Integer,int[]> <br/>
The key is the identifier of the item (i.e. 0-9 for server0, 10-19 for server1, ...). <br/> The value is an array with first item the version, the second the value.

* **TxnId**: Object combining an unique int (id) for a transaction of a single coordinator, and the client actorRef that requested the transaction.
<br/>Override equals to compare the clients.
<br/>Override hashCode to return the id.

* **OngoingTxn**: Map<TxnId,Set\<ActorRef\>\>
<br/> Bind the TxnId with all the servers contacted during the trasaction. Will be used to ask "commit?" in the validation phase.

* **Private Workspace**: TODO