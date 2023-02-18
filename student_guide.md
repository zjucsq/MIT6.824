# Background

# Implementing Raft

In fact, Figure 2 is extremely precise, and every single statement it makes should be treated, in specification terms, as **MUST**, not as **SHOULD**.

## The importance of details

- The heartbeats are not special. By accepting the RPC, the follower is implicitly telling the leader that their log matches the leader’s log up to and including the prevLogIndex included in the AppendEntries arguments.
- Another issue many had (often immediately after fixing the issue above), was that, upon receiving a heartbeat, they would truncate the follower’s log following prevLogIndex, and then append any entries included in the AppendEntries arguments. $\color{red}{TODO}$
# Debugging Raft

## Livelocks

- Election timer: Specifically, you should only restart your election timer if 
  1. you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer); 
  2. you are starting an election; 
  3. you grant a vote to another peer.
- In particular, note that if you are a candidate (i.e., you are currently running an election), but the election timer fires, you should start another election. This is important to avoid the system stalling due to delayed or dropped RPCs. $\color{red}{TODO}$
- Ensure that you follow the second rule in “Rules for Servers” before handling an incoming RPC. The second rule states: `If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)`  $\color{red}{TODO}$

## Incorrect RPC handlers




## Failure to follow The Rules

## Term confusion

## An aside on optimizations

# Applications on top of Raft

## Applying client operations

## Duplicate detection

## Hairy corner-cases









