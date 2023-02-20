# Background

# Implementing Raft

In fact, Figure 2 is extremely precise, and every single statement it makes should be treated, in specification terms, as **MUST**, not as **SHOULD**.

## The importance of details

- The heartbeats are not special. By accepting the RPC, the follower is implicitly telling the leader that their log matches the leader’s log up to and including the prevLogIndex included in the AppendEntries arguments.
- Another issue many had (often immediately after fixing the issue above), was that, upon receiving a heartbeat, they would truncate the follower’s log following prevLogIndex, and then append any entries included in the AppendEntries arguments. 
  - Examples: follower origin log: 11222; rpc call1: 33344; rpc call2: 33344444. (Because the leader does not receive the response) of rpc call1, so the two rpc calls have some duplicate logs). Now rpc call2 arrives before rpc call1.
  - We must handle these two situations well: 1) rpc call1 arrives before rpc call2; 2) rpc call2 arrives before rpc call1.

# Debugging Raft

## Livelocks

- Election timer: Specifically, you should only restart your election timer if 
  1. you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer); 
  2. you are starting an election; 
  3. you grant a vote to another peer.
- In particular, note that if you are a candidate (i.e., you are currently running an election), but the election timer fires, you should start another election. This is important to avoid the system stalling due to delayed or dropped RPCs.
- Ensure that you follow the second rule in “Rules for Servers” before handling an incoming RPC. The second rule states: `If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)`

## Incorrect RPC handlers

- If a step says “reply false”, this means you should reply immediately, and not perform any of the subsequent steps.
- If you get an AppendEntries RPC with a prevLogIndex that points beyond the end of your log, you should handle it the same as if you did have that entry but the term did not match (i.e., reply false).
- Check 2 for the AppendEntries RPC handler should be executed even if the leader didn’t send any entries. $\textcolor{red}{TODO}$
- The min in the final step (#5) of AppendEntries is necessary, and it needs to be computed with the index of the last new entry. It is not sufficient to simply have the function that applies things from your log between lastApplied and commitIndex stop when it reaches the end of your log. This is because you may have entries in your log that differ from the leader’s log after the entries that the leader sent you (which all match the ones in your log). Because #3 dictates that you only truncate your log if you have conflicting entries, those won’t be removed, and if leaderCommit is beyond the entries the leader sent you, you may apply incorrect entries.
- It is important to implement the “up-to-date log” check exactly as described in section 5.4. No cheating and just checking the length!

## Failure to follow The Rules

- If commitIndex > lastApplied at any point during execution, you should apply a particular log entry. It is not crucial that you do it straight away (for example, in the AppendEntries RPC handler), but it is important that you ensure that this application is only done by one entity. Specifically, you will need to either have a dedicated “applier”, or to lock around these applies, so that some other routine doesn’t also detect that entries need to be applied and also tries to apply.
- Make sure that you check for commitIndex > lastApplied either periodically, or after commitIndex is updated (i.e., after matchIndex is updated). For example, if you check commitIndex at the same time as sending out AppendEntries to peers, you may have to wait until the next entry is appended to the log before applying the entry you just sent out and got acknowledged.
- If a leader sends out an AppendEntries RPC, and it is rejected, but not because of log inconsistency (this can only happen if our term has passed), then you should immediately step down, and not update nextIndex. If you do, you could race with the resetting of nextIndex if you are re-elected immediately.
- A leader is not allowed to update commitIndex to somewhere in a previous term (or, for that matter, a future term). Thus, as the rule says, you specifically need to check that log[N].term == currentTerm. This is because Raft leaders cannot be sure an entry is actually committed (and will not ever be changed in the future) if it’s not from their current term. This is illustrated by Figure 8 in the paper.
- While nextIndex and matchIndex are generally updated at the same time to a similar value (specifically, nextIndex = matchIndex + 1), the two serve quite different purposes. nextIndex is a guess as to what prefix the leader shares with a given follower. It is generally quite optimistic (we share everything), and is moved backwards only on negative responses. matchIndex is used for safety. It is a conservative measurement of what prefix of the log the leader shares with a given follower. 

## Term confusion

- Term confusion refers to servers getting confused by RPCs that come from old terms. In general, this is not a problem when receiving an RPC, since the rules in Figure 2 say exactly what you should do when you see an old term. However, **Figure 2 generally doesn’t discuss what you should do when you get old RPC replies.** From experience, we have found that by far the simplest thing to do is to first record the term in the reply (it may be higher than your current term), and then to compare the current term with the term you sent in your original RPC. If the two are different, drop the reply and return. Only if the two terms are the same should you continue processing the reply. There may be further optimizations you can do here with some clever protocol reasoning, but this approach seems to work well. And not doing it leads down a long, winding path of blood, sweat, tears and despair.  $\textcolor{red}{TODO}$
- A related, but not identical problem is that of assuming that your state has not changed between when you sent the RPC, and when you received the reply. A good example of this is setting matchIndex = nextIndex - 1, or matchIndex = len(log) when you receive a response to an RPC. This is not safe, because both of those values could have been updated since when you sent the RPC. Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.

## An aside on optimizations



# Applications on top of Raft

## Applying client operations

## Duplicate detection

## Hairy corner-cases









