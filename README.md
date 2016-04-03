# Rafute

An implementation of Raft Consensus Algorithm in Elixir

This is not for production use.

## Example

```
## Create cluster including local and remote nodes 
## You can omit locale node names 
iex> Rafute.Client.create_cluster [
...>   :rafute1,
...>   {:rafute2, :"node2@127.0.0.1"},
...>   {:rafute3, :"node2@127.0.0.1"}
...> ]
:ok

## You can send a query to any nodes because it will be redirected to the leader node  
iex> Rafute.Client.write {:rafute3, :"node2@127.0.0.1"}, "a", "b"
:ok
iex> Rafute.Client.read :rafute1, "a"
{:ok, "b"}

## If the leader node(process) die, new leader process will be elected and the logs will be replicated
iex> :rafute1 |> Process.whereis |> Process.exit(:kill)
true
```

## Implemented features
* Leader Election / Log Replication

## Not Implemented features
* Membership Change
* Log Compaction
