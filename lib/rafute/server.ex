defmodule Rafute.Server do
  @behaviour :gen_fsm

  require Logger

  alias Rafute.{
    RequestVoteRPC,
    RequestVoteRPCReply,
    AppendEntriesRPC,
    AppendEntriesRPCReply,
    Entry,
    Command
  }

  def start_link(name, servers) do
    :gen_fsm.start_link({:local, name}, __MODULE__, [name, servers], [])
  end

  def init([me, servers]) do
    ## TODO: other backends
    ## TODO: backend_sup
    {backend, backend_state} = Rafute.Backend.Agent.start_link
    state = %{
      me: {me, node()},
      servers: servers,

      ## TODO: persist
      current_term: 0,
      voted_for: nil,
      logs: [],

      log_info: %{last_index: 0, last_term: 0},
      commit_index: 0,

      next_index: %{},
      match_index: %{},

      votes: MapSet.new(),
      election_timer_ref: nil,
      heartbeat_timer_ref: nil,
      leader: nil,

      backend: backend,
      backend_state: backend_state,

      client_index: %{},
    }
    state = set_election_timer(state)
    {:ok, :follower, state}
  end

  def follower(:election_timeout, state) do
    state = become_candidate(state)
    {:next_state, :candidate, state}
  end
  def follower(%RequestVoteRPC{} = rpc, state) do
    handle_request_vote(:follower, rpc, state)
  end
  def follower(%AppendEntriesRPC{} = rpc, state) do
    handle_append_entries(:follower, rpc, state)
  end
  def follower(_message, state) do
    {:next_state, :follower, state}
  end

  def follower(%Command{}, _from, state) do
    reply = {:error, {:redirect, state.leader}}
    {:reply, reply, :follower, state}
  end

  def candidate(:election_timeout, state) do
    ## for a case of single node
    if (MapSet.size(state.votes) + 1) > (state.servers |> length() |> div(2)) do
      state = become_leader(state)
      {:next_state, :leader, state}
    else
      state = become_candidate(state)
      {:next_state, :candidate, state}
    end
  end
  def candidate(%RequestVoteRPC{term: term} = rpc, %{current_term: current_term} = state) when term > current_term do
    handle_request_vote(:candidate, rpc, state)
  end
  def candidate(%RequestVoteRPC{} = rpc, state) do
    reject_vote(rpc.from, state.me, state.current_term)
    {:next_state, :candidate, state}
  end
  def candidate(%RequestVoteRPCReply{term: term}, %{current_term: current_term} = state) when term < current_term do
    {:next_state, :candidate, state}
  end
  def candidate(%RequestVoteRPCReply{vote_granted: false, term: term}, %{current_term: current_term} = state) when term == current_term do
    {:next_state, :candidate, state}
  end
  def candidate(%RequestVoteRPCReply{vote_granted: false, term: term}, %{current_term: current_term} = state) when term > current_term do
    state = become_follower(term, state)
    {:next_state, :follower, state}
  end
  def candidate(%RequestVoteRPCReply{vote_granted: true, from: from}, state) do
    votes = MapSet.put(state.votes, from)
    ## including me
    if (MapSet.size(votes) + 1) > (state.servers |> length() |> div(2)) do
      state = become_leader(state)
      {:next_state, :leader, state}
    else
      {:next_state, :candidate, %{state|votes: votes}}
    end
  end
  def candidate(%AppendEntriesRPC{} = rpc, state) do
    handle_append_entries(:candidate, rpc, state)
  end
  def candidate(_message, state) do
    {:next_state, :candidate, state}
  end

  def candidate(%Command{}, _from, state) do
    {:reply, {:error, {:redirect, state.leader}}, :candidate, state}
  end

  def leader(:heartbeat_timeout, state) do
    heartbeat = %AppendEntriesRPC{
      term: state.current_term,
      leader_id: state.me,
      prev_log_index: state.log_info.last_index,
      prev_log_term: state.log_info.last_term,
      entries: [],
      leader_commit: state.commit_index,
      from: state.me,
    }
    broadcast(heartbeat, state)
    state = set_heartbeat_timer(state)
    {:next_state, :leader, state}
  end
  def leader(%RequestVoteRPC{term: term} = rpc, %{current_term: current_term} = state) when term > current_term do
    handle_request_vote(:leader, rpc, state)
  end
  def leader(%RequestVoteRPC{} = rpc, state) do
    reject_vote(rpc.from, state.me, state.current_term)
    {:next_state, :leader, state}
  end
  def leader(%AppendEntriesRPC{term: term, from: from}, %{current_term: current_term, leader: leader}) when term == current_term and from != leader do
    raise "Duplicated leader #{from} and #{leader}"
  end
  def leader(%AppendEntriesRPC{} = rpc, state) do
    handle_append_entries(:leader, rpc, state)
  end
  def leader(%AppendEntriesRPCReply{term: term}, %{current_term: current_term} = state) when term > current_term do
    state = become_follower(term, state)
    {:next_state, :follower, state}
  end
  def leader(%AppendEntriesRPCReply{term: term}, %{current_term: current_term} = state) when term < current_term do
    {:next_state, :leader, state}
  end
  def leader(%AppendEntriesRPCReply{from: from, success: false}, state) do
    next_index = state.next_index[from] - 1
    {entries, prev_log_index, prev_log_term} = get_entries_from(next_index, state)
    rpc = %AppendEntriesRPC{
      term: state.current_term,
      leader_id: state.me,
      leader_commit: state.commit_index,
      entries: entries,
      prev_log_index: prev_log_index,
      prev_log_term: prev_log_term,
      from: state.me,
    }
    send_rpc(from, rpc)
    {:next_state, :leader, state}
  end
  def leader(%AppendEntriesRPCReply{from: from, index: index, success: true}, state) do
    state =
      state
      |> put_in([:match_index, from], index)
      |> put_in([:next_index, from], index + 1)
    commitable_index =
      state.commit_index
      |> Stream.iterate(&(&1 + 1))
      |> Enum.find_value(fn(index) ->
            ## including me
            count = Enum.count(state.match_index, fn({_, mi}) -> mi >= index end) + 1
            count <= (state.servers |> length() |> div(2)) && index
         end)
      |> (&(&1 - 1)).()
    state = commit_logs(:leader, commitable_index, state)
    {:next_state, :leader, state}
  end
  def leader(_message, state) do
    {:next_state, :leader, state}
  end

  def leader(%Command{type: :read} = command, _from, state) do
    value = state.backend.exec(command, state.backend_state)
    {:reply, {:ok, value}, :leader, state}
  end
  def leader(%Command{} = command, from, state) do
    index = state.log_info.last_index + 1
    entry = %Entry{command: command, index: index, term: state.current_term}
    state =
      state
      |> put_in([:log_info, :last_index], index)
      |> put_in([:client_index, index], from)
    state = append_entries([entry], state)
    rpc = %AppendEntriesRPC{
      term: state.current_term,
      leader_id: state.me,
      leader_commit: state.commit_index,
      from: state.me
    }
    for server <- state.servers, server != state.me do
      next_index = state.next_index[server]
      {entries, prev_log_index, prev_log_term} = get_entries_from(next_index, state)
      rpc = %{rpc | entries: entries, prev_log_index: prev_log_index, prev_log_term: prev_log_term}
      send_rpc(server, rpc)
    end
    state = set_heartbeat_timer(state)
    {:next_state, :leader, state}
  end

  def handle_event(_event, state_name, state) do
    {:next_state, state_name, state}
  end

  def handle_sync_event(_event, state_name, state) do
    {:next_state, state_name, state}
  end

  def handle_sync_event(_event, _from, state_name, state) do
    {:next_state, state_name, state}
  end

  def handle_info(_info, state_name, state) do
    {:next_state, state_name, state}
  end

  def terminate(_reason, _state_name, _state) do
  end

  def code_change(_old, state_name, state, _extra) do
    {:ok, state_name, state}
  end

  defp handle_request_vote(state_name, %RequestVoteRPC{} = rpc, state) do
    up_to_date = rpc.last_log_term > state.log_info.last_term or
                 (rpc.last_log_term == state.log_info.last_term and rpc.last_log_index >= state.log_info.last_index)
    cond do
      rpc.term > state.current_term && up_to_date ->
        state = become_follower(rpc.term, state)
        grant_vote(rpc.from, state.me, state.current_term)
        {:next_state, state_name, %{state|voted_for: rpc.from}}
      rpc.term == state.current_term && up_to_date ->
        if state.voted_for do
          reject_vote(rpc.from, state.me, state.current_term)
          {:next_state, state_name, state}
        else
          grant_vote(rpc.from, state.me, state.current_term)
          {:next_state, state_name, %{state|voted_for: rpc.from}}
        end
      true ->
        reject_vote(rpc.from, state.me, state.current_term)
        {:next_state, state_name, state}
    end
  end

  defp handle_append_entries(state_name, %AppendEntriesRPC{term: term} = rpc,
                             %{current_term: current_term} = state) when term < current_term do
    send_rpc(rpc.from, %AppendEntriesRPCReply{term: state.current_term, success: false, from: state.me})
    {:next_state, state_name, state}
  end
  defp handle_append_entries(_, rpc, state) do
    state = become_follower(rpc.term, state)
    state = %{state | leader: rpc.from}
    if check_log(rpc.prev_log_index, rpc.prev_log_term, state) do
      state = append_entries(rpc.entries, state)
      state = commit_logs(:follower, rpc.leader_commit, state)
      send_rpc(rpc.from, %AppendEntriesRPCReply{term: state.current_term, success: true, index:
                                                state.log_info.last_index, from: state.me})
      {:next_state, :follower, state}
    else
      send_rpc(rpc.from, %AppendEntriesRPCReply{term: state.current_term, success: false, from: state.me})
      {:next_state, :follower, state}
    end
  end

  ## TODO: Rafute.Log.check/3
  defp check_log(_, _, %{logs: []}) do
    true
  end
  defp check_log(prev_log_index, prev_log_term, state) do
    Enum.any?(state.logs, &(&1.index == prev_log_index and &1.term == prev_log_term))
  end

  ## TODO: Ragute.Log.append_entries/2
  defp append_entries([], state) do
    state
  end
  defp append_entries([%Entry{index: index}|_] = entries, state) do
    logs = Enum.drop_while(state.logs, &(&1.index >= index))
    [%Entry{index: index, term: term}|_] = logs = Enum.reverse(entries) ++ logs
    %{state | logs: logs, log_info: %{last_index: index, last_term: term}}
  end

  ## TODO: Ragute.Log.get_entries_from
  defp get_entries_from(index, state) do
    case Enum.split_while(state.logs, &(&1.index >= index)) do
      {entries, []} ->
        {entries, 0, 0}
      {entries, [%Entry{index: index, term: term}|_]} ->
        {entries, index, term}
    end
  end

  defp commit_logs(_, index, %{commit_index: commit_index} = state) when index <= commit_index do
    state
  end
  defp commit_logs(:leader, index, state) do
    Logger.debug("#{elem(state.me, 0)}: commit logs")
    indexes =
      state.logs
      |> Enum.drop_while(&(&1.index > index))
      |> Enum.take_while(&(&1.index > state.commit_index))
      |> Enum.reverse
      |> Enum.map(fn(entry) ->
           state.backend.exec(entry.command, state.backend_state)
           :gen_fsm.reply(state.client_index[entry.index], :ok)
           entry.index
         end)
    client_index = Enum.reduce(indexes, state.client_index, fn(index, acc) -> Map.delete(acc, index) end)
    %{state | commit_index: index, client_index: client_index}
  end
  defp commit_logs(_, index, state) do
    Logger.debug("#{elem(state.me, 0)}: commit logs")
    state.logs
    |> Enum.drop_while(&(&1.index > index))
    |> Enum.take_while(&(&1.index >= state.commit_index))
    |> Enum.reverse
    |> Enum.each(&state.backend.exec(&1.command, state.backend_state))
    %{state | commit_index: index}
  end

  defp become_follower(term, state) do
    state = %{state|current_term: term, voted_for: nil}
    state = set_election_timer(state)
    state
  end

  defp become_candidate(state) do
    state = %{state|current_term: state.current_term + 1, votes: MapSet.new()}
    rpc = %RequestVoteRPC{
      term: state.current_term,
      candidate_id: state.me,
      last_log_index: state.log_info.last_index,
      last_log_term: state.log_info.last_term,
      from: state.me,
    }
    broadcast(rpc, state)
    state = set_election_timer(state)
    state
  end

  defp become_leader(state) do
    state = stop_election_timer(state)
    next_index = for server <- state.servers, server != state.me, into: %{}, do: {server, state.log_info.last_index + 1}
    match_index = for server <- state.servers, server != state.me, into: %{}, do: {server, 0}
    state = set_heartbeat_timer(state)
    Logger.debug("#{elem(state.me, 0)}: become leader")
    %{state | leader: state.me, next_index: next_index, match_index: match_index, client_index: %{}}
  end

  defp grant_vote(to, from, term) do
    send_rpc(to, %RequestVoteRPCReply{term: term, vote_granted: true, from: from})
  end

  defp reject_vote(to, from, term) do
    send_rpc(to, %RequestVoteRPCReply{term: term, vote_granted: false, from: from})
  end

  defp broadcast(rpc, state) do
    for server <- state.servers, server != state.me, do: send_rpc(server, rpc)
  end

  defp send_rpc(to, message) do
    :gen_fsm.send_event(to, message)
  end

  defp stop_election_timer(state) do
    stop_timer(:election_timeout, state)
  end

  defp set_election_timer(state) do
    set_timer(:election_timeout, :rand.uniform(500) + 500, state)
  end

  defp set_heartbeat_timer(state) do
    set_timer(:heartbeat_timeout, 200, state)
  end

  @timer_ref_name %{election_timeout: :election_timer_ref,
                    heartbeat_timeout: :heartbeat_timer_ref}

  defp stop_timer(name, state) do
    ref_name = @timer_ref_name[name]
    ref = state[ref_name]
    if ref, do: :gen_fsm.cancel_timer(ref)
    %{state | ref_name => nil}
  end

  defp set_timer(name, duration, state) do
    ref_name = @timer_ref_name[name]
    ref = state[ref_name]
    if ref, do: :gen_fsm.cancel_timer(ref)
    stop_timer(name, state)
    ref = :gen_fsm.send_event_after(duration, name)
    %{state | ref_name => ref}
  end
end
