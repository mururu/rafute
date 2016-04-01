defmodule Rafute.RequestVoteRPC do
  defstruct term: 0,
            candidate_id: nil,
            last_log_term: 0,
            last_log_index: 0,
            from: nil
end

defmodule Rafute.RequestVoteRPCReply do
  defstruct term: 0,
            vote_granted: false,
            from: nil
end

defmodule Rafute.AppendEntriesRPC do
  defstruct term: 0,
            leader_id: nil,
            prev_log_term: 0,
            prev_log_index: 0,
            entries: [],
            leader_commit: 0,
            from: nil
end

defmodule Rafute.AppendEntriesRPCReply do
  defstruct term: 0,
            success: false,
            index: 0,
            from: nil
end

defmodule Rafute.Entry do
  defstruct command: nil,
            term: 0,
            index: 0
end

defmodule Rafute.Command do
  defstruct type: nil,
            args: []
end
