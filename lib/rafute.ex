defmodule Rafute do
  use Application

  def start(_type, _args) do
    Rafute.Supervisor.start_link
  end

  def create_cluster(servers) do
    servers = Enum.map(servers, &fullname/1)
    Enum.each(servers, fn(server) ->
      case server do
        {name, n} when n == node() ->
            Rafute.Supervisor.start_server(name, servers)
        {name, n} ->
            :rpc.call(n, Rafute.Supervisor, :start_server, [name, servers])
      end
    end)
  end

  defp fullname(server) when is_atom(server), do: {server, node()}
  defp fullname({_, _} = server), do: server
end
