defmodule Rafute do
  use Application

  def start(_type, _args) do
    Rafute.Supervisor.start_link
  end

  def create_cluster(servers) do
    for server <- servers do
      case server do
        {name, n} when n == node() ->
            Rafute.Supervisor.start_server(name, servers)
        {name, n} ->
            :rpc.call(n, Rafute.Supervisor, :start_server, [name, servers])
      end
    end
    :ok
  end
end
