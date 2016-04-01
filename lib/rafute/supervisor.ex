defmodule Rafute.Supervisor do
  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    supervise([], strategy: :one_for_one)
  end

  def start_server(name, servers) do
    child = worker(Rafute.Server, [name, servers], id: name)
    Supervisor.start_child(__MODULE__, child)
  end
end
