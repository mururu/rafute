defmodule Rafute.Client do
  alias Rafute.Command

  def read(server, key) do
    case server |> fullname() |> :gen_fsm.sync_send_event(%Command{type: :read, args: {key}}) do
      {:ok, value} ->
        {:ok, value}
      {:error, {:redirect, leader}} ->
        read(leader, key)
      {:error, reason} ->
        {:error, reason}
    end
  end

  def write(server, key, value) do
    case server |> fullname() |> :gen_fsm.sync_send_event(%Command{type: :write, args: {key, value}}) do
      :ok ->
        :ok
      {:error, {:redirect, leader}} ->
        write(leader, key, value)
      {:error, reason} ->
        {:error, reason}
    end
  end

  def create_cluster(servers) do
    servers |> Enum.map(&fullname/1) |> Rafute.create_cluster()
  end

  defp fullname(server) when is_atom(server), do: {server, node()}
  defp fullname({_, _} = server), do: server
end
