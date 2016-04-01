defmodule Rafute.Client do
  alias Rafute.Command

  def read(server, key) do
    case :gen_fsm.sync_send_event(server, %Command{type: :read, args: {key}}) do
      {:ok, value} ->
        {:ok, value}
      {:error, {:redirect, leader}} ->
        read(leader, key)
      {:error, reason} ->
        {:error, reason}
    end
  end

  def write(server, key, value) do
    case :gen_fsm.sync_send_event(server, %Command{type: :write, args: {key, value}}) do
      :ok ->
        :ok
      {:error, {:redirect, leader}} ->
        write(leader, key, value)
      {:error, reason} ->
        {:error, reason}
    end
  end

  def create_cluster(servers) do
    Rafute.create_cluster(servers)
  end
end
