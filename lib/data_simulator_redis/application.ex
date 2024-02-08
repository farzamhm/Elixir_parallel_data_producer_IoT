defmodule DataSimulatorRedis.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Redix, name: :redis_conn, host: {System.get_env("REDIS_HOST")} , port=6379, ssl: true},
      DataSimulatorRedis.RedisPopulater
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: DataSimulatorRedis.Supervisor]
    {:ok, supervisor_pid} = Supervisor.start_link(children, opts)
      # Check Redis connection
    if DataSimulatorRedis.RedisPopulater.check_redis_connection() == :ok do
      {:ok, supervisor_pid}
    else
      {:error, :redis_connection_failed}
    end
  end
end
