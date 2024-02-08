defmodule DataSimulatorRedis.RedisPopulater do
  use GenServer
  require Logger

  @spec start_link(any) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_init_arg) do
    start_work()
    {:ok, []}
  end

  def start_work() do
    Process.send_after(self(), :loop, 1000)
  end

  def handle_info(:loop, state) do
    number_of_projects = String.to_integer(System.get_env("NUMBER_OF_PROJECTS"))
    number_of_points = String.to_integer(System.get_env("NUMBER_OF_POINTS"))
    delay = String.to_integer(System.get_env("DELAY"))
    spawn(fn -> loop(number_of_projects, number_of_points, delay) end)
    {:noreply, state}
  end

  # def generate_readings(path_base, number_of_points) do
  #   {:ok, json_msg} =
  #     Enum.map(1..number_of_points, fn point_number ->
  #       %{
  #         path: "#{path_base}FAKE_POINT_#{point_number}",
  #         value: "#{:rand.uniform(100)}",
  #         timestamp: "#{DateTime.to_string(DateTime.utc_now())}"
  #       }
  #     end)
  #     #|> JSON.encode()

  #   json_msg
  # end

  # def start_link(opts \\ [])

  # def start_link(opts) do
  #     Redix.start_link("rediss://#{System.get_env("REDIS_HOST")}", [ ssl: true ] ++ opts)
  # end

  def read_redis_stream do
    conn = :redis_conn
    {:ok, response} = Redix.command(conn, ["XREAD", "STREAMS", "redis_project_stream", "0"])

    parse_redis_stream_response(response)
  end

  defp parse_redis_stream_response(response) do
    case response do
      [["redis_project_stream", messages]] ->
        Enum.map(messages, fn [id, ["data", data]] ->
          %{
            id: id,
            data: JSON.decode!(data)
          }
        end)
      _ ->
        []
    end
  end


  def generate_readings(number_of_points) do
    Enum.map(1..number_of_points, fn point_number ->
      %{
        path: "FAKE_SYSTEM/FAKE_POINT_#{point_number}",
        value: "#{:rand.uniform(100)}",
        timestamp: "#{DateTime.to_string(DateTime.utc_now())}"
      }
    end)
  end
  # def fill_redis(number_of_projects, number_of_points) do
  #   {:ok, conn} = Redix.start_link("redis://#{System.get_env("REDIS_HOST")}")

  #   Enum.each(1..number_of_projects, fn project_number ->
  #     Redix.command(conn, [
  #       "SET",
  #       "redis_project_#{project_number}",
  #       generate_readings("FAKE_SYSTEM/", number_of_points)
  #     ])
  #   end)
  # end
  def start_link(_) do
    # Check Redis connection
    case check_redis_connection() do
      :ok ->
        # Continue with the startup process
      {:error, reason} ->
        # Log error and handle it appropriately
    end
  end

  defp check_redis_connection do
    conn = :redis_conn
    case Redix.command(conn, ["PING"]) do
      {:ok, "PONG"} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  def fill_redis(number_of_projects, number_of_points) do
    conn= :redis_conn

    Enum.each(1..number_of_projects, fn project_number ->
      readings = generate_readings(number_of_points)
      json_data = %{
        meta_data: "project_#{project_number}",
        readings: readings
      } |> JSON.encode!()
      Logger.info("Json_data: #{inspect(json_data)}")
      Redix.command(conn, ["XADD", "redis_project_stream", "*", "data", json_data])
    end)
  end

  def loop(number_of_projects, number_of_points, delay) do
    Logger.info(
      "Populating Redis with #{number_of_projects} projects, each with #{number_of_points} points."
    )

    start_time = DateTime.utc_now() |> DateTime.to_unix()
    fill_redis(number_of_projects, number_of_points)
    end_time = DateTime.utc_now() |> DateTime.to_unix()
    offset_delay = delay - (end_time - start_time)
    Logger.info("Finished populating Redis Keys, delyaing #{offset_delay} seconds.")
    :timer.sleep(:timer.seconds(offset_delay))
    loop(number_of_projects, number_of_points, delay)
  end
end
