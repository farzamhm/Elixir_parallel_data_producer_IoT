defmodule DataSimulatorRedisTest do
  use ExUnit.Case
  doctest DataSimulatorRedis

  test "greets the world" do
    assert DataSimulatorRedis.hello() == :world
  end
end
