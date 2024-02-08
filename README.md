# DataSimulator2Redis
## Description

A combination of Redis and an Elixir based data simulator. It simulates a configurable number of readings for a configurable number of fake devices.

This is for testing IoT/HVAC ingestion pipelines.

The application is setup to create a docker container, but it can be also be run using a combination of `docker run...` and  `iex -S mix` 

## Configuration

The configuration variables that need to be filled out:  
 * `NUMBER_OF_PROJECTS` - The number of simulated projects to create, stored as individual keys
 * `NUMBER_OF_POINTS` - The number of points to created simulated data for, per project
 * `Delay` - The number of seconds to delay between the start of one population cycle to the start of the next
 * `REDIS_HOST` - The Redis host address to send the simulated data to. Keep it at `127.0.0.1` if you are using the full docker container version of this app.
  
  
The configuration method is different based on whether you want to run it in Docker, or via `iex`.  
 * Docker configuration is done via the `docker.env` file in the `config` directory.  
 * The `iex` configuration is done via the `set_envs.sh` file in the root directory. This needs to
 be ran before you fire up the `iex console`, as it sets the ENV vars that will be used. 

## How to run it
  
### Via Docker  

#### On Linux  
Steps needed to run it in a docker container:
 1. Clone the repo
 2. Set the variables in `config/docker.env`
 3. run `sudo make build` (Disregard the deprecation warnings)
 4. run `docker-compose up`
  
  
  
### Via IEx
The steps below assume you have the following installed on your system :  
 * `Elixir` version `1.14` or higher  
 * `Erlang` with `OTP` version `24` or higher  
  
Steps to run it via `iex`:
 1. Clone the repo
 2. Set the variables in `set_envs.sh`
 3. run `chmod +x set_envs.sh` - this only needs to be done once
 4. run `. ./set_envs.sh`
 5. run `mix deps.get`
 6. run `iex -S mix`
