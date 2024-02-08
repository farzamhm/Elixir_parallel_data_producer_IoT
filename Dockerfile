FROM hexpm/elixir:1.15.6-erlang-24.3.4.12-alpine-3.16.6

# The following are build arguments used to change variable parts of the image.
# The name of your application/release (required)
ARG APP_NAME=:data_simulator_redis
# The version of the application we are building (required)
ARG APP_VSN=0.1.0
# The environment to build with
ARG MIX_ENV=prod

ENV APP_NAME=${APP_NAME} \
    APP_VSN=${APP_VSN} \
    MIX_ENV=${MIX_ENV} 

RUN apk update && \
  apk upgrade --no-cache && \
  apk add --no-cache \
    bash \
    nodejs \
    yarn \
    git \
    build-base

# Install Redis.
RUN \
  cd /tmp && \
  wget http://download.redis.io/redis-stable.tar.gz && \
  tar xvzf redis-stable.tar.gz && \
  cd redis-stable && \
  make && \
  make install && \
  cp -f src/redis-sentinel /usr/local/bin && \
  mkdir -p /etc/redis && \
  cp -f *.conf /etc/redis && \
  rm -rf /tmp/redis-stable* && \
  sed -i 's/^\(bind .*\)$/# \1/' /etc/redis/redis.conf && \
  sed -i 's/^\(daemonize .*\)$/# \1/' /etc/redis/redis.conf && \
  sed -i 's/^\(dir .*\)$/# \1\ndir \/data/' /etc/redis/redis.conf && \
  sed -i 's/^\(logfile .*\)$/# \1/' /etc/redis/redis.conf && \
  sed -i 's/protected-mode yes/protected-mode no/' /etc/redis/redis.conf

# Define mountable directories.
VOLUME ["/data"]

# By convention, /opt is typically used for applications
WORKDIR /opt/app

# This copies our app source code into the build container
COPY . .

# This step installs all the build tools we'll need
RUN \
  mix local.rebar --force && \
  mix local.hex --force

RUN mix do deps.get, compile

RUN \
  mkdir -p /opt/built && \
  mix release &&\
  cp _build/${MIX_ENV}/${MIX_ENV}-${APP_VSN}.tar.gz /opt/app && \
  cd /opt/app && \
  tar -xzf ${MIX_ENV}-${APP_VSN}.tar.gz && \
  rm ${MIX_ENV}-${APP_VSN}.tar.gz


ENV REPLACE_OS_VARS=false \
    APP_NAME=${APP_NAME} \
    ERL_CRASH_DUMP='/tmp'

# CMD redis-server /etc/redis/redis.conf &; /opt/app/bin/${MIX_ENV} start;

ENTRYPOINT redis-server /etc/redis/redis.conf --daemonize yes && \
  /opt/app/bin/${MIX_ENV} start

EXPOSE 6379