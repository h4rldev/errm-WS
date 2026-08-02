-module (errm_ws).
-export ([start/1]).
-export ([upgrade/4, send_text/2, send_binary/2, close/1]).


-type handler_state() :: term().
-type ws_opts() :: #{
  port => non_neg_integer(),
  handler_mod => module(),
  handler_args => term(),
  acceptor_count => non_neg_integer(),
  max_frame_size => non_neg_integer(),
  timeout => non_neg_integer(),
  compression => #{
    enabled => boolean(),
    threshold => non_neg_integer()
  }
}.

-export_type ([handler_state/0, ws_opts/0]).

-spec start(Options :: ws_opts()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start(Options) ->
  Options1 = add_compression_defaults(Options),
  errm_ws_acceptor:start(Options1).


-spec upgrade(Sock :: gen_tcp:socket(), RequestMap :: map(), HandlerMod :: module(), HandlerArgs :: term()) -> ok.
upgrade(Sock, RequestMap, HandlerMod, HandlerArgs) ->
  DefaultMaxFrameSize = 1 bsl 20,
  DefaultTimeout = 60,
  HandlerArgs1 = add_compression_to_args(HandlerArgs),
  case errm_ws_connection:start_upgraded(Sock, RequestMap, HandlerMod, HandlerArgs1, DefaultMaxFrameSize, DefaultTimeout) of
    {ok, Pid} ->
      unlink(Pid),
      ok = gen_tcp:controlling_process(Sock, Pid),
      Pid ! {start, Sock},
      logger:debug("[errm_ws]: Websocket, running on ~p", [Pid]),
      ok;
    {error, Reason} ->
      logger:error("[errm_ws]: Failed to upgrade connection: ~p", [Reason]),
      gen_tcp:close(Sock),
      ok;
    ignore ->
      logger:error("[errm_ws]: Failed to upgrade connection: unknown"),
      gen_tcp:close(Sock),
      ok
  end.

-spec send_text(Connection :: pid(), Data :: binary()) -> ok.
send_text(Connection, Data) when is_pid(Connection), is_binary(Data) ->
  Connection ! {send, text, Data},
  ok.

-spec send_binary(Connection :: pid(), Data :: binary()) -> ok.
send_binary(Connection, Data) when is_pid(Connection), is_binary(Data) ->
  Connection ! {send, binary, Data},
  ok.

-spec close(Connection :: pid()) -> ok.
close(Connection) when is_pid(Connection) ->
  Connection ! close,
  ok.


add_compression_defaults(Options) ->
  Compression = maps:get(compression, Options, #{}),
  Compression1 = Compression#{threshold => maps:get(threshold, Compression, 1024), enabled => maps:get(enabled, Compression, false)},
  Options#{compression => Compression1}.

add_compression_to_args(HandlerArgs) ->
  Compression = maps:get(compression, HandlerArgs, #{}),
  Compression1 = Compression#{threshold => maps:get(threshold, Compression, 1024), enabled => maps:get(enabled, Compression, false)},
  HandlerArgs#{compression => Compression1}.

