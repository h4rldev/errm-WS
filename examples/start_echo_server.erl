-module(start_echo_server).
-export([start/0, start/1, stop/0]).

-spec start() -> {ok, pid()} | {error, term()}.
start() ->
  start(8080).

-spec start(port()) -> {ok, pid()} | {error, term()}.
start(Port) when is_integer(Port), Port > 0 ->
  Options = #{
    port => Port,
    handler_mod => echo_handler, % Module implementing errm_ws_handler behaviour
    handler_args => #{},
    acceptor_count => 4, 
    max_frame_size => 1 bsl 20, % 1 MiB
    timeout => 60, % Seconds
    compression => #{
      enabled => true,
      threshold => 1024 % 1 KiB
    }
  },
  case errm_ws:start(Options) of
    {ok, Pid} ->
      io:format("Echo server started on port ~p~n", [Port]),
      {ok, Pid};
    Other -> Other
  end.

-spec stop() -> ok.
stop() ->
  case whereis(errm_ws_acceptor) of
    undefined ->
      io:format("Server not running~n"),
      ok;
    Pid ->
      gen_server:stop(Pid),
      io:format("Server stopped~n"),
      ok
  end.
