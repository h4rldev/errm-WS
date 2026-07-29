-module (errm_ws_acceptor).
-behaviour (gen_server).

-export ([start/1, start_link/1]).
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export ([acceptor_loop/5]).

-ifdef(TEST).
-export ([parse_http_request/1]).
-endif.


-record(state, {
  listen_sock    :: gen_tcp:socket() | undefined,
  port           :: non_neg_integer(),
  acceptors      :: [pid()],
  handler_mod    :: module(),
  handler_args   :: term(),
  max_frame_size :: non_neg_integer(),
  timeout        :: non_neg_integer()
}).

-spec start(Options :: errm_ws:ws_opts()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
  start(Options) ->
    start_link(Options).

-spec start_link(Options :: errm_ws:ws_opts()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_link(Options) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).


-spec init(Options :: errm_ws:ws_opts()) -> {ok, State :: #state{}} | {stop, Reason :: term()}.
init(Options) ->
  process_flag(trap_exit, true),
  Port          = maps:get(port, Options),
  HandlerMod    = maps:get(handler_mod, Options),
  HandlerArgs   = maps:get(handler_args, Options),
  AcceptorCount = maps:get(acceptor_count, Options, erlang:system_info(schedulers_online) * 2),
  MaxFrameSize  = maps:get(max_frame_size, Options, 1 bsl 20), % 1 MiB Max default.
  Timeout       = maps:get(timeout, Options, 60),

  case gen_tcp:listen(Port, [binary, {packet, raw}, {active, false}, {reuseaddr, true}, {nodelay, true}, {send_timeout, 30000}, {keepalive, true}, {backlog, 1024}]) of
    {ok, ListenSock} ->
      {ok, ActualPort} = inet:port(ListenSock),
      logger:debug("[errm_ws] Listening on port ~p with ~p acceptors", [ActualPort, AcceptorCount]),
      Acceptors = [spawn_acceptor(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout) || _ <- lists:seq(1, AcceptorCount)],
      {ok, #state{listen_sock = ListenSock, port = ActualPort, acceptors = Acceptors, handler_mod = HandlerMod, handler_args = HandlerArgs, max_frame_size = MaxFrameSize, timeout = Timeout}};
    {error, Reason} ->
      {stop, {cannot_listen, Reason}}
  end.

-spec handle_call(Req :: term(), From :: {pid(), term()}, State :: #state{}) -> {reply, {ok, non_neg_integer()}, #state{}}.
handle_call(_Req, _From, State) ->
  {reply, {ok, State#state.port}, State}.

-spec handle_cast(Req :: term(), State :: #state{}) -> {noreply, #state{}}.
handle_cast(_Req, State) ->
  {noreply, State}.

-spec handle_info(Info :: term(), State :: #state{}) -> {noreply, #state{}}.
handle_info({'EXIT', Pid, Reason}, State=#state{acceptors=Accs, listen_sock=LS}) when LS =/= undefined ->
  case lists:member(Pid, Accs) of
    true ->
      logger:debug("[errm_ws] Acceptor ~p restarted (reason: ~p)", [Pid, Reason]),
      NewAcceptor = spawn_acceptor(State#state.listen_sock, State#state.handler_mod, State#state.handler_args, State#state.max_frame_size, State#state.timeout),
      Rest = [A || A <- Accs, A =/= Pid],
      {noreply, State#state{acceptors = [NewAcceptor | Rest]}};
    false ->
      {noreply, State}
  end;
handle_info(_Info, State) ->
  {noreply, State}.

-spec terminate(Reason :: term(), State :: #state{}) -> ok.
terminate(_Reason, #state{listen_sock=undefined}) -> ok;
terminate(_Reason, #state{listen_sock=Sock}) ->
  gen_tcp:close(Sock),
  ok.


-spec spawn_acceptor(ListenSock :: gen_tcp:socket(), HandlerMod :: module(), HandlerArgs :: term(), MaxFrameSize :: non_neg_integer(), Timeout :: non_neg_integer()) -> pid().
spawn_acceptor(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout) ->
  spawn_link(fun() -> acceptor_loop(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout) end).


-spec acceptor_loop(ListenSock :: gen_tcp:socket(), HandlerMod :: module(), HandlerArgs :: term(), MaxFrameSize :: non_neg_integer(), Timeout :: non_neg_integer()) -> no_return().
acceptor_loop(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout) ->
  case is_map(HandlerArgs) of
    true ->
      case gen_tcp:accept(ListenSock) of
        {ok, ClientSock} ->
          case read_http_request(ClientSock, Timeout) of
            {ok, RequestMap, _Rest} ->
              CompressionConfig = maps:get(compression, HandlerArgs, #{enabled => false}),
              CompressionEnabled = case CompressionConfig of
                CompConfig when is_map(CompConfig) -> maps:get(enabled, CompConfig, false)
              end,
              RequestMap1 = RequestMap#{compression => CompressionEnabled},

              case errm_ws_connection:start_with_handshake(
                     ClientSock, RequestMap1, HandlerMod,
                     HandlerArgs, MaxFrameSize, Timeout) of
                {ok, Pid} ->
                  ok = gen_tcp:controlling_process(ClientSock, Pid),
                  Pid ! {start, ClientSock};
                {error, Reason} ->
                  logger:error("[errm_ws] Failed to start connection: ~p", [Reason]),
                  gen_tcp:close(ClientSock)
              end,
              acceptor_loop(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout);
            {error, Reason} ->
              logger:error("[errm_ws] Failed to read HTTP request: ~p", [Reason]),
              gen_tcp:close(ClientSock),
              acceptor_loop(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout)
          end;
        {error, Reason} ->
          logger:error("[errm_ws] Failed to accept connection: ~p", [Reason]),
          acceptor_loop(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout)
      end;
    false ->
      logger:error("[errm_ws] Failed to accept connection: invalid handler arguments"),
      acceptor_loop(ListenSock, HandlerMod, HandlerArgs, MaxFrameSize, Timeout)
  end.


-spec read_http_request(Sock :: gen_tcp:socket(), Timeout :: non_neg_integer()) -> {ok, RequestMap :: map(), Rest :: binary()} | {error, Reason :: term()}.
read_http_request(Sock, Timeout) ->
  read_loop(Sock, <<>>, Timeout, 8 bsl 10).

read_loop(_Sock, Acc, _Timeout, MaxLen) when byte_size(Acc) >= MaxLen ->
  {error, request_too_large};
read_loop(Sock, Acc, Timeout, MaxLen) ->
  case gen_tcp:recv(Sock, 0, Timeout * 1000) of
    {ok, Data} when Data =/= <<>>, is_binary(Data) ->
      NewAcc = <<Acc/binary, Data/binary>>,
      case binary:match(NewAcc, <<"\r\n\r\n">>) of
        nomatch ->
          read_loop(Sock, NewAcc, Timeout, MaxLen);
        {Pos, _} ->
          <<HeaderData:Pos/binary, Rest/binary>> = NewAcc,
          case parse_http_request(HeaderData) of
            {ok, RequestMap} ->
              {ok, RequestMap, Rest};
            {error, Reason} ->
              {error, Reason}
          end
      end;
    {error, Reason1} ->
      {error, Reason1}
  end.


-spec parse_http_request(HeaderData :: binary()) -> {ok, RequestMap :: map()} | {error, Reason :: term()}.
parse_http_request(HeaderData) ->
  Lines = binary:split(HeaderData, <<"\r\n">>, [global, trim]),
  case Lines of
    [] -> {error, empty_request};
    [RequestLine | Rest] ->
      case parse_request_line(RequestLine) of
        {ok, Method, Path} ->
          Headers = parse_headers(Rest, #{}),
          {ok, #{method => Method, path => Path, headers => Headers}};
        {error, Reason} ->
          {error, Reason}
      end
  end.

parse_request_line(RequestLine) ->
  Parts = binary:split(RequestLine, <<" ">>, [global]),
  case Parts of
    [Method, Path, _Version] ->
      {ok, Method, Path};
    _ -> {error, invalid_request_line}
  end.

parse_headers([], Acc) -> Acc;
parse_headers([Line | Rest], Acc) ->
  case binary:split(Line, <<":">>) of
    [Name, Value] ->
      NameLower = string:lowercase(Name),
      parse_headers(Rest, Acc#{string:trim(NameLower) => string:trim(Value)});
    _ ->
      parse_headers(Rest, Acc)
  end.
