-module (errm_ws_connection).
-behaviour (gen_server).

-export ([start_with_handshake/6, start_upgraded/6]).
-export ([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).


-record(state, {
  sock           :: gen_tcp:socket(),
  handler_mod    :: module(),
  handler_state  :: term(),
  buffer         :: binary(),
  frag_buffer    :: binary(),
  frag_opcode    :: undefined | text | binary,
  frag_rsv1      :: 0 | 1,
  max_frame_size :: non_neg_integer(),
  timeout        :: non_neg_integer(),
  timers         :: reference() | undefined,
  compress_enabled :: boolean(),
  compress_threshold :: non_neg_integer(),
  deflate_context :: zlib:zstream() | undefined,
  inflate_context :: zlib:zstream() | undefined,
  start_mode :: with_handshake | upgraded | undefined,
  start_args :: map() | undefined
}).

-type state() :: #state{}.
-type process_result() :: {ok, state()} | {stop, state()} | {error, term(), state()}.

-spec start_with_handshake(Sock :: gen_tcp:socket(), RequestMap :: map(), HandlerMod :: module(), HandlerArgs :: term(), MaxFrameSize :: non_neg_integer(), Timeout :: non_neg_integer()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_with_handshake(Sock, RequestMap, HandlerMod, HandlerArgs, MaxFrameSize, Timeout) ->
  gen_server:start_link(?MODULE, [with_handshake, Sock, RequestMap, HandlerMod, HandlerArgs, MaxFrameSize, Timeout], []).

-spec start_upgraded(Sock :: gen_tcp:socket(), RequestMap :: map(), HandlerMod :: module(), HandlerArgs :: term(), MaxFrameSize :: non_neg_integer(), Timeout :: non_neg_integer()) -> {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
start_upgraded(Sock, RequestMap, HandlerMod, HandlerArgs, MaxFrameSize, Timeout) ->
  gen_server:start_link(?MODULE, [upgraded, Sock, RequestMap, HandlerMod, HandlerArgs, MaxFrameSize, Timeout], []).


init([Mode, Sock, RequestMap, HandlerMod, HandlerArgs, MaxFrameSize, Timeout]) ->
  process_flag(trap_exit, true),
  {ok, #state{
    sock = Sock,
    handler_mod = HandlerMod,
    handler_state = undefined,
    buffer = <<>>,
    frag_buffer = <<>>,
    frag_opcode = undefined,
    frag_rsv1 = 0,
    max_frame_size = MaxFrameSize,
    timeout = Timeout,
    timers = undefined,
    compress_enabled = false,
    compress_threshold = 1024,
    deflate_context = undefined,
    inflate_context = undefined,
    start_mode = Mode,
    start_args = #{
      request_map => RequestMap,
      handler_args => HandlerArgs
    }
  }}.


handle_call(_Req, _From, State) ->
  {reply, {error, unknown_call}, State}.

handle_cast(_Req, State) ->
  {noreply, State}.

handle_info({start, Sock}, State=#state{sock=Sock, start_mode=Mode, start_args=StartArgs}) ->
  case do_handshake_and_take_ownership(Sock, Mode, StartArgs) of
    {ok, ValidatedMap} ->
      HandlerMod = State#state.handler_mod,
      HandlerArgs = maps:get(handler_args, StartArgs),
      Timeout = State#state.timeout,

      case HandlerMod:init(ValidatedMap, HandlerArgs) of
        {ok, HandlerState} ->
          Timers = start_timer(Timeout),
          CompressionConfig = maps:get(compression, HandlerArgs, #{
            enabled => false,
            threshold => 1024
          }),
          CompressionEnabled = maps:get(enabled, CompressionConfig, false),
          CompressionThreshold = maps:get(threshold, CompressionConfig, 1024),
          Negotiated = maps:get(compression, ValidatedMap, false),

          {DefCtx, InfCtx} = case CompressionEnabled andalso Negotiated of
            true ->
              Def = zlib:open(),
              zlib:deflateInit(Def, default, deflated, -15, 8, default),
              Inf = zlib:open(),
              zlib:inflateInit(Inf, -15),
              {Def, Inf};
            false ->
              {undefined, undefined}
          end,

          {noreply, State#state{
            handler_state = HandlerState,
            compress_enabled = CompressionEnabled andalso Negotiated,
            compress_threshold = CompressionThreshold,
            deflate_context = DefCtx,
            inflate_context = InfCtx,
            timers = Timers,
            start_mode = undefined,
            start_args = undefined
          }};
        {error, Reason} ->
          gen_tcp:close(Sock),
          {stop, {handler_init_failed, Reason}, State}
      end;
    {error, Reason} ->
      gen_tcp:close(Sock),
      {stop, {handshake_failed, Reason}, State}
  end;

handle_info({tcp, Sock, Data}, State=#state{sock=Sock}) ->
  State1 = reset_timer(State),
  handle_tcp_data(Data, State1);

handle_info({tcp_closed, Sock}, State=#state{sock=Sock}) ->
  {stop, normal, State};

handle_info({tcp_error, Sock, Reason}, State=#state{sock=Sock}) ->
    logger:error("[errm_ws]: Socket error: ~p", [Reason]),
    {stop, {socket_error, Reason}, State};

handle_info(close, State=#state{sock=Sock}) ->
  gen_tcp:send(Sock, errm_ws_frame:encode_close()),
  {stop, normal, State};

handle_info(timeout, State=#state{timeout=Timeout}) when Timeout > 0 ->
  logger:debug("[errm_ws]: Timeout reached, closing connection"),
  {stop, normal, State};

handle_info({send, text, Data}, State=#state{sock=Sock, compress_enabled = true, deflate_context = DefCtx, compress_threshold = Threshold}) ->
  DataBin = iolist_to_binary(Data),
  {Rsv1, Payload} = case byte_size(DataBin) >= Threshold of
    true -> {1, compress_payload(DefCtx, DataBin)};
    false -> {0, DataBin}
  end,
  Frame = errm_ws_frame:encode_frame(Payload, 1, 1, Rsv1),
  gen_tcp:send(Sock, Frame),
  {noreply, State};
handle_info({send, text, Data}, State=#state{sock=Sock}) ->
  DataBin = iolist_to_binary(Data),
  Frame = errm_ws_frame:encode_text(DataBin),
  gen_tcp:send(Sock, Frame),
  {noreply, State};

handle_info({send, binary, Data}, State=#state{sock=Sock, compress_enabled = true, deflate_context = DefCtx, compress_threshold = Threshold}) ->
  DataBin = iolist_to_binary(Data),
  {Rsv1, Payload} = case byte_size(DataBin) >= Threshold of
    true -> {1, compress_payload(DefCtx, DataBin)};
    false -> {0, DataBin}
  end,
  Frame = errm_ws_frame:encode_frame(Payload, 1, 2, Rsv1),
  gen_tcp:send(Sock, Frame),
  {noreply, State};
handle_info({send, binary, Data}, State=#state{sock=Sock}) ->
  DataBin = iolist_to_binary(Data),
  Frame = errm_ws_frame:encode_binary(DataBin),
  gen_tcp:send(Sock, Frame),
  {noreply, State};

handle_info(Info, State=#state{handler_mod=Mod, handler_state=HandlerState}) ->
  case Mod:handle_info(Info, HandlerState) of
    {ok, NewHandlerState} ->
      {noreply, State#state{handler_state=NewHandlerState}};
    {close, NewHandlerState} ->
      {stop, normal, State#state{handler_state=NewHandlerState}};
    {error, Reason} ->
      logger:error("[errm_ws] Handler info error: ~p", [Reason]),
      {stop, {handler_error, Reason}, State}
  end;

handle_info(Info, State) ->
  logger:warning("[errm_ws] Unhandled message: ~p", [Info]),
  {noreply, State}.

terminate(Reason, #state{handler_mod=Mod, handler_state=HandlerState, sock=Sock, deflate_context = DefCtx, inflate_context = InfCtx}) ->
  Mod:terminate(Reason, HandlerState),
  compress_close_if_open(DefCtx),
  compress_close_if_open(InfCtx),
  gen_tcp:close(Sock),
  ok.


-spec handle_tcp_data(Data :: binary(), State :: state()) -> {noreply, State :: state()} | {stop, Reason :: term(), State :: state()}.
handle_tcp_data(Data, State) ->
  Buffer = State#state.buffer,
  NewBuffer = <<Buffer/binary, Data/binary>>,
  case errm_ws_frame:decode(NewBuffer, State#state.max_frame_size) of
    {ok, Frame, Rest} ->
      case process_frame(Frame, State) of
        {ok, State1} ->
          handle_tcp_data(Rest, State1);
        {stop, State1} ->
          inet:setopts(State#state.sock, [{active, once}]),
          {stop, normal, State1#state{buffer = Rest}};
        {error, Reason, State1} ->
          logger:error("[errm_ws]: Failed to process frame: ~p", [Reason]),
          {stop, {process_error, Reason}, State1}
      end;
    {partial, _} ->
      inet:setopts(State#state.sock, [{active, once}]),
      {noreply, State#state{buffer = NewBuffer}};
    {error, Reason} ->
      logger:error("[errm_ws]: Failed to decode frame: ~p", [Reason]),
      {stop, {frame_error, Reason}, State}
  end.


-spec process_frame(Frame :: errm_ws_frame:frame(), State :: state()) -> process_result().
process_frame(#{opcode := text, payload := Payload, fin := 1, rsv1 := 1}, State=#state{compress_enabled=true, inflate_context=InfCtx}) ->
  Decompressed = decompress_payload(InfCtx, Payload),
  Mod = State#state.handler_mod,
  case erlang:function_exported(Mod, handle_text, 2) of
    true ->
      case Mod:handle_text(Decompressed, State#state.handler_state) of
        {ok, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
        {close, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState}};
        {error, Reason} ->
          logger:error("[errm_ws] Handler text error: ~p", [Reason]),
          {error, Reason, State}
      end;
    false ->
      logger:warning("[errm_ws] Received text frame but handler doesn't implement handle_text/2"),
      errm_ws_frame:send_close(State#state.sock, <<16#03, "Unsupported text">>),
      {stop, State}
  end;
process_frame(#{opcode := text, payload := Payload, fin := 1, rsv1 := 0}, State) ->
  Mod = State#state.handler_mod,
  case erlang:function_exported(Mod, handle_text, 2) of
    true ->
      case Mod:handle_text(Payload, State#state.handler_state) of
        {ok, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
        {close, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState}};
        {error, Reason} ->
          logger:error("[errm_ws] Handler text error: ~p", [Reason]),
          {error, Reason, State}
      end;
    false ->
      logger:warning("[errm_ws] Received text frame but handler doesn't implement handle_text/2"),
      errm_ws_frame:send_close(State#state.sock, <<16#03, "Unsupported text">>),
      {stop, State}
  end;
process_frame(#{opcode := text, payload := Payload, fin := 0, rsv1 := Rsv1}, State) ->
  Mod = State#state.handler_mod,
  case erlang:function_exported(Mod, handle_text, 2) of
    true ->
      {ok, State#state{frag_buffer = Payload, frag_opcode = text, frag_rsv1 = Rsv1}};
    false ->
      logger:warning("[errm_ws] Received text fragment but handler doesn't support text"),
      errm_ws_frame:send_close(State#state.sock, <<16#03, "Unsupported text">>),
      {stop, State}
  end;
process_frame(#{opcode := binary, payload := Payload, fin := 1, rsv1 := 1}, State=#state{compress_enabled=true, inflate_context=InfCtx}) ->
  Decompressed = decompress_payload(InfCtx, Payload),
  Mod = State#state.handler_mod,
  case Mod:handle_binary(Decompressed, State#state.handler_state) of
    {ok, NewHandlerState} ->
      {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
    {close, NewHandlerState} ->
      {ok, State#state{handler_state = NewHandlerState}};
    {error, Reason} ->
      logger:error("[errm_ws] Handler binary error: ~p", [Reason]),
      {error, Reason, State}
  end;
process_frame(#{opcode := binary, payload := Payload, fin := 1, rsv1 := 0}, State) ->
  Mod = State#state.handler_mod,
  case Mod:handle_binary(Payload, State#state.handler_state) of
    {ok, NewHandlerState} ->
      {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
    {close, NewHandlerState} ->
      {ok, State#state{handler_state = NewHandlerState}};
    {error, Reason} ->
      logger:error("[errm_ws] Handler binary error: ~p", [Reason]),
      {error, Reason, State}
  end;
process_frame(#{opcode := binary, payload := Payload, fin := 0, rsv1 := Rsv1}, State) ->
  {ok, State#state{frag_buffer = Payload, frag_opcode = binary, frag_rsv1 = Rsv1}};
process_frame(#{opcode := continuation, payload := Payload, fin := 1}, State) ->
  FragBuffer = State#state.frag_buffer,
  Complete = <<FragBuffer/binary, Payload/binary>>,
  Rsv1 = State#state.frag_rsv1,
  case State#state.frag_opcode of
    text ->
      Decompressed = case Rsv1 =:= 1 andalso State#state.compress_enabled of
        true -> decompress_payload(State#state.inflate_context, Complete);
        false -> Complete
      end,
      Mod = State#state.handler_mod,
      case Mod:handle_text(Decompressed, State#state.handler_state) of
        {ok, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
        {close, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState}};
        {error, Reason} ->
          {error, Reason, State}
      end;
    binary ->
      Decompressed = case Rsv1 =:= 1 andalso State#state.compress_enabled of
        true -> decompress_payload(State#state.inflate_context, Complete);
        false -> Complete
      end,
      Mod = State#state.handler_mod,
      case erlang:function_exported(Mod, handle_binary, 2) of
        true ->
          case Mod:handle_binary(Decompressed, State#state.handler_state) of
            {ok, NewHandlerState} ->
              {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
            {close, NewHandlerState} ->
              {ok, State#state{handler_state = NewHandlerState}};
            {error, Reason} ->
              {error, Reason, State}
          end;
        false ->
          logger:warning("[errm_ws] Received binary fragment but handler doesn't support binary"),
          errm_ws_frame:send_close(State#state.sock, <<16#03, "Unsupported binary">>),
          {stop, State}
      end;
    undefined ->
      logger:error("[errm_ws] Continuation frame without a start opcode"),
      {ok, State}
  end;
process_frame(#{opcode := continuation, payload := Payload, fin := 0}, State) ->
  FragBuffer = State#state.frag_buffer,
  NewFrag = <<FragBuffer/binary, Payload/binary>>,
  {ok, State#state{frag_buffer = NewFrag}};
process_frame(#{opcode := ping, payload := Payload}, State) ->
  Mod = State#state.handler_mod,
  errm_ws_frame:send_pong(State#state.sock, Payload),

  case erlang:function_exported(Mod, handle_ping, 2) of
    true ->
      case Mod:handle_ping(Payload, State#state.handler_state) of
        {ok, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState}};
        {close, NewHandlerState} ->
          {stop, State#state{handler_state = NewHandlerState}};
        {error, Reason} ->
          logger:error("[errm_ws] Handler ping error: ~p", [Reason]),
          {error, Reason, State}
      end;
    false ->
      {ok, State}
  end;
process_frame(#{opcode := pong, payload := Payload}, State) ->
  Mod = State#state.handler_mod,

  case erlang:function_exported(Mod, handle_pong, 2) of
    true ->
      case Mod:handle_pong(Payload, State#state.handler_state) of
        {ok, NewHandlerState} ->
          {ok, State#state{handler_state = NewHandlerState, frag_buffer = <<>>, frag_opcode = undefined}};
        {close, NewHandlerState} ->
          {stop, State#state{handler_state = NewHandlerState}};
        {error, Reason} ->
          logger:error("[errm_ws] Handler pong error: ~p", [Reason]),
          {error, Reason, State}
      end;
    false ->
      {ok, State}
  end;
process_frame(#{opcode := close, payload := Payload}, State) ->
  errm_ws_frame:send_close(State#state.sock, Payload),
  {stop, State}.


start_timer(Timeout) when Timeout > 0 ->
  erlang:send_after(Timeout * 1000, self(), timeout);
start_timer(_) ->
  undefined.

reset_timer(State=#state{timeout=Timeout, timers=OldTimer}) when Timeout > 0 ->
  case OldTimer of
    undefined -> ok;
    Ref -> erlang:cancel_timer(Ref)
  end,
  NewTimer = erlang:send_after(Timeout * 1000, self(), timeout),
  State#state{timers = NewTimer};
reset_timer(State) ->
  State.

compress_close_if_open(undefined) -> ok;
compress_close_if_open(Ctx) -> zlib:close(Ctx).

-spec compress_payload(Ctx :: zlib:zstream() | undefined, Data :: binary()) -> binary().

compress_payload(undefined, Data) ->
  Data;
compress_payload(Ctx, Data) ->
  zlib:deflateReset(Ctx),
  case zlib:deflate(Ctx, Data, finish) of
    {error, Reason} ->
      logger:error("[errm_ws] Compression failed: ~p", [Reason]),
      Data;
    Compressed ->
      zlib:deflateReset(Ctx),
      iolist_to_binary(Compressed)
  end.

-spec decompress_payload(Ctx :: zlib:zstream() | undefined, Data :: binary()) -> binary().

decompress_payload(undefined, Data) ->
  Data;

decompress_payload(Ctx, Data) ->
  zlib:inflateReset(Ctx),
  case zlib:inflate(Ctx, Data) of
    {error, Reason} ->
      logger:error("[errm_ws] Decompression failed: ~p", [Reason]),
      Data;
    {need_dictionary, _Adler32, Output} ->
      logger:warning("[errm_ws] Decompression needs dictionary, using raw output"),
      iolist_to_binary(Output);
    Decompressed ->
      zlib:inflateReset(Ctx),
      iolist_to_binary(Decompressed)
  end.



-spec do_handshake_and_take_ownership(Sock :: gen_tcp:socket(), Mode :: with_handshake | upgraded, StartArgs :: map()) -> {ok, RequestMap :: map()} | {error, Reason :: term()}.
do_handshake_and_take_ownership(Sock, with_handshake, StartArgs) ->
  RequestMap = maps:get(request_map, StartArgs),
  case errm_ws_handshake:validate(RequestMap) of
    {ok, ValidatedMap} ->
      Response = errm_ws_handshake:build_response(ValidatedMap),
      gen_tcp:send(Sock, Response),
      ok = gen_tcp:controlling_process(Sock, self()),
      inet:setopts(Sock, [{active, once}, {packet, raw}, {nodelay, true}]),
      {ok, ValidatedMap};
    {error, Reason} ->
      {error, Reason}
  end;

do_handshake_and_take_ownership(Sock, upgraded, StartArgs) ->
  ok = gen_tcp:controlling_process(Sock, self()),
  inet:setopts(Sock, [{active, once}, {packet, raw}, {nodelay, true}]),
  RequestMap = maps:get(request_map, StartArgs),
  {ok, RequestMap}.
