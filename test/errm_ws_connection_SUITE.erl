-module (errm_ws_connection_SUITE).
-compile (export_all).
-include_lib ("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
  [simple_handshake_test, compression_test].

init_per_suite(Config) ->
  application:ensure_all_started(logger),
  Config.

end_per_suite(_Config) -> ok.

simple_handshake_test(_Config) ->
  {ok, Listen} = gen_tcp:listen(0, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
  {ok, Port} = inet:port(Listen),
  {ok, Client} = gen_tcp:connect("localhost", Port, [binary, {packet, raw}, {active, false}]),
  {ok, Server} = gen_tcp:accept(Listen, 1000),

  Key = <<"dGhlIHNhbXBsZSBub25jZQ==">>,
  RequestLines = [
    "GET /ws HTTP/1.1\r\n",
    "Upgrade: websocket\r\n",
    "Connection: Upgrade\r\n",
    "Sec-WebSocket-Key: ", Key, "\r\n",
    "Sec-WebSocket-Version: 13\r\n",
    "\r\n"
  ],

  RequestBin = iolist_to_binary(RequestLines),
  ok = gen_tcp:send(Client, RequestBin),

  {ok, HeaderData} = gen_tcp:recv(Server, 0, 1000),
  _HeaderData_ = not_a_test_to_binary(HeaderData),
  {ok, RequestMap} = errm_ws_acceptor:parse_http_request(_HeaderData_),

  HandlerArgs = #{compression => #{enabled => true, threshold => 10}},
  RequestMap1 = RequestMap#{compression => true},

  MaxFrameSize = 1 bsl 20,
  Timeout = 2,
  {ok, Pid} = errm_ws_connection:start_with_handshake(
    Server, RequestMap1, dummy_ws_handler, HandlerArgs, MaxFrameSize, Timeout
  ),

  ok = gen_tcp:controlling_process(Server, Pid),
  Pid ! {start, Server},

  timer:sleep(100),

  {ok, Data} = gen_tcp:recv(Client, 0, 5000),
  _Data_ = not_a_test_to_binary(Data),
  ?assertNotEqual(binary:match(_Data_, <<"101 Switching Protocols">>), nomatch),

  Payload = binary:copy(<<"x">>, 100),
  Frame = errm_ws_frame:encode_text(Payload),
  ok = gen_tcp:send(Client, Frame),

  {ok, Response} = gen_tcp:recv(Client, 0, 5000),
  _Response_ = not_a_test_to_binary(Response),
  {ok, Decoded, <<>>} = errm_ws_frame:decode(_Response_, MaxFrameSize),
  ?assertEqual(Payload, maps:get(payload, Decoded)),

  gen_tcp:close(Client),
  gen_tcp:close(Server),
  gen_tcp:close(Listen),
  ok.

compression_test(_Config) ->
  {ok, Listen} = gen_tcp:listen(0, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
  {ok, Port} = inet:port(Listen),
  {ok, Client} = gen_tcp:connect("localhost", Port, [binary, {packet, raw}, {active, false}]),
  {ok, Server} = gen_tcp:accept(Listen, 1000),

  Key = <<"dGhlIHNhbXBsZSBub25jZQ==">>,
  RequestLines = [
    "GET /ws HTTP/1.1\r\n",
    "Upgrade: websocket\r\n",
    "Connection: Upgrade\r\n",
    "Sec-WebSocket-Key: ", Key, "\r\n",
    "Sec-WebSocket-Version: 13\r\n",
    "Sec-WebSocket-Extensions: permessage-deflate\r\n",
    "\r\n"
  ],

  RequestBin = iolist_to_binary(RequestLines),
  ok = gen_tcp:send(Client, RequestBin),

  {ok, HeaderData} = gen_tcp:recv(Server, 0, 1000),
  _HeaderData_ = not_a_test_to_binary(HeaderData),
  {ok, RequestMap} = errm_ws_acceptor:parse_http_request(_HeaderData_),

  HandlerArgs = #{compression => #{enabled => true, threshold => 10}},
  RequestMap1 = RequestMap#{compression => true},

  MaxFrameSize = 1 bsl 20,
  Timeout = 2,
  {ok, Pid} = errm_ws_connection:start_with_handshake(
    Server, RequestMap1, dummy_ws_handler, HandlerArgs, MaxFrameSize, Timeout
  ),

  ok = gen_tcp:controlling_process(Server, Pid),
  Pid ! {start, Server},

  timer:sleep(100),

  {ok, Data} = gen_tcp:recv(Client, 0, 5000),
  _Data_ = not_a_test_to_binary(Data),
  ?assertNotEqual(binary:match(_Data_, <<"101 Switching Protocols">>), nomatch),

  Payload = binary:copy(<<"x">>, 100),
  Frame = errm_ws_frame:encode_text(Payload),
  ok = gen_tcp:send(Client, Frame),

  {ok, Response} = gen_tcp:recv(Client, 0, 5000),
  _Response_ = not_a_test_to_binary(Response),
  {ok, Decoded, <<>>} = errm_ws_frame:decode(_Response_, MaxFrameSize),

  ActualPayload = case maps:get(rsv1, Decoded, 0) of
    1 -> decompress_raw(maps:get(payload, Decoded));
    _ -> maps:get(payload, Decoded)
  end,
  ?assertEqual(Payload, ActualPayload),

  gen_tcp:close(Client),
  gen_tcp:close(Server),
  gen_tcp:close(Listen),
  ok.

not_a_test_to_binary(Data) when is_list(Data) ->
  list_to_binary(Data);
not_a_test_to_binary(Data) when is_binary(Data) ->
  Data;
not_a_test_to_binary(_) -> ok.


decompress_raw(Data) ->
  Ctx = zlib:open(),
  ok = zlib:inflateInit(Ctx, -15),
  Result = case zlib:inflate(Ctx, Data) of
    {error, Reason} ->
      ct:fail("Decompression failed: ~p", [Reason]);
    {need_dictionary, _, Output} ->
      iolist_to_binary(Output);
    Decompressed ->
      iolist_to_binary(Decompressed)
  end,
  zlib:inflateEnd(Ctx),
  Result.
