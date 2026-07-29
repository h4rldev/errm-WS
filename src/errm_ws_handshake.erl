-module (errm_ws_handshake).
-export ([validate/1, build_response/1]).
-export ([accept_key/1]).

-type request_map() :: #{
  method => binary(),
  path => binary(),
  headers => #{binary() => binary()},
  compression => boolean()
}.


-spec validate(RequestMap :: request_map()) -> {ok, RequestMap :: request_map()} | {error, Reason :: term()}.
validate(#{method := <<"GET">>, headers := Headers, compression := Enabled}) ->
  case validate_headers(Headers) of
    ok ->
      Compression = Enabled andalso parse_extensions(maps:get(<<"sec-websocket-extensions">>, Headers, undefined)),
      {ok, #{headers => Headers, compression => Compression}};
    Error -> Error
  end;
validate(_) ->
  {error, method_not_get}.

validate_headers(Headers) ->
  Upgrade = maps:get(<<"upgrade">>, Headers, <<"">>),
  Connection = maps:get(<<"connection">>, Headers, <<"">>),
  Version = maps:get(<<"sec-websocket-version">>, Headers, <<"">>),
  Key = maps:get(<<"sec-websocket-key">>, Headers, <<"">>),

  case {binary_to_lower(Upgrade), binary_to_lower(Connection), binary_to_lower(Version), validate_key(Key)} of
    {<<"websocket">>, ConnectionStr, <<"13">>, ok} ->
      case lists:member(<<"upgrade">>, binary:split(ConnectionStr, <<",">>, [global, trim_all])) of
        true -> ok;
        false -> {error, connection_header_missing_upgrade}
      end;
    {_, _, _, _} ->
      {error, invalid_handshake_headers}
  end.

-spec validate_key(Key :: undefined | binary()) -> ok | {error, Reason :: term()}.
validate_key(undefined) -> {error, missing_sec_websocket_key};
validate_key(Key) when byte_size(Key) >= 16, byte_size(Key) =< 24 -> ok;
validate_key(_) -> {error, invalid_sec_websocket_key_length}.

-spec build_response(RequestMap :: request_map()) -> iolist().
build_response(#{headers := #{<<"sec-websocket-key">> := Key}, compression := true}) ->
  Accept = accept_key(Key),
  [
   "HTTP/1.1 101 Switching Protocols\r\n",
   "Upgrade: websocket\r\n",
   "Connection: Upgrade\r\n",
   "Sec-WebSocket-Accept: ", Accept, "\r\n",
   "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover\r\n",
   "\r\n"
  ];
build_response(#{headers := #{<<"sec-websocket-key">> := Key}}) ->
  Accept = accept_key(Key),
  [
   "HTTP/1.1 101 Switching Protocols\r\n",
   "Upgrade: websocket\r\n",
   "Connection: Upgrade\r\n",
   "Sec-WebSocket-Accept: ", Accept, "\r\n",
   "\r\n"
  ];
build_response(_) ->
  "HTTP/1.1 400 Bad Request\r\n\r\n".

-spec accept_key(Key :: binary()) -> binary().
accept_key(Key) ->
  Magic = <<"258EAFA5-E914-47DA-95CA-C5AB0DC85B11">>,
  Hash = crypto:hash(sha, [Key, Magic]),
  base64:encode(Hash).


-spec parse_extensions(Header :: undefined | binary()) -> boolean().
parse_extensions(undefined) -> false;
parse_extensions(Header) ->
  Exts = binary:split(Header, <<",">>, [global, trim_all]),
  lists:any(fun(E) -> binary:match(binary_to_lower(E), <<"permessage-deflate">>) =/= nomatch end, Exts).


binary_to_lower(Bin) when is_binary(Bin) ->
  List = string:to_lower(binary_to_list(Bin)),
  list_to_binary(List);
binary_to_lower(List) when is_list(List) ->
  List2 = string:to_lower(List),
  list_to_binary(List2).
 
