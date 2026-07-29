-module (errm_ws_handshake_tests).
-include_lib ("eunit/include/eunit.hrl").

valid_request_test() ->
  Request = #{
    method => <<"GET">>,
    headers => #{
      <<"upgrade">> => <<"websocket">>,
      <<"connection">> => <<"upgrade">>,
      <<"sec-websocket-version">> => <<"13">>,
      <<"sec-websocket-key">> => <<"dGhlIHNhbXBsZSBub25jZQ==">>
    },
    compression => false
  },
  ?assertMatch({ok, _}, errm_ws_handshake:validate(Request)).

invalid_method_test() ->
  Request = #{method => <<"POST">>, headers => #{}},
  ?assertMatch({error, method_not_get}, errm_ws_handshake:validate(Request)).

missing_connection_test() ->
  Request = #{
    method => <<"GET">>, 
    headers => #{
      <<"upgrade">> => <<"websocket">>,
      <<"sec-websocket-version">> => <<"13">>,
      <<"sec-websocket-key">> => <<"dGhlIHNhbXBsZSBub25jZQ==">>
    },
    compression => false
  },
  ?assertEqual({error, connection_header_missing_upgrade}, errm_ws_handshake:validate(Request)).

accept_key_test() ->
  Key = <<"dGhlIHNhbXBsZSBub25jZQ==">>,
  Expected = <<"s3pPLMBiTxaQ9kYGzzhZRbK+xOo=">>,
  ?assertEqual(Expected, errm_ws_handshake:accept_key(Key)).

build_response_test() ->
  Request = #{
    headers => #{
      <<"sec-websocket-key">> => <<"dGhlIHNhbXBsZSBub25jZQ==">>
    },
    compression => false
  },
  Response = errm_ws_handshake:build_response(Request),
  Expected = <<"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n">>,
  ?assertEqual(Expected, iolist_to_binary(Response)).


compression_extension_test() ->
  Headers = #{
    <<"upgrade">> => <<"websocket">>,
    <<"connection">> => <<"upgrade">>,
    <<"sec-websocket-version">> => <<"13">>,
    <<"sec-websocket-key">> => <<"dGhlIHNhbXBsZSBub25jZQ==">>,
    <<"sec-websocket-extensions">> => <<"permessage-deflate; client_max_window_bits">>
  },
  Request = #{method => <<"GET">>, headers => Headers, compression => true},
  {ok, Validated} = errm_ws_handshake:validate(Request),
  ?assertEqual(true, maps:get(compression, Validated)),

  Response = errm_ws_handshake:build_response(Validated),
  ?assert(binary:match(iolist_to_binary(Response), <<"Sec-WebSocket-Extensions">>) =/= nomatch).
