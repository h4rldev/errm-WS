-module (errm_ws_handler_tests).
-include_lib ("eunit/include/eunit.hrl").

ensure_handler_loaded() ->
  case code:ensure_loaded(dummy_ws_handler) of
    {module, dummy_ws_handler} ->
      ok;
    {error, Reason} ->
      error({failed_to_load_handler, Reason})
  end.

all_required_callbacks_exported_test() ->
  ensure_handler_loaded(),
  ?assert(erlang:function_exported(dummy_ws_handler, init, 2)),
  ?assert(erlang:function_exported(dummy_ws_handler, handle_text, 2)),
  ?assert(erlang:function_exported(dummy_ws_handler, handle_info, 2)),
  ?assert(erlang:function_exported(dummy_ws_handler, terminate, 2)).

optional_callbacks_exported_test() ->
  ensure_handler_loaded(),
  ?assert(erlang:function_exported(dummy_ws_handler, handle_binary, 2)),
  ?assert(erlang:function_exported(dummy_ws_handler, handle_ping, 2)),
  ?assert(erlang:function_exported(dummy_ws_handler, handle_pong, 2)).

callback_return_types_test() ->
  ensure_handler_loaded(),
  {ok, State} = dummy_ws_handler:init(#{}, []),
  {ok, State1} = dummy_ws_handler:handle_text(<<"test">>, State),
  {ok, State2} = dummy_ws_handler:handle_binary(<<>>, State1),
  {ok, State3} = dummy_ws_handler:handle_ping(<<>>, State2),
  {ok, State4} = dummy_ws_handler:handle_pong(<<>>, State3),
  {ok, State5} = dummy_ws_handler:handle_info(test_msg, State4),
  ok = dummy_ws_handler:terminate(normal, State5),
  ?assert(is_map(State5)).
