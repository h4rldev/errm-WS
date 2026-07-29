-module (errm_ws_example).
-export ([main/1]).

main(Args) ->
  Port = case Args of
    [PortStr] -> list_to_integer(PortStr);
    _ -> 8080
  end,
  start_echo_server:start(Port),

  receive
    stop -> start_echo_server:stop()
  end.
