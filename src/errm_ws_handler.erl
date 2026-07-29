-module (errm_ws_handler).
-export_type([request_info/0, handler_state/0]).
-optional_callbacks([handle_binary/2, handle_ping/2, handle_pong/2]).

-type request_info() :: #{
  headers => #{binary() => binary()},
  path => binary(),
  peer => {inet:ip_address(), inet:port_number()} | undefined
}.

-type handler_state() :: term().

-callback init(RequestInfo :: request_info(), Args :: term()) -> 
  {ok, State :: handler_state()} | {error, Reason :: term()}.

-callback handle_text(Data :: binary(), State :: handler_state()) -> 
  {ok, NewState :: handler_state()} | {close, NewState :: handler_state()} | {error, Reason :: term()}.

-callback handle_binary(Data :: binary(), State :: handler_state()) -> 
  {ok, NewState :: handler_state()} | {close, NewState :: handler_state()} | {error, Reason :: term()}.

-callback handle_info(Info :: term(), State :: handler_state()) -> 
  {ok, NewState :: handler_state()} | {close, NewState :: handler_state()} | {error, Reason :: term()}.

-callback handle_ping(Data :: binary(), State :: handler_state()) ->
    {ok, NewState :: handler_state()} | {close, NewState :: handler_state()} | {error, Reason :: term()}.

-callback handle_pong(Data :: binary(), State :: handler_state()) ->
    {ok, NewState :: handler_state()} | {close, NewState :: handler_state()} | {error, Reason :: term()}.

-callback terminate(Reason :: term(), State :: handler_state()) -> ok.
