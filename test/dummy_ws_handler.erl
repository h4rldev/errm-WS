-module(dummy_ws_handler).
-behaviour(errm_ws_handler).

-export([init/2, handle_text/2, handle_binary/2, handle_info/2, terminate/2]).
-export([handle_ping/2, handle_pong/2]).

init(_RequestInfo, _Args) ->
  {ok, #{}}.

handle_text(Data, State) ->
  errm_ws:send_text(self(), Data),
  {ok, State}.

handle_binary(_Data, State) ->
  {ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

handle_ping(_Data, State) ->
  {ok, State}.

handle_pong(_Data, State) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.
