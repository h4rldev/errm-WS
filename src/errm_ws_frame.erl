-module (errm_ws_frame).

-export ([decode/2]).
-export ([encode_text/1, encode_binary/1, encode_close/0, encode_close/1, encode_pong/1]).
-export ([send_pong/2, send_close/2]).
-export ([encode_frame/4]).

-export_type([opcode/0, frame/0]).

-type opcode() :: continuation | text | binary | ping | pong | close.
-type frame() :: #{
  opcode => opcode(),
  payload => binary(),
  fin => 0 | 1,
  rsv1 => 0 | 1
}.


-spec decode(Buffer :: binary(), MaxFrameSize :: non_neg_integer()) -> {ok, Frame :: frame(), Rest :: binary()} | {partial, Rest :: binary()} | {error, Reason :: term()}.
decode(Buffer, MaxFrameSize) ->
  decode_frame(Buffer, MaxFrameSize).


decode_frame(<<_:2/binary, _/binary>> = Buffer, MaxFrameSize) ->
  <<Fin:1, Rsv1:1, _Rsv2:1, _Rsv3:1, Opcode:4, Mask:1, PayloadLen:7, Rest/binary>> = Buffer,
  case PayloadLen of
    126 ->
      case Rest of
        <<Len:16, Rest2/binary>> ->
          decode_payload(Rest2, Len, MaxFrameSize, Opcode, Mask, Fin, Rsv1);
        _ ->
          {partial, Buffer}
      end;
    127 ->
      case Rest of
        <<Len:64, Rest2/binary>> ->
          decode_payload(Rest2, Len, MaxFrameSize, Opcode, Mask, Fin, Rsv1);
        _ ->
          {partial, Buffer}
      end;
    _ ->
      decode_payload(Rest, PayloadLen, MaxFrameSize, Opcode, Mask, Fin, Rsv1)
  end;
decode_frame(Buffer, _MaxFrameSize) ->
  {partial, Buffer}.


decode_payload(_Rest, Len, MaxFrameSize, _Opcode, _Mask, _Fin, _Rsv1) when Len > MaxFrameSize ->
  {error, frame_too_large};
decode_payload(Rest, Len, _MaxFrameSize, Opcode, 1, Fin, Rsv1) ->
  case Rest of 
    <<MaskKey:4/binary, Payload:Len/binary, Rest2/binary>> ->
      Unmasked = unmask(Payload, MaskKey),
      OpcodeAtom = opcode_to_atom(Opcode),
      {ok, #{opcode => OpcodeAtom, payload => Unmasked, fin => Fin, rsv1 => Rsv1}, Rest2};
    _ ->
      {partial, Rest}
  end;
decode_payload(Rest, Len, _MaxFrameSize, Opcode, 0, Fin, Rsv1) ->
  case Rest of
    <<Payload:Len/binary, Rest2/binary>> ->
      OpcodeAtom = opcode_to_atom(Opcode),
      {ok, #{opcode => OpcodeAtom, payload => Payload, fin => Fin, rsv1 => Rsv1}, Rest2};
    _ ->
      {partial, Rest}
  end.

-spec unmask(Data :: binary(), MaskKey :: binary()) -> binary().

unmask(Data, MaskKey) ->
    << M1:8, M2:8, M3:8, M4:8 >> = MaskKey,
    unmask_loop(Data, M1, M2, M3, M4, 0, <<>>).
unmask_loop(<<>>, _, _, _, _, _, Acc) -> Acc;
unmask_loop(<<B:8, Rest/binary>>, M1, M2, M3, M4, Index, Acc) ->
  MaskByte = case Index rem 4 of
    0 -> M1;
    1 -> M2;
    2 -> M3;
    3 -> M4
  end,
  unmask_loop(Rest, M1, M2, M3, M4, Index+1, <<Acc/binary, (B bxor MaskByte)>>).

-spec opcode_to_atom(0..15) -> opcode().
opcode_to_atom(0) -> continuation;
opcode_to_atom(1) -> text;
opcode_to_atom(2) -> binary;
opcode_to_atom(8) -> close;
opcode_to_atom(9) -> ping;
opcode_to_atom(10) -> pong.


-spec encode_text(Payload :: binary()) -> Encoded :: binary().
encode_text(Payload) ->
  encode_frame(Payload, 1, 1, 0).

-spec encode_binary(Payload :: binary()) -> Encoded :: binary().
encode_binary(Payload) ->
  encode_frame(Payload, 1, 2, 0).

-spec encode_close() -> Encoded :: binary().
encode_close() ->
  encode_close(<<>>).

-spec encode_close(Data :: binary()) -> Encoded :: binary().
encode_close(Payload) ->
  encode_frame(Payload, 1, 8, 0).

-spec encode_pong(Payload :: binary()) -> Encoded :: binary().
encode_pong(Payload) ->
  encode_frame(Payload, 1, 10, 0).


-spec encode_frame(Payload :: binary(), Fin :: 0|1, Opcode :: 0..15, Rsv1 :: 0|1) -> Encoded :: binary().
encode_frame(Payload, Fin, Opcode, Rsv1) ->
  Len = byte_size(iolist_to_binary(Payload)),
  Header = if
    Len =< 125 ->
      <<Fin:1, Rsv1:1, 0:2, Opcode:4, 0:1, Len:7>>;
    Len =< 65535 ->
      <<Fin:1, Rsv1:1, 0:2, Opcode:4, 0:1, 126:7, Len:16>>;
    true ->
      <<Fin:1, Rsv1:1, 0:2, Opcode:4, 0:1, 127:7, Len:64>>
  end,
  <<Header/binary, Payload/binary>>.


-spec send_pong(Sock :: gen_tcp:socket(), Payload :: binary()) -> ok | {error, Reason :: term()}.
send_pong(Sock, Payload) ->
  Frame = encode_pong(Payload),
  gen_tcp:send(Sock, Frame).

-spec send_close(Sock :: gen_tcp:socket(), Payload :: binary()) -> ok | {error, Reason :: term()}.
send_close(Sock, Payload) ->
  Frame = encode_close(Payload),
  gen_tcp:send(Sock, Frame).


