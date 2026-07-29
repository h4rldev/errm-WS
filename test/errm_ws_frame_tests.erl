-module (errm_ws_frame_tests).
-include_lib ("eunit/include/eunit.hrl").

mask(Data, MaskKey) ->
    << M1, M2, M3, M4 >> = MaskKey,
    mask_loop(Data, M1, M2, M3, M4, 0, <<>>).

mask_loop(<<>>, _, _, _, _, _, Acc) -> Acc;
mask_loop(<< B:8, Rest/binary >>, M1, M2, M3, M4, Index, Acc) ->
    MaskByte = case Index rem 4 of
        0 -> M1;
        1 -> M2;
        2 -> M3;
        3 -> M4
    end,
    mask_loop(Rest, M1, M2, M3, M4, Index + 1, << Acc/binary, (B bxor MaskByte) >>).

encode_decode_text_test() ->
  Payload = <<"Hello, WebSocket!">>,
  Encoded = errm_ws_frame:encode_text(Payload),
  {ok, Frame, Rest} = errm_ws_frame:decode(Encoded, 1 bsl 20),
  ?assertEqual(#{opcode => text, payload => Payload, fin => 1, rsv1 => 0}, Frame),
  ?assertEqual(<<>>, Rest).

masked_frame_test() ->
  Payload = <<"Hello, Masked!">>,
  MaskKey = <<16#12, 16#34, 16#56, 16#78>>,
  MaskedPayload = mask(Payload, MaskKey),
  Frame = <<1:1, 0:3, 1:4, 1:1, (byte_size(Payload)):7, MaskKey/binary, MaskedPayload/binary>>,
  {ok, DecodedFrame, Rest} = errm_ws_frame:decode(Frame, 1 bsl 20),
  ?assertEqual(#{opcode => text, payload => Payload, fin => 1, rsv1 => 0}, DecodedFrame),
  ?assertEqual(<<>>, Rest).

extended_length_test() ->
  Payload = binary:copy(<<"x">>, 200),
  Encoded = errm_ws_frame:encode_text(Payload),
  {ok, Frame, <<>>} = errm_ws_frame:decode(Encoded, 1 bsl 20),
  ?assertEqual(Payload, maps:get(payload, Frame)).

fragmentation_test() ->
  Part1 = errm_ws_frame:encode_frame(<<"first ">>, 0, 1, 0),
  Part2 = errm_ws_frame:encode_frame(<<"second">>, 1, 0, 0),
  Buffer = <<Part1/binary, Part2/binary>>,
  {ok, Frame1, Rest} = errm_ws_frame:decode(Buffer, 1 bsl 20),
  ?assertEqual(#{opcode => text, payload => <<"first ">>, fin => 0, rsv1 => 0}, Frame1),
  {ok, Frame2, <<>>} = errm_ws_frame:decode(Rest, 1 bsl 20),
  ?assertEqual(#{opcode => continuation, payload => <<"second">>, fin => 1, rsv1 => 0}, Frame2).

frame_too_large_test() ->
  Payload = binary:copy(<<"x">>, 2000),
  Encoded = errm_ws_frame:encode_text(Payload),
  ?assertMatch({error, frame_too_large}, errm_ws_frame:decode(Encoded, 100)).

rsv1_frame_test() ->
  Payload = <<"Hello, Compressed WebSocket!">>,
  Frame = errm_ws_frame:encode_frame(Payload, 1, 1, 1),
  {ok, Decoded, <<>>} = errm_ws_frame:decode(Frame, 1 bsl 20),
  ?assertEqual(1, maps:get(rsv1, Decoded)),
  ?assertEqual(Payload, maps:get(payload, Decoded)).

