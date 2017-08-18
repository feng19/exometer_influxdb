-module(exometer_report_influxdb).

-behaviour(exometer_report).

%% gen_server callbacks
-export([
    exometer_init/1,
    exometer_info/2,
    exometer_cast/2,
    exometer_call/3,
    exometer_report/5,
    exometer_subscribe/5,
    exometer_unsubscribe/4,
    exometer_newentry/2,
    exometer_setopts/4,
    exometer_terminate/2
]).

-compile({inline, [
    prepare_batch_send/1, prepare_reconnect/0,
    merge_tags/2,
    convert_time_unit/2, unix_time/1,
    metric_elem_to_list/1, name/1, key/1, value/1,
    flatten_fields/1, flatten_tags/1,
    make_packet/4, metric_tags_binary/2
]}).
-compile(inline_list_funcs).

-ifdef(TEST).
-export([
    evaluate_subscription_options/5,
    make_packet/4,
    metric_tags_binary/2
]).
-endif.

-include_lib("exometer_core/include/exometer.hrl").

-define(DEFAULT_HOST, <<"127.0.0.1">>).
-define(DEFAULT_DB, <<"exometer">>).
-define(DEFAULT_PROTOCOL, http).
-define(DEFAULT_PORT, 8086).
-define(DEFAULT_USERNAME, undefined).
-define(DEFAULT_PASSWORD, undefined).
-define(DEFAULT_PRECISION, u).
-define(DEFAULT_SERIES_NAME, undefined).
-define(DEFAULT_FORMATTING, []).
-define(DEFAULT_TIMESTAMP_OPT, false).
-define(DEFAULT_BATCH_WINDOW_SIZE, 0).
-define(DEFAULT_AUTOSUBSCRIBE, false).
-define(DEFAULT_SUBSCRIPTIONS_MOD, undefined).

-define(VALID_PRECISIONS, [n, u, ms, s, m, h]).

-define(HTTP(Proto), (Proto =:= http orelse Proto =:= https)).

-ifdef(TEST).
-define(UNIX_TIME, 1456993525).
-define(MILLI_UNIX_TIME, 1456993524527).
-define(MICRO_UNIX_TIME, 1456993524527361).
-define(NANO_UNIX_TIME, 1456993524527361000).
-else.
-define(UNIX_TIME, erlang:system_time(second)).
-define(MILLI_UNIX_TIME, erlang:system_time(millisecond)).
-define(MICRO_UNIX_TIME, erlang:system_time(microsecond)).
-define(NANO_UNIX_TIME, erlang:system_time(nanosecond)).
-endif.

-include("log.hrl").

-type options() :: [{atom(), any()}].
-type value() :: any().
-type callback_result() :: {ok, state()} | any().
-type precision() :: n | u | ms | s | m | h.
-type protocol() :: http | udp.

-record(state, {
    protocol :: protocol(),
    http_url :: binary(),
    db :: binary(), % for http
    username :: undefined | binary(), % for http
    password :: undefined | binary(), % for http
    host :: inet:ip_address() | inet:hostname(), % for udp
    port :: inet:port_number(),  % for udp
    timestamping :: boolean(),
    precision :: precision(),
    collected_count = 0 :: integer(),
    collected_metrics = [] :: list(),
    batch_window_size = 0 :: integer(),
    tags :: map(),
    series_name :: atom(),
    formatting :: list(),
    metrics :: map(),
    autosubscribe :: boolean(),
    subscriptions_module :: module(),
    connection :: gen_udp:socket() | reference()}).
-type state() :: #state{}.


%% ===================================================================
%% Public API
%% ===================================================================
-spec exometer_init(options()) -> callback_result().
exometer_init(Opts) ->
    Host = get_opt(host, Opts, ?DEFAULT_HOST),
    Protocol = get_opt(protocol, Opts, ?DEFAULT_PROTOCOL),
    Port = get_opt(port, Opts, ?DEFAULT_PORT),
    DB = get_opt(db, Opts, ?DEFAULT_DB),
    Username = get_opt(username, Opts, ?DEFAULT_USERNAME),
    Password = get_opt(password, Opts, ?DEFAULT_PASSWORD),
    TimestampOpt = get_opt(timestamping, Opts, ?DEFAULT_TIMESTAMP_OPT),
    BatchWinSize = get_opt(batch_window_size, Opts, ?DEFAULT_BATCH_WINDOW_SIZE),
    {Timestamping, Precision} = evaluate_timestamp_opt(TimestampOpt),
    Tags = [{key(Key), Value} || {Key, Value} <- get_opt(tags, Opts, [])],
    SeriesName = get_opt(series_name, Opts, ?DEFAULT_SERIES_NAME),
    Formatting = get_opt(formatting, Opts, ?DEFAULT_FORMATTING),
    Autosubscribe = get_opt(autosubscribe, Opts, ?DEFAULT_AUTOSUBSCRIBE),
    SubscriptionsMod = get_opt(subscriptions_module, Opts, ?DEFAULT_SUBSCRIPTIONS_MOD),
    MergedTags = merge_tags([{<<"host">>, net_adm:localhost()}], Tags),
    Url = http_url(Protocol, DB, Timestamping, Precision),
    State = #state{
        protocol = Protocol,
        http_url = Url,
        db = DB,
        username = Username,
        password = Password,
        host = binary_to_list(Host),
        port = Port,
        timestamping = Timestamping,
        precision = Precision,
        tags = MergedTags,
        series_name = SeriesName,
        formatting = Formatting,
        batch_window_size = BatchWinSize,
        autosubscribe = Autosubscribe,
        subscriptions_module = SubscriptionsMod,
        metrics = maps:new()
    },
    code:load_file(hackney_tcp),
    code:load_file(hackney_ssl),
    case connect(Protocol, Host, Port, Username, Password) of
        {ok, Connection} ->
            ?info("InfluxDB reporter connecting success: ~p", [Opts]),
            {ok, State#state{connection = Connection}};
        Error ->
            ?error("InfluxDB reporter connecting error: ~p", [Error]),
            prepare_reconnect(),
            {ok, State}
    end.

-spec exometer_report(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), value(), state()) -> callback_result().
exometer_report(_Metric, _DataPoint, _Extra, _Value,
    #state{connection = undefined} = State) ->
    ?info("InfluxDB reporter isn't connected and will reconnect."),
    {ok, State};
exometer_report(Metric, DataPoint, _Extra, Value, #state{metrics = Metrics} = State) ->
    case maps:get(Metric, Metrics, not_found) of
        MetricTagsBinary when is_binary(MetricTagsBinary) ->
            maybe_send(MetricTagsBinary, maps:from_list([{DataPoint, Value}]), State);
        Error ->
            ?warning("InfluxDB reporter got trouble when looking ~p metric's tag: ~p",
                [Metric, Error]),
            Error
    end.

-spec exometer_subscribe(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:interval(), exometer_report:extra(), state()) -> callback_result().
exometer_subscribe(Metric, _DataPoint, _Interval, SubscribeOpts,
    #state{metrics = Metrics, tags = DefaultTags,
        series_name = DefaultSeriesName,
        formatting = DefaultFormatting} = State) ->
    case evaluate_subscription_options(Metric, SubscribeOpts, DefaultTags,
        DefaultSeriesName, DefaultFormatting) of
        {[], _} ->
            exit({invalid_metric_name, []});
        {MetricName, Tags} ->
            MetricTagsBinary = metric_tags_binary(MetricName, Tags),
            NewMetrics = maps:put(Metric, MetricTagsBinary, Metrics),
            {ok, State#state{metrics = NewMetrics}}
    end.

-spec exometer_unsubscribe(exometer_report:metric(), exometer_report:datapoint(),
    exometer_report:extra(), state()) -> callback_result().
exometer_unsubscribe(Metric, _DataPoint, _Extra, #state{metrics = Metrics} = State) ->
    {ok, State#state{metrics = maps:remove(Metric, Metrics)}}.

-spec exometer_call(any(), pid(), state()) ->
    {reply, any(), state()} | {noreply, state()} | any().
exometer_call(_Unknown, _From, State) ->
    {ok, State}.

-spec exometer_cast(any(), state()) -> {noreply, state()} | any().
exometer_cast(_Unknown, State) ->
    {ok, State}.

-spec exometer_info(any(), state()) -> callback_result().
exometer_info({exometer_influxdb, send}, #state{collected_metrics = []} = State) ->
    {ok, State};
exometer_info({exometer_influxdb, send}, #state{precision = Precision,
    collected_metrics = CollectedMetrics} = State) ->
    Packets = [[make_packet(MetricTagsBinary, Fileds, Timestamping, Precision), "\n"]
        || {MetricTagsBinary, Fileds, Timestamping} <- CollectedMetrics],
    send(Packets, State#state{collected_count = 0, collected_metrics = []});
exometer_info({exometer_influxdb, reconnect}, State) ->
    reconnect(State);
exometer_info(_Unknown, State) ->
    {ok, State}.

-spec exometer_newentry(exometer:entry(), state()) -> callback_result().
exometer_newentry(_Entry, #state{autosubscribe = true, subscriptions_module = undefined} = State) ->
    ?warning("InfluxDB reporter has activated autosubscribe option, "
    "but subscriptions module is undefined."),
    {ok, State};
exometer_newentry(#exometer_entry{name = Name, type = Type},
    #state{autosubscribe = true, subscriptions_module = Module} = State) when is_atom(Module) ->
    subscribe(Module:subscribe(Name, Type)),
    {ok, State};
exometer_newentry(_Entry, #state{autosubscribe = true} = State) ->
    ?warning("InfluxDB reporter has activated autosubscribe option, "
    "but subscriptions module is not atom."),
    {ok, State};
exometer_newentry(_Entry, State) ->
    {ok, State}.

-spec exometer_setopts(exometer:entry(), options(),
    exometer:status(), state()) -> callback_result().
exometer_setopts(_Metric, _Options, _Status, State) ->
    {ok, State}.

-spec exometer_terminate(any(), state()) -> any().
exometer_terminate(Reason, _) ->
    ?info("InfluxDB reporter is terminating with reason: ~p~n", [Reason]),
    ignore.


%% ===================================================================
%% Internal functions
%% ===================================================================
-spec connect(protocol(), string() | binary(), integer(),
    undefined | iodata(), undefined | iodata()) ->
    {ok, pid() | reference()} | {error, term()}.
connect(Proto, Host, Port, Username, Password) when ?HTTP(Proto) ->
    {ok, _} = application:ensure_all_started(hackney),
    Options =
        case {Username, Password} of
            {undefined, _} -> [];
            {_, undefined} -> [];
            _ -> [{basic_auth, {Username, Password}}]
        end ++ [{pool, false}],
    Transport =
        case Proto of
            http ->
                case code:is_loaded(hackney_tcp) of
                    false -> hackney_tcp_transport;
                    _ -> hackney_tcp
                end;
            https ->
                case code:is_loaded(hackney_ssl) of
                    false -> hackney_ssl_transport;
                    _ -> hackney_ssl
                end
        end,
    hackney:connect(Transport, Host, Port, Options);
connect(udp, _, _, _, _) -> gen_udp:open(0);
connect(Protocol, _, _, _, _) -> {error, {Protocol, not_supported}}.

-spec reconnect(state()) -> {ok, state()}.
reconnect(#state{protocol = Protocol, host = Host, port = Port,
    username = Username, password = Password} = State) ->
    case connect(Protocol, Host, Port, Username, Password) of
        {ok, Connection} ->
            ?info("InfluxDB reporter reconnecting success: ~p",
                [{Protocol, Host, Port, Username, Password}]),
            {ok, State#state{connection = Connection}};
        Error ->
            ?error("InfluxDB reporter reconnecting error: ~p", [Error]),
            prepare_reconnect(),
            {ok, State#state{connection = undefined}}
    end.

prepare_batch_send(Time) ->
    erlang:send_after(Time, self(), {exometer_influxdb, send}).

prepare_reconnect() ->
    erlang:send_after(1000, self(), {exometer_influxdb, reconnect}).

-spec maybe_send(binary(), map(), state()) ->
    {ok, state()} | {error, term()}.
maybe_send(MetricTagsBinary, Fields,
    #state{batch_window_size = 0, timestamping = Timestamping, precision = Precision} = State) ->
    Packet = make_packet(MetricTagsBinary, Fields, Timestamping, Precision),
    send(Packet, State);
maybe_send(MetricTagsBinary, Fields,
    #state{batch_window_size = BatchWinSize,
        precision = Precision,
        timestamping = Timestamping,
        collected_count = CollectedCount,
        collected_metrics = CollectedMetrics} = State) ->
    NewCollectedMetrics = [{MetricTagsBinary, Fields, Timestamping andalso unix_time(Precision)} | CollectedMetrics],
    CollectedCount == 0 andalso prepare_batch_send(BatchWinSize),
    {ok, State#state{collected_count = CollectedCount + 1, collected_metrics = NewCollectedMetrics}}.

-spec send(binary() | list(), state()) ->
    {ok, state()} | {error, term()}.
send(Packet, #state{protocol = Proto, connection = Connection, http_url = Url} = State) when ?HTTP(Proto) ->
    case hackney:send_request(Connection, {post, Url, [], Packet}) of
        {ok, 204, _, Ref} ->
            hackney:body(Ref),
            {ok, State};
        {ok, Status, _Headers, Ref} ->
            {ok, Body} = hackney:body(Ref),
            ?warning("InfluxDB reporter got unexpected response with code ~p"
            " and body: ~p. Reconnecting ...", [Status, Body]),
            reconnect(State);
        {error, _} = Error ->
            ?error("InfluxDB reporter HTTP sending error: ~p", [Error]),
            reconnect(State)
    end;
send(Packet, #state{protocol = udp, connection = Socket,
    host = Host, port = Port} = State) ->
    case gen_udp:send(Socket, Host, Port, Packet) of
        ok ->
            {ok, State};
        Error ->
            ?error("InfluxDB reporter UDP sending error: ~p", [Error]),
            reconnect(State)
    end;
send(_, #state{protocol = Protocol}) -> {error, {Protocol, not_supported}}.

-spec merge_tags(list() | map(), list() | map()) -> map().
merge_tags(Tags, AdditionalTags) when is_list(Tags) ->
    merge_tags(maps:from_list(Tags), AdditionalTags);
merge_tags(Tags, AdditionalTags) when is_list(AdditionalTags) ->
    merge_tags(Tags, maps:from_list(AdditionalTags));
merge_tags(Tags, AdditionalTags) when not is_map(AdditionalTags) -> Tags;
merge_tags(Tags, AdditionalTags) -> maps:merge(Tags, AdditionalTags).

-spec subscribe(list() | {exometer_report:metric(),
    exometer_report:datapoint(),
    exometer_report:interval(),
    exometer_report:extra()}) -> ok.
subscribe(Subscribtions) when is_list(Subscribtions) ->
    [subscribe(Subscribtion) || Subscribtion <- Subscribtions];
subscribe({Name, DataPoint, Interval, Extra, Retry}) when is_boolean(Retry) ->
    exometer_report:subscribe(?MODULE, Name, DataPoint, Interval, Extra, Retry);
subscribe({Name, DataPoint, Interval, Extra}) ->
    exometer_report:subscribe(?MODULE, Name, DataPoint, Interval, Extra);
subscribe(_Name) ->
    [].

-spec get_opt(atom(), list(), any()) -> any().
get_opt(K, Opts, Default) ->
    exometer_util:get_opt(K, Opts, Default).

%% LINE PROTOCOL
-define(SEP(V), case V of <<>> -> <<>>; [] -> <<>>; _ -> <<$,>> end).

-spec convert_time_unit(integer(), erlang:time_unit() | minutes | hours) ->
    integer().
convert_time_unit(Secs, hours) ->
    round(convert_time_unit(Secs, minutes) / 60);
convert_time_unit(Secs, minutes) ->
    round(Secs / 60).

-spec unix_time(precision() | undefined) -> integer() | undefined.
unix_time(n) -> ?NANO_UNIX_TIME;
unix_time(u) -> ?MICRO_UNIX_TIME;
unix_time(ms) -> ?MILLI_UNIX_TIME;
unix_time(s) -> ?UNIX_TIME;
unix_time(m) -> convert_time_unit(?UNIX_TIME, minutes);
unix_time(h) -> convert_time_unit(?UNIX_TIME, hours);
unix_time(_) -> undefined.

-spec metric_to_string(list()) -> string().
metric_to_string([Final]) -> metric_elem_to_list(Final);
metric_to_string([H | T]) ->
    metric_elem_to_list(H) ++ "_" ++ metric_to_string(T).

-spec metric_elem_to_list(atom() | string() | integer()) -> string().
metric_elem_to_list(E) when is_atom(E) -> atom_to_list(E);
metric_elem_to_list(E) when is_binary(E) -> binary_to_list(E);
metric_elem_to_list(E) when is_list(E) -> E;
metric_elem_to_list(E) when is_integer(E) -> integer_to_list(E).

-spec name(exometer_report:metric() | atom()) -> binary().
name(Metric) when is_atom(Metric) -> atom_to_binary(Metric, utf8);
name(Metric) -> iolist_to_binary(metric_to_string(Metric)).

-define(KEY(Key), binary:replace(Key, [<<" ">>, <<$,>>, <<$=>>], <<$\\>>, [global, {insert_replaced, 1}])).
-spec key(integer() | atom() | list() | binary()) -> binary().
key(K) when is_integer(K) -> ?KEY(integer_to_binary(K));
key(K) when is_list(K) -> ?KEY(list_to_binary(K));
key(K) when is_atom(K) -> ?KEY(atom_to_binary(K, utf8));
key(K) when is_binary(K) -> ?KEY(K).

-define(VALUE(Value), [$", binary:replace(Value, <<$">>, <<$\\, $">>, [global]), $"]).
-spec value(any()) -> binary() | list().
value(V) when is_integer(V) -> [integer_to_binary(V), $i];
value(V) when is_float(V) -> float_to_binary(V);
value(V) when is_atom(V) -> ?VALUE(atom_to_binary(V, utf8));
value(V) when is_list(V) -> ?VALUE(list_to_binary(V));
value(V) when is_binary(V) -> ?VALUE(V).

-spec flatten_fields(map()) -> list().
flatten_fields(Fields) ->
    maps:fold(
        fun(K, V, Acc) ->
            [Acc, ?SEP(Acc), key(K), $=, value(V)]
        end, <<>>, Fields).

-spec flatten_tags(map() | list()) -> list().
flatten_tags(Tags) when is_map(Tags) -> flatten_tags(maps:to_list(Tags));
flatten_tags(Tags) ->
    lists:foldl(
        fun({K, V}, Acc) ->
            [Acc, ?SEP(Acc), key(K), $=, key(V)]
        end, [], lists:keysort(1, Tags)).

-spec make_packet(binary(), map(), boolean() | non_neg_integer(), precision()) ->
    list().
make_packet(MetricTagsBinary, Fields, Timestamping, Precision) ->
    BinaryFields = flatten_fields(Fields),
    case Timestamping of
        false ->
            [MetricTagsBinary, " ", BinaryFields, " "];
        true ->
            [MetricTagsBinary, " ", BinaryFields, " ", integer_to_binary(unix_time(Precision))];
        Timestamp when is_integer(Timestamp) -> % for batch sending with timestamp
            [MetricTagsBinary, " ", BinaryFields, " ", integer_to_binary(Timestamp)]
    end.

-spec evaluate_timestamp_opt({boolean(), precision()} | boolean())
        -> {boolean(), precision()}.
evaluate_timestamp_opt({Term, Precision}) when is_boolean(Term) ->
    case lists:member(Precision, ?VALID_PRECISIONS) of
        true ->
            {Term, Precision};
        false ->
            exit(invalid_precision)
    end;
evaluate_timestamp_opt(Term) when is_boolean(Term) ->
    {Term, ?DEFAULT_PRECISION};
evaluate_timestamp_opt(_) ->
    exit(invalid_timestamp_option).

-spec del_indices(list(), [integer()]) -> list().
del_indices(List, Indices) ->
    SortedIndices = lists:reverse(lists:usort(Indices)),
    case length(SortedIndices) == length(Indices) of
        true ->
            del_indices1(List, SortedIndices);
        false ->
            exit({invalid_indices, Indices})
    end.

-spec del_indices1(list(), [integer()]) -> list().
del_indices1(List, []) -> List;
del_indices1([], Indices = [_Index | _Indices1]) -> exit({too_many_indices, Indices});
del_indices1(List, [Index | Indices]) when length(List) >= Index, Index > 0 ->
    {L1, [_ | L2]} = lists:split(Index - 1, List),
    del_indices1(L1 ++ L2, Indices);
del_indices1(_List, Indices) ->
    exit({invalid_indices, Indices}).

-spec evaluate_subscription_options(list(), [{atom(), value()}], map(), atom(), [{atom(), value()}])
        -> {list() | atom(), map()}.
evaluate_subscription_options(MetricId, undefined, DefaultTags, DefaultSeriesName, DefaultFormatting) ->
    evaluate_subscription_options(MetricId, [], DefaultTags, DefaultSeriesName, DefaultFormatting);
evaluate_subscription_options(MetricId, Options, DefaultTags, DefaultSeriesName, DefaultFormatting) ->
    TagOpts = proplists:get_value(tags, Options, []),
    TagsResult = evaluate_subscription_tags(MetricId, TagOpts),
    FormattingOpts = proplists:get_value(formatting, Options, DefaultFormatting),
    FormattingResult = evaluate_subscription_formatting(TagsResult, FormattingOpts),
    SeriesName = proplists:get_value(series_name, Options, DefaultSeriesName),
    {FinalMetricId, NewTags} = evaluate_subscription_series_name(FormattingResult, SeriesName),
    TagMap = maps:from_list(NewTags),
    FinalTags = merge_tags(DefaultTags, TagMap),
    {FinalMetricId, FinalTags}.

-spec evaluate_subscription_tags(list(), [{atom(), value()}]) ->
    {list(), [{atom(), value()}], [integer()]}.
evaluate_subscription_tags(MetricId, TagOpts) ->
    evaluate_subscription_tags(MetricId, TagOpts, [], []).

-spec evaluate_subscription_tags(list(), [{atom(), value()}], [{atom(), value()}], [integer()]) ->
    {list(), [{atom(), value()}], [integer()]}.
evaluate_subscription_tags(MetricId, [], TagAcc, PosAcc) ->
    {MetricId, TagAcc, PosAcc};
evaluate_subscription_tags(MetricId, [{TagKey, {from_name, Pos}} | TagOpts], TagAcc, PosAcc)
    when is_number(Pos), length(MetricId) >= Pos, Pos > 0 ->
    NewTagAcc = TagAcc ++ [{TagKey, lists:nth(Pos, MetricId)}],
    NewPosAcc = PosAcc ++ [Pos],
    evaluate_subscription_tags(MetricId, TagOpts, NewTagAcc, NewPosAcc);
evaluate_subscription_tags(MetricId, [TagOpt = {TagKey, {from_name, Name}} | TagOpts], TagAcc, PosAcc) ->
    case string:str(MetricId, [Name]) of
        0 ->
            exit({invalid_tag_option, TagOpt});
        Index ->
            NewTagAcc = TagAcc ++ [{TagKey, Name}],
            NewPosAcc = PosAcc ++ [Index],
            evaluate_subscription_tags(MetricId, TagOpts, NewTagAcc, NewPosAcc)
    end;
evaluate_subscription_tags(MetricId, [Tag = {_Key, _Value} | Tags], TagAcc, PosAcc) ->
    evaluate_subscription_tags(MetricId, Tags, TagAcc ++ [Tag], PosAcc);
evaluate_subscription_tags(_MetricId, [Tag | _], _TagAcc, _PosAcc) ->
    exit({invalid_tag_option, Tag}).

-spec evaluate_subscription_formatting({list(), [{atom(), value()}], [integer()]}, term())
        -> {list(), [{atom(), value()}]}.
evaluate_subscription_formatting({MetricId, Tags, FromNameIndices}, FormattingOpts) ->
    ToPurge = proplists:get_value(purge, FormattingOpts, []),
    KeysToPurge = proplists:get_all_values(tag_keys, ToPurge),
    ValuesToPurge = proplists:get_all_values(tag_values, ToPurge),
    PurgedTags = [{TagKey, TagValue} || {TagKey, TagValue} <- Tags,
        lists:member(TagKey, KeysToPurge) == false,
        lists:member(TagValue, ValuesToPurge) == false],
    FromNamePurge = proplists:get_value(all_from_name, ToPurge, true),
    PurgedMetricId =
        case FromNamePurge of
            true -> del_indices(MetricId, FromNameIndices);
            false -> MetricId
        end,
    {PurgedMetricId, PurgedTags}.

-spec evaluate_subscription_series_name({list(), [{atom(), value()}]}, atom())
        -> {list() | atom(), [{atom(), value()}]}.
evaluate_subscription_series_name({MetricId, Tags}, undefined) -> {MetricId, Tags};
evaluate_subscription_series_name({_MetricId, Tags}, SeriesName) -> {SeriesName, Tags}.

-spec http_url(Protocol :: protocol(), DB :: binary(),
    Timestamping :: boolean(), Precision :: precision()) -> Url :: binary().
http_url(Protocol, DB, Timestamping, Precision) when ?HTTP(Protocol) ->
    QsVals =
        case Timestamping of
            false ->
                [{<<"db">>, DB}];
            true ->
                [{<<"db">>, DB}, {<<"precision">>, Precision}]
        end,
    hackney_url:make_url(<<"/">>, <<"write">>, QsVals);
http_url(_Protocol, _DB, _Timestamping, _Precision) -> <<>>.

-spec metric_tags_binary(Measurement :: exometer_report:metric(), Tags :: map() | list()) ->
    MetricTagsBinary :: binary().
metric_tags_binary(Measurement, Tags) ->
    BinaryTags = flatten_tags(Tags),
    iolist_to_binary([name(Measurement), ?SEP(BinaryTags), BinaryTags]).