from django.conf import settings

from posthog.clickhouse.kafka_engine import trim_quotes_expr
from posthog.clickhouse.table_engines import (
    Distributed,
    ReplicationScheme,
    AggregatingMergeTree,
)

TABLE_BASE_NAME = "raw_sessions"
RAW_SESSIONS_DATA_TABLE = lambda: f"sharded_{TABLE_BASE_NAME}"

TRUNCATE_RAW_SESSIONS_TABLE_SQL = (
    lambda: f"TRUNCATE TABLE IF EXISTS {RAW_SESSIONS_DATA_TABLE()} ON CLUSTER '{settings.CLICKHOUSE_CLUSTER}'"
)
DROP_RAW_SESSION_TABLE_SQL = (
    lambda: f"DROP TABLE IF EXISTS {RAW_SESSIONS_DATA_TABLE()} ON CLUSTER '{settings.CLICKHOUSE_CLUSTER}'"
)
DROP_RAW_SESSION_MATERIALIZED_VIEW_SQL = (
    lambda: f"DROP MATERIALISED VIEW IF EXISTS {TABLE_BASE_NAME}_mv ON CLUSTER '{settings.CLICKHOUSE_CLUSTER}'"
)
DROP_RAW_SESSION_VIEW_SQL = lambda: f"DROP VIEW IF EXISTS {TABLE_BASE_NAME}_v ON CLUSTER '{settings.CLICKHOUSE_CLUSTER}'"


# if updating these column definitions
# you'll need to update the explicit column definitions in the materialized view creation statement below
RAW_SESSIONS_TABLE_BASE_SQL = """
CREATE TABLE IF NOT EXISTS {table_name} ON CLUSTER '{cluster}'
(
    -- order components:
    team_id Int64,
    session_id_date_part UInt64, -- time component of the id, also used in the sample key
    session_id_v7 UInt128, -- integer representation of a uuidv7
 
    -- ClickHouse will pick the latest value of distinct_id for the session
    -- this is fine since even if the distinct_id changes during a session
    distinct_id AggregateFunction(argMax, String, DateTime64(6, 'UTC')),

    min_timestamp SimpleAggregateFunction(min, DateTime64(6, 'UTC')),
    max_timestamp SimpleAggregateFunction(max, DateTime64(6, 'UTC')),

    urls SimpleAggregateFunction(groupUniqArrayArray, Array(String)),
    entry_url AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    exit_url AggregateFunction(argMax, String, DateTime64(6, 'UTC')),

    initial_referring_domain AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_utm_source AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_utm_campaign AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_utm_medium AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_utm_term AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_utm_content AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_gclid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_gad_source AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_gclsrc AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_dclid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_gbraid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_wbraid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_fbclid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_msclkid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_twclid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_li_fat_id AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_mc_cid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_igshid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),
    initial_ttclid AggregateFunction(argMin, String, DateTime64(6, 'UTC')),

    -- count pageview, screen or autocapture events, up to a maximum of 2, for bounce rate calculation
    -- use uniqUpTo as it will not double-count if an event is sent twice, which is something we have seen in production,
    -- whilst being quick to query and not taking up too much space
    pageview_and_screen_count_up_to_2 AggregateFunction(uniqUpTo(2), UUID, Int64),
    autocapture_count_up_to_2 AggregateFunction(uniqUpTo(2), UUID, Int64),

    -- count pageview and screen events for providing totals. Unclear if we can use this as it is not idempotent
    pageview_and_screen_count SimpleAggregateFunction(sum, Int64),
    -- do a clickhouse uniq as an alternative for providing totals, this is slower than sum but is idempotent
    pageview_and_screen_uniq AggregateFunction(uniq, UUID, Int64),

) ENGINE = {engine}
"""

RAW_SESSIONS_DATA_TABLE_ENGINE = lambda: AggregatingMergeTree(TABLE_BASE_NAME, replication_scheme=ReplicationScheme.SHARDED)

RAW_SESSIONS_TABLE_SQL = lambda: (
        RAW_SESSIONS_TABLE_BASE_SQL
        + """
    PARTITION BY toYYYYMM(fromUnixTimestamp(intDiv(toUInt64(bitShiftRight(session_id_v7, 80)), 1000))) -- convert uuidv7 into date
    ORDER BY (team_id, session_id_v7, sipHash64(session_id_v7)) -- the hash is only present to make sampling work
    SAMPLE BY sipHash64(session_id_v7)
    SETTINGS index_granularity=512
"""
).format(
    table_name=RAW_SESSIONS_DATA_TABLE(),
    cluster=settings.CLICKHOUSE_CLUSTER,
    engine=RAW_SESSIONS_DATA_TABLE_ENGINE(),
)


def source_column(column_name: str) -> str:
    return trim_quotes_expr(f"JSONExtractRaw(properties, '{column_name}')")


RAW_SESSION_TABLE_MV_SELECT_SQL = (
    lambda: """
SELECT

accurateCastOrNull(events.`$session_id`, 'UUID') as session_id_v7,
team_id,

-- always use the most recent distinct_id, this can be used to approximate the number of users
argMaxState(distinct_id, timestamp) as distinct_id,

min(timestamp) AS min_timestamp,
max(timestamp) AS max_timestamp,

groupUniqArray({current_url_property}) AS urls,
argMinState({current_url_property}, timestamp) as entry_url,
argMaxState({current_url_property}, timestamp) as exit_url,

argMinState({referring_domain_property}, timestamp) as initial_referring_domain,
argMinState({utm_source_property}, timestamp) as initial_utm_source,
argMinState({utm_campaign_property}, timestamp) as initial_utm_campaign,
argMinState({utm_medium_property}, timestamp) as initial_utm_medium,
argMinState({utm_term_property}, timestamp) as initial_utm_term,
argMinState({utm_content_property}, timestamp) as initial_utm_content,
argMinState({gclid_property}, timestamp) as initial_gclid,
argMinState({gad_source_property}, timestamp) as initial_gad_source,
argMinState({gclsrc_property}, timestamp) as initial_gclsrc,
argMinState({dclid_property}, timestamp) as initial_dclid,
argMinState({gbraid_property}, timestamp) as initial_gbraid,
argMinState({wbraid_property}, timestamp) as initial_wbraid,
argMinState({fbclid_property}, timestamp) as initial_fbclid,
argMinState({msclkid_property}, timestamp) as initial_msclkid,
argMinState({twclid_property}, timestamp) as initial_twclid,
argMinState({li_fat_id_property}, timestamp) as initial_li_fat_id,
argMinState({mc_cid_property}, timestamp) as initial_mc_cid,
argMinState({igshid_property}, timestamp) as initial_igshid,
argMinState({ttclid_property}, timestamp) as initial_ttclid,


uniqUpTo(2)(if(event='$pageview' OR event='$screen', uuid, NULL)) as pageview_and_screen_count_up_to_2,
uniqUpTo(2)(if(event='$autocapture', uuid, NULL)) as autocapture_count_up_to_2,

sumIf(1, event='$pageview' OR event='$screen') as pageview_and_screen_count,
uniq(if(event='$pageview' OR event='$screen', uuid, NULL)) as pageview_and_screen_uniq,

FROM {database}.sharded_events
WHERE bitAnd(bitShiftRight(accurateCastOrNull(events.`$session_id`, 'UUID'), 76), 0xF) == 7 -- has a session id and is valid uuidv7
GROUP BY `$session_id`, team_id
""".format(
        database=settings.CLICKHOUSE_DATABASE,
        current_url_property=source_column("$current_url"),
        referring_domain_property=source_column("$referring_domain"),
        utm_source_property=source_column("utm_source"),
        utm_campaign_property=source_column("utm_campaign"),
        utm_medium_property=source_column("utm_medium"),
        utm_term_property=source_column("utm_term"),
        utm_content_property=source_column("utm_content"),
        gclid_property=source_column("gclid"),
        gad_source_property=source_column("gad_source"),
        gclsrc_property=source_column("gclsrc"),
        dclid_property=source_column("dclid"),
        gbraid_property=source_column("gbraid"),
        wbraid_property=source_column("wbraid"),
        fbclid_property=source_column("fbclid"),
        msclkid_property=source_column("msclkid"),
        twclid_property=source_column("twclid"),
        li_fat_id_property=source_column("li_fat_id"),
        mc_cid_property=source_column("mc_cid"),
        igshid_property=source_column("igshid"),
        ttclid_property=source_column("ttclid"),
    )
)

RAW_SESSIONS_TABLE_MV_SQL = (
    lambda: """
CREATE MATERIALIZED VIEW IF NOT EXISTS {table_name} ON CLUSTER '{cluster}'
TO {database}.{target_table}
AS
{select_sql}
""".format(
        table_name=f"{TABLE_BASE_NAME}_mv",
        target_table=f"writable_{TABLE_BASE_NAME}",
        cluster=settings.CLICKHOUSE_CLUSTER,
        database=settings.CLICKHOUSE_DATABASE,
        select_sql=RAW_SESSION_TABLE_MV_SELECT_SQL(),
    )
)

RAW_SESSION_TABLE_UPDATE_SQL = (
    lambda: """
ALTER TABLE {table_name} MODIFY QUERY
{select_sql}
""".format(
        table_name=f"{TABLE_BASE_NAME}_mv",
        select_sql=RAW_SESSION_TABLE_MV_SELECT_SQL(),
    )
)

# Distributed engine tables are only created if CLICKHOUSE_REPLICATED

# This table is responsible for writing to sharded_sessions based on a sharding key.
WRITABLE_RAW_SESSIONS_TABLE_SQL = lambda: RAW_SESSIONS_TABLE_BASE_SQL.format(
    table_name=f"writable_{TABLE_BASE_NAME}",
    cluster=settings.CLICKHOUSE_CLUSTER,
    engine=Distributed(
        data_table=RAW_SESSIONS_DATA_TABLE(),
        # shard via session_id so that all events for a session are on the same shard
        sharding_key="sipHash64(session_id_v7)",
    ),
)

# This table is responsible for reading from sessions on a cluster setting
DISTRIBUTED_RAW_SESSIONS_TABLE_SQL = lambda: RAW_SESSIONS_TABLE_BASE_SQL.format(
    table_name=TABLE_BASE_NAME,
    cluster=settings.CLICKHOUSE_CLUSTER,
    engine=Distributed(
        data_table=RAW_SESSIONS_DATA_TABLE(),
        sharding_key="sipHash64(session_id_v7)",
    ),
)

# This is the view that can be queried directly, that handles aggregation of potentially multiple rows per session.
# Most queries won't use this directly as they will want to pre-filter rows before aggregation, but it's useful for
# debugging
RAW_SESSIONS_VIEW_SQL = (
    lambda: f"""
CREATE OR REPLACE VIEW {TABLE_BASE_NAME}_v ON CLUSTER '{settings.CLICKHOUSE_CLUSTER}' AS
SELECT
    session_id_v7,
    team_id,
    any(distinct_id) as distinct_id,
    min(min_timestamp) as min_timestamp,
    max(max_timestamp) as max_timestamp,
    arrayDistinct(arrayFlatten(groupArray(urls)) )AS urls,
    argMinMerge(entry_url) as entry_url,
    argMaxMerge(exit_url) as exit_url,
    argMinMerge(initial_utm_source) as initial_utm_source,
    argMinMerge(initial_utm_campaign) as initial_utm_campaign,
    argMinMerge(initial_utm_medium) as initial_utm_medium,
    argMinMerge(initial_utm_term) as initial_utm_term,
    argMinMerge(initial_utm_content) as initial_utm_content,
    argMinMerge(initial_referring_domain) as initial_referring_domain,
    argMinMerge(initial_gclid) as initial_gclid,
    argMinMerge(initial_gad_source) as initial_gad_source,
    argMinMerge(initial_gclsrc) as initial_gclsrc,
    argMinMerge(initial_dclid) as initial_dclid,
    argMinMerge(initial_gbraid) as initial_gbraid,
    argMinMerge(initial_wbraid) as initial_wbraid,
    argMinMerge(initial_fbclid) as initial_fbclid,
    argMinMerge(initial_msclkid) as initial_msclkid,
    argMinMerge(initial_twclid) as initial_twclid,
    argMinMerge(initial_li_fat_id) as initial_li_fat_id,
    argMinMerge(initial_mc_cid) as initial_mc_cid,
    argMinMerge(initial_igshid) as initial_igshid,
    argMinMerge(initial_ttclid) as initial_ttclid,
    uniqUpToMerge(2)(pageview_and_screen_count_up_to_2) as pageview_and_screen_count_up_to_2,
    uniqUpToMerge(2)(autocapture_count_up_to_2) as autocapture_count_up_to_2,
    sum(pageview_and_screen_count) as pageview_and_screen_count,
    uniqMerge(pageview_and_screen_uniq) as pageview_and_screen_uniq
FROM sessions
GROUP BY session_id_v7, team_id
"""
)

RAW_SELECT_SESSION_PROP_STRING_VALUES_SQL = """
SELECT
    value,
    count(value)
FROM (
    SELECT
        {property_expr} as value
    FROM
        sessions
    WHERE
        team_id = %(team_id)s AND
        {property_expr} IS NOT NULL AND
        {property_expr} != ''
    ORDER BY session_id DESC
    LIMIT 100000
)
GROUP BY value
ORDER BY count(value) DESC
LIMIT 20
"""

RAW_SELECT_SESSION_PROP_STRING_VALUES_SQL_WITH_FILTER = """
SELECT
    value,
    count(value)
FROM (
    SELECT
        {property_expr} as value
    FROM
        sessions
    WHERE
        team_id = %(team_id)s AND
        {property_expr} ILIKE %(value)s
    ORDER BY session_id DESC
    LIMIT 100000
)
GROUP BY value
ORDER BY count(value) DESC
LIMIT 20
"""
