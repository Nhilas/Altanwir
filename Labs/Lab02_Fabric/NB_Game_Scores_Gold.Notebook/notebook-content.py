# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c683a58d-3109-458d-8cb3-da991c23a31e",
# META       "default_lakehouse_name": "IGDBAnalytics_Dev",
# META       "default_lakehouse_workspace_id": "d1206eb3-2259-44b8-844a-409f1a63f284",
# META       "known_lakehouses": [
# META         {
# META           "id": "c683a58d-3109-458d-8cb3-da991c23a31e"
# META         },
# META         {
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Imports

# CELL ********************

import struct
import pyodbc
import notebookutils
import json

from pyspark.sql import functions as f
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

environment = "dev"
load_type = "reload"   # valid options: "full", "reload", "targeted"
run_id = "dev_igdb_smoothing_02"

# format this as a list. only used if load_type = 'targeted'. only accepts gameKeys
targeted_reload = [ 'fee1882ab7f5f816b65f0cd5b277fb74c058352c5a95c6e302f07bc423aa717f', '9b82015126416c80cc13505a3f254f33336e37432509bab854553afd2b51f4fb', 'f6121e9cec01d2dc9c3f8762f2ed088c6e4b3cdf32b26a970a73e3eae5dd3351']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Variables

# CELL ********************

lakehouse_name = "IGDBAnalytics" if environment == "prod" else "IGDBAnalytics_Dev"
audit_schema = "dev" if environment == "dev" else "steam"

games_path = f"{lakehouse_name}.silver.games"

reviews_path = f"{lakehouse_name}.gold.factReviews"

target_path = f"{lakehouse_name}.gold.factGameScores"
target_table = DeltaTable.forName(spark, target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constants

# CELL ********************

audit_server = '22jgi2dsfxnu5lmyn6ifyaro5e-wnxcbukzek4ejbckicpruy7sqq.datawarehouse.fabric.microsoft.com'
audit_database = 'IGDBAudit'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Gold gaming scores ETL Initiated with load_type = '{load_type}', for run_id = '{run_id}'")
print(f"Environment = {environment}\n Lakehouse = {lakehouse_name}\n Audit = {audit_database}.{audit_schema}")
print(f"Loading {target_path} from:\n\t- {games_path}\n\t- {reviews_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Functions

# MARKDOWN ********************

# ## connect_audit_wh

# CELL ********************

def connect_audit_wh():

    # token formation
    token = notebookutils.credentials.getToken("pbi")
    token_bytes = token.encode("utf-16-le")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)

    # connection string + connection
    conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={audit_server};DATABASE={audit_database};Encrypt=yes;TrustServerCertificate=no"
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})

    return conn

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## insert_version

# CELL ********************

def insert_version(audit_row, latest_source_version):
    insert_query = f"""
        insert into {audit_schema}.versionControl (
            table_name
            , run_id
            , change_type
            , commit_version
            , commit_timestamp
            , rows_inserted
            , rows_updated
            , latest_source_version
            , audit_notes
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    full_audit_row = audit_row[0].asDict()
    audit_notes = json.dumps(full_audit_row, default=str)

    insert_parameters = [
        target_path
        , run_id
        , audit_row[0]['operation']
        , audit_row[0]['version']
        , audit_row[0]['timestamp']
        , int(audit_row[0]['operationMetrics']['numTargetRowsInserted'])
        , int(audit_row[0]['operationMetrics']['numTargetRowsUpdated'])
        , latest_source_version
        , audit_notes
    ]

    conn = connect_audit_wh()
    db_cursor = conn.cursor()

    try:
        db_cursor.execute(insert_query, insert_parameters)
        conn.commit()
    except Exception as e:
        print(f"Failed to insert version: {e}")
        conn.rollback()
    else:
        print(f"Successfully logged audit for {target_path} in {audit_schema}.versionControl with the audit_row = {audit_notes}")
    finally:
        db_cursor.close()
        conn.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Main

# CELL ********************

if load_type in ['full', 'reload']:
    from_clause = games_path
elif load_type == 'targeted' and targeted_reload:
    sep = "', '"
    gameKey_predicate = f"'{sep.join(targeted_reload)}'"
    from_clause = f"{games_path}\n\twhere gameKey in ({gameKey_predicate})"
else:
    print(f"Invalid load_type: {load_type}! Shutting down")
    notebookutils.notebook.exit("Wrong load_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Calculating global priors...")

review_prior_query = f"""
    with weighted_sentiment_review_prior as (
        select
            sum(coalesce(sentimentDirection, 0) * reviewInfluenceScore) / nullif(sum(reviewInfluenceScore),0) as weightedSentiment
            , sum(voteDirection * reviewInfluenceScore) / nullif(sum(reviewInfluenceScore),0) as weightedVote
        from {reviews_path}
    )
    select 
        ( weightedSentiment + 1 ) / 2 as sentiment_prior
        , ( weightedVote + 1 ) / 2 as vote_prior
    from weighted_sentiment_review_prior
"""

igdb_prior_query = f"""
    select 
        avg(aggregatedRating)/100 as igdb_rating_prior
    from {games_path}
        where ifnull(aggregatedRating,0) > 0
"""

review_prior_row = spark.sql(review_prior_query).collect()[0]
weighted_sentiment_prior = review_prior_row.sentiment_prior
weighted_vote_prior = review_prior_row.vote_prior

igdb_rating_prior = spark.sql(igdb_prior_query).collect()[0]['igdb_rating_prior']

print(f"""Established priors:
    * IGDB Rating ~= {round(igdb_rating_prior, 4)}
    * Weighted Sentiment Compound ~= {round(weighted_sentiment_prior, 4)}
    * Weighted Vote ~= {round(weighted_vote_prior, 4)}""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_query = f"""
with silver_games as (
    select
        gameKey
        , aggregatedRating / 100.0 as pctIGDBRating
        , aggregatedRatingSourceCount as IGDBSourceCount
    from {from_clause}
)

, igdb_games_smoothed as (
    select *
        , pctIGDBRating - (pctIGDBRating - {igdb_rating_prior}) * pow(2, -log10(IGDBSourceCount + 1)) as smoothedIGDBRating
    from silver_games
    where
        ifnull(pctIGDBRating,0) > 0
        and ifnull(IGDBSourceCount,0) > 0
)

, review_stats as (
    select
        r.gameKey
        , count(*) as totalReviews
        , sum(case when sentimentDirection is not Null then 1 else 0 end) as sentimentReviews

        , sum(case when sentimentCompound >= 0.05 then 1.0 else 0.0 end) as positiveSentiment
        , sum(case when sentimentCompound <= -0.05 then 1.0 else 0.0 end) as negativeSentiment
        , sum(case when sentimentCompound > -0.05 and sentimentCompound < 0.05 then 1.0 else 0.0 end) as neutralSentiment

        , sum(sentimentDirection * reviewInfluenceScore) / nullif(sum(reviewInfluenceScore),0) as weightedSentiment
        , sum(voteDirection * reviewInfluenceScore) / nullif(sum(reviewInfluenceScore),0) as weightedVote
        , sum(case when votedUp = True then 1.0 else 0.0 end) as votedUp        

        , sum(case when writtenDuringEarlyAccess = True then 1.0 else 0.0 end) as earlyAccess
        , sum(case when containsBugReport = True then 1.0 else 0.0 end) as bugReports
        , sum(case when refunded = True then 1.0 else 0.0 end) as refunded                

        , avg(playtimeAtReview) as avgPlaytimeAtReview
        , avg(wordCount) as avgWordCount
        , avg(emotionalIntensity) as avgEmotionalIntensity
    from {reviews_path} as r
    inner join silver_games as g
        on r.gameKey = g.gameKey
    group by
        r.gameKey
)

, enhanced_review_stats as (
    select *
        , positiveSentiment / totalReviews as pctPositiveSentiment
        , neutralSentiment / totalReviews as pctNeutralSentiment
        , negativeSentiment / totalReviews as pctNegativeSentiment    

        , votedUp / totalReviews as pctVotedUp

        , earlyAccess / totalReviews as pctEarlyAccess
        , bugReports / totalReviews as pctBugReports
        , refunded / totalReviews as pctRefunded 

        , ( weightedSentiment + 1 ) / 2 as pctWeightedSentiment
        , ( weightedVote + 1 ) / 2 as pctWeightedVote
    from review_stats
)

, rating_review_stats as (
    select *
        , pctWeightedSentiment - (pctWeightedSentiment - {weighted_sentiment_prior}) * pow(2, -log10(sentimentReviews + 1)) as weightedSentimentRating
        , pctWeightedVote - (pctWeightedVote - {weighted_vote_prior}) * pow(2, -log10(totalReviews + 1)) as weightedVoteRating
        , pctVotedUp - (pctVotedUp - 0.5) * pow(2, -log10(totalReviews + 1)) as voteRating
    from enhanced_review_stats
)

select
    gr.gameKey

    -- IGDB ratings and source counts
    , pctIGDBRating
    , smoothedIGDBRating
    
    , IGDBSourceCount

    -- total reviews & total reviews with text that could be parsed for sentiment
    , totalReviews
    , sentimentReviews

    -- emotional intensity is the ratio of caps and '!'/'?' to length
    , avgPlaytimeAtReview
    , avgWordCount
    , avgEmotionalIntensity    

    -- results from sentiment analysis, total will ~= 1
    , pctPositiveSentiment
    , pctNeutralSentiment
    , pctNegativeSentiment

    /*  weightedSentiment => the weighted average of the overall sentiment scored * the overall review influence    
        pctWeightedSentiment => converts weightedSentiment from a [-1, 1] range to [0, 1]. it is the overall % of positive sentiment using the vader formula
        weightedSentimentRating => pctWeightedSentiment adjusted by total reviews using the steam label formula

        weightedVote => the weighted average of the overall % thumbs up ratio adjusted by the review influence score
        pctWeightedVote => same logic as pctWeightedSentiment
        weightedVoteRating => same logic as weightedSentimentRating

        sentimentVoteAlignment => the difference between sentiment and vote. 
            positive number = feelings are more positive than weighted vote rating suggests
            negative number = feelings are more negative than weighted vote rating suggests
    */
    , weightedSentimentRating - weightedVoteRating as sentimentVoteAlignment
    , weightedSentiment
    , pctWeightedSentiment
    , weightedSentimentRating

    , weightedVote
    , pctWeightedVote    
    , weightedVoteRating

    -- steam's formula; uses the likes to total ratio and adjust it based on total reviews
    , pctVotedUp
    , voteRating

    -- simple percentages of how much of the total reviews meet the stated condition
    , pctEarlyAccess
    , pctBugReports
    , pctRefunded
from igdb_games_smoothed as gr
left join rating_review_stats as grs
    on gr.gameKey = grs.gameKey
"""

df_games_processed = spark.sql(source_query)

columns_to_hash = [c for c in df_games_processed.columns if c not in ['gameKey'] ]

df_game_scores = df_games_processed \
    .withColumn("hash", f.sha2(f.concat_ws("|", *[f.col(c) for c in columns_to_hash]), 256))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Merge

# CELL ********************

if load_type == "full":
    print(f"Load type is '{load_type}', truncating table {target_path}...")    

    truncate_query = f"truncate table {target_path}"
    spark.sql(truncate_query)

    print("Truncate completed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

version_before = target_table.history(1).collect()[0][0]

print(f"Merge target: {target_path}. Executing merge...")

target_table.alias("t").merge(
    df_game_scores.alias("s"),
    "t.gameKey = s.gameKey"
).whenMatchedUpdate(
    condition="t.hash != s.hash",
    set={
        "t.pctIGDBRating":                "s.pctIGDBRating",
        "t.smoothedIGDBRating":           "s.smoothedIGDBRating",
        "t.IGDBSourceCount":              "s.IGDBSourceCount",
        "t.totalReviews":                 "s.totalReviews",
        "t.sentimentReviews":             "s.sentimentReviews",
        "t.avgPlaytimeAtReview":          "s.avgPlaytimeAtReview",
        "t.avgWordCount":                 "s.avgWordCount",
        "t.avgEmotionalIntensity":        "s.avgEmotionalIntensity",
        "t.pctPositiveSentiment":         "s.pctPositiveSentiment",
        "t.pctNeutralSentiment":          "s.pctNeutralSentiment",
        "t.pctNegativeSentiment":         "s.pctNegativeSentiment",
        "t.sentimentVoteAlignment":       "s.sentimentVoteAlignment",
        "t.weightedSentiment":            "s.weightedSentiment",
        "t.pctWeightedSentiment":         "s.pctWeightedSentiment",
        "t.weightedSentimentRating":      "s.weightedSentimentRating",
        "t.weightedVote":                 "s.weightedVote",
        "t.pctWeightedVote":              "s.pctWeightedVote",
        "t.weightedVoteRating":           "s.weightedVoteRating",
        "t.pctVotedUp":                   "s.pctVotedUp",
        "t.voteRating":                   "s.voteRating",
        "t.pctEarlyAccess":               "s.pctEarlyAccess",
        "t.pctBugReports":                "s.pctBugReports",
        "t.pctRefunded":                  "s.pctRefunded",
        "t.update_run_id":                f"'{run_id}'",
        "t.hash":                         "s.hash",
    }
).whenNotMatchedInsert(
    values={
        "gameKey":                    "s.gameKey",
        "pctIGDBRating":              "s.pctIGDBRating",
        "smoothedIGDBRating":         "s.smoothedIGDBRating",
        "IGDBSourceCount":            "s.IGDBSourceCount",
        "totalReviews":               "s.totalReviews",
        "sentimentReviews":           "s.sentimentReviews",
        "avgPlaytimeAtReview":        "s.avgPlaytimeAtReview",
        "avgWordCount":               "s.avgWordCount",
        "avgEmotionalIntensity":      "s.avgEmotionalIntensity",
        "pctPositiveSentiment":       "s.pctPositiveSentiment",
        "pctNeutralSentiment":        "s.pctNeutralSentiment",
        "pctNegativeSentiment":       "s.pctNegativeSentiment",
        "sentimentVoteAlignment":     "s.sentimentVoteAlignment",
        "weightedSentiment":          "s.weightedSentiment",
        "pctWeightedSentiment":       "s.pctWeightedSentiment",
        "weightedSentimentRating":    "s.weightedSentimentRating",
        "weightedVote":               "s.weightedVote",
        "pctWeightedVote":            "s.pctWeightedVote",
        "weightedVoteRating":         "s.weightedVoteRating",
        "pctVotedUp":                 "s.pctVotedUp",
        "voteRating":                 "s.voteRating",
        "pctEarlyAccess":             "s.pctEarlyAccess",
        "pctBugReports":              "s.pctBugReports",
        "pctRefunded":                "s.pctRefunded",
        "insert_run_id":              f"'{run_id}'",
        "update_run_id":              "null",
        "hash":                       "s.hash",
    }
).execute()

audit_row = target_table.history(1).collect()
version_after = audit_row[0][0]

if version_before == version_after:
    print("Merge executed. No rows affected")
else:
    insert_version(audit_row=audit_row, latest_source_version=None)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Optimize

# CELL ********************

if version_before != version_after:
    print(f"OPTIMIZE {target_path} to cluster new additions...")

    optimize_query = f"OPTIMIZE {target_path}"
    spark.sql(optimize_query)

    print(f"OPTIMIZE Completed!")    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
