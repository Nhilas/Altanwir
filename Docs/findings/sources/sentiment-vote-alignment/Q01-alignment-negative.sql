-- Q01 — Negative alignment tail, recognizable IPs, totalReviews >= 50000
-- v2 refactor (Amendment 1): add pctEarlyAccess, genres, themes (collapsed via STRING_AGG)
-- Output: G:/Work/Altanwir-scratch/findings-results/Q01-alignment-negative.csv

COPY (
  WITH whitelist(pattern) AS (VALUES
    ('%Cyberpunk 2077%'), ('%The Witcher 3%'), ('%Skyrim%'), ('%Fallout 4%'),
    ('%Fallout: New Vegas%'), ('%Baldur''s Gate 3%'), ('%Starfield%'), ('%Elden Ring%'),
    ('%Dark Souls%'), ('%Sekiro%'), ('%Doom%'), ('%Counter-Strike%'),
    ('%Dota 2%'), ('%Team Fortress 2%'), ('%Half-Life%'), ('%Portal%'),
    ('%Left 4 Dead%'), ('%Garry''s Mod%'), ('%Borderlands%'), ('%Battlefield 2042%'),
    ('%Apex Legends%'), ('%PUBG%'), ('%Lost Ark%'), ('%Path of Exile%'),
    ('%Diablo IV%'), ('%Monster Hunter%'), ('%Sea of Thieves%'), ('%Rocket League%'),
    ('%Red Dead Redemption 2%'), ('%Grand Theft Auto V%'), ('%Hades%'), ('%Stardew Valley%'),
    ('%Hollow Knight%'), ('%Cuphead%'), ('%Celeste%'), ('%Undertale%'),
    ('%Among Us%'), ('%Vampire Survivors%'), ('%Balatro%'), ('%Slay the Spire%'),
    ('%Disco Elysium%'), ('%Outer Wilds%'), ('%Tunic%'), ('%Risk of Rain 2%'),
    ('%Dead Cells%'), ('%Civilization VI%'), ('%Total War: Warhammer%'), ('%Cities: Skylines%'),
    ('%RimWorld%'), ('%Factorio%'), ('%Stellaris%'), ('%Crusader Kings III%'),
    ('%Project Zomboid%'), ('%Resident Evil 4%'), ('%Phasmophobia%'), ('%Lethal Company%'),
    ('%Subnautica%'), ('%Terraria%'), ('%Valheim%'), ('%ARK: Survival%'),
    ('%Rust%'), ('%DayZ%'), ('%7 Days to Die%'), ('%Helldivers 2%'),
    ('%Palworld%'), ('%Black Myth: Wukong%'), ('%Marvel Rivals%'), ('%Deep Rock Galactic%')
  ),
  game_genres AS (
    SELECT gameKey, genreName
    FROM gold.vw_gameCatalogue
    WHERE genreName IS NOT NULL AND genreName <> 'Unknown'
    GROUP BY gameKey, genreName
  ),
  game_themes AS (
    SELECT gameKey, themeName
    FROM gold.vw_gameCatalogue
    WHERE themeName IS NOT NULL AND themeName <> 'Unknown'
    GROUP BY gameKey, themeName
  ),
  collapsed_genres AS (
    SELECT gameKey, STRING_AGG(genreName, ', ' ORDER BY genreName) AS genres FROM game_genres GROUP BY gameKey
  ),
  collapsed_themes AS (
    SELECT gameKey, STRING_AGG(themeName, ', ' ORDER BY themeName) AS themes FROM game_themes GROUP BY gameKey
  )
  SELECT
    fgs.gameName,
    fgs.sentimentVoteAlignment,
    fgs.totalReviews,
    fgs.weightedSentimentRating,
    fgs.weightedVoteRating,
    fgs.steamRatingLabel,
    fgs.avgPlaytimeAtReviewHours,
    fgs.avgWordCount,
    fgs.pctNegativeSentiment,
    fgs.pctBugReports,
    fgs.pctEarlyAccess,
    fgs.pctRefunded,
    cg.genres,
    ct.themes
  FROM gold.vw_factGameScores fgs
  LEFT JOIN collapsed_genres cg ON cg.gameKey = fgs.gameKey
  LEFT JOIN collapsed_themes ct ON ct.gameKey = fgs.gameKey
  WHERE fgs.totalReviews >= 50000
    AND EXISTS (SELECT 1 FROM whitelist w WHERE fgs.gameName ILIKE w.pattern)
  ORDER BY fgs.sentimentVoteAlignment ASC
  LIMIT 10
) TO 'G:/Work/Altanwir-scratch/findings-results/Q01-alignment-negative.csv' (HEADER, DELIMITER ',');
