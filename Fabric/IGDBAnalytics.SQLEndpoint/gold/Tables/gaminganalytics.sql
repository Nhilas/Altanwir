CREATE TABLE [gold].[gaminganalytics] (

    [gameKey] varchar(8000) NULL,
    [gameName] varchar(8000) NULL,
    [aggregatedRating] float NULL,
    [percentileRating] float NULL,
    [ratingTier] varchar(8000) NULL,
    [aggregatedRatingSourceCount] bigint NULL,
    [percentileSourceCount] float NULL,
    [sourceCountTier] varchar(8000) NULL,
    [genreKey] varchar(8000) NULL,
    [genreName] varchar(8000) NULL,
    [platformKey] varchar(8000) NULL,
    [platformName] varchar(8000) NULL,
    [platformType] varchar(8000) NULL,
    [themeKey] varchar(8000) NULL,
    [themeName] varchar(8000) NULL,
    [hash] varchar(8000) NULL
);
