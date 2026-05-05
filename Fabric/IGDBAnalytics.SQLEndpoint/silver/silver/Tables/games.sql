CREATE TABLE [silver].[games] (

	[gameKey] varchar(8000) NULL, 
	[gameName] varchar(8000) NULL, 
	[gameId] bigint NULL, 
	[genreId] varchar(8000) NULL, 
	[themeId] varchar(8000) NULL, 
	[platformId] varchar(8000) NULL, 
	[releasedOn] date NULL, 
	[externalRating] float NULL, 
	[externalRatingSourcesCount] bigint NULL, 
	[igdbRating] float NULL, 
	[igdbRatingSourceCount] bigint NULL, 
	[aggregatedRating] float NULL, 
	[aggregatedRatingSourceCount] bigint NULL, 
	[hash] varchar(8000) NULL
);