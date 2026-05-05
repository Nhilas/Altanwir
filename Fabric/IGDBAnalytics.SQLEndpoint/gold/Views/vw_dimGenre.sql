-- Auto Generated (Do not modify) FBD09DE5CC26A6F81273232DC3A7401C8695C744FE08DC7707C2F36BEE34AD42
create view gold.vw_dimGenre as
select
    genreKey
    , genreName
from silver.genres
