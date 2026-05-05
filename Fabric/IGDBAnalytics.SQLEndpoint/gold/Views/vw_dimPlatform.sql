-- Auto Generated (Do not modify) 12EDCC4780A57C7D48E56D61382E174C221AA5B3A0FB92879ACB56AFD00D0D32



create view gold.vw_dimPlatform as
select
    platformKey
    , platformType
    , platformName
from silver.platforms
