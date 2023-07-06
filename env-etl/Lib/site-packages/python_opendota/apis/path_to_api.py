import typing_extensions

from python_opendota.paths import PathValues
from python_opendota.apis.paths.matches_match_id import MatchesMatchId
from python_opendota.apis.paths.players_by_rank import PlayersByRank
from python_opendota.apis.paths.players_account_id import PlayersAccountId
from python_opendota.apis.paths.players_account_id_wl import PlayersAccountIdWl
from python_opendota.apis.paths.players_account_id_recent_matches import (
    PlayersAccountIdRecentMatches,
)
from python_opendota.apis.paths.players_account_id_matches import (
    PlayersAccountIdMatches,
)
from python_opendota.apis.paths.players_account_id_heroes import PlayersAccountIdHeroes
from python_opendota.apis.paths.players_account_id_peers import PlayersAccountIdPeers
from python_opendota.apis.paths.players_account_id_pros import PlayersAccountIdPros
from python_opendota.apis.paths.players_account_id_totals import PlayersAccountIdTotals
from python_opendota.apis.paths.players_account_id_counts import PlayersAccountIdCounts
from python_opendota.apis.paths.players_account_id_histograms_field import (
    PlayersAccountIdHistogramsField,
)
from python_opendota.apis.paths.players_account_id_wardmap import (
    PlayersAccountIdWardmap,
)
from python_opendota.apis.paths.players_account_id_wordcloud import (
    PlayersAccountIdWordcloud,
)
from python_opendota.apis.paths.players_account_id_ratings import (
    PlayersAccountIdRatings,
)
from python_opendota.apis.paths.players_account_id_rankings import (
    PlayersAccountIdRankings,
)
from python_opendota.apis.paths.players_account_id_refresh import (
    PlayersAccountIdRefresh,
)
from python_opendota.apis.paths.pro_players import ProPlayers
from python_opendota.apis.paths.pro_matches import ProMatches
from python_opendota.apis.paths.public_matches import PublicMatches
from python_opendota.apis.paths.parsed_matches import ParsedMatches
from python_opendota.apis.paths.explorer import Explorer
from python_opendota.apis.paths.metadata import Metadata
from python_opendota.apis.paths.distributions import Distributions
from python_opendota.apis.paths.search import Search
from python_opendota.apis.paths.rankings import Rankings
from python_opendota.apis.paths.benchmarks import Benchmarks
from python_opendota.apis.paths.status import Status
from python_opendota.apis.paths.health import Health
from python_opendota.apis.paths.request_job_id import RequestJobId
from python_opendota.apis.paths.request_match_id import RequestMatchId
from python_opendota.apis.paths.find_matches import FindMatches
from python_opendota.apis.paths.heroes import Heroes
from python_opendota.apis.paths.hero_stats import HeroStats
from python_opendota.apis.paths.heroes_hero_id_matches import HeroesHeroIdMatches
from python_opendota.apis.paths.heroes_hero_id_matchups import HeroesHeroIdMatchups
from python_opendota.apis.paths.heroes_hero_id_durations import HeroesHeroIdDurations
from python_opendota.apis.paths.heroes_hero_id_players import HeroesHeroIdPlayers
from python_opendota.apis.paths.heroes_hero_id_item_popularity import (
    HeroesHeroIdItemPopularity,
)
from python_opendota.apis.paths.leagues import Leagues
from python_opendota.apis.paths.leagues_league_id import LeaguesLeagueId
from python_opendota.apis.paths.leagues_league_id_matches import LeaguesLeagueIdMatches
from python_opendota.apis.paths.leagues_league_id_teams import LeaguesLeagueIdTeams
from python_opendota.apis.paths.teams import Teams
from python_opendota.apis.paths.teams_team_id import TeamsTeamId
from python_opendota.apis.paths.teams_team_id_matches import TeamsTeamIdMatches
from python_opendota.apis.paths.teams_team_id_players import TeamsTeamIdPlayers
from python_opendota.apis.paths.teams_team_id_heroes import TeamsTeamIdHeroes
from python_opendota.apis.paths.replays import Replays
from python_opendota.apis.paths.records_field import RecordsField
from python_opendota.apis.paths.live import Live
from python_opendota.apis.paths.scenarios_item_timings import ScenariosItemTimings
from python_opendota.apis.paths.scenarios_lane_roles import ScenariosLaneRoles
from python_opendota.apis.paths.scenarios_misc import ScenariosMisc
from python_opendota.apis.paths.schema import Schema
from python_opendota.apis.paths.constants_resource import ConstantsResource
from python_opendota.apis.paths.constants import Constants

PathToApi = typing_extensions.TypedDict(
    "PathToApi",
    {
        PathValues.MATCHES_MATCH_ID: MatchesMatchId,
        PathValues.PLAYERS_BY_RANK: PlayersByRank,
        PathValues.PLAYERS_ACCOUNT_ID: PlayersAccountId,
        PathValues.PLAYERS_ACCOUNT_ID_WL: PlayersAccountIdWl,
        PathValues.PLAYERS_ACCOUNT_ID_RECENT_MATCHES: PlayersAccountIdRecentMatches,
        PathValues.PLAYERS_ACCOUNT_ID_MATCHES: PlayersAccountIdMatches,
        PathValues.PLAYERS_ACCOUNT_ID_HEROES: PlayersAccountIdHeroes,
        PathValues.PLAYERS_ACCOUNT_ID_PEERS: PlayersAccountIdPeers,
        PathValues.PLAYERS_ACCOUNT_ID_PROS: PlayersAccountIdPros,
        PathValues.PLAYERS_ACCOUNT_ID_TOTALS: PlayersAccountIdTotals,
        PathValues.PLAYERS_ACCOUNT_ID_COUNTS: PlayersAccountIdCounts,
        PathValues.PLAYERS_ACCOUNT_ID_HISTOGRAMS_FIELD: PlayersAccountIdHistogramsField,
        PathValues.PLAYERS_ACCOUNT_ID_WARDMAP: PlayersAccountIdWardmap,
        PathValues.PLAYERS_ACCOUNT_ID_WORDCLOUD: PlayersAccountIdWordcloud,
        PathValues.PLAYERS_ACCOUNT_ID_RATINGS: PlayersAccountIdRatings,
        PathValues.PLAYERS_ACCOUNT_ID_RANKINGS: PlayersAccountIdRankings,
        PathValues.PLAYERS_ACCOUNT_ID_REFRESH: PlayersAccountIdRefresh,
        PathValues.PRO_PLAYERS: ProPlayers,
        PathValues.PRO_MATCHES: ProMatches,
        PathValues.PUBLIC_MATCHES: PublicMatches,
        PathValues.PARSED_MATCHES: ParsedMatches,
        PathValues.EXPLORER: Explorer,
        PathValues.METADATA: Metadata,
        PathValues.DISTRIBUTIONS: Distributions,
        PathValues.SEARCH: Search,
        PathValues.RANKINGS: Rankings,
        PathValues.BENCHMARKS: Benchmarks,
        PathValues.STATUS: Status,
        PathValues.HEALTH: Health,
        PathValues.REQUEST_JOB_ID: RequestJobId,
        PathValues.REQUEST_MATCH_ID: RequestMatchId,
        PathValues.FIND_MATCHES: FindMatches,
        PathValues.HEROES: Heroes,
        PathValues.HERO_STATS: HeroStats,
        PathValues.HEROES_HERO_ID_MATCHES: HeroesHeroIdMatches,
        PathValues.HEROES_HERO_ID_MATCHUPS: HeroesHeroIdMatchups,
        PathValues.HEROES_HERO_ID_DURATIONS: HeroesHeroIdDurations,
        PathValues.HEROES_HERO_ID_PLAYERS: HeroesHeroIdPlayers,
        PathValues.HEROES_HERO_ID_ITEM_POPULARITY: HeroesHeroIdItemPopularity,
        PathValues.LEAGUES: Leagues,
        PathValues.LEAGUES_LEAGUE_ID: LeaguesLeagueId,
        PathValues.LEAGUES_LEAGUE_ID_MATCHES: LeaguesLeagueIdMatches,
        PathValues.LEAGUES_LEAGUE_ID_TEAMS: LeaguesLeagueIdTeams,
        PathValues.TEAMS: Teams,
        PathValues.TEAMS_TEAM_ID: TeamsTeamId,
        PathValues.TEAMS_TEAM_ID_MATCHES: TeamsTeamIdMatches,
        PathValues.TEAMS_TEAM_ID_PLAYERS: TeamsTeamIdPlayers,
        PathValues.TEAMS_TEAM_ID_HEROES: TeamsTeamIdHeroes,
        PathValues.REPLAYS: Replays,
        PathValues.RECORDS_FIELD: RecordsField,
        PathValues.LIVE: Live,
        PathValues.SCENARIOS_ITEM_TIMINGS: ScenariosItemTimings,
        PathValues.SCENARIOS_LANE_ROLES: ScenariosLaneRoles,
        PathValues.SCENARIOS_MISC: ScenariosMisc,
        PathValues.SCHEMA: Schema,
        PathValues.CONSTANTS_RESOURCE: ConstantsResource,
        PathValues.CONSTANTS: Constants,
    },
)

path_to_api = PathToApi(
    {
        PathValues.MATCHES_MATCH_ID: MatchesMatchId,
        PathValues.PLAYERS_BY_RANK: PlayersByRank,
        PathValues.PLAYERS_ACCOUNT_ID: PlayersAccountId,
        PathValues.PLAYERS_ACCOUNT_ID_WL: PlayersAccountIdWl,
        PathValues.PLAYERS_ACCOUNT_ID_RECENT_MATCHES: PlayersAccountIdRecentMatches,
        PathValues.PLAYERS_ACCOUNT_ID_MATCHES: PlayersAccountIdMatches,
        PathValues.PLAYERS_ACCOUNT_ID_HEROES: PlayersAccountIdHeroes,
        PathValues.PLAYERS_ACCOUNT_ID_PEERS: PlayersAccountIdPeers,
        PathValues.PLAYERS_ACCOUNT_ID_PROS: PlayersAccountIdPros,
        PathValues.PLAYERS_ACCOUNT_ID_TOTALS: PlayersAccountIdTotals,
        PathValues.PLAYERS_ACCOUNT_ID_COUNTS: PlayersAccountIdCounts,
        PathValues.PLAYERS_ACCOUNT_ID_HISTOGRAMS_FIELD: PlayersAccountIdHistogramsField,
        PathValues.PLAYERS_ACCOUNT_ID_WARDMAP: PlayersAccountIdWardmap,
        PathValues.PLAYERS_ACCOUNT_ID_WORDCLOUD: PlayersAccountIdWordcloud,
        PathValues.PLAYERS_ACCOUNT_ID_RATINGS: PlayersAccountIdRatings,
        PathValues.PLAYERS_ACCOUNT_ID_RANKINGS: PlayersAccountIdRankings,
        PathValues.PLAYERS_ACCOUNT_ID_REFRESH: PlayersAccountIdRefresh,
        PathValues.PRO_PLAYERS: ProPlayers,
        PathValues.PRO_MATCHES: ProMatches,
        PathValues.PUBLIC_MATCHES: PublicMatches,
        PathValues.PARSED_MATCHES: ParsedMatches,
        PathValues.EXPLORER: Explorer,
        PathValues.METADATA: Metadata,
        PathValues.DISTRIBUTIONS: Distributions,
        PathValues.SEARCH: Search,
        PathValues.RANKINGS: Rankings,
        PathValues.BENCHMARKS: Benchmarks,
        PathValues.STATUS: Status,
        PathValues.HEALTH: Health,
        PathValues.REQUEST_JOB_ID: RequestJobId,
        PathValues.REQUEST_MATCH_ID: RequestMatchId,
        PathValues.FIND_MATCHES: FindMatches,
        PathValues.HEROES: Heroes,
        PathValues.HERO_STATS: HeroStats,
        PathValues.HEROES_HERO_ID_MATCHES: HeroesHeroIdMatches,
        PathValues.HEROES_HERO_ID_MATCHUPS: HeroesHeroIdMatchups,
        PathValues.HEROES_HERO_ID_DURATIONS: HeroesHeroIdDurations,
        PathValues.HEROES_HERO_ID_PLAYERS: HeroesHeroIdPlayers,
        PathValues.HEROES_HERO_ID_ITEM_POPULARITY: HeroesHeroIdItemPopularity,
        PathValues.LEAGUES: Leagues,
        PathValues.LEAGUES_LEAGUE_ID: LeaguesLeagueId,
        PathValues.LEAGUES_LEAGUE_ID_MATCHES: LeaguesLeagueIdMatches,
        PathValues.LEAGUES_LEAGUE_ID_TEAMS: LeaguesLeagueIdTeams,
        PathValues.TEAMS: Teams,
        PathValues.TEAMS_TEAM_ID: TeamsTeamId,
        PathValues.TEAMS_TEAM_ID_MATCHES: TeamsTeamIdMatches,
        PathValues.TEAMS_TEAM_ID_PLAYERS: TeamsTeamIdPlayers,
        PathValues.TEAMS_TEAM_ID_HEROES: TeamsTeamIdHeroes,
        PathValues.REPLAYS: Replays,
        PathValues.RECORDS_FIELD: RecordsField,
        PathValues.LIVE: Live,
        PathValues.SCENARIOS_ITEM_TIMINGS: ScenariosItemTimings,
        PathValues.SCENARIOS_LANE_ROLES: ScenariosLaneRoles,
        PathValues.SCENARIOS_MISC: ScenariosMisc,
        PathValues.SCHEMA: Schema,
        PathValues.CONSTANTS_RESOURCE: ConstantsResource,
        PathValues.CONSTANTS: Constants,
    }
)
