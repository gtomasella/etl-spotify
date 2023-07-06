import typing_extensions

from python_opendota.apis.tags import TagValues
from python_opendota.apis.tags.benchmarks_api import BenchmarksApi
from python_opendota.apis.tags.constants_api import ConstantsApi
from python_opendota.apis.tags.distributions_api import DistributionsApi
from python_opendota.apis.tags.explorer_api import ExplorerApi
from python_opendota.apis.tags.find_matches_api import FindMatchesApi
from python_opendota.apis.tags.health_api import HealthApi
from python_opendota.apis.tags.hero_stats_api import HeroStatsApi
from python_opendota.apis.tags.heroes_api import HeroesApi
from python_opendota.apis.tags.leagues_api import LeaguesApi
from python_opendota.apis.tags.live_api import LiveApi
from python_opendota.apis.tags.matches_api import MatchesApi
from python_opendota.apis.tags.metadata_api import MetadataApi
from python_opendota.apis.tags.parsed_matches_api import ParsedMatchesApi
from python_opendota.apis.tags.players_api import PlayersApi
from python_opendota.apis.tags.players_by_rank_api import PlayersByRankApi
from python_opendota.apis.tags.pro_matches_api import ProMatchesApi
from python_opendota.apis.tags.pro_players_api import ProPlayersApi
from python_opendota.apis.tags.public_matches_api import PublicMatchesApi
from python_opendota.apis.tags.rankings_api import RankingsApi
from python_opendota.apis.tags.records_api import RecordsApi
from python_opendota.apis.tags.replays_api import ReplaysApi
from python_opendota.apis.tags.request_api import RequestApi
from python_opendota.apis.tags.scenarios_api import ScenariosApi
from python_opendota.apis.tags.schema_api import SchemaApi
from python_opendota.apis.tags.search_api import SearchApi
from python_opendota.apis.tags.status_api import StatusApi
from python_opendota.apis.tags.teams_api import TeamsApi

TagToApi = typing_extensions.TypedDict(
    "TagToApi",
    {
        TagValues.BENCHMARKS: BenchmarksApi,
        TagValues.CONSTANTS: ConstantsApi,
        TagValues.DISTRIBUTIONS: DistributionsApi,
        TagValues.EXPLORER: ExplorerApi,
        TagValues.FIND_MATCHES: FindMatchesApi,
        TagValues.HEALTH: HealthApi,
        TagValues.HERO_STATS: HeroStatsApi,
        TagValues.HEROES: HeroesApi,
        TagValues.LEAGUES: LeaguesApi,
        TagValues.LIVE: LiveApi,
        TagValues.MATCHES: MatchesApi,
        TagValues.METADATA: MetadataApi,
        TagValues.PARSED_MATCHES: ParsedMatchesApi,
        TagValues.PLAYERS: PlayersApi,
        TagValues.PLAYERS_BY_RANK: PlayersByRankApi,
        TagValues.PRO_MATCHES: ProMatchesApi,
        TagValues.PRO_PLAYERS: ProPlayersApi,
        TagValues.PUBLIC_MATCHES: PublicMatchesApi,
        TagValues.RANKINGS: RankingsApi,
        TagValues.RECORDS: RecordsApi,
        TagValues.REPLAYS: ReplaysApi,
        TagValues.REQUEST: RequestApi,
        TagValues.SCENARIOS: ScenariosApi,
        TagValues.SCHEMA: SchemaApi,
        TagValues.SEARCH: SearchApi,
        TagValues.STATUS: StatusApi,
        TagValues.TEAMS: TeamsApi,
    },
)

tag_to_api = TagToApi(
    {
        TagValues.BENCHMARKS: BenchmarksApi,
        TagValues.CONSTANTS: ConstantsApi,
        TagValues.DISTRIBUTIONS: DistributionsApi,
        TagValues.EXPLORER: ExplorerApi,
        TagValues.FIND_MATCHES: FindMatchesApi,
        TagValues.HEALTH: HealthApi,
        TagValues.HERO_STATS: HeroStatsApi,
        TagValues.HEROES: HeroesApi,
        TagValues.LEAGUES: LeaguesApi,
        TagValues.LIVE: LiveApi,
        TagValues.MATCHES: MatchesApi,
        TagValues.METADATA: MetadataApi,
        TagValues.PARSED_MATCHES: ParsedMatchesApi,
        TagValues.PLAYERS: PlayersApi,
        TagValues.PLAYERS_BY_RANK: PlayersByRankApi,
        TagValues.PRO_MATCHES: ProMatchesApi,
        TagValues.PRO_PLAYERS: ProPlayersApi,
        TagValues.PUBLIC_MATCHES: PublicMatchesApi,
        TagValues.RANKINGS: RankingsApi,
        TagValues.RECORDS: RecordsApi,
        TagValues.REPLAYS: ReplaysApi,
        TagValues.REQUEST: RequestApi,
        TagValues.SCENARIOS: ScenariosApi,
        TagValues.SCHEMA: SchemaApi,
        TagValues.SEARCH: SearchApi,
        TagValues.STATUS: StatusApi,
        TagValues.TEAMS: TeamsApi,
    }
)
