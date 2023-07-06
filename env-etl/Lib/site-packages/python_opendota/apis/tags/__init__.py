# do not import all endpoints into this module because that uses a lot of memory and stack frames
# if you need the ability to import all endpoints from this module, import them with
# from python_opendota.apis.tag_to_api import tag_to_api

import enum


class TagValues(str, enum.Enum):
    BENCHMARKS = "benchmarks"
    CONSTANTS = "constants"
    DISTRIBUTIONS = "distributions"
    EXPLORER = "explorer"
    FIND_MATCHES = "findMatches"
    HEALTH = "health"
    HERO_STATS = "hero stats"
    HEROES = "heroes"
    LEAGUES = "leagues"
    LIVE = "live"
    MATCHES = "matches"
    METADATA = "metadata"
    PARSED_MATCHES = "parsed matches"
    PLAYERS = "players"
    PLAYERS_BY_RANK = "playersByRank"
    PRO_MATCHES = "pro matches"
    PRO_PLAYERS = "pro players"
    PUBLIC_MATCHES = "public matches"
    RANKINGS = "rankings"
    RECORDS = "records"
    REPLAYS = "replays"
    REQUEST = "request"
    SCENARIOS = "scenarios"
    SCHEMA = "schema"
    SEARCH = "search"
    STATUS = "status"
    TEAMS = "teams"
