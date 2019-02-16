"""
Contains contants and helper methods for the server and client operations.
"""

def parseInt(num, default=0):
    return default if not num or not num.isdigit() else int(num)

def isTrue(value):
    return (isinstance(value, bool) and value) or (isinstance(value, str) and value.lower() == 'true')

def colored_print(info):
    print("\033[96m {}\033[00m" .format(info))

MAX_LOG_FILE_SIZE = 25 * 1024 * 1024
LOG_BACKUP_COUNT = 20
MAX_RECORD_SIZE_PER_PAGE = 120
NO_RECORD_FOUND = object()

COLLECTIONS = [
    'NEW_FREE',
    'NEW_PAID',
    'TOP_FREE',
    'TOP_PAID',
    'TOP_GROSSING',
    'TRENDING'
]

CATEGORIES = [
    "GAME",
    "GAME_ACTION",
    "GAME_ADVENTURE",
    "GAME_ARCADE",
    "GAME_BOARD",
    "GAME_CARD",
    "GAME_CASINO",
    "GAME_CASUAL",
    "GAME_EDUCATIONAL",
    "GAME_MUSIC",
    "GAME_PUZZLE",
    "GAME_RACING",
    "GAME_ROLE_PLAYING",
    "GAME_SIMULATION",
    "GAME_SPORTS",
    "GAME_STRATEGY",
    "GAME_TRIVIA",
    "GAME_WORD"
]