import logging as log
import calendar
import time
import os

for folder in ['log/', 'opt/']:
    if not os.path.exists(folder):
        os.makedirs(folder)
epoch = calendar.timegm(time.gmtime())

get_file_name = lambda folder, extension: '{}/{}_{}.{}'.format(
    folder, 
    os.path.basename(__file__)[:-3], 
    epoch, 
    extension
)
log_file_path = get_file_name('log', 'log')
opt_file_path = get_file_name('opt', 'json')

log.basicConfig(
    filename=log_file_path,
    format='%(asctime)s,%(msecs)d %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=log.DEBUG
)

import asyncio
import play_scraper as play
from collections import deque
import constants
import json
import functools
from concurrent.futures import ThreadPoolExecutor

game_info_map = {}
# process_queue = deque()
tasks = []
loop = asyncio.get_event_loop()

PROCESS_EXECUTOR = ThreadPoolExecutor(max_workers=constants.MAX_WORKERS)

@asyncio.coroutine
def runner(task):
    yield from loop.run_in_executor(PROCESS_EXECUTOR, task)

def has_more_records(records):
    return len(records) == constants.MAX_RECORD_SIZE_PER_PAGE

def extract_id_from_app_info(app_info):
    return app_info.get('app_id')

def play_executor(fn, retryCount=0):
    if retryCount >= 5:
        return []
    try:
        result = fn()
    except ValueError:
        log.exception('PLAY_SCRAPER_ERROR')
        games = []
    except:
        games = play_executor(fn, retryCount+1)
    else:
        games = result

    persist_and_gather_more_apps_using_similar(games)
    return games

def filter_unique_and_update_map(game):
    app_id = game.get('app_id')
    app_info = game_info_map.get(app_id, constants.NO_RECORD_FOUND)
    game_info_map[app_id] = game
    return app_info == constants.NO_RECORD_FOUND

def persist_and_gather_more_apps_using_similar(games):
    new_unique_games = list(map(extract_id_from_app_info, filter(
        filter_unique_and_update_map,
        games
    )))
    if new_unique_games:
        log.info('adding {} unique records to event_loop'.format(len(new_unique_games)))
        for game in new_unique_games:
            tasks.append(loop.create_task(runner(functools.partial(
                get_apps_similar_to, 
                game
            ))))

def get_apps_similar_to(app_id):
    play_executor(functools.partial(play.similar, app_id))

def get_apps_by_collection_category(coln, catg, page=0):
    games = play_executor(functools.partial(
        play.collection, 
        collection=coln, 
        category=catg, 
        results=constants.MAX_RECORD_SIZE_PER_PAGE, 
        page=page
    ))
    if has_more_records(games):
        get_apps_by_collection_category(coln, catg, page+1)

def dump_data_to_disc():
    if not game_info_map:
        return log.warning('NO_RECORD_FOUND')
    try:
        log.info('attempting to write scraped data to file: {}'.format(opt_file_path))
        with open(opt_file_path, 'w') as opt_file:
            json.dump(game_info_map, opt_file)
    except IOError:
        log.exception('ERROR_WHILE_DUMPING_DATA')
    else:
        log.info('successfully writen scraped data to file: {}'.format(opt_file_path))

def post_processing():
    total_records_collected = len(game_info_map.keys())
    print('total records collected: {}'.format(total_records_collected))
    log.info('TOTAL_RECORDS_COLLECTED: {}'.format(total_records_collected))
    dump_data_to_disc()
    log.info('PROGRAM_GRACEFULLY_TERMINATED')

def main():
    for coln in constants.COLLECTIONS.keys():
        for catg in constants.CATEGORIES.keys():
            tasks.append(loop.create_task(runner(functools.partial(
                get_apps_by_collection_category, 
                coln, 
                catg
            ))))
    loop.run_forever()

if __name__ == '__main__':
    start = time.time()
    try:
        main()
    except KeyboardInterrupt:
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        log.warning('SCRAPING_TERMINATED_BEFORE_COMPLETION')
        post_processing()
        exit()
    except:
        log.exception('UNKNOWN_EXCEPTION_ENCOUNTERED')
        raise
    else:
        post_processing()
    finally:
        end = time.time()
        total_time_taken = end-start
        print('total time taken for execution: {} seconds'.format(total_time_taken))
        log.info('TOTAL_TIME_TAKEN_FOR_EXECUTION: {}'.format(total_time_taken))