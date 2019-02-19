import asyncio
import functools
import time
import logging as log
import json
import concurrent
import threading
import os
import re

from play_fetch import PlayFetch as pf

from play_helper import(
    COLLECTIONS,
    CATEGORIES,
    NO_RECORD_FOUND,
    MAX_RECORD_SIZE_PER_PAGE,
    MAX_GAME_INFO_PER_OPT_FILE,
    OPT_FILE_REGEX
)

CANCELLED_STATUSES = [
    'SHUTDOWN_INITIATED', 'TERMINATED', 'CORRUPTED'
]

CLOSED_STATUSES = [
    'TERMINATED', 'CORRUPTED', 'COMPLETED'
]

def delegate_manager(context):
    thread_name = threading.currentThread().getName()
    manager_id = context.get('manager_id')
    manager_info_map = context.get('manager_info_map')
    manager_info = manager_info_map.get(manager_id)
    log.debug('*** thread: {} trying to delgate a new manager ***'.format(thread_name))
    try:
        loop = asyncio.new_event_loop()
    except RuntimeError:
        log.exception('@@@ failed to create event loop for thread: {} @@@'.format(thread_name))
        loop = asyncio.new_event_loop()
        manager_info.fail_to_initialize()
    else:
        log.debug('*** successfully created event loop for thread: {} ***'.format(thread_name))
        loop.create_task(manager_info.activate(manager_info_map))
        loop.run_forever()
        try:
            loop.close()
        except:
            log.exception('@@@ failed to close loop for thread: {} @@@'.format(thread_name))
        else:
            log.info('*** successfully closed loop for thread: {} ***'.format(thread_name))

class InitiatedPlayManager():
    def __init__(self, manager_id, process_type='DISCOVER', read_dir='opt',
            opt_path_prefix='default', opt_path=None, status='INITIATED'):
        self.id = manager_id
        self.process_type = process_type
        self.read_dir = read_dir
        self.opt_path = opt_path if opt_path else self._determine_opt_file_path(opt_path_prefix)
        self.status = status
        self.failures = []

    def _determine_opt_file_path(self, opt_path_prefix):
        if self.process_type == 'DETAILS':
            return '{}_{}_detailed.json'.format(
                opt_path_prefix,
                self.id
            )
        else:
            return '{}_{}.json'.format(
                opt_path_prefix,
                self.id
            )
        
    def peek(self):
        return dict(
            process_id=self.id,
            status=self.status
        )
    async def activate(self, manager_info_map):
        async with pf(persist=True) as play:
            manager = PlayManager(self, play, is_delegated=True)
            manager_info_map[self.id] = manager
            if self.process_type == 'DETAILS':
                await manager.fetch_detailed_info_for_apps()
            else:
                await manager.discover_apps()

    def fail_to_initialize(self, cause):
        self.status = 'CORRUPTED'
        self.failures.append('INITIALIZATION_FAILURE')
        log.error('@@@ failed to initialize the manager: {}, cause is: {} @@@'.format(self.id, cause))

class PlayManager(InitiatedPlayManager):
    def __init__(self, parent_manager, play, is_delegated=False):
        log.info('*** inside PlayManager.__init__ ***')
        super().__init__(parent_manager.id, 
            process_type=parent_manager.process_type, 
            read_dir=parent_manager.read_dir,
            opt_path=parent_manager.opt_path,
            status='RUNNING'
        )
        self._loop = asyncio.get_event_loop()
        self._play = play
        self.info_map = dict()
        self.detailed_info_map = dict()
        self._tasks = []
        self._shielded_tasks = []
        self.start_datetime = time.ctime()
        self.stop_datetime = None
        self._start_time = time.time()
        self.time_taken = 0
        self.records_found = 0
        self.records_processed = 0
        self.records_failed = 0
        self.records = []
        self.is_successfully_dumped = None
        self._shutdown_tasks = []
        self.is_delegated = is_delegated

    def is_cancelled(self):
        return self.status in CANCELLED_STATUSES
        
    def peek(self, show_records=False):
        opt = dict(
            process_id=self.id,
            process_type=self.process_type,
            status=self.status,
            started_at=self.start_datetime,
            stopped_at=self.stop_datetime,
            failures=self.failures,
            records_collected=self.records_found
        )
        if self.process_type == 'DETAILS':
            opt.update(dict(
                records_processed=self.records_processed,
                records_failed=self.records_failed
            ))
            if self.status in CLOSED_STATUSES:
                opt.update(dict(
                    time_taken=self.time_taken,
                    optfile=self.opt_path
                ))
            else:
                opt.update(dict(
                    time_elapsed=self.time_taken
                ))
        else:
            if self.status in CLOSED_STATUSES:
                opt.update(dict(
                    time_taken=self.time_taken,
                    optfile=self.opt_path
                ))
            else:
                opt.update(dict(
                    time_elapsed=time.time() - self._start_time
                ))
        
        if show_records:
            opt['records'] = self.records
        log.info('*** peek results for manager [{}]: {}'.format(self.id, opt))
        return opt
    
    def _register_task(self, coro, shield=False):
        if not self.is_cancelled():
            task = self._loop.create_task(coro)
            if shield:
                self._shielded_tasks.append(task)
            else:
                self._tasks.append(task)

    async def _terminate_tasks(self):
        log.info('*** terminating tasks for manager: {} ***'.format(self.id))
        # TODO: All tasks are not getting properly terminated
        # $ref: Exception #3 @ observed_error.log

        for task in self._tasks:
            if not task.done():
                try:
                    task.cancel()
                    log.info('*** task successfully cancelled ***')
                except asyncio.CancelledError:
                    log.warning('### task already cancelled ###')
                except:
                    log.exception('@@@ unkown exception while cancelling task @@@')
        if self._shielded_tasks:
            try:
                log.info('*** waiting for shielded tasks to complete ***')
                await asyncio.gather(*self._shielded_tasks)
            except:
                log.exception('@@@ unable to await for all shielded tasks @@@')
            else:
                log.info('*** shielded tasks successfully completed ***')
            finally:
                log.info('$$$ printing shielded tasks $$$')
                log.info(self._shielded_tasks)

    def _stop_loop(self):
        log.info('*** terminating loop for manager: {} ***'.format(self.id))
        try:
            if self._loop.is_running():
                self._loop.stop()
        except:
            log.exception('@@@ failed to stop loop for manager: {} @@@'.format(self.id))
        else:
            log.info('*** successfully stopped loop for manager: {} ***'.format(self.id))

    def _write_to_file_with_retry(self, file_idx, records, retry=2):
        if retry <= 0:
            return False
        try:
            file_path = '{}.{}'.format(self.opt_path, file_idx)
            log.info('*** attempting to write data to file: {} ***'.format(file_path))
            with open(file_path, 'w') as opt_file:
                json.dump(records, opt_file)
        except:
            log.exception('@@@ failed to dump data to file: {} @@@'.format(file_path))
            return self._write_to_file_with_retry(file_idx, records, retry=retry-1)
        else:
            return True

    def _dump_data(self):
        log.info('*** dumping data for manager: {} ***'.format(self.id))
        result_source = self.detailed_info_map if self.process_type == 'DETAILS' else self.info_map
        
        if not result_source:
            log.warning('### no records found ###')
            return
        
        games = list(result_source.values())
        file_idx = 0
        selected_games = games[
            file_idx * MAX_GAME_INFO_PER_OPT_FILE:
            (file_idx + 1) * MAX_GAME_INFO_PER_OPT_FILE
        ]
        while selected_games:
            log.info('*** dumping data for manager: {}, file_idx: {}, selected_games: {} ***'.format(
                self.id,
                file_idx,
                len(selected_games)
            ))
            dump_result = self._write_to_file_with_retry(file_idx, selected_games)
            self.is_successfully_dumped = dump_result if file_idx == 0 else (
                dump_result and self.is_successfully_dumped
            )
            file_idx += 1
            selected_games = games[
                file_idx * MAX_GAME_INFO_PER_OPT_FILE:
                (file_idx + 1) * MAX_GAME_INFO_PER_OPT_FILE
            ]
        
        if self.is_successfully_dumped:
            log.info('*** successfully dumped data for manager: {} ***'.format(self.id))
        else:
            self.failures.append('DATA_DUMP_FAILURE')
            log.warning('### failed to properly dump data for manager: {} ###'.format(self.id))
        self.records = list(result_source.keys())

    def _release_heavy_objects(self):
        if self.is_delegated:
            self.info_map = {}
            self._tasks = []
            self._shielded_tasks = []
            self._shutdown_tasks = []

    async def _shutdown(self, is_completed=False, callback=None):
        await self._terminate_tasks()
        await self._play.force_close()
        self.time_taken = time.time() - self._start_time
        self._dump_data()
        log.info('*** manager: {}; records_found: {}; time_taken: {} ***'.format(
            self.id,
            self.records_found,
            self.time_taken
        ))
        self.stop_datetime = time.ctime()
        self.status = 'TERMINATED' if not is_completed else 'COMPLETED'
        self._release_heavy_objects()
        if callable(callback):
            callback(self)
        log.info('*** manager: {} successfully shut ***'.format(self.id))
        self._stop_loop()

    async def shutdown(self, is_completed=False, wait=False, callback=None):
        if not self.is_delegated and self.is_cancelled():
            log.info('*** awaiting previously initiated shut down manager: {} ***'.format(self.id))
            await asyncio.gather(*self._shutdown_tasks)
            return self.peek()
        log.info('*** shutting down manager: {} ***'.format(self.id))
        self.status = 'SHUTDOWN_INITIATED'
        task = self._loop.create_task(self._shutdown(
            is_completed=is_completed,
            callback=callback
        ))
        self._shutdown_tasks.append(task)
        if not self.is_delegated or wait:
            await task
        return self.peek()

    @staticmethod
    def _has_more_records(records, page_size):
        return records and len(records) == page_size

    def _filter_unique_and_update_map(self, game):
        app_id = game.get('app_id')
        app_info = self.info_map.get(app_id, NO_RECORD_FOUND)
        if app_info != NO_RECORD_FOUND:
            return False
        self.info_map[app_id] = game
        self.records_found += 1
        return True

    def _persist_and_determine_recent_apps(self, games):
        return [] if games is None else list(filter(
            self._filter_unique_and_update_map,
            games
        ))

    async def _retriable_request(self, task, retry_limit=3, shield=False):
        if retry_limit <= 0 or (self.is_cancelled() and not shield):
            return None
        try:
            opt = await task()
        except ValueError:
            log.info('*** pagination exception encountered ***')
            opt = None
        except concurrent.futures._base.CancelledError:
            log.warning('### failed due to cancellation ###')
            opt = None
        except:
            log.exception('@@@ retrying on unknown exception @@@')
            opt = await self._retriable_request(task, retry_limit-1, shield=shield)
        return opt

    async def _play_gatherer(self, task):
        games = await self._retriable_request(task)
        unique_games = self._persist_and_determine_recent_apps(games)
        if unique_games:
            log.info('*** {} unique games recently added ***'.format(len(unique_games)))
            for game in unique_games:
                self._register_task(
                    self.fetch_apps_by_similarity(game.get('app_id'))
                )
        return games

    async def fetch_app_details(self, app_id):
        log.info('*** fetching app details for: {} ***'.format(app_id))
        app_info = self.info_map.get(app_id)

        if app_info.get('developer_email'):
            log.info('*** app detailed info already exists for: {} ***'.format(app_info))
            self.detailed_info_map[app_id] = app_info
            self.records_processed += 1
            return

        app_info = await self._retriable_request(functools.partial(
            self._play.details,
            app_id
        ), shield=True)
        if app_info is None:
            log.warning('### unable to fetch app details for: {} ###'.format(app_id))
            self.records_failed += 1
            return
        
        self.detailed_info_map[app_id] = app_info
        self.records_processed += 1
        log.info('*** successfully fetched app details for: {} ***'.format(app_id))

    async def fetch_apps_by_similarity(self, app_id):
        log.info('*** fetching apps similar to: {} ***'.format(app_id))
        await self._play_gatherer(functools.partial(
            self._play.similar,
            app_id
        ))

    async def fetch_apps_by_collection(self, coln, catg, page=0, results=MAX_RECORD_SIZE_PER_PAGE):
        log.info('*** fetching page for: {}/{} ***'.format(coln, catg))
        games = await self._play_gatherer(functools.partial(
            self._play.collection,
            coln, catg, 
            page=page, 
            results=results
        ))
        if PlayManager._has_more_records(games, results):
            log.info('*** fetching more pages for {}/{} ***'.format(coln, catg))
            await self.fetch_apps_by_collection(coln, catg, page=page+1, results=results)

    async def discover_apps(self):
        for coln in COLLECTIONS[:]:
            for catg in CATEGORIES[:]:
                self._register_task(
                    self.fetch_apps_by_collection(coln, catg)
                )

    def _get_filenames_from_read_dir(self, retry=2):
        if retry <= 0:
            return []
        try:
            log.info('*** reading filenames from read_dir: {} ***'.format(self.read_dir))
            files = list(map(
                lambda filename: '{}/{}'.format(self.read_dir, filename), 
                filter(lambda file: re.match(OPT_FILE_REGEX, file), os.listdir(self.read_dir))
            ))
        except:
            log.exception('@@@ failed to read filenames from read_dir: {} @@@'.format(self.read_dir))
            files = self._get_filenames_from_read_dir(retry=retry-1)
        else:
            log.info('*** successfully read filenames from read_dir: {} ***'.format(self.read_dir))
        
        return files

    def _load_file_and_update_info_map(self, file, retry=2):
        if retry <= 0:
            return
        try:
            log.info('*** loading data from file: {} ***'.format(file))
            with open(file) as file_data:
                file_content = json.load(file_data)
                if isinstance(file_content, dict):
                    games = file_content.values()
                elif isinstance(file_content, list):
                    games = file_content
                else:
                    games = None
                    log.error('@@@ unknown file data format found in: {} @@@'.format(file))
                
                if games:
                    for game in games:
                        game_info = self.info_map.get(game.get('app_id'), NO_RECORD_FOUND)
                        if game_info == NO_RECORD_FOUND:
                            self.info_map[game.get('app_id')] = game
                            self.records_found += 1
                        else:
                            game_info.update(game)
        except:
            log.exception('@@@ failed to read data from file: {} @@@'.format(file))
            self._load_file_and_update_info_map(file, retry=retry-1)
        else:
            log.info('*** successfully loaded data from file: {} ***'.format(file))
            
    def load_previous_results(self):
        files = self._get_filenames_from_read_dir()
        log.info('*** processing [{}] files to retrieve previous records ***'.format(len(files)))
        if not files:
            log.warning('### no previously discovered apps found in: {} ###'.format(self.read_dir))
        for file in files:
            self._load_file_and_update_info_map(file) 
        log.info('*** loaded all available records in: {} ***'.format(self.read_dir))

    async def fetch_detailed_info_on_done(self):
        await asyncio.gather(*self._tasks)
        log.info('*** successfully retrieved detailed info for apps by manager: {} ***'.format(self.id))
        await self.shutdown(is_completed=True, wait=True)

    async def fetch_detailed_info_for_apps(self):
        self.load_previous_results()
        for app_id in list(self.info_map.keys())[:10]:
            self._register_task(self.fetch_app_details(app_id))
        self._loop.create_task(self.fetch_detailed_info_on_done())