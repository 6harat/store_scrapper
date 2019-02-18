import asyncio
import functools
import time
import logging as log
import json
import concurrent
import threading

from play_fetch import PlayFetch as pf

from play_helper import(
    COLLECTIONS,
    CATEGORIES,
    NO_RECORD_FOUND,
    MAX_RECORD_SIZE_PER_PAGE,
    MAX_GAME_INFO_PER_OPT_FILE
)

CANCELLED_STATUSES = [
    'SHUTDOWN_INITIATED', 'TERMINATED', 'CORRUPTED'
]

async def activate_manager(context):
    async with pf(persist=True) as play:
        manager_id = context.get('manager_id')
        manager = PlayManager(manager_id, play, context.get('opt_file_prefix'), is_delegated=True)
        manager_info_map = context.get('manager_info_map')
        if manager_info_map:
            manager_info_map[manager_id] = manager
        await manager.discover_apps()

def delegate_manager(context):
    thread_name = threading.currentThread().getName()
    log.debug('*** thread: {} trying to delgate a new manager ***'.format(thread_name))
    try:
        loop = asyncio.new_event_loop()
    except RuntimeError:
        log.exception('@@@ failed to create event loop for thread: {} @@@'.format(thread_name))
        loop = asyncio.new_event_loop()
        manager_id = context.get('manager_id')
        manager_info = context.get('manager_info_map').get(manager_id)
        if manager_info:
            manager_info.statu
    else:
        log.debug('*** successfully created event loop for thread: {} ***'.format(thread_name))
        loop.create_task(activate_manager(context))
        loop.run_forever()

class InitiatedPlayManager():
    def __init__(self, manager_id, status='INITIATED'):
        self.id = manager_id
        self.status = status
        self.failure_cause = None
    def peek(self):
        return dict(
            process_id=self.id,
            status=self.status
        )
    def fail_to_initialize(self, cause):
        self.status = 'CORRUPTED'
        self.failure_cause = cause

class PlayManager(InitiatedPlayManager):
    def __init__(self, manager_id, play, opt_path_prefix, is_delegated=False):
        log.info('*** inside PlayManager.__init__ ***')
        super().__init__(manager_id, status='RUNNING')
        self._loop = asyncio.get_event_loop()
        self._play = play
        self.info_map = dict()
        self._tasks = []
        self._shielded_tasks = []
        self.start_datetime = time.ctime()
        self.stop_datetime = None
        self._start_time = time.time()
        self.time_taken = 0
        self.records_found = 0
        self.records = []
        self.opt_path = '{}_{}.json'.format(
            opt_path_prefix,
            self.id
        )
        self.is_successfully_dumped = None
        self._shutdown_tasks = []
        self.is_delegated = is_delegated

    def is_cancelled(self):
        return self.status in CANCELLED_STATUSES
        
    def peek(self):
        opt = dict(
            process_id=self.id,
            status=self.status,
            started_at=self.start_datetime,
            stopped_at=self.stop_datetime
        )
        if self.is_cancelled():
            opt.update(dict(
                time_taken=self.time_taken,
                optfile=self.opt_path,
                records_collected=self.records_found
            ))
        else:
            opt.update(dict(
                time_elapsed=time.time() - self._start_time,
                records_collected=len(list(self.info_map.keys()))
            ))
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

    def terminate_loop(self):
        log.info('*** terminating loop for manager: {} ***'.format(self.id))
        try:
            if self._loop.is_running():
                self._loop.stop()
            if not self._loop.is_closed():
                self._loop.close()
        except RuntimeError:
            # TODO: Loop is not getting properly terminated. However, thread is reclaimed
            # $ref: Exception #2 @ observed_error.log
            log.exception('@@@ failed to terminate loop for manager: {} @@@'.format(self.id))
        else:
            log.info('*** successfully terminated loop for manager: {} ***'.format(self.id))

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
        if not self.info_map:
            log.warning('### no records found ###')
            return
        games = list(self.info_map.values())
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
            log.info('*** failed to properly dump data for manager: {} ***'.format(self.id))
        self.records_found = len(games)
        self.records = list(self.info_map.keys())

    def _release_heavy_objects(self):
        if self.is_delegated:
            self.info_map = {}
            self._tasks = []
            self._shielded_tasks = []
            self._shutdown_tasks = []

    async def _shutdown(self, callback=None):
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
        self.status = 'TERMINATED'
        self._release_heavy_objects()
        if callable(callback):
            callback(self)
        log.info('*** manager: {} successfully shut ***'.format(self.id))

    async def shutdown(self, callback=None):
        if self.is_cancelled():
            log.info('*** awaiting previously initiated shut down manager: {} ***'.format(self.id))
            return await asyncio.gather(*self._shutdown_tasks)
        log.info('*** shutting down manager: {} ***'.format(self.id))
        self.status = 'SHUTDOWN_INITIATED'
        task = self._loop.create_task(self._shutdown(callback))
        self._shutdown_tasks.append(task)
        if not callable(callback):
            await task

    @staticmethod
    def _has_more_records(records, page_size):
        return records and len(records) == page_size

    def _filter_unique_and_update_map(self, game):
        app_id = game.get('app_id')
        app_info = self.info_map.get(app_id, NO_RECORD_FOUND)
        if app_info != NO_RECORD_FOUND:
            return False
        self.info_map[app_id] = game
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
        game = await self._retriable_request(functools.partial(
            self._play.details,
            app_id
        ), shield=True)
        if game is not None:
            log.info('*** succesfully fetched app details: {} ***'.format(game))
            app_info = self.info_map.get(app_id, NO_RECORD_FOUND)
            if app_info == NO_RECORD_FOUND:
                self.info_map[app_id] = game
            else:
                self.info_map[app_id].update(game)
        else:
            log.warning('### unable to fetch app details for: {} ###'.format(app_id))

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