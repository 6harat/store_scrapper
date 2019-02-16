import asyncio
import functools
import time
import logging as log
import json
import concurrent

from uuid import uuid1 as uid

from play_helper import(
    COLLECTIONS,
    CATEGORIES,
    NO_RECORD_FOUND,
    MAX_RECORD_SIZE_PER_PAGE
)

class PlayManager():
    def __init__(self, play, opt_path_prefix):
        log.info('*** inside PlayManager.__init__ ***')
        self.id = str(uid())
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
        self.is_cancelled = False
        self.opt_path = '{}_{}.json'.format(
            opt_path_prefix,
            self.id
        )
        self.is_dumped = False

    def peek(self):
        opt = dict(
            process_id=self.id,
            started_at=self.start_datetime,
            stopped_at=self.stop_datetime,
            is_cancelled=self.is_cancelled,
            time_elapsed=time.time() - self._start_time,
            records_collected=len(list(self.info_map.keys()))
        )
        log.info('*** peek results for manager [{}]: {}'.format(self.id, opt))
        return opt
    
    def _register_task(self, coro, shield=False):
        if not self.is_cancelled:
            task = self._loop.create_task(coro)
            if shield:
                self._shielded_tasks.append(task)
            else:
                self._tasks.append(task)

    async def _terminate_tasks(self):
        log.info('*** terminating tasks for manager: {} ***'.format(self.id))
        for task in self._tasks:
            if not task.done():
                try:
                    task.cancel()
                    log.info('*** task successfully cancelled ***')
                except asyncio.CancelledError:
                    log.warning('### task already cancelled ###')
                except Exception:
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

    def _terminate_loop(self):
        """
        UNUSED_FUNCTION: To be removed
        """
        log.info('*** terminating loop for manager: {} ***'.format(self.id))
        try:
            if self._loop.is_running():
                self._loop.stop()
            if not self._loop.is_closed():
                self._loop.close()
        except Exception:
            log.exception('@@@ unknown exception while terminating loop @@@')
        else:
            log.info('*** successfully terminated loop ***')

    def _dump_data(self):
        log.info('*** dumping data for manager: {} ***'.format(self.id))
        if not self.info_map:
            log.warning('### no records found ###')
        games = list(self.info_map.values())
        try:
            log.info('*** attempting to write data to file: {} ***'.format(self.opt_path))
            with open(self.opt_path, 'w') as opt_file:
                json.dump(games, opt_file)
        except IOError:
            log.exception('@@@ io error while dumping data to file: {} @@@'.format(self.opt_path))
        except:
            log.exception('@@@ unknown error encountered while dumping data to file: {} @@@'.format(self.opt_path))
        else:
            self.is_dumped = True
            log.info('*** data successfully dumped at: {} ***'.format(self.opt_path))
        self.records_found = len(games)
        self.records = list(self.info_map.keys())

    async def shutdown(self):
        log.info('*** shutting down manager: {} ***'.format(self.id))
        if self.is_cancelled:
            return
        self.is_cancelled = True
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
        log.info('*** manager: {} successfully shut ***'.format(self.id))

    @staticmethod
    def _has_more_records(records, page_size):
        return records and len(records) == page_size

    def _filter_unique_and_update_map(self, game):
        app_id = game.get('app_id')
        app_info = self.info_map.get(app_id, NO_RECORD_FOUND)
        if app_info != NO_RECORD_FOUND:
            return False
        self.info_map[app_id] = game
        self._register_task(
            self.fetch_app_details(app_id),
            shield=True
        )
        return True

    def _persist_and_determine_recent_apps(self, games):
        return [] if games is None else list(filter(
            self._filter_unique_and_update_map,
            games
        ))

    async def _retriable_request(self, task, retry_limit=3, shield=False):
        if retry_limit <= 0 or (self.is_cancelled and not shield):
            return None
        try:
            opt = await task()
        except ValueError:
            log.info('*** pagination exception encountered ***')
            opt = None
        except concurrent.futures._base.CancelledError:
            log.warning('### failed due to cancellation ###')
            opt = None
        except Exception:
            log.exception('@@@ retrying on unknown exception @@@')
            opt = await self._retriable_request(task, retry_limit-1, shield=shield)
        finally:
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