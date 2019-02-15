import asyncio
import functools
import time

from uuid import uuid1 as uid

from constants import(
    COLLECTIONS,
    CATEGORIES,
    NO_RECORD_FOUND,
    MAX_RECORD_SIZE_PER_PAGE
)

class PlayManager():
    def __init__(self, play):
        print('*** inside PlayManager.__init__ ***')
        self.id = str(uid())
        self._loop = asyncio.new_event_loop()
        print('*** event loop being used is {} ****'.format(self._loop))
        self._play = play
        self.info_map = dict()
        self._tasks = []
        self._start_time = time.time()
        self.time_taken = 0
        self.records_found = 0
        self.records = []
        self.is_cancelled = False

    def peek(self):
        return dict(
            time_elapsed=time.time() - self._start_time,
            records_collected=len(list(self.info_map.keys()))
        )
    
    def _register_task(self, coro):
        if not self.is_cancelled:
            task = self._loop.create_task(coro)
            self._tasks.append(task)

    def _terminate_tasks(self):
        print('*** terminating tasks ***')
        for task in self._tasks:
            if not task.done():
                try:
                    task.cancel()
                    print('*** task successfully cancelled ***')
                except asyncio.CancelledError:
                    print('*** task already cancelled ***')
                except Exception as e:
                    print('*** unkown exception while cancelling task: {}'.format(e))

    def _dump_data(self):
        print('*** dumping data ***')
        return list(self.info_map.keys())

    async def shutdown(self):
        print('*** inside PlayManager.shutdown ***')
        self.is_cancelled = True
        self._terminate_tasks()
        await self._play.force_close()
        self.time_taken = time.time() - self._start_time
        self.records = self._dump_data()
        self.records_found = len(self.records)

    @staticmethod
    def _has_more_records(records, page_size):
        return records and len(records) == page_size

    def _filter_unique_and_update_map(self, game):
        app_info = self.info_map.get(game.get('app_id'), NO_RECORD_FOUND)
        self.info_map[game.get('app_id')] = game
        return app_info == NO_RECORD_FOUND

    def _persist_and_determine_recent_apps(self, games):
        return [] if games is None else list(filter(
            self._filter_unique_and_update_map,
            games
        ))

    async def _play_executor(self, task, retry_count=0):
        if retry_count >= 5:
            return []
        
        try:
            result = await task()
        except ValueError:
            print('@@@ known exception encountered @@@')
            games = []
        except Exception as e:
            print(e)
            print('@@@ unknown exception encountered @@@')
            games = await self._play_executor(task, retry_count+1)
        else:
            print('*** data successfully retrieved ***')
            games = result
        
        recent_games = self._persist_and_determine_recent_apps(games)
        if recent_games:
            print('*** {} recent games added ***'.format(len(recent_games)))

    async def get_apps_by_collection(self, coln, catg, page=0, results=MAX_RECORD_SIZE_PER_PAGE):
        games = await self._play_executor(functools.partial(
            self._play.collection,
            coln, catg, 
            page=page, 
            results=results
        ))
        if PlayManager._has_more_records(games, results):
            print('*** fetching more pages for {}/{} ***'.format(coln, catg))

    async def discover_apps(self):
        for coln in COLLECTIONS:
            for catg in CATEGORIES:
                self._register_task(
                    self.get_apps_by_collection(coln, catg)
                )