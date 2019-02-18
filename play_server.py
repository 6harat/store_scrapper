import logging as log
from logging.handlers import RotatingFileHandler
import calendar
import time
import os
from play_helper import(
    parseInt,
    isTrue,
    colored_print,
    MAX_LOG_FILE_SIZE,
    LOG_BACKUP_COUNT,
    EXECUTOR_LOOP_MANAGER_POISON
)

def setup_logging_and_provide_file_paths():
    for folder in ['log/', 'opt/']:
        if not os.path.exists(folder):
            os.makedirs(folder)
    epoch = calendar.timegm(time.gmtime())
    get_file_prefix = lambda folder: '{}/{}_{}'.format(
        folder,
        os.path.basename(__file__)[:-3],
        epoch
    )
    get_file_name = lambda folder, extension: '{}.{}'.format(
        get_file_prefix(folder),
        extension
    )
    log_file_path = get_file_name('log', 'log')
    rotating_log_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=MAX_LOG_FILE_SIZE,
        backupCount=LOG_BACKUP_COUNT
    )
    log.basicConfig(
        format='%(asctime)s,%(msecs)d %(levelname)-5s [%(threadName)s | %(filename)s:%(lineno)d] %(message)s',
        datefmt='%Y-%m-%d:%H:%M:%S',
        level=log.DEBUG,
        handlers=[
            rotating_log_handler
        ]
    )

    opt_file_path_prefix = get_file_prefix('opt')
    return (log_file_path, opt_file_path_prefix)

from aiohttp import web
from play_fetch import PlayFetch as pf
from play_manager import (
    InitiatedPlayManager as ipm,
    PlayManager as pm,
    delegate_manager,
    CANCELLED_STATUSES,
)
import json
import asyncio
from queue import Queue
from uuid import uuid1 as uid
from concurrent.futures import ThreadPoolExecutor

executor_pool_manager_q = Queue()
executor_pool = ThreadPoolExecutor(max_workers=10)

def terminate_manager_loop_on_done(q):
    print('*** manager loop terminator running ***')
    is_not_completed = True
    while(is_not_completed):
        entity = q.get()
        log.info('*** q message received: {} ***'.format(entity))
        if entity == EXECUTOR_LOOP_MANAGER_POISON:
            log.info('*** received poison pill for executor loop manager ***')
            is_not_completed = False
        else:
            entity.terminate_loop()
            log.info('*** loop terminated for manager: {} ***'.format(entity.id))
    print('*** manager loop terminator stopped ***')
routes = web.RouteTableDef()

@routes.get('/detail')
async def detail(request):
    app_id = request.query.get('app_id')
    log.info('*** fetching detail for: {} ***'.format(app_id))
    if app_id is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='app_id'
        ), status=400)
    async with pf() as play:
        opt = await play.details(app_id)
        return web.json_response(opt)

@routes.get('/collection')
async def collection(request):
    coln_id = request.query.get('coln_id')
    catg_id = request.query.get('catg_id')
    page = parseInt(request.query.get('page'), default=0)
    results = parseInt(request.query.get('results'), default=120)
    log.info('*** fetching collection for coln: {}; catg: {}; page: {}; results: {} ***'.format(coln_id, catg_id, page, results))
    if coln_id is None or catg_id is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field=['coln_id', 'catg_id']
        ), status=400)
    async with pf() as play:
        opt = await play.collection(coln_id, catg_id, page=page, results=results)
        return web.json_response(opt)

@routes.get('/similar')
async def similar(request):
    app_id = request.query.get('app_id')
    log.info('*** fetching similar for: {} ***'.format(app_id))
    if app_id is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='app_id'
        ), status=400)
    async with pf() as play:
        opt = await play.similar(app_id)
        return web.json_response(opt)

@routes.get('/view')
async def view(request):
    log.info('*** collecting manager ***')
    managers = app['managers']
    if not managers:
        return web.json_response(dict(
            message='NO_ACTIVE_MANAGERS_FOUND',
            details='Process not found or already killed'
        ), status=404)
    return web.json_response(dict(
        message='ACTIVE_MANAGERS_FOUND',
        managers=list(map(
            lambda m: m.peek(), 
            managers.values()
        )),
        logfile=app['log_file_path']
    ))

@routes.post('/start')
async def start(request):
    log.info('*** starting new process manager ***')
    manager_id = str(uid())
    app['managers'][manager_id] = ipm(manager_id)
    context = dict(
        opt_file_prefix=app['opt_file_path_prefix'],
        manager_info_map=app['managers'],
        manager_id=manager_id
    )
    executor_pool.map(delegate_manager, [context])
    return web.json_response(dict(
        message='PROCESS_INITIATED',
        process_id=manager_id,
        logfile=app['log_file_path']
    ))

@routes.post('/stop')
async def stop(request):
    pid = request.query.get('pid')
    show_records = isTrue(request.query.get('show_records'))
    log.info('*** stopping process manager: {} ***'.format(pid))
    if pid is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='pid'
        ), status=400)
    manager = app['managers'].get(pid)
    if manager is None:
        return web.json_response(dict(
            message='NOT_FOUND',
            details='Process not found or already killed'
        ), status=404)
    elif manager.status == 'INITIATED':
        return web.json_response(dict(
            message='CANNOT_KILL_MANAGER_UNTIL_FULLY_INITIATED',
            details='Cannot kill a manager at the time of initiation'
        ), status=422)

    if manager.is_delegated:
        if not manager.is_cancelled():
            await manager.shutdown(callback=post_manager_shutdown)
        message='SHUTDOWN_INITIATED'
        warnings = []
    else:
        await manager.shutdown()
        message='PROCESS_STOPPED'
        app['managers'].pop(manager.id, None)
        warnings = [] if manager.is_dumped else [ 'UNABLE_TO_DUMP_DATA_TO_FILE' ]

    return web.json_response(dict(
        message=message,
        warnings=warnings,
        process_id=pid,
        status=manager.status,
        records_collected=manager.records_found,
        total_time_taken=manager.time_taken,
        logfile=app['log_file_path'],
        optfile=manager.opt_path,
        records=manager.records if show_records else None
    ))

@routes.post('/flush')
async def flush(request):
    """
    Not yet implemented
    """
    pid = request.query.get('pid')
    show_records = isTrue(request.query.get('show_records'))
    log.info('*** flushing process manager: {} ***'.format(pid))
    if pid is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='pid'
        ), status=400)
    manager = app['managers'].get(pid)
    if manager is None:
        return web.json_response(dict(
            message='NOT_FOUND',
            details='Process not found or already killed'
        ), status=422)
    elif manager.status == 'INITIATED':
        return web.json_response(dict(
            message='CANNOT_FLUSH_MANAGER_UNTIL_FULLY_INITIATED',
            details='Cannot flush a manager at the time of initiation'
        ), status=422)
    elif manager.is_cancelled():
        return web.json_response(dict(
            message='CANNOT_FLUSH_MANAGER_IN_CANCELLED_STATE',
            details='Cannot flush a manager in Cancelled {} status'.format(CANCELLED_STATUSES)
        ), status=422)

    return web.json_response(dict(
        message='METHOD_NOT_ALLOWED',
        details='Implementation pending'
    ), status=405)

@routes.get('/peek')
async def peek(request):
    pid = request.query.get('pid')
    log.info('*** peeking process manager: {} ***'.format(pid))
    if pid is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='pid'
        ), status=400)
    manager = app['managers'].get(pid)
    if manager is None:
        return web.json_response(dict(
            message='NOT_FOUND',
            details='Process not found or already killed'
        ), status=422)
    opt = manager.peek()
    return web.json_response(dict(
        opt,
        message='PROCESS_PEEKED',
        logfile=app['log_file_path']
    ))

def post_manager_shutdown(manager):
    log.info('*** received shutdown callback for manager: {} ***'.format(manager.id))
    executor_pool_manager_q.put(manager)

async def on_startup(app):
    print('========   Starting Google Play Crawler   ========')
    colored_print('(Press CTRL+C only ONCE for quitting otherwise data dump will fail)\n')
    executor_pool.map(terminate_manager_loop_on_done, [executor_pool_manager_q])

async def on_shutdown(app):
    log.info('*** gracefully shutting down pending managers ***')
    active_managers = list(filter(lambda manager: not manager.is_cancelled(), app['managers'].values()))
    print('\n======== Shutting down [{}] active managers ========'.format(len(active_managers)))
    colored_print('(DON\'T press CTRL+C again)')
    for manager in active_managers:
        await manager.shutdown()
    executor_pool_manager_q.put(EXECUTOR_LOOP_MANAGER_POISON)
    executor_pool.shutdown(wait=True)
    print('======== Application gracefully terminated ========')

if __name__ == '__main__':
    log_file_path, opt_file_path_prefix = setup_logging_and_provide_file_paths()
    app_args = dict(
        host='localhost',
        port=8384
    )
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app['managers'] = dict()
    app['opt_file_path_prefix'] = opt_file_path_prefix
    app['log_file_path'] = log_file_path
    app.add_routes(routes)
    web.run_app(app, **app_args)