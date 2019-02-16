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
    LOG_BACKUP_COUNT
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
        format='%(asctime)s,%(msecs)d %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s',
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
from play_manager import PlayManager as pm
import json
import asyncio

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
            logfile=app['log_file_path']
        ))
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
    async with pf(persist=True) as play:
        manager = pm(play, app['opt_file_path_prefix'])
        app['managers'][manager.id] = manager
        await manager.discover_apps()
        return web.json_response(dict(
            message='PROCESS_STARTED',
            process_id=manager.id,
            logfile=app['log_file_path']
        ))

@routes.get('/stop')
async def stop(request):
    pid = request.query.get('pid')
    show_records = request.query.get('show_records')
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
        ), status=422)
    await manager.shutdown()
    app['managers'].pop(manager.id, None)
    warnings = [] if manager.is_dumped else [ 'UNABLE_TO_DUMP_DATA_TO_FILE' ]
    return web.json_response(dict(
        message='PROCESS_STOPPED',
        warnings=warnings,
        process_id=pid,
        records_collected=manager.records_found,
        total_time_taken=manager.time_taken,
        logfile=app['log_file_path'],
        optfile=manager.opt_path,
        records=manager.records if isTrue(show_records) else None
    ))

@routes.get('/flush')
async def flush(request):
    pid = request.query.get('pid')
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
    return web.json_response(dict(
        message='RESULTS_FLUSHED',
        process_id=pid,
        records_collected='',
        time_elapsed='',
        logfile=app['log_file_path'],
        optfile=''
    ))

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
        message='PROCESS_PEEKED',
        process_id=pid,
        records_collected=opt.get('records_collected'),
        time_elapsed=opt.get('time_elapsed'),
        logfile=app['log_file_path']
    ))

async def on_startup(app):
    print('======== Starting Play Manager Server ========')
    colored_print('(Press CTRL+C only ONCE for quitting otherwise data dump will fail)\n')

async def on_shutdown(app):
    log.info('*** gracefully shutting down pending managers ***')
    print('\n======== Gracefully shutting down [{}] Managers ========'.format(len(app['managers'])))
    colored_print('(DON\'T press CTRL+C again)')
    for manager in app['managers'].values():
        await manager.shutdown()

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