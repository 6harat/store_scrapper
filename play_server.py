from aiohttp import web
from play_fetch import PlayFetch as pf
from play_manager import PlayManager as pm
import json

from play_helper import(
    parseInt
)

from pydash import(
    set_,
    get as get_
)

routes = web.RouteTableDef()
app_args = dict(
    host='localhost',
    port=8384
)
app = web.Application()
app['managers'] = dict()

@routes.get('/detail')
async def detail(request):
    app_id = request.query.get('app_id')
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
    if app_id is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='app_id'
        ), status=400)
    async with pf() as play:
        opt = await play.similar(app_id)
        return web.json_response(opt)

@routes.post('/start')
async def start(request):
    async with pf(persist=True) as play:
        manager = pm(play)
        set_(app, 'managers.{}'.format(manager.id), manager)
        await manager.discover_apps()
        return web.json_response(dict(
            message='PROCESS_STARTED',
            process_id=get_(manager, 'id'),
            logfile=''
        ))

@routes.get('/stop')
async def stop(request):
    pid = request.query.get('pid')
    if pid is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='pid'
        ), status=400)
    manager = get_(app, 'managers.{}'.format(pid))
    if manager is None:
        return web.json_response(dict(
            message='NOT_FOUND',
            details='Process not found or already killed'
        ), status=422)
    await manager.shutdown()
    return web.json_response(dict(
        message='PROCESS_STOPPED',
        process_id=pid,
        records_collected=manager.records_found,
        total_time_taken=manager.time_taken,
        logfile='',
        optfile='',
        records=manager.records
    ))

@routes.get('/flush')
async def flush(request):
    pid = request.query.get('pid')
    if pid is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='pid'
        ), status=400)
    manager = get_(app, 'managers.{}'.format(pid))
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
        logfile='',
        optfile=''
    ))

@routes.get('/peek')
async def peek(request):
    pid = request.query.get('pid')
    if pid is None:
        return web.json_response(dict(
            message='MISSING_REQUIRED_PARAMETER',
            location='query',
            field='pid'
        ), status=400)
    manager = get_(app, 'managers.{}'.format(pid))
    if manager is None:
        return web.json_response(dict(
            message='NOT_FOUND',
            details='Process not found or already killed'
        ), status=422)
    opt = manager.peek()
    return web.json_response(dict(
        message='PROCESS_PEEKED',
        process_id=pid,
        records_collected=get_(opt, 'records_collected'),
        time_elapsed=get_(opt, 'time_elapsed'),
        logfile=''
    ))

app.add_routes(routes)
web.run_app(app, **app_args)