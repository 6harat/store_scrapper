from aiohttp import web

routes = web.RouteTableDef()
app_args = dict(
    host='localhost',
    port=8384
)
app = web.Application()

@routes.post('/start')
async def start(request):
    return web.json_response(dict(
        message="process_started",
        process_id="",
        logfile=""
    ))

@routes.get('/stop/{pid}')
async def stop(request):
    pid = request.match_info['pid']
    return web.json_response(dict(
        message="process_stopped",
        process_id="",
        records_collected="",
        total_time_taken="",
        logfile="",
        optfile=""
    ))

@routes.get('/flush/{pid}')
async def flush(request):
    pid = request.match_info['pid']
    return web.json_response(dict(
        message="results_flushed",
        process_id="",
        records_collected="",
        time_elapsed="",
        logfile="",
        optfile=""
    ))

@routes.get('/peek/{pid}')
async def peek(request):
    pid = request.match_info['pid']
    return web.json_response(dict(
        message="process_peeked",
        process_id="",
        records_collected="",
        time_elapsed="",
        logfile=""
    ))

app.add_routes(routes)
web.run_app(app, **app_args)