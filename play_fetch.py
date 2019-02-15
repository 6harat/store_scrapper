from aiohttp import (
    ClientSession,
    ClientResponseError
)
from play_scraper import (
    utils,
    lists,
    settings
)
from bs4 import BeautifulSoup, SoupStrainer

class PlayFetch():

    async def __aenter__(self, headers=utils.default_headers(), timeout=30, read_timeout=10, conn_timeout=10, hl='en', gl='us'):
        self._session = ClientSession(
            headers=headers,
            timeout=timeout,
            read_timeout=read_timeout,
            conn_timeout=conn_timeout
        )
        self._params = dict(
            hl=hl,
            gl=gl
        )
        return self

    async def __aexit__(self, **err):
        await self._session.close()
        self._session = None

    async def send_request(self, method, url, data=None, params={}, allow_redirects=False):
        req_args = dict(
            method=method,
            url=url,
            params=params,
            data=utils.generate_post_data() if not data and method == 'POST' else data,
            allow_redirects=allow_redirects
        )

        async with self._session.request(**req_args) as response:
            response.raise_for_status()
            return response.text()
    
    async def details(self, app_id):
        url = utils.build_url('details', app_id)
        try:
            response = await self.send_request('GET', url, params=self._params)
            soup = BeautifulSoup(response, 'lxml')
        except ClientResponseError as e:
            raise ValueError('INVALID_APPLICATION_ID: {app}. {error}'.format(
                app=app_id,
                error=e
            ))
        app_json = utils.parse_app_details(soup)
        app_json.update({
            'app_id': app_id,
            'url': url
        })
        return app_json

    async def collection(self, coln_id, catg_id=None, results=None, page=None):
        coln_name = coln_id if coln_id.startswith('promotion') else lists.COLLECTIONS.get(coln_id)
        if coln_name is None:
            raise ValueError('INVALID_COLLECTION_ID: {coln}'.format(
                coln=coln_id
            ))

        catg_name = '' if catg_id is None else lists.CATEGORIES.get(catg_id)
        if catg_name is None:
            raise ValueError('INVALID_CATEGORY_ID: {catg}'.format(
                catg=catg_id
            ))
        results = settings.NUM_RESULTS if results is None else results
        if results > 120:
            raise ValueError('Number of results cannot be more than 120.')

        page = 0 if page is None else page
        if page * results > 500:
            raise ValueError('Start (page * results) cannot be greater than 500.')

        url = utils.build_collection_url(catg_name, coln_name)
        data = utils.generate_post_data(results, page)
        try:
            response = await self.send_request('POST', url, data, params=self._params)
            soup = BeautifulSoup(response, 'lxml')
        except ClientResponseError as e:
            raise ValueError('INVALID_COLLECTION_OR_CATEGORY_ID: {coln}; {catg} {error}'.format(
                coln=coln_id,
                catg=catg_id,
                error=e
            ))
        apps = list(map(
            utils.parse_card_info, 
            soup.select('div[data-uitype="500"]')
        ))
        return apps

    async def similar(self, app_id):
        url = utils.build_url('similar', app_id)
        try:
            response = await self.send_request('GET', url, params=self._params, allow_redirects=True)
            soup = BeautifulSoup(response, 'lxml')
        except ClientResponseError as e:
            raise ValueError('INVALID_APPLICATION_ID: {app}. {error}'.format(
                app=app_id,
                error=e
            ))
        apps = list(map(
            utils.parse_card_info, 
            soup.select('div[data-uitype="500"]')
        ))
        return apps