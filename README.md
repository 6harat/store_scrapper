# STORE SCRAPPER
Program to collect information about games available on Google Play Store

# STEPS:
+ **go to a location of your choice**: `my_location`
+ **clone the code**:
    - current_folder:     `my_location`
    - execute_command:    `git clone git@github.com:Gulats/store_scrapper.git`
+ **create virtualenv for store_scrapper**:
    - current_folder:     `my_location`
    - execute_commands:
        * `pip install virtualenv`                          (if virtualenv is not already installed)
        * `virtualenv -p <path-to-python3.7> store_scrapper`(creates a virtualenv for project)
+ **entering virtualenv**:
    - current_folder:     `my_location`
    - execute_commands:
        * `cd store_scrapper`                               (change directory location)
        * `source bin/activate`                             (or equivalent windows activate cmd)
+ **installing dependencies**:
    - current_folder:     `store_scrapper`
    - execute_command:    `pip install -r requirements.txt` (installs all required dependencies)
+ **starting program**:
    - current_folder:     `store_scrapper`
    - execute_command:    `python play_server.py`           (start execution)
+ **interacting with program**:
    - install Postman on desktop [https://www.getpostman.com/downloads/] or add plugin to Chrome [https://chrome.google.com/webstore/detail/postman/fhbjgbiflinjbdggehcddcbncdddomop?hl=en]
    - open Postman and click on import and choose the `store_scrapper/store_scrapper_postman.json` file.
    - use the following APIs for the listed tasks:
        * see all active managers:
            + Collection Name: `GET`   _View_
            + Path: `/view`
        * start a new manager:                  `POST`  _Start_     `/start`
        * peek an existing manager:             `GET`   _Peek_      `/peek?pid=<pid>`
        * flush records of an existing manager: `POST`  _Flush_ (NOT YET IMPLEMENTED)  
        * stop an existing manager:             `POST`  _Stop_      `/stop?pid=<pid>&show_records=<bool>`
    - additional APIs for basic testing:
        * get detail by app_id:                 `GET`   _Detail_    `/detail?app_id=<app_id>`
        * get apps by collection and category:  `GET`   _Collection_`/collection?catg_id=<catg>&coln_id=<coln>&page=<page>&results=<page_size>`
        * get apps by similar to a given app:   `GET`   _Similar_   `/similar?app_id=<app_id>`
+ **stopping program**:
    - execute_command:    `press ctrl+c`                    (stop  execution)
    The log file gets generated at `log/play_server_<timestamp>.log`
    On pressing `ctrl+c` the execution of the program is stopped and the program attempts to gracefully shutdown active managers (if not previously stopped using the REST API).
    _NOTE_: Press CTRL+C **ONLY ONCE** otherwise data dump will fail. The data for each running manager is written at `opt/sweeper_<timestamp>_<manager_id>.json`
+ **exiting virtualenv**:
    - current_folder:     `store_scraper`
    - execute_command:    `deactivate`                      (or equivalent windows deactivate cmd)

# TODOS:
+ add support to read the past json file first and then start executing again so as to aggregate data over and above the one previously collected.
+ tweak `hl` and `gl` params in `play_scraper.collection` and `play_scraper.similar` fucntion calls if they yield new games.
+ experiment with `play_scraper.search` if it can help gather more records.
+ use separate event loops in separate threads for each manager and keep the REST API event loop separate to increase throughput.