# store_scrapper
Program to collect information about games available on Google Play Store

# STEPS:
+ go to a location of your choice say 'my_location'
+ clone the code:
    - current_folder:     my_location
    - execute_command:    `git clone git@github.com:Gulats/store_scrapper.git`
+ create virtualenv for store_scrapper:
    - current_folder:     my_location
    - execute_commands:
        * `pip install virtualenv`                          (if virtualenv is not already installed)
        * `virtualenv -p <path-to-python3.7> store_scrapper`(creates a virtualenv for project)
+ running program:
    - current_folder:     my_location
    - execute_commands:
        * `cd store_scrapper`                               (change directory location)
        * `source bin/activate`                             (or equivalent windows activate command)
        * `pip install play_scraper`                        (install project dependencies)
        * `python -i sweeper.py`                            (start execution)
        * `press ctrl+c`                                    (this stops the program)

    _NOTE_: You might need to press ctrl+d and ctrl+c as well again in that order to properly exit from the command line (this is a bug in the program and will be fixed soon).
    The log file gets generated at log/sweeper_<timestamp>.log
    On pressing ctrl+c the execution of the program is stopped and the data collected so far is dumped into opt/sweeper_<timestamp>.json
    Total number of records and time taken for execution is printed in both console and the log file.
+ closing out:
    - current_folder:     store_scraper
    - execute_command:    deactivate                      (or equivalent windows deactivate command)

# TODOS:
+ add support to read the past json file first and then start executing again so as to aggregate data over and above the one previously collected.
+ add retry mechanism for play_scraper requests that failed via connection timeout issues or other HTTP isues.
+ tweak `hl` and `gl` params in `play_scraper.collection` and `play_scraper.similar` fucntion calls if they yield new games.
+ experiment with `play_scraper.search` if it can help gather more records.