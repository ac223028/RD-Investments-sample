import urllib3
import zipline.data.bundles.alpaca_api as ingest

import trading_calendars
from trading_calendars import TradingCalendar

if __name__ == "__main__":

    import pandas as pd

    from datetime import timedelta

    from zipline.data.bundles import register
    from zipline.data import bundles as bundles_module
    import os

    import time

    dates = []
    starting_month = 1  # default is 1 for January

    # this will control which dates to get
    for i in [2020]:
        for j in range(starting_month, 13):
            dates.append(f"{i}-{j}")

    print(dates)

    package_name = month

    cal: TradingCalendar = trading_calendars.get_calendar('NYSE')
    end_date = pd.Timestamp('now', tz='utc').date() - timedelta(days=1)

    start_date = pd.Timestamp(package_name + '-1 0:00', tz='utc')
    start_date -= timedelta(days=70)
    while not cal.is_session(start_date):
        print("Error-start:", start_date)
        start_date -= timedelta(days=1)
    
    end_date = pd.Timestamp(start_date)
    end_date += timedelta(days=101)
    while not cal.is_session(str(end_date)):
        print("Error-end:", end_date)
        end_date += timedelta(days=1)
    
    # start_date = end_date - timedelta(days=260)  # I think this is trading days; there are ~260 trading days a year
    # while not cal.is_session(start_date):
    #     start_date += timedelta(days=1)
    
    print("start:", start_date)  # 2007-03-12
    print("end:", end_date)  # 2021-06-04
    
    initialize_client()
    
    start_time = time.time()
    
    register(
        package_name,  # This is the name of the ingest package
        # api_to_bundle(interval=['1d', '1m']),
        # api_to_bundle(interval=['1m']),
        api_to_bundle(interval=['1d']),
        calendar_name='NYSE',
        start_session=start_date,
        end_session=end_date
    )
    
    while True:
        try:
    
            print("registered:", package_name)
    
            assets_version = ((),)[0]  # just a weird way to create an empty tuple
    
            bundles_module.ingest(
                package_name,
                os.environ,
                assets_versions=assets_version,
                show_progress=True,
            )
        except urllib3.exceptions.SSLError:
            print("Retrying...")
            continue
        break
    
    print(f"--- {package_name} took {timedelta(seconds=time.time() - start_time)} ---")