import pprint

import pytz
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import pandas_datareader.data as web
from sklearn.model_selection import train_test_split
from zipline.api import pipeline_output
from zipline.pipeline import Pipeline
from zipline.pipeline.factors import RSI, FastStochasticOscillator

from zipline.utils.calendars import get_calendar
from zipline.api import *
from zipline.data import bundles
from zipline import run_algorithm
from zipline.utils.events import date_rules, time_rules

from sklearn.ensemble import RandomForestClassifier

import TA_factors

def get_benchmark(symbol=None, start=None, end=None):
    bm = web.DataReader(symbol,
                        'iex',
                        pd.Timestamp(start),
                        pd.Timestamp(end))['close']
    bm.index = pd.to_datetime(bm.index + " 21:00:00+00:00")
    return bm.pct_change(periods=1).fillna(0)


def make_pipeline():
    return Pipeline(
        columns={
            'CORREL': TA_factors.CORREL(),
            'FastStochasticOscillator': FastStochasticOscillator(),
        },
    )


def initialize(context):
    attach_pipeline(make_pipeline(), 'my_pipeline')
    schedule_function(rebalance, date_rules.every_day(), time_rules.market_open(hours=6, minutes=25))

    # read in data
    df = pd.read_csv("...")

    df = df[df.CORREL < 0.7]

    X = df[['CORREL', 'FastStochasticOscillator']]
    y = df['cut']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

    # train model here
    print('training model')
    model = RandomForestClassifier(n_estimators=3)
    model.fit(X_train, y_train)
    print('training model complete')

    # save model to some context variable
    context.model = model


def rebalance(context, data):
    weights = context.weights
    positions = context.portfolio.positions

    for asset in positions:  # sell everything that is no longer "True"
        order_target_percent(asset, 0)

    for asset in weights:
        order_target_percent(asset, weights[asset])


def getWeights(context):
    df = context.pipeline_data.fillna(0)
    s = set()

    model = context.model

    # look at every row of inputs and predict them from the model saved in the context object

    for row in df.itertuples():
        pred = model.predict([[row.CORREL, row.FastStochasticOscillator]])[0]
        if pred == 1:
            s.add(row.Index)

    N = len(s)
    if N == 0:
        return dict()
    N = 1.0 / float(N)
    result = {c: N for c in s}

    return result


def before_trading_start(context, data):
    context.pipeline_data = pipeline_output('my_pipeline')
    context.weights = getWeights(context)  # is a dictionary


if __name__ == '__main__':
    bundle_name = 'full'
    bundle_data = bundles.load(bundle_name)

    # Set the trading calendar
    trading_calendar = get_calendar('NYSE')

    start = pd.Timestamp(datetime(2015, 1, 1, tzinfo=pytz.UTC))
    end = pd.Timestamp(datetime(2020, 10, 31, tzinfo=pytz.UTC))

    r = run_algorithm(start=start,
                      end=end,
                      initialize=initialize,
                      capital_base=100000,
                      # handle_data=handle_data,
                      # benchmark_returns=get_benchmark(symbol="SPY",
                      #                                 start=start.date().isoformat(),
                      #                                 end=end.date().isoformat()),
                      bundle=bundle_name,
                      broker=None,
                      state_filename="./demo.state",
                      trading_calendar=trading_calendar,
                      before_trading_start=before_trading_start,
                      # analyze=analyze,
                      data_frequency='daily'
                      )
    fig, axes = plt.subplots(1, 1, figsize=(16, 5))
    r.algorithm_period_return.plot(color='blue')

    plt.legend(['Algo', 'Benchmark'])
    plt.ylabel("Returns", color='black', size=20)
    plt.show()
