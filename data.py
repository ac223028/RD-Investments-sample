import os
import numpy as np
import pandas as pd

from zipline.data import bundles
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.utils.calendars import get_calendar
from zipline.data.data_portal import DataPortal

from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine

from zipline.pipeline.factors.technical import *
from zipline.pipeline import Pipeline, CustomFilter

from pipeline_live.data.sources import iex
import TA_factors

os.listdir(os.environ['ZIPLINE_ROOT'])

with open(os.environ['ZIPLINE_TRADER_CONFIG'], 'r') as f:
    data = f.read()
    print(data[:20])

print("bundles:", bundles.bundles.keys())

bundle_name = 'full'

bundle_data = bundles.load(bundle_name)

# Set the dataloader
pricing_loader = USEquityPricingLoader.without_fx(bundle_data.equity_daily_bar_reader, bundle_data.adjustment_reader)


# Define the function for the get_loader parameter
def choose_loader(column):
    if column not in USEquityPricing.columns:
        raise Exception('Column not in USEquityPricing')
    return pricing_loader


class IsPrimaryShareEmulation(CustomFilter):
    # not sure this works
    inputs = ()
    window_length = 1

    def compute(self, today, symbols, out, *inputs):
        company = iex.company()
        fin = iex.key_stats()
        # pprint.pprint(fin)
        file = open("...", "r") # the elipsis is a file name
        l = [c.strip().split(",")[1][7:-2].split(" [") for c in file.readlines()]
        map = {int(c[0]): c[1] for c in l}
        k = map.keys()

        ary = np.array([
            # symbol
            symbol in k and
            map[symbol] in company and
            'country' in company[map[symbol]] and
            [company[map[symbol]].get('country') == 'US'] and
            map[symbol] in fin and
            fin[map[symbol]]["beta"] > 0
            for symbol in symbols
        ], dtype=bool)
        out[:] = ary


# Set the trading calendar
trading_calendar = get_calendar('NYSE')

start_date = "replace this string with a pandas time stamp"
end_date = "replace this string with a pandas time stamp"

# Create a data portal
data_portal = DataPortal(bundle_data.asset_finder,
                         trading_calendar=trading_calendar,
                         first_trading_day=start_date,
                         equity_daily_reader=bundle_data.equity_daily_bar_reader,
                         adjustment_reader=bundle_data.adjustment_reader)

# Create a Pipeline engine
engine = SimplePipelineEngine(get_loader=choose_loader,
                              asset_finder=bundle_data.asset_finder)

import pprint
import alpaca_trade_api as tradeapi

from pylivetrader.assets import AssetFinder

api = tradeapi.REST()
a = api.list_assets(status="active")

for i in a:
    if i.symbol == "SPY":
        s = i

company = iex.company()

pipeline = Pipeline({
    'Alpha101': TA_factors.Alpha101(),
    'liquidity': TA_factors.Liquidity(),
    'OneDayDifferenceLabel': TA_factors.OneDayDifferenceLabel(),
    'rsi': RSI(window_length=10),
    'BollingerBands': BollingerBands(window_length=10, k=2),
    'aroon': Aroon(window_length=10),
    'FastStochasticOscillator': FastStochasticOscillator(),
    'IchimokuKinkoHyo': IchimokuKinkoHyo(),
    'TrueRange': TrueRange(),
    'MovingAverageConvergenceDivergenceSignal': MovingAverageConvergenceDivergenceSignal(),
    'AVGPRICE': TA_factors.AVGPRICE(),
    'MEDPRICE': TA_factors.MEDPRICE(),
    'TYPPRICE': TA_factors.TYPPRICE(),
    'WCLPRICE': TA_factors.WCLPRICE(),
    'OBV': TA_factors.OBV(),
    'APO': TA_factors.APO(),
    'AROONOSC': TA_factors.AROONOSC(),
    'BOP': TA_factors.BOP(),
    'CCI': TA_factors.CCI(),
    'MOM': TA_factors.MOM(),
    'PPO': TA_factors.PPO(),
    'ROC': TA_factors.ROC(),
    'ROCP': TA_factors.ROCP(),
    'ROCR': TA_factors.ROCR(),
    'ROCR100': TA_factors.ROCR100(),
    'ULTOSC': TA_factors.ULTOSC(),
    'WILLR': TA_factors.WILLR(),
    'BETA': TA_factors.BETA(),
    'CORREL': TA_factors.CORREL(),
    'LINEARREG': TA_factors.LINEARREG(),
    'LINEARREG_ANGLE': TA_factors.LINEARREG_ANGLE(),
    'LINEARREG_INTERCEPT': TA_factors.LINEARREG_INTERCEPT(),
    'LINEARREG_SLOPE': TA_factors.LINEARREG_SLOPE(),
    'STDDEV': TA_factors.STDDEV(),
    'TSF': TA_factors.TSF(),
    'VAR': TA_factors.VAR(),
    'DEMA': TA_factors.DEMA(),
    'MA': TA_factors.MA(),
    'MIDPOINT': TA_factors.MIDPOINT(),
    'MIDPRICE': TA_factors.MIDPRICE(),
    'SAR': TA_factors.SAR(),
    'SAREXT': TA_factors.SAREXT(),
    'SMA': TA_factors.SMA(),
    'TEMA': TA_factors.TEMA(),
    'TRIMA': TA_factors.TRIMA(),
    'WMA': TA_factors.WMA(),
    'CDL2CROWS': TA_factors.CDL2CROWS(),
    'CDL3BLACKCROWS': TA_factors.CDL3BLACKCROWS(),
    'CDL3INSIDE': TA_factors.CDL3INSIDE(),
    'CDL3LINESTRIKE': TA_factors.CDL3LINESTRIKE(),
    'CDL3OUTSIDE': TA_factors.CDL3OUTSIDE(),
    'CDL3STARSINSOUTH': TA_factors.CDL3STARSINSOUTH(),
    'CDL3WHITESOLDIERS': TA_factors.CDL3WHITESOLDIERS(),
    'CDLABANDONEDBABY': TA_factors.CDLABANDONEDBABY(),
    'CDLADVANCEBLOCK': TA_factors.CDLADVANCEBLOCK(),
    'CDLBELTHOLD': TA_factors.CDLBELTHOLD(),
    'CDLBREAKAWAY': TA_factors.CDLBREAKAWAY(),
    'CDLCLOSINGMARUBOZU': TA_factors.CDLCLOSINGMARUBOZU(),
    'CDLCONCEALBABYSWALL': TA_factors.CDLCONCEALBABYSWALL(),
    'CDLCOUNTERATTACK': TA_factors.CDLCOUNTERATTACK(),
    'CDLDARKCLOUDCOVER': TA_factors.CDLDARKCLOUDCOVER(),
    'CDLDOJI': TA_factors.CDLDOJI(),
    'CDLDOJISTAR': TA_factors.CDLDOJISTAR(),
    'CDLDRAGONFLYDOJI': TA_factors.CDLDRAGONFLYDOJI(),
    'CDLENGULFING': TA_factors.CDLENGULFING(),
    'CDLEVENINGDOJISTAR': TA_factors.CDLEVENINGDOJISTAR(),
    'CDLEVENINGSTAR': TA_factors.CDLEVENINGSTAR(),
    'CDLGAPSIDESIDEWHITE': TA_factors.CDLGAPSIDESIDEWHITE(),
    'CDLGRAVESTONEDOJI': TA_factors.CDLGRAVESTONEDOJI(),
    'CDLHAMMER': TA_factors.CDLHAMMER(),
    'CDLHANGINGMAN': TA_factors.CDLHANGINGMAN(),
    'CDLHARAMI': TA_factors.CDLHARAMI(),
    'CDLHARAMICROSS': TA_factors.CDLHARAMICROSS(),
    'CDLHIGHWAVE': TA_factors.CDLHIGHWAVE(),
    'CDLHIKKAKE': TA_factors.CDLHIKKAKE(),
    'CDLHIKKAKEMOD': TA_factors.CDLHIKKAKEMOD(),
    'CDLHOMINGPIGEON': TA_factors.CDLHOMINGPIGEON(),
    'CDLIDENTICAL3CROWS': TA_factors.CDLIDENTICAL3CROWS(),
    'CDLINNECK': TA_factors.CDLINNECK(),
    'CDLINVERTEDHAMMER': TA_factors.CDLINVERTEDHAMMER(),
    'CDLKICKING': TA_factors.CDLKICKING(),
    'CDLKICKINGBYLENGTH': TA_factors.CDLKICKINGBYLENGTH(),
    'CDLLADDERBOTTOM': TA_factors.CDLLADDERBOTTOM(),
    'CDLLONGLEGGEDDOJI': TA_factors.CDLLONGLEGGEDDOJI(),
    'CDLLONGLINE': TA_factors.CDLLONGLINE(),
    'CDLMARUBOZU': TA_factors.CDLMARUBOZU(),
    'CDLMATCHINGLOW': TA_factors.CDLMATCHINGLOW(),
    'CDLMATHOLD': TA_factors.CDLMATHOLD(),
    'CDLMORNINGDOJISTAR': TA_factors.CDLMORNINGDOJISTAR(),
    'CDLMORNINGSTAR': TA_factors.CDLMORNINGSTAR(),
    'CDLONNECK': TA_factors.CDLONNECK(),
    'CDLPIERCING': TA_factors.CDLPIERCING(),
    'CDLRICKSHAWMAN': TA_factors.CDLRICKSHAWMAN(),
    'CDLRISEFALL3METHODS': TA_factors.CDLRISEFALL3METHODS(),
    'CDLSEPARATINGLINES': TA_factors.CDLSEPARATINGLINES(),
    'CDLSHOOTINGSTAR': TA_factors.CDLSHOOTINGSTAR(),
    'CDLSHORTLINE': TA_factors.CDLSHORTLINE(),
    'CDLSPINNINGTOP': TA_factors.CDLSPINNINGTOP(),
    'CDLSTALLEDPATTERN': TA_factors.CDLSTALLEDPATTERN(),
    'CDLSTICKSANDWICH': TA_factors.CDLSTICKSANDWICH(),
    'CDLTAKURI': TA_factors.CDLTAKURI(),
    'CDLTASUKIGAP': TA_factors.CDLTASUKIGAP(),
    'CDLTHRUSTING': TA_factors.CDLTHRUSTING(),
    'CDLTRISTAR': TA_factors.CDLTRISTAR(),
    'CDLUNIQUE3RIVER': TA_factors.CDLUNIQUE3RIVER(),
    'CDLUPSIDEGAP2CROWS': TA_factors.CDLUPSIDEGAP2CROWS(),
    'CDLXSIDEGAP3METHODS': TA_factors.CDLXSIDEGAP3METHODS(),
},
    screen=IsPrimaryShareEmulation()
)

# Run our pipeline for the given start and end dates
pipeline_output = engine.run_pipeline(pipeline, start_date, end_date)

pipeline_output.head()
pipeline_output.to_csv(f"data\\{bundle_name}.csv")
