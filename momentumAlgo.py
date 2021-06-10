
import quantopian.algorithm as algo
import quantopian.optimize as opt
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import QTradableStocksUS
from quantopian.pipeline.data.morningstar import Fundamentals as morn
#from quantopian.pipeline.data.factset import Fundamentals as fs
from quantopian.pipeline.factors import Latest, Returns, CustomFactor, SimpleMovingAverage
from quantopian.pipeline.classifiers.morningstar import Sector
from quantopian.algorithm import attach_pipeline, pipeline_output

#Constraints 
max_leverage = 1.0
total_pos = 500
max_short = 10/total_pos
max_long = 10/total_pos

class Momentum1(CustomFactor):
    # Gives us the returns from last month
    inputs = [Returns(window_length = 20)]
    window_length = 20
    
    def compute(self, today, assets, out, lag_returns):
        out[:] = lag_returns[0]
        
class Momentum2(CustomFactor):
    # Gives us the returns from last quarter
    inputs = [Returns(window_length = 63)]
    window_length = 63
    
    def compute(self, today, assets, out, lag_returns):
        out[:] = lag_returns[0]


def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Rebalance every day, 1 hour after market open.
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_open(hours=1),
    )

    # Record tracking variables at the end of each day.
    algo.schedule_function(
        record_vars,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(),
    )

    # Create our dynamic stock selector.
    algo.attach_pipeline(make_pipeline(), 'pipeline')


def make_pipeline():
    """
    A function to create our dynamic stock selector (pipeline). Documentation
    on pipeline can be found here:
    https://www.quantopian.com/help#pipeline-title
    """

    # Base universe set to the QTradableStocksUS
    base_universe = QTradableStocksUS()

    # Factor of yesterday's close price.
    yesterday_close = USEquityPricing.close.latest
    
    
    #Data factors
    ev_ebitda_inverse = 1/Latest([morn.ev_to_ebitda])
    ev_ebit_inverse = (Latest([morn.enterprise_value])/Latest([morn.ebit]))
    fcf_ev = Latest([morn.cash_return])
    roe = Latest([morn.roe])
    
    w_ev_ebitda_inverse = ev_ebitda_inverse.winsorize(min_percentile=0.10, max_percentile=0.90)
    w_ev_ebit_inverse = ev_ebit_inverse.winsorize(min_percentile=0.10, max_percentile=0.90)
    w_fcf_ev = fcf_ev.winsorize(min_percentile=0.10, max_percentile=0.90)
    w_roe = roe.winsorize(min_percentile=0.10, max_percentile=0.90) 
    
    qi_value = (w_ev_ebitda_inverse.zscore() + w_ev_ebit_inverse.zscore() + w_fcf_ev.zscore() + w_roe.zscore())
    
    #qi_value = (w_ev_ebitda_inverse.zscore() + w_fcf_ev.zscore() + w_roe.zscore())
    
    momentum1 = Momentum1()
    momentum2 = Momentum2()
    income_growth = Latest([morn.net_income_growth])
    peg = Latest([morn.peg_ratio])
    current = Latest([morn.current_ratio])
    pe = Latest([morn.pe_ratio])

    
    w_momentum1 = momentum1.winsorize(min_percentile=0.10, max_percentile=0.90) 
    w_momentum2 = momentum2.winsorize(min_percentile=0.10, max_percentile=0.90)
    w_income_growth = income_growth.winsorize(min_percentile=0.10, max_percentile=0.90) 
    w_peg = peg.winsorize(min_percentile=0.10, max_percentile=0.90)
    w_current = current.winsorize(min_percentile=0.10, max_percentile=0.90)
    w_pe = pe.winsorize(min_percentile=0.10, max_percentile=0.90)
    
    factor = (w_momentum1.zscore() + w_momentum2.zscore() + w_income_growth.zscore() + w_peg.zscore()+ w_current.zscore() + w_roe.zscore() + w_ev_ebitda_inverse.zscore() + w_pe.zscore())
    
    longs = factor.top(total_pos//2, mask = base_universe & qi_value.percentile_between(5, 45))
    shorts = factor.bottom(total_pos//2, mask = base_universe & qi_value.percentile_between(5, 45))
    
    ls_screen = (longs|shorts)
    
    
    pipeline = Pipeline(
        columns={
            'close': yesterday_close,
            'longs': longs,
            'shorts': shorts,
            'factor': factor
        },
        screen = (base_universe & qi_value.percentile_between(0, 30) & ls_screen)
    )
    return pipeline


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    context.pipeline_data = algo.pipeline_output('pipeline')



def rebalance(context, data):
    """
    Execute orders according to our schedule_function() timing.
    """
        # Retrieve output of the pipeline    
    alpha = context.pipeline_data.factor
    objective = opt.MaximizeAlpha(alpha)
    constraints = []
    constraints.append(opt.MaxGrossExposure(max_leverage))
    constraints.append(opt.DollarNeutral())
    constraints.append(opt.PositionConcentration.with_equal_bounds(min = -max_short, max = max_long))
    algo.order_optimal_portfolio(objective=objective, constraints=constraints)    


def record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    algo.record(num_positions=len(context.portfolio.positions))    
        
    algo.record(leverage=context.account.leverage)

def handle_data(context, data):
    """
    Called every minute.
    """
    pass