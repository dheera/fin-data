import numpy as np
from .option_price_provider import OptionPriceProvider

class OptionSpread:
    def __init__(self, trade_type, center, width, price_provider: OptionPriceProvider):
        """
        trade_type: one of "iron_butterfly", "iron_condor", "vertical_call_credit", 
                    "vertical_call_debit", "vertical_put_credit", "vertical_put_debit"
        center: center strike for the trade
        width: spread width
        price_provider: an instance of OptionPriceProvider
        """
        self.trade_type = trade_type
        self.center = center
        self.width = width
        self.price_provider = price_provider
        self.legs = self.get_trade_legs(trade_type, center, width)

    @staticmethod
    def get_trade_legs(trade_type, center, width):
        legs = {}
        if trade_type == "iron_butterfly":
            legs["short_call"] = ("C", center, +1)
            legs["short_put"]  = ("P", center, +1)
            legs["long_call"]  = ("C", center + width, -1)
            legs["long_put"]   = ("P", center - width, -1)
        elif trade_type == "iron_condor":
            legs["short_call"] = ("C", center + width/2, +1)
            legs["long_call"]  = ("C", center + 3*width/2, -1)
            legs["short_put"]  = ("P", center - width/2, +1)
            legs["long_put"]   = ("P", center - 3*width/2, -1)
        elif trade_type == "vertical_call_credit":
            legs["short_call"] = ("C", center, +1)
            legs["long_call"]  = ("C", center + width, -1)
        elif trade_type == "vertical_call_debit":
            legs["long_call"]  = ("C", center, -1)
            legs["short_call"] = ("C", center + width, +1)
        elif trade_type == "vertical_put_credit":
            legs["short_put"] = ("P", center, +1)
            legs["long_put"]  = ("P", center - width, -1)
        elif trade_type == "vertical_put_debit":
            legs["long_put"]  = ("P", center, -1)
            legs["short_put"] = ("P", center - width, +1)
        else:
            raise ValueError("Invalid trade type.")
        return legs

    def get_initial_credit(self, trade_time):
        """
        Computes the initial credit of the spread by summing each leg’s premium.
        """
        initial_credit = 0
        for leg in self.legs.values():
            price = self.price_provider.lookup_leg_price(leg, trade_time)
            if np.isnan(price):
                raise ValueError(f"Could not retrieve price for leg {leg} at time {trade_time}")
            initial_credit += leg[2] * price
        return initial_credit

    def compute_expiration_payoff(self, S_range, trade_time):
        """
        Computes the expiration payoff for a range of underlying prices S_range.
        """
        initial_credit = self.get_initial_credit(trade_time)
        payoff = []
        for S in S_range:
            leg_payoff = 0
            for (opt_type, strike, pos) in self.legs.values():
                if opt_type == "C":
                    intrinsic = max(S - strike, 0)
                else:
                    intrinsic = max(strike - S, 0)
                leg_payoff += pos * -intrinsic
            payoff.append(initial_credit + leg_payoff)
        return np.array(payoff)

