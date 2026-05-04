from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from companyFinancialCalc import CompanyFinancialCalculator, ValuationResults


class ValuationStrategy(ABC):
    @abstractmethod
    def run(self, calculator: "CompanyFinancialCalculator") -> "ValuationResults":
        raise NotImplementedError


class DefaultValuationStrategy(ValuationStrategy):
    def run(self, calculator: "CompanyFinancialCalculator") -> "ValuationResults":
        from companyFinancialCalc import ValuationResults

        fcf, fcff, fcf_per_share, delta_nwc = calculator.calc_fcf()
        calculator.calc_forecast_metrics()
        wacc = calculator.calc_wacc()
        roic = calculator.roic()
        intrinsic, div_price = calculator.fcff_forecast()

        calculator.company_overview["ROIC"] = roic
        calculator.company_overview["WACC"] = wacc
        calculator.company_overview["IntrinsicPrice"] = intrinsic
        calculator.company_overview["DividendPrice"] = div_price

        return ValuationResults(
            intrinsic_price=intrinsic,
            dividend_price=div_price,
            wacc=wacc,
            roic=roic,
            fcf=fcf,
            fcff=fcff,
            fcf_per_share=fcf_per_share,
            delta_nwc=delta_nwc,
        )
