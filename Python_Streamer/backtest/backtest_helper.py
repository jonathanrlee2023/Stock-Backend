from dataclasses import dataclass
from typing import Any, Iterable, Dict, List, Tuple, Optional
import numpy as np
from math import sqrt, isfinite

# Optional: example structure that matches your earlier usage
@dataclass(slots=True)
class PricePoint:
    price: float
    timestamp: Any  # datetime or similar


def compute_strategy_stats(
    points: Iterable[Any],
    periods_per_year: int = 252,
    risk_free_rate_annual: float = 0.035,
    var_alpha: float = 0.05,
) -> Dict[str, Any]:
    """
    Compute key performance and risk metrics from a series of total capital points.

    Inputs:
      - points: iterable of objects or dicts with total capital and timestamp.
                Capital field can be named 'capital', 'price', 'equity', or 'value'.
                Timestamp field can be named 'timestamp', 'datetime', or 'time'.
      - periods_per_year: frequency for annualization (252=daily, 52=weekly, 12=monthly).
      - risk_free_rate_annual: annual risk-free rate (e.g., 0.02 for 2%).
      - var_alpha: tail probability for historical VaR/CVaR (e.g., 0.05 = 5%).

    Returns: dict with metrics and series (returns, drawdowns).
    """
    capitals = points["Capital"].to_numpy(dtype=float)
    times = points["datetime"].astype(str).to_numpy()

    # Basic sanity checks
    if not np.all(np.isfinite(capitals)) or capitals[0] <= 0:
        raise ValueError("Capitals must be finite and the first value must be > 0.")

    # Period returns
    returns = capitals[1:] / capitals[:-1] - 1.0
    n = returns.size
    if n == 0:
        raise ValueError("Insufficient data for returns.")

    # Annualization helpers
    rf_period = risk_free_rate_annual / periods_per_year
    mean_r = returns.mean()
    std_r = returns.std(ddof=1) if n > 1 else np.nan
    vol_annual = std_r * sqrt(periods_per_year) if isfinite(std_r) else np.nan

    # Sharpe (using excess mean and total volatility)
    excess = returns - rf_period
    sharpe = (excess.mean() / std_r * sqrt(periods_per_year)) if (isfinite(std_r) and std_r > 0) else np.nan

    # Sortino (downside deviation relative to rf)
    downside_excess = np.where(excess < 0, excess, 0)
    downside_deviation = np.sqrt(np.mean(downside_excess**2)) * np.sqrt(periods_per_year)
    sortino = (excess.mean() * periods_per_year) / downside_deviation if downside_deviation > 0 else np.nan

    # Equity-based stats
    total_return = capitals[-1] / capitals[0] - 1.0
    total_days = (points["datetime"].max() - points["datetime"].min()).days
    years = total_days / 365.25
    cagr = (capitals[-1] / capitals[0]) ** (1 / years) - 1.0 if years > 0 else np.nan

    # Drawdowns
    running_max = np.maximum.accumulate(capitals)
    drawdowns = capitals / running_max - 1.0
    max_drawdown = float(drawdowns.min())
    # Calmar ratio
    calmar = (cagr / abs(max_drawdown)) if max_drawdown < 0 else np.nan

    # Period “win rate” and “profit factor” (period-based, not trade-based)
    win_rate = float((returns > 0).mean())
    gross_gains = returns[returns > 0].sum()
    gross_losses = -returns[returns < 0].sum()
    profit_factor_period = (gross_gains / gross_losses) if gross_losses > 0 else np.inf

    # Historical VaR / CVaR on period returns
    if 0 < var_alpha < 1:
        var = float(np.quantile(returns, var_alpha))
        cvar = float(returns[returns <= var].mean()) if (returns <= var).any() else var
    else:
        var, cvar = np.nan, np.nan

    # Pack results
    return {
        "counts": {
            "periods": int(n),
        },
        "performance": {
            "total_return": float(total_return) * 100,
            "cagr": float(cagr) * 100,
            "period_mean_return": float(mean_r),
            "annualized_volatility": float(vol_annual*100),
            "sharpe": float(sharpe),
            "sortino": float(sortino),
            "standard_deviation": float(std_r*100),
            "calmar": float(calmar),
        },
        "risk": {
            "max_drawdown": float(max_drawdown) * 100,
            "var": {"alpha": var_alpha, "value": float(var)},
            "cvar": {"alpha": var_alpha, "value": float(cvar)},
        },
        "period_dynamics": {
            "win_rate": float(win_rate),
            "profit_factor_period": float(profit_factor_period),
        },
        "series": {
            "returns": returns.tolist(),
            "drawdowns": drawdowns.tolist(),
            "capitals": capitals.tolist(),
            "timestamps": times.tolist() if hasattr(times, "tolist") else list(times),
        },
    }
