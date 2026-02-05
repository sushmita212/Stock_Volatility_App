from __future__ import annotations

import numpy as np


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.mean(np.abs(y_true - y_pred)))


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def qlike(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-12) -> float:
    """Quasi-likelihood loss commonly used for volatility forecasts.

    Assumes y_true,y_pred are variances or volatilities squared? In our API we use volatility.
    We convert to variance for loss stability.

    QLIKE(var, varhat) = log(varhat) + var/varhat
    """

    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)

    var = np.maximum(y_true**2, eps)
    varhat = np.maximum(y_pred**2, eps)
    return float(np.mean(np.log(varhat) + (var / varhat)))
