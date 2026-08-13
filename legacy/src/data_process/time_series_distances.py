"""Distance utilities for time-series clustering.

This module adapts the DTW idea from the user-provided ``ts_dist.py`` but avoids
name collisions with the repository's ``src/utils.py`` and replaces deprecated
``np.float`` usage with standard ``float``/``np.float64``.
"""
from __future__ import annotations

from typing import Literal

import numpy as np

try:  # numba is optional, but recommended for speed.
    from numba import njit
except ImportError:  # pragma: no cover - only used when numba is unavailable.
    def njit(*args, **kwargs):  # type: ignore[no-redef]
        def decorator(func):
            return func
        return decorator


DTWMode = Literal["dependent", "independent"]


def as_2d_time_series(x: np.ndarray | list[float] | list[list[float]]) -> np.ndarray:
    """Return a time series as a 2-D array with shape ``(n_features, n_steps)``.

    A 1-D series ``[x_1, ..., x_T]`` is interpreted as one feature over ``T``
    time steps and reshaped to ``(1, T)``.
    """
    arr = np.asarray(x, dtype=float)
    if arr.ndim == 1:
        return arr.reshape(1, -1)
    if arr.ndim == 2:
        return arr
    raise ValueError(
        "Expected a 1-D or 2-D time series. "
        f"Received an array with shape {arr.shape}."
    )


def z_normalize_rows(x: np.ndarray, eps: float = 1e-12) -> np.ndarray:
    """Z-normalize each feature/row of a time series independently."""
    arr = np.asarray(x, dtype=float)
    mean = arr.mean(axis=1, keepdims=True)
    std = arr.std(axis=1, keepdims=True)
    std = np.where(std < eps, 1.0, std)
    return (arr - mean) / std



@njit(cache=True)
def _dtw_distance_numba(x: np.ndarray, y: np.ndarray, window: int) -> float:
    """Compute multidimensional DTW with L1 local cost.

    Parameters
    ----------
    x, y:
        Arrays with shape ``(n_features, n_steps)``.
    window:
        Sakoe-Chiba window. Use a large value for unconstrained DTW.
    """
    n_x = x.shape[1]
    n_y = y.shape[1]
    w = max(window, abs(n_x - n_y))

    dp = np.empty((n_x + 1, n_y + 1), dtype=np.float64)
    dp[:, :] = np.inf
    dp[0, 0] = 0.0

    for i in range(1, n_x + 1):
        j_start = max(1, i - w)
        j_end = min(n_y + 1, i + w + 1)
        for j in range(j_start, j_end):
            cost = 0.0
            for feature in range(x.shape[0]):
                cost += abs(x[feature, i - 1] - y[feature, j - 1])
            dp[i, j] = cost + min(dp[i - 1, j], dp[i, j - 1], dp[i - 1, j - 1])

    return float(dp[n_x, n_y])


def dtw_distance(
    x: np.ndarray | list[float] | list[list[float]],
    y: np.ndarray | list[float] | list[list[float]],
    *,
    window: int | None = None,
    mode: DTWMode = "dependent",
    normalize: bool = False,
) -> float:
    """Compute the Dynamic Time Warping distance between two time series.

    Parameters
    ----------
    x, y:
        Time series. A 1-D array is interpreted as one feature over time.
        A 2-D array must follow ``(n_features, n_steps)``.
    window:
        Optional Sakoe-Chiba window. ``None`` uses unconstrained DTW.
    mode:
        ``"dependent"`` computes a joint multidimensional alignment.
        ``"independent"`` sums one DTW distance per feature.
    normalize:
        If ``True``, z-normalizes each series before computing DTW.
    """
    x_arr = as_2d_time_series(x)
    y_arr = as_2d_time_series(y)

    if x_arr.shape[0] != y_arr.shape[0]:
        raise ValueError(
            "Both time series must have the same number of features. "
            f"Got {x_arr.shape[0]} and {y_arr.shape[0]}."
        )

    if normalize:
        x_arr = z_normalize_rows(x_arr)
        y_arr = z_normalize_rows(y_arr)

    if window is None:
        window_int = max(x_arr.shape[1], y_arr.shape[1])
    elif window < 0:
        raise ValueError("window must be None or a non-negative integer.")
    else:
        window_int = int(window)

    if mode == "dependent":
        return _dtw_distance_numba(x_arr, y_arr, window_int)

    if mode == "independent":
        total = 0.0
        for feature in range(x_arr.shape[0]):
            total += _dtw_distance_numba(
                x_arr[feature : feature + 1, :],
                y_arr[feature : feature + 1, :],
                window_int,
            )
        return float(total)

    raise ValueError('mode must be either "dependent" or "independent".')
