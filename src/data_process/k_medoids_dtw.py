"""K-medoids clustering with Dynamic Time Warping distance.

This is a DTW-compatible replacement for the Euclidean ``sklearn.KMeans`` step.
It uses medoids instead of arithmetic centroids, which makes the cluster center
an actual observed load profile. This is a robust choice when the distance is
DTW and no Euclidean mean-centroid objective is available.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import numpy as np

from src.data_process.time_series_distances import dtw_distance


DistanceCache = Dict[Tuple[int, int], float]


@dataclass(slots=True)
class KMedoidsDTW:
    """Cluster equal-length time series using K-medoids and DTW.

    Parameters
    ----------
    n_clusters:
        Number of clusters.
    max_iter:
        Maximum number of assignment/update iterations.
    window:
        Optional Sakoe-Chiba window for DTW. For 24-hour curves, values like
        2, 3, or 4 usually restrict unrealistic alignments while allowing shifts.
    normalize:
        If ``True``, each time series is z-normalized before DTW. Use this when
        the shape matters more than the absolute consumption level.
    n_init:
        Number of random initializations. The best solution by DTW inertia is kept.
    random_state:
        Seed for reproducibility.
    verbose:
        Print progress messages.
    """

    n_clusters: int
    max_iter: int = 30
    window: Optional[int] = None
    normalize: bool = False
    n_init: int = 5
    random_state: int = 42
    verbose: bool = False

    labels_: Optional[np.ndarray] = None
    medoid_indices_: Optional[np.ndarray] = None
    cluster_centers_: Optional[np.ndarray] = None
    inertia_: Optional[float] = None
    n_iter_: int = 0

    def fit(self, X: np.ndarray) -> "KMedoidsDTW":
        """Fit the model on a 2-D array ``(n_samples, n_timesteps)``."""
        X_arr = self._validate_X(X)
        n_samples = X_arr.shape[0]
        rng = np.random.default_rng(self.random_state)

        best_inertia = np.inf
        best_labels: Optional[np.ndarray] = None
        best_medoids: Optional[np.ndarray] = None
        best_n_iter = 0

        for init_idx in range(self.n_init):
            medoid_indices = rng.choice(n_samples, size=self.n_clusters, replace=False)
            cache: DistanceCache = {}

            if self.verbose:
                print(f"DTW K-medoids init {init_idx + 1}/{self.n_init}: {medoid_indices}")

            for iteration in range(1, self.max_iter + 1):
                labels, inertia = self._assign_labels(X_arr, medoid_indices, cache)
                new_medoids = self._update_medoids(X_arr, labels, medoid_indices, cache)

                if np.array_equal(np.sort(new_medoids), np.sort(medoid_indices)):
                    medoid_indices = new_medoids
                    break

                medoid_indices = new_medoids

            labels, inertia = self._assign_labels(X_arr, medoid_indices, cache)

            if self.verbose:
                print(f"  inertia={inertia:.6f}, iter={iteration}")

            if inertia < best_inertia:
                best_inertia = inertia
                best_labels = labels.copy()
                best_medoids = medoid_indices.copy()
                best_n_iter = iteration

        if best_labels is None or best_medoids is None:
            raise RuntimeError("KMedoidsDTW did not produce a valid clustering.")

        self.labels_ = best_labels
        self.medoid_indices_ = best_medoids
        self.cluster_centers_ = X_arr[best_medoids]
        self.inertia_ = float(best_inertia)
        self.n_iter_ = best_n_iter
        return self

    def fit_predict(self, X: np.ndarray) -> np.ndarray:
        """Fit the model and return cluster labels."""
        self.fit(X)
        if self.labels_ is None:  # defensive, fit() already validates this
            raise RuntimeError("Model has no labels after fit.")
        return self.labels_

    def _validate_X(self, X: np.ndarray) -> np.ndarray:
        X_arr = np.asarray(X, dtype=float)
        if X_arr.ndim != 2:
            raise ValueError(
                "KMedoidsDTW expects a 2-D matrix with shape "
                "(n_samples, n_timesteps)."
            )
        if X_arr.shape[0] < self.n_clusters:
            raise ValueError(
                f"n_samples={X_arr.shape[0]} must be >= n_clusters={self.n_clusters}."
            )
        if self.n_clusters < 2:
            raise ValueError("n_clusters must be >= 2.")
        if self.max_iter < 1:
            raise ValueError("max_iter must be >= 1.")
        if self.n_init < 1:
            raise ValueError("n_init must be >= 1.")
        if not np.isfinite(X_arr).all():
            raise ValueError("Input data contains NaN or infinite values.")
        return X_arr

    def _distance(self, X: np.ndarray, i: int, j: int, cache: DistanceCache) -> float:
        if i == j:
            return 0.0
        key = (i, j) if i < j else (j, i)
        if key not in cache:
            cache[key] = dtw_distance(
                X[i],
                X[j],
                window=self.window,
                normalize=self.normalize,
            )
        return cache[key]

    def _assign_labels(
        self,
        X: np.ndarray,
        medoid_indices: np.ndarray,
        cache: DistanceCache,
    ) -> tuple[np.ndarray, float]:
        n_samples = X.shape[0]
        labels = np.empty(n_samples, dtype=int)
        inertia = 0.0

        for i in range(n_samples):
            distances = np.array(
                [self._distance(X, i, int(medoid), cache) for medoid in medoid_indices],
                dtype=float,
            )
            label = int(np.argmin(distances))
            labels[i] = label
            inertia += float(distances[label])

        return labels, float(inertia)

    def _update_medoids(
        self,
        X: np.ndarray,
        labels: np.ndarray,
        old_medoids: np.ndarray,
        cache: DistanceCache,
    ) -> np.ndarray:
        new_medoids = old_medoids.copy()
        all_indices = np.arange(X.shape[0])

        for cluster_id in range(self.n_clusters):
            members = all_indices[labels == cluster_id]

            if len(members) == 0:
                # Re-seed empty cluster with the point that is farthest from its
                # current assigned medoid. This keeps the algorithm moving.
                distances_to_current = np.array(
                    [self._distance(X, i, int(old_medoids[labels[i]]), cache) for i in all_indices]
                )
                new_medoids[cluster_id] = int(np.argmax(distances_to_current))
                continue

            best_candidate = int(members[0])
            best_cost = np.inf
            for candidate in members:
                cost = 0.0
                for other in members:
                    cost += self._distance(X, int(candidate), int(other), cache)
                if cost < best_cost:
                    best_cost = cost
                    best_candidate = int(candidate)

            new_medoids[cluster_id] = best_candidate

        return new_medoids
