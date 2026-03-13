import os
import math
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any

import requests
import numpy as np


def must_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing env {name}")
    return v


def sb_url(table: str) -> str:
    base = must_env("SUPABASE_URL").rstrip("/")
    return f"{base}/rest/v1/{table}"


def sb_headers() -> Dict[str, str]:
    key = must_env("SUPABASE_SERVICE_ROLE_KEY")
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def sb_get(table: str, query: str) -> List[Dict[str, Any]]:
    url = f"{sb_url(table)}?{query}"
    r = requests.get(url, headers=sb_headers(), timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    return r.json()


def sb_insert(table: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    url = sb_url(table)
    r = requests.post(url, headers=sb_headers(), json=rows, timeout=60)
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"POST {url} -> {r.status_code} {r.text}")


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mae(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.mean(np.abs(y_true - y_pred)))


def standardize_fit(X: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    mu = X.mean(axis=0)
    sigma = X.std(axis=0)
    sigma = np.where(sigma == 0, 1.0, sigma)
    return mu, sigma


def standardize_apply(X: np.ndarray, mu: np.ndarray, sigma: np.ndarray) -> np.ndarray:
    return (X - mu) / sigma


def add_bias(X: np.ndarray) -> np.ndarray:
    return np.hstack([np.ones((X.shape[0], 1)), X])


def linreg_fit(X: np.ndarray, y: np.ndarray, l2: float = 0.0) -> np.ndarray:
    # Closed-form ridge: (X^T X + l2 I)^-1 X^T y
    # X is already with bias column included
    XT = X.T
    A = XT @ X
    if l2 > 0:
        I = np.eye(A.shape[0])
        I[0, 0] = 0.0  # don't regularize bias
        A = A + l2 * I
    w = np.linalg.solve(A, XT @ y)
    return w


def lasso_fit_coordinate_descent(X: np.ndarray, y: np.ndarray, alpha: float, iters: int = 2000) -> np.ndarray:
    # Simple coordinate descent on standardized features, with bias (not penalized)
    # X must include bias in col0, other cols standardized.
    n, p = X.shape
    w = np.zeros(p)
    # Initialize bias
    w[0] = y.mean()

    # Precompute
    X_col_norm2 = (X ** 2).sum(axis=0)

    def soft_threshold(rho: float, lam: float) -> float:
        if rho < -lam:
            return rho + lam
        if rho > lam:
            return rho - lam
        return 0.0

    for _ in range(iters):
        for j in range(p):
            # residual without feature j
            y_pred = X @ w
            r = y - y_pred + w[j] * X[:, j]
            rho = (X[:, j] * r).sum()

            if j == 0:
                # bias: no penalty
                w[j] = rho / (X_col_norm2[j] if X_col_norm2[j] != 0 else 1.0)
            else:
                w[j] = soft_threshold(rho, alpha) / (X_col_norm2[j] if X_col_norm2[j] != 0 else 1.0)

    return w


def build_supervised(rows: List[Dict[str, Any]], horizon_min: int = 1):
    # rows sorted ascending by ts_min for an asset
    # X at time t -> y at t+h
    # features: [v, i, t, soc] at t
    # target: voltage at t+h
    rows = [r for r in rows if r.get("voltage_v_avg") is not None]
    if len(rows) < (horizon_min + 5):
        return None

    v = np.array([float(r["voltage_v_avg"]) for r in rows], dtype=float)
    i = np.array([float(r["current_a_avg"]) for r in rows], dtype=float)
    t = np.array([float(r["temperature_c_avg"]) for r in rows], dtype=float)
    soc = np.array([float(r["soc_pct_avg"]) for r in rows], dtype=float)
    ts = [r["ts_min"] for r in rows]

    X = np.stack([v, i, t, soc], axis=1)
    y = v[horizon_min:]
    X = X[:-horizon_min, :]
    ts_X = ts[:-horizon_min]
    ts_y = ts[horizon_min:]

    return X, y, ts_X, ts_y


def time_split(n: int, train_frac=0.70, val_frac=0.15):
    n_train = int(math.floor(n * train_frac))
    n_val = int(math.floor(n * val_frac))
    n_test = n - n_train - n_val
    if n_test <= 0:
        n_test = 1
        if n_val > 1:
            n_val -= 1
        else:
            n_train -= 1
    return n_train, n_val, n_test


def main():
    horizon = int(os.environ.get("MODEL_HORIZON_MIN", "1"))
    limit_per_asset = int(os.environ.get("MODEL_LIMIT_PER_ASSET", "20000"))

    # discover assets from calibration_baseline (same approach as UI)
    assets = sb_get("calibration_baseline", "select=asset_id&order=asset_id.asc&limit=20000")
    asset_ids = sorted({str(a["asset_id"]).strip() for a in assets if str(a.get("asset_id", "")).strip()})

    if not asset_ids:
        raise RuntimeError("No assets found in calibration_baseline")

    out_rows = []
    now = datetime.now(timezone.utc).isoformat()

    for asset_id in asset_ids:
        q = (
            "select=asset_id,ts_min,voltage_v_avg,current_a_avg,temperature_c_avg,soc_pct_avg"
            f"&asset_id=eq.{requests.utils.quote(asset_id)}"
            "&order=ts_min.asc"
            f"&limit={limit_per_asset}"
        )
        rows = sb_get("telemetry_1m", q)
        sup = build_supervised(rows, horizon_min=horizon)
        if sup is None:
            continue

        X, y, ts_X, ts_y = sup
        n = X.shape[0]
        n_train, n_val, n_test = time_split(n)

        X_train = X[:n_train]
        y_train = y[:n_train]
        X_val = X[n_train : n_train + n_val]
        y_val = y[n_train : n_train + n_val]
        X_test = X[n_train + n_val :]
        y_test = y[n_train + n_val :]

        # Standardize (fit on train only)
        mu, sigma = standardize_fit(X_train)
        X_train_s = standardize_apply(X_train, mu, sigma)
        X_val_s = standardize_apply(X_val, mu, sigma)
        X_test_s = standardize_apply(X_test, mu, sigma)

        # Add bias
        Xb_train = add_bias(X_train_s)
        Xb_val = add_bias(X_val_s)
        Xb_test = add_bias(X_test_s)

        # 1) LinReg
        w_lin = linreg_fit(Xb_train, y_train, l2=0.0)
        yhat_lin = Xb_test @ w_lin

        # 2) Ridge (choose small l2)
        w_ridge = linreg_fit(Xb_train, y_train, l2=1.0)
        yhat_ridge = Xb_test @ w_ridge

        # 3) Lasso (alpha)
        w_lasso = lasso_fit_coordinate_descent(Xb_train, y_train, alpha=0.01, iters=800)
        yhat_lasso = Xb_test @ w_lasso

        metrics = [
            ("linreg", yhat_lin),
            ("ridge", yhat_ridge),
            ("lasso", yhat_lasso),
        ]

        for model_name, yhat in metrics:
            out_rows.append(
                {
                    "asset_id": asset_id,
                    "model_name": model_name,
                    "target": "voltage_v_avg",
                    "horizon_min": horizon,
                    "rmse": rmse(y_test, yhat),
                    "mae": mae(y_test, yhat),
                    "n_train": int(n_train),
                    "n_val": int(n_val),
                    "n_test": int(n_test),
                    "train_start_ts": ts_y[0] if ts_y else None,
                    "train_end_ts": ts_y[n_train - 1] if n_train > 0 else None,
                    "test_start_ts": ts_y[n_train + n_val] if (n_train + n_val) < len(ts_y) else None,
                    "test_end_ts": ts_y[-1] if ts_y else None,
                    "trained_at": now,
                }
            )

        print(f"[models] asset={asset_id} n={n} train={n_train} val={n_val} test={n_test}")

    # insert metrics
    if out_rows:
        sb_insert("model_metrics", out_rows)
        print(f"[models] inserted {len(out_rows)} rows into model_metrics")
    else:
        print("[models] no rows inserted (not enough data?)")


if __name__ == "__main__":
    main()