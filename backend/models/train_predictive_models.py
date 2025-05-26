#!/usr/bin/env python3
"""
train_predictive_models.py

Three‐tiered, CV‐tuned LightGBM pipeline with enriched features.
"""

import logging
from pathlib import Path
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import RandomizedSearchCV, TimeSeriesSplit

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
def load_and_filter(path: Path, min_years: int = 4) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=['ds'])
    df['active'] = df['funding'] > 0
    counts = df.groupby('organization_id')['active'].sum()
    keep = counts[counts >= min_years].index
    df = df[df['organization_id'].isin(keep)].copy()
    df.sort_values(['organization_id','ds'], inplace=True)
    df.drop(columns=['active'], inplace=True)
    logger.info(f"Loaded {len(df)} rows for {df['organization_id'].nunique()} orgs")
    return df

def engineer_features(
    df: pd.DataFrame,
    lag_years=(1,2,3),
    roll_windows=(3,)
) -> pd.DataFrame:
    # categoricals
    for c in ('country','activity_type','sme'):
        df[c] = df[c].fillna('missing').astype('category')
    df.sort_values(['organization_id','ds'], inplace=True)

    # funding lags & rolls
    for lag in lag_years:
        df[f'fund_lag{lag}'] = df.groupby('organization_id')['funding'].shift(lag)
    for w in roll_windows:
        df[f'fund_roll{w}_mean'] = (
            df.groupby('organization_id')['funding']
              .rolling(w).mean().shift(1)
              .reset_index(level=0,drop=True)
        )

    # deliverables + publications
    for lag in (1,):
        df[f'deliv_lag{lag}'] = df.groupby('organization_id')['deliverable_count'].shift(lag)
        df[f'pub_lag{lag}']   = df.groupby('organization_id')['publication_count'].shift(lag)

    # cyclic year encoding
    df['year'] = df['ds'].dt.year
    period = df['year'].max() - df['year'].min() + 1
    df['year_sin'] = np.sin(2*np.pi*(df['year'] - df['year'].min())/period)
    df['year_cos'] = np.cos(2*np.pi*(df['year'] - df['year'].min())/period)

    # drop nas
    before = len(df)
    df.dropna(subset=['fund_lag1'], inplace=True)
    logger.info(f"Dropped {before - len(df)} rows missing lag1 → {len(df)} remain")
    return df

def split_tiers(df: pd.DataFrame, low_q=0.50, high_q=0.95) -> Dict[str,pd.DataFrame]:
    tot = df.groupby('organization_id')['funding'].sum()
    ql, qh = tot.quantile([low_q, high_q])
    tiers = {
        'low': df[df['organization_id'].isin(tot[tot<=ql].index)],
        'mid': df[df['organization_id'].isin(tot[(tot>ql)&(tot<=qh)].index)],
        'high':df[df['organization_id'].isin(tot[tot>qh].index)]
    }
    for t,sub in tiers.items():
        logger.info(f"Tier '{t}': {sub['organization_id'].nunique()} orgs, {len(sub)} rows")
    return tiers

def tune_and_train(
    df: pd.DataFrame,
    features: list,
    objective: str,
    param_dist: dict,
    n_trials: int = 20
) -> Tuple[lgb.LGBMRegressor, Dict[str,float]]:
    last = df['ds'].dt.year.max()
    train = df[df['ds'].dt.year < last]
    test  = df[df['ds'].dt.year == last]

    Xtr = train[features];  ytr = np.log1p(train['funding'])
    Xte = test[features];   yte = np.log1p(test['funding'])

    tscv = TimeSeriesSplit(n_splits=4)
    base = lgb.LGBMRegressor(objective=objective, random_state=42, verbose=-1)
    rs = RandomizedSearchCV(
        base, param_dist, n_iter=n_trials,
        cv=tscv, scoring='neg_root_mean_squared_error',
        random_state=42, n_jobs=-1, verbose=1
    )
    rs.fit(Xtr, ytr)
    best = rs.best_estimator_
    logger.info("Best params: %s", rs.best_params_)

    # final fit
    best.fit(Xtr, ytr)
    pred = best.predict(Xte)
    actual = np.expm1(yte);  pred_raw = np.expm1(pred)

    rmse = np.sqrt(mean_squared_error(actual,pred_raw))
    mae  = mean_absolute_error(actual,pred_raw)
    r2   = r2_score(actual,pred_raw)
    logger.info(f"{objective.upper():7s} → RMSE={rmse:,.0f}, MAE={mae:,.0f}, R2={r2:.4f}")

    return best, {'rmse':rmse,'mae':mae,'r2':r2}


def funding_summary(df):
    tot = df.groupby('organization_id')['funding'].sum()
    n_orgs = len(tot)
    ql, qh = tot.quantile([0.50, 0.95])
    low_ids  = tot[tot <= ql].index
    mid_ids  = tot[(tot > ql) & (tot <= qh)].index
    high_ids = tot[tot > qh].index

    def info(ids, name):
        rows = df[df['organization_id'].isin(ids)]
        total_fund = rows['funding'].sum()
        n = len(ids)
        logger.info(f"{name.upper():4s} tier: {n} orgs, {len(rows)} rows, total funding: {total_fund:,.0f}")
        return n, total_fund

    info(low_ids,  'low')
    info(mid_ids,  'mid')
    info(high_ids, 'high')
    logger.info(f"ALL ORGS: {n_orgs} organizations, total funding: {df['funding'].sum():,.0f}")



def main():
    data_path = Path(__file__).parents[2] / 'data' / 'aggregated' / 'panel_annual_full.csv'
    df = load_and_filter(data_path)
    df = engineer_features(df)
    
    funding_summary(df)

    # feature list
    topic_cols = [str(i) for i in range(27)]
    lag_feats  = [f'fund_lag{i}' for i in (1,2,3)]
    roll_feats = ['fund_roll3_mean']
    aux_feats  = ['deliv_lag1','pub_lag1','year_sin','year_cos','time_idx']
    features   = ['country','activity_type','sme'] + lag_feats + roll_feats + aux_feats + topic_cols

    tiers = split_tiers(df)
    results = {}

    # tuning grids
    grids = {
        'low': {'n_estimators':[200,400,600],'learning_rate':[0.05,0.1],'num_leaves':[31,50]},
        'mid': {'n_estimators':[400,600,800],'learning_rate':[0.03,0.05],'num_leaves':[50,70]},
        'high':{'n_estimators':[500,800],'learning_rate':[0.01,0.03],'num_leaves':[64,100]}
    }

    
    objectives = {'low':'regression','mid':'huber','high':'tweedie'}

    for tier in ('low','mid','high'):
        logger.info(f"--- Tier {tier.upper()} ---")
        obj = objectives[tier]
        model,metrics = tune_and_train(
            tiers[tier], features,
            objective=obj,
            param_dist=grids[tier],
            n_trials=20
        )
        results[tier] = metrics

    logger.info("Final metrics per tier:")
    for t,m in results.items():
        logger.info(f"{t}: RMSE={m['rmse']:,.0f}, MAE={m['mae']:,.0f}, R2={m['r2']:.4f}")

if __name__=="__main__":
    main()
