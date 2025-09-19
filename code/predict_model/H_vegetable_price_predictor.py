#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高波動蔬菜價格預測器
===================

功能說明：
1. 使用預訓練的XGBoost模型預測高波動蔬菜未來7天價格
2. 從資料庫自動獲取歷史資料
3. 支援批量日期範圍預測
4. 支援多線程並行處理提高效率
5. 自動更新實際價格和MAPE計算
6. 將預測結果存入price_predictions表
"""

# 引入必要的函式庫
import pickle  # 模型序列化
import numpy as np  # 數值計算
import pandas as pd  # 資料處理
from datetime import datetime, timedelta, date  # 日期時間處理
import argparse  # 命令列參數解析
import logging  # 日誌記錄
import os  # 作業系統相關功能
from sqlalchemy import create_engine, text  # 資料庫連接
from typing import List, Dict, Optional  # 型別提示
from concurrent.futures import ThreadPoolExecutor, as_completed  # 多線程處理
import threading  # 線程控制
import time  # 時間相關功能

# ============== 日期設定區 ==============
# 配置預測執行的日期範圍
# predict_date欄位表示執行預測的日期，target_date表示預測目標日期
#
# 批量補齊歷史預測資料範例：
# PREDICT_START_DATE = "2025-08-03"  # 開始日期 (YYYY-MM-DD)
# PREDICT_END_DATE = "2025-08-28"    # 結束日期 (YYYY-MM-DD)
#
# 補齊單一日期範例：
# PREDICT_START_DATE = "2025-08-25"  # 指定日期 (YYYY-MM-DD)
# PREDICT_END_DATE = "2025-08-25"    # 同一日期 (YYYY-MM-DD)

# 預設值：使用今天的日期執行預測
PREDICT_START_DATE = None
PREDICT_END_DATE = None

# ============== 執行參數設定區 ==============

# 多線程處理設定
USE_MULTITHREADING = True          # 是否啟用多線程並行處理 (True/False)
MAX_WORKERS = 10                   # 最大線程數 (建議為CPU核心數-2)

# 蔬菜選擇設定
VEGE_IDS = None                    # 指定要預測的蔬菜ID列表，例: [1, 2, 3]
                                   # None 表示處理所有可用的高波動蔬菜

# 歷史資料設定
HISTORICAL_DAYS = 180              # 使用多少天的歷史資料來產生預測特徵

# 資料庫連接設定
DB_CONNECTION = 'postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres'

# 模型檔案路徑設定
MODEL_PATH = None                  # 自訂模型路徑，None表示使用預設路徑
# =======================================

# 預測模型使用的特徵欄位列表
# 這些特徵是從原始訓練程式碼中提取的最佳特徵組合
SELECTED_FEATURES = [
    "y_above_ma30", "y_above_ma7", "y_lag_1", "y_ma_7", "y_ma_14",
    "y_ma_30", "y_lag_3", "y_lag_7", "day_sin", "y_lag_14",
    "dayofyear", "y_lag_30", "dayofweek", "day_cos", "Temperature_ma_30",
    "y_volatility_14", "StnPres_std_30", "y_volatility_7", "y_change_1",
    "Temperature_ma_14", "Precp_ma_30", "Temperature", "StnPres",
    "Temperature_delta1",
]

# 天氣相關的欄位列表
WEATHER_COLS = ["StnPres", "Temperature", "RH", "WS", "Precp", "typhoon"]

# 設定日誌系統，記錄程式執行過程
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_time_features(df):
    """添加時間特徵"""
    df = df.copy()
    ds = df["ds"]
    df["dayofweek"] = ds.dt.dayofweek
    df["dayofyear"] = ds.dt.dayofyear
    df["day_sin"] = np.sin(2 * np.pi * df["dayofyear"] / 365.0)
    df["day_cos"] = np.cos(2 * np.pi * df["dayofyear"] / 365.0)
    return df

def add_weather_features(df):
    """添加天氣特徵 - 處理缺失資料"""
    df = df.copy()
    windows = [3, 7, 14, 30]
    for col in WEATHER_COLS:
        if col not in df.columns:
            continue
        
        df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # 前向填充天氣資料的缺失值（天氣通常變化較緩慢）
        df[col] = df[col].ffill()
        df[col] = df[col].bfill()
        
        base = df[col].shift(1)
        
        # 調整最小期數以適應缺失資料
        for w in windows:
            min_periods = max(1, w // 3)
            df[f"{col}_ma_{w}"] = base.rolling(w, min_periods=min_periods).mean()
            df[f"{col}_std_{w}"] = base.rolling(w, min_periods=min_periods).std()
        
        # 長期統計，更靈活的最小期數
        roll_mean = base.rolling(30, min_periods=5).mean()
        roll_std = base.rolling(30, min_periods=5).std()
        df[f"{col}_delta1"] = base.diff(1)
        df[f"{col}_z30"] = (base - roll_mean) / (roll_std.replace(0, np.nan))
        df[f"{col}_z30"] = df[f"{col}_z30"].fillna(0)
    return df

def add_price_features(df):
    """添加價格特徵 - 處理缺失資料"""
    df = df.copy().sort_values("ds").reset_index(drop=True)
    
    # 添加滯後特徵，使用前向填充處理缺失值
    for lag in [1, 3, 7, 14, 30]:
        df[f"y_lag_{lag}"] = df["y"].shift(lag)
    
    # 添加移動平均，調整最小期數以適應缺失資料
    for w in [7, 14, 30]:
        min_periods = max(1, w // 3)  # 最小期數設為窗口的1/3
        df[f"y_ma_{w}"] = df["y"].shift(1).rolling(w, min_periods=min_periods).mean()
    
    # 價格變化
    df["y_change_1"] = df["y"].shift(1) - df["y"].shift(2)
    
    # 波動率，調整最小期數
    df["y_volatility_7"] = df["y"].shift(1).rolling(7, min_periods=3).std()
    df["y_volatility_14"] = df["y"].shift(1).rolling(14, min_periods=5).std()
    
    # 比較特徵，處理NaN值
    df["y_above_ma7"] = (df["y"] > df["y_ma_7"]).astype(int)
    df["y_above_ma30"] = (df["y"] > df["y_ma_30"]).astype(int)
    
    # 填充NaN值
    df["y_above_ma7"] = df["y_above_ma7"].fillna(0)
    df["y_above_ma30"] = df["y_above_ma30"].fillna(0)
    
    return df

def build_features(df):
    """建立所有特徵"""
    df = add_time_features(df)
    if any(col in df.columns for col in WEATHER_COLS):
        df = add_weather_features(df)
    df = add_price_features(df)
    return df

class DatabaseVegetablePricePredictor:
    """從資料庫獲取資料並存入預測結果的蔬菜價格預測器"""
    
    def __init__(self, model_path: str, db_connection_string: str):
        """
        初始化預測器
        
        參數:
        model_path: 模型檔案路徑
        db_connection_string: 資料庫連接字串
        """
        # 載入模型
        with open(model_path, 'rb') as f:
            self.data = pickle.load(f)
        self.models = self.data['models']
        logger.info(f"已載入 {len(self.models)} 個蔬菜模型")
        
        # 建立資料庫連接
        self.engine = create_engine(db_connection_string)
        logger.info("資料庫連接已建立")
    
    def get_available_vegetables(self) -> List[int]:
        """從資料庫獲取可用的蔬菜ID"""
        try:
            # 查詢有歷史資料的蔬菜
            query = """
            SELECT DISTINCT vege_id 
            FROM public.high_volatility_merged 
            WHERE avg_price_per_kg IS NOT NULL 
            ORDER BY vege_id
            """
            with self.engine.connect() as conn:
                result = pd.read_sql(text(query), conn)
            available_in_db = set(result['vege_id'].tolist())
            
            # 篩選出模型中也有的蔬菜
            model_vege_ids = set(int(vid) for vid in self.models.keys())
            common_vegetables = sorted(list(available_in_db & model_vege_ids))
            
            logger.info(f"資料庫中有 {len(available_in_db)} 個蔬菜，模型中有 {len(model_vege_ids)} 個，共同有 {len(common_vegetables)} 個")
            return common_vegetables
            
        except Exception as e:
            logger.error(f"獲取可用蔬菜失敗: {e}")
            return []
    
    def get_historical_data(self, vege_id: int, days: int = 180, cutoff_date: date = None) -> pd.DataFrame:
        """
        從資料庫獲取指定蔬菜的歷史資料
        
        參數:
        vege_id: 蔬菜ID
        days: 獲取最近多少天的資料
        cutoff_date: 歷史資料截止日期，防止資料洩漏
        
        返回:
        歷史資料DataFrame
        """
        try:
            # 修改查詢策略：先獲取最新的資料，然後往前取指定天數
            # 加入日期過濾以防止資料洩漏
            date_filter = ""
            if cutoff_date:
                date_filter = f"AND \"ObsTime\" < '{cutoff_date}'"
            
            query = f"""
            SELECT "ObsTime", avg_price_per_kg, vege_id, "StnPres", "Temperature", 
                   "RH", "WS", "Precp", typhoon
            FROM public.high_volatility_merged 
            WHERE vege_id = {vege_id}
                AND avg_price_per_kg IS NOT NULL
                {date_filter}
            ORDER BY "ObsTime" DESC
            LIMIT {days * 2}
            """
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            # 如果獲得的資料還是不夠，嘗試獲取更多歷史資料
            if len(df) < 60:
                logger.info(f"蔬菜 {vege_id} 最近資料不足，嘗試獲取更多歷史資料")
                query_extended = f"""
                SELECT "ObsTime", avg_price_per_kg, vege_id, "StnPres", "Temperature", 
                       "RH", "WS", "Precp", typhoon
                FROM public.high_volatility_merged 
                WHERE vege_id = {vege_id}
                    AND avg_price_per_kg IS NOT NULL
                    {date_filter}
                ORDER BY "ObsTime" DESC
                LIMIT 500
                """
                with self.engine.connect() as conn:
                    df = pd.read_sql(text(query_extended), conn)
            
            # 重新排序為時間順序
            if len(df) > 0:
                df = df.sort_values('ObsTime').reset_index(drop=True)
            
            if len(df) == 0:
                logger.warning(f"蔬菜 {vege_id} 沒有找到歷史資料")
                return pd.DataFrame()
            
            # 準備資料格式
            df['ds'] = pd.to_datetime(df['ObsTime'])
            df['y'] = df['avg_price_per_kg']
            
            logger.info(f"蔬菜 {vege_id} 獲取到 {len(df)} 筆歷史資料 ({df['ds'].min().date()} ~ {df['ds'].max().date()})")
            return df
            
        except Exception as e:
            logger.error(f"獲取蔬菜 {vege_id} 歷史資料失敗: {e}")
            return pd.DataFrame()
    
    def predict_next_7_days(self, vege_id: int, historical_data: pd.DataFrame, 
                           predict_date: date = None) -> pd.DataFrame:
        """
        預測未來7天價格
        
        參數:
        vege_id: 蔬菜ID
        historical_data: 歷史資料
        predict_date: 預測基準日期 (預設為今天)
        
        返回:
        預測結果DataFrame
        """
        if str(vege_id) not in self.models:
            logger.error(f"找不到蔬菜 {vege_id} 的模型")
            return pd.DataFrame()
        
        # 調整最低資料需求，考慮市場休息日
        min_required_days = 20  # 降低最低需求，因為可能有休息日
        if len(historical_data) < min_required_days:
            logger.warning(f"蔬菜 {vege_id} 歷史資料不足 ({len(historical_data)} < {min_required_days})")
            return pd.DataFrame()
        
        model_info = self.models[str(vege_id)]
        model = model_info['model']
        feature_names = model_info['feature_names']
        
        if predict_date is None:
            # 優先使用全域設定的日期，如果沒有設定則使用今天
            # 注意：這裡只處理單一日期的情況，批量處理在上層函數處理
            if PREDICT_START_DATE is not None:
                predict_date = pd.to_datetime(PREDICT_START_DATE).date()
            else:
                predict_date = date.today()
        elif isinstance(predict_date, str):
            predict_date = pd.to_datetime(predict_date).date()
        
        df = historical_data.copy()
        # 使用昨天作為預測基準日期，這樣預測的第一天就是今天
        # 因為歷史資料只到昨天，以昨天為基準進行預測更符合邏輯
        yesterday = predict_date - timedelta(days=1)
        last_date = pd.to_datetime(yesterday)
        
        predictions = []
        work_df = df.copy()
        
        try:
            for i in range(1, 8):
                target_date = last_date + timedelta(days=i)
                
                # 從最近資料點取得天氣資料作為預測期間的天氣
                if len(work_df) > 0:
                    last_weather = work_df.iloc[-1]
                    weather_data = {col: last_weather.get(col, 0) for col in WEATHER_COLS if col in work_df.columns}
                else:
                    weather_data = {}
                
                # 添加新行
                new_row = {col: np.nan for col in work_df.columns}
                new_row['ds'] = target_date
                new_row['vege_id'] = vege_id
                for col, val in weather_data.items():
                    new_row[col] = val
                
                work_df = pd.concat([work_df, pd.DataFrame([new_row])], ignore_index=True)
                work_df = work_df.sort_values('ds').reset_index(drop=True)
                
                # 建立特徵
                feat_df = build_features(work_df.copy())
                available_features = [f for f in feature_names if f in feat_df.columns]
                
                # 降低特徵數量要求，因為部分特徵可能因資料缺失而無法計算
                min_features = max(5, len(feature_names) // 2)  # 至少5個特徵或原特徵的一半
                if len(available_features) < min_features:
                    logger.warning(f"蔬菜 {vege_id} 可用特徵過少 ({len(available_features)} < {min_features})")
                    # 不立即中斷，嘗試用現有特徵繼續
                
                X = feat_df.iloc[[-1]][available_features].fillna(0.0)
                
                # 額外檢查：如果所有特徵都是0或NaN，跳過這一天
                if X.isna().all().all() or (X == 0).all().all():
                    logger.warning(f"蔬菜 {vege_id} 第{i}天特徵全為空，跳過")
                    continue
                
                # 預測
                prediction = model.predict(X)[0]
                prediction = max(0.01, float(prediction))
                
                # 更新工作資料框的y值供下次預測使用
                work_df.iloc[-1, work_df.columns.get_loc('y')] = prediction
                
                predictions.append({
                    'vege_id': int(vege_id),
                    'predict_date': predict_date,
                    'target_date': target_date.date(),
                    'predict_price': round(prediction, 2),
                    'actual_price': None,
                    'mape': None
                })
            
            logger.info(f"蔬菜 {vege_id} 成功預測 {len(predictions)} 天")
            return pd.DataFrame(predictions)
            
        except Exception as e:
            logger.error(f"蔬菜 {vege_id} 預測過程發生錯誤: {e}")
            return pd.DataFrame()
    
    def save_predictions_to_db(self, predictions_df: pd.DataFrame) -> int:
        """
        將預測結果存入資料庫
        
        參數:
        predictions_df: 預測結果DataFrame
        
        返回:
        插入的記錄數
        """
        if len(predictions_df) == 0:
            logger.warning("沒有預測結果可以存入")
            return 0
        
        try:
            # 逐筆插入，避免參數綁定問題
            inserted_count = 0
            
            with self.engine.begin() as conn:
                for _, row in predictions_df.iterrows():
                    insert_sql = f"""
                    INSERT INTO public.price_predictions (vege_id, predict_date, target_date, predict_price, actual_price, mape)
                    VALUES ({row['vege_id']}, '{row['predict_date']}', '{row['target_date']}', {row['predict_price']}, NULL, NULL)
                    ON CONFLICT (vege_id, target_date) 
                    DO UPDATE SET
                        predict_date = EXCLUDED.predict_date,
                        predict_price = EXCLUDED.predict_price
                    """
                    
                    try:
                        conn.execute(text(insert_sql))
                        inserted_count += 1
                    except Exception as e:
                        logger.warning(f"插入單筆記錄失敗: {e}")
                        continue
            
            logger.info(f"成功存入 {inserted_count} 筆預測記錄到資料庫")
            return inserted_count
            
        except Exception as e:
            logger.error(f"存入預測結果失敗: {e}")
            return 0
    
    def update_actual_prices_and_mape(self, predict_dates: Optional[List[date]] = None) -> int:
        """
        依據 predict_date 的前三天範圍，更新預測記錄的實際價格與 MAPE。

        參數:
        predict_dates: 要檢查的 predict_date 清單；
                       若為 None，則以系統當天為基準，檢查 (今天-7) ~ (今天-1)。

        返回:
        更新的記錄數
        """
        try:
            updated_count = 0

            if predict_dates is None:
                # 維持相容行為：以執行當天回溯 7 日內未填 actual 的記錄
                query = """
                SELECT pp.id, pp.vege_id, pp.target_date, pp.predict_price
                FROM public.price_predictions pp
                WHERE pp.target_date >= CURRENT_DATE - INTERVAL '7 days'
                    AND pp.target_date < CURRENT_DATE
                    AND pp.actual_price IS NULL
                """
                with self.engine.connect() as conn:
                    predictions_to_update = pd.read_sql(text(query), conn)

                for _, row in predictions_to_update.iterrows():
                    actual_query = """
                    SELECT avg_price_per_kg
                    FROM public.daily_avg_price
                    WHERE vege_id = :vege_id AND "ObsTime"::date = :target_date
                    """
                    with self.engine.connect() as conn:
                        actual_result = pd.read_sql(text(actual_query), conn, params={
                        'vege_id': int(row['vege_id']),
                        'target_date': row['target_date']
                        })

                    if len(actual_result) > 0 and not pd.isna(actual_result.iloc[0]['avg_price_per_kg']):
                        actual_price = float(actual_result.iloc[0]['avg_price_per_kg'])
                        predict_price = float(row['predict_price'])
                        mape = abs((actual_price - predict_price) / actual_price) * 100

                        update_sql = """
                        UPDATE public.price_predictions
                        SET actual_price = :actual_price, mape = :mape
                        WHERE id = :record_id
                        """
                        with self.engine.begin() as conn:
                            conn.execute(text(update_sql), {
                                'actual_price': actual_price,
                                'mape': round(mape, 2),
                                'record_id': int(row['id'])
                            })
                        updated_count += 1
                        logger.info(f"更新蔬菜 {row['vege_id']} 日期 {row['target_date']} 的實際價格: {actual_price:.2f}, MAPE: {mape:.2f}%")
            else:
                # 依每個 predict_date 的前三天 (d-3 ~ d-1) 進行更新
                # 去重確保不重複查詢
                unique_dates = sorted(set(pd.to_datetime(d).date() if isinstance(d, str) else d for d in predict_dates))

                for pd_date in unique_dates:
                    start_date = pd_date - timedelta(days=3)
                    end_date = pd_date - timedelta(days=1)

                    query = text(
                        """
                        SELECT pp.id, pp.vege_id, pp.target_date, pp.predict_price
                        FROM public.price_predictions pp
                        WHERE pp.target_date >= :start_date
                          AND pp.target_date <= :end_date
                          AND pp.actual_price IS NULL
                        """
                    )
                    with self.engine.connect() as conn:
                        predictions_to_update = pd.read_sql(query, conn, params={
                        'start_date': start_date,
                        'end_date': end_date,
                        })

                    if len(predictions_to_update) == 0:
                        logger.info(f"predict_date={pd_date} 前三天無需更新的記錄")
                        continue

                    for _, row in predictions_to_update.iterrows():
                        actual_query = """
                        SELECT avg_price_per_kg
                        FROM public.daily_avg_price
                        WHERE vege_id = :vege_id AND "ObsTime"::date = :target_date
                        """
                        with self.engine.connect() as conn:
                            actual_result = pd.read_sql(text(actual_query), conn, params={
                            'vege_id': int(row['vege_id']),
                            'target_date': row['target_date']
                            })

                        if len(actual_result) > 0 and not pd.isna(actual_result.iloc[0]['avg_price_per_kg']):
                            actual_price = float(actual_result.iloc[0]['avg_price_per_kg'])
                            predict_price = float(row['predict_price'])
                            mape = abs((actual_price - predict_price) / actual_price) * 100

                            update_sql = """
                            UPDATE public.price_predictions
                            SET actual_price = :actual_price, mape = :mape
                            WHERE id = :record_id
                            """
                            with self.engine.begin() as conn:
                                conn.execute(text(update_sql), {
                                    'actual_price': actual_price,
                                    'mape': round(mape, 2),
                                    'record_id': int(row['id'])
                                })
                            updated_count += 1
                            logger.info(f"[predict_date={pd_date}] 更新蔬菜 {row['vege_id']} 日期 {row['target_date']} 的實際價格: {actual_price:.2f}, MAPE: {mape:.2f}%")

            logger.info(f"共更新 {updated_count} 筆實際價格記錄")
            return updated_count

        except Exception as e:
            logger.error(f"更新實際價格失敗: {e}")
            return 0
    
    def predict_single_date(self, predict_date: date, vege_ids: List[int], 
                           historical_days: int) -> Dict:
        """
        為單一日期的所有指定蔬菜執行預測
        
        參數:
        predict_date: 預測日期
        vege_ids: 蔬菜ID列表
        historical_days: 使用多少天的歷史資料
        
        返回:
        單一日期的執行結果
        """
        thread_id = threading.current_thread().ident
        logger.info(f"[線程 {thread_id}] 開始處理預測日期: {predict_date}")
        
        date_results = {
            'date': predict_date,
            'successful_predictions': 0,
            'failed_predictions': 0,
            'total_records_saved': 0,
            'thread_id': thread_id
        }
        
        for vege_id in vege_ids:
            try:
                # 獲取歷史資料（加入日期截止過濾以防資料洩漏）
                yesterday = predict_date - timedelta(days=1)
                historical_data = self.get_historical_data(vege_id, historical_days, yesterday)
                
                if len(historical_data) < 30:
                    logger.debug(f"[線程 {thread_id}] 蔬菜 {vege_id} 歷史資料不足，跳過")
                    date_results['failed_predictions'] += 1
                    continue
                
                # 執行預測
                predictions = self.predict_next_7_days(vege_id, historical_data, predict_date)
                
                if len(predictions) > 0:
                    # 存入資料庫
                    saved_count = self.save_predictions_to_db(predictions)
                    date_results['successful_predictions'] += 1
                    date_results['total_records_saved'] += saved_count
                else:
                    date_results['failed_predictions'] += 1
                    
            except Exception as e:
                logger.error(f"[線程 {thread_id}] 蔬菜 {vege_id} 預測失敗: {e}")
                date_results['failed_predictions'] += 1
        
        logger.info(f"[線程 {thread_id}] 完成日期 {predict_date}: 成功 {date_results['successful_predictions']}, 失敗 {date_results['failed_predictions']}")
        return date_results
    
    def predict_single_vegetable(self, vege_id: int, predict_date: date, 
                               historical_days: int) -> Dict:
        """
        為單一蔬菜執行預測（用於多線程按蔬菜並行）
        
        參數:
        vege_id: 蔬菜ID
        predict_date: 預測日期
        historical_days: 使用多少天的歷史資料
        
        返回:
        單一蔬菜的執行結果
        """
        thread_id = threading.current_thread().ident
        logger.debug(f"[線程 {thread_id}] 開始處理蔬菜 {vege_id}, 日期: {predict_date}")
        
        vege_results = {
            'vege_id': vege_id,
            'predict_date': predict_date,
            'successful': False,
            'records_saved': 0,
            'thread_id': thread_id,
            'error': None
        }
        
        try:
            # 獲取歷史資料（加入日期截止過濾以防資料洩漏）
            yesterday = predict_date - timedelta(days=1)
            historical_data = self.get_historical_data(vege_id, historical_days, yesterday)
            
            if len(historical_data) < 30:
                logger.debug(f"[線程 {thread_id}] 蔬菜 {vege_id} 歷史資料不足，跳過")
                vege_results['error'] = '歷史資料不足'
                return vege_results
            
            # 執行預測
            predictions = self.predict_next_7_days(vege_id, historical_data, predict_date)
            
            if len(predictions) > 0:
                # 存入資料庫
                saved_count = self.save_predictions_to_db(predictions)
                vege_results['successful'] = True
                vege_results['records_saved'] = saved_count
                logger.debug(f"[線程 {thread_id}] 蔬菜 {vege_id} 預測成功，存入 {saved_count} 筆記錄")
            else:
                vege_results['error'] = '預測失敗'
                
        except Exception as e:
            logger.error(f"[線程 {thread_id}] 蔬菜 {vege_id} 預測失敗: {e}")
            vege_results['error'] = str(e)
        
        return vege_results
    
    def run_prediction_for_vegetables(self, vege_ids: List[int] = None) -> Dict:
        """
        為指定蔬菜執行預測並存入資料庫
        
        參數:
        vege_ids: 蔬菜ID列表 (None表示所有可用蔬菜)
        
        返回:
        執行結果統計
        """
        if vege_ids is None:
            vege_ids = self.get_available_vegetables()
        
        # 使用全域參數設定
        historical_days = HISTORICAL_DAYS
        use_multithreading = USE_MULTITHREADING
        max_workers = MAX_WORKERS
        
        logger.info(f"開始為 {len(vege_ids)} 個蔬菜執行預測")
        
        results = {
            'total_vegetables': len(vege_ids),
            'successful_predictions': 0,
            'failed_predictions': 0,
            'total_records_saved': 0
        }
        
        # 處理日期區間設定
        if PREDICT_START_DATE is not None and PREDICT_END_DATE is not None:
            # 使用日期區間
            start_date = pd.to_datetime(PREDICT_START_DATE).date()
            end_date = pd.to_datetime(PREDICT_END_DATE).date()
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            predict_dates = [d.date() for d in date_range]
            logger.info(f"批量預測日期範圍: {start_date} 到 {end_date} ({len(predict_dates)} 天)")
        else:
            # 使用單一日期（今天）
            predict_dates = [date.today()]
            logger.info(f"使用今天日期執行預測: {predict_dates[0]}")
        
        # 統計變數調整為支援多日期
        total_dates = len(predict_dates)
        results['total_dates'] = total_dates
        results['completed_dates'] = 0
        
        # 根據參數選擇執行模式
        if use_multithreading:
            logger.info(f"使用多線程模式，最大線程數: {max_workers}")
            start_time = time.time()
            
            if len(predict_dates) > 1:
                # 多日期：按日期並行處理
                logger.info(f"多日期模式：按日期並行處理 {len(predict_dates)} 個日期")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 為每個日期提交任務
                    future_to_date = {}
                    for predict_date in predict_dates:
                        future = executor.submit(self.predict_single_date, predict_date, vege_ids, historical_days)
                        future_to_date[future] = predict_date
                    
                    # 收集結果
                    for future in as_completed(future_to_date):
                        predict_date = future_to_date[future]
                        try:
                            date_result = future.result()
                            # 聚合結果
                            results['successful_predictions'] += date_result['successful_predictions']
                            results['failed_predictions'] += date_result['failed_predictions']
                            results['total_records_saved'] += date_result['total_records_saved']
                            results['completed_dates'] += 1
                            
                            progress = results['completed_dates'] / total_dates * 100
                            logger.info(f"完成日期 {predict_date}，進度: {results['completed_dates']}/{total_dates} ({progress:.1f}%)")
                            
                        except Exception as e:
                            logger.error(f"日期 {predict_date} 處理失敗: {e}")
                            results['completed_dates'] += 1
                            
            else:
                # 單日期：按蔬菜並行處理
                predict_date = predict_dates[0]
                logger.info(f"單日期模式：按蔬菜並行處理 {len(vege_ids)} 個蔬菜，日期: {predict_date}")
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # 為每個蔬菜提交任務
                    future_to_vege = {}
                    for vege_id in vege_ids:
                        future = executor.submit(self.predict_single_vegetable, vege_id, predict_date, historical_days)
                        future_to_vege[future] = vege_id
                    
                    # 收集結果
                    completed_veges = 0
                    for future in as_completed(future_to_vege):
                        vege_id = future_to_vege[future]
                        try:
                            vege_result = future.result()
                            if vege_result['successful']:
                                results['successful_predictions'] += 1
                                results['total_records_saved'] += vege_result['records_saved']
                            else:
                                results['failed_predictions'] += 1
                            
                            completed_veges += 1
                            if completed_veges % 10 == 0 or completed_veges == len(vege_ids):
                                progress = completed_veges / len(vege_ids) * 100
                                logger.info(f"完成蔬菜預測進度: {completed_veges}/{len(vege_ids)} ({progress:.1f}%)")
                                
                        except Exception as e:
                            logger.error(f"蔬菜 {vege_id} 處理失敗: {e}")
                            results['failed_predictions'] += 1
                            completed_veges += 1
                
                results['completed_dates'] = 1
            
            elapsed_time = time.time() - start_time
            logger.info(f"多線程處理完成，耗時: {elapsed_time:.2f} 秒")
            
        else:
            # 單線程模式：序列處理（原始邏輯）
            logger.info("使用單線程模式")
                
            for predict_date in predict_dates:
                logger.info(f"\n=== 處理預測日期: {predict_date} ===")
                
                date_result = self.predict_single_date(predict_date, vege_ids, historical_days)
                
                # 聚合結果
                results['successful_predictions'] += date_result['successful_predictions']
                results['failed_predictions'] += date_result['failed_predictions']
                results['total_records_saved'] += date_result['total_records_saved']
                results['completed_dates'] += 1
                
                logger.info(f"完成日期 {predict_date} 的預測，進度: {results['completed_dates']}/{total_dates}")
        
        logger.info(f"批量預測完成。處理日期: {results['completed_dates']}, 成功: {results['successful_predictions']}, 失敗: {results['failed_predictions']}, 共存入: {results['total_records_saved']} 筆記錄")
        
        # 更新實際價格：使用本次處理的 predict_date 清單作為基準的前三天
        updated_actual = self.update_actual_prices_and_mape(predict_dates)
        results['updated_actual_prices'] = updated_actual

        return results

def main():
    # 獲取當前腳本所在目錄的絕對路徑
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_model_path = os.path.join(script_dir, 'models_high_next7.pkl')
    
    # 使用程式內設定的參數
    model_path = MODEL_PATH if MODEL_PATH is not None else default_model_path
    db_connection = DB_CONNECTION
    vege_ids = VEGE_IDS
    historical_days = HISTORICAL_DAYS
    use_multithreading = USE_MULTITHREADING
    max_workers = MAX_WORKERS
    
    # 保留命令行參數支援 (可選)
    parser = argparse.ArgumentParser(description='蔬菜價格預測器 - 從資料庫獲取資料並存入預測結果')
    parser.add_argument('--list_vegetables', action='store_true', help='列出可用的蔬菜ID')
    
    args = parser.parse_args()
    
    try:
        # 建立預測器
        predictor = DatabaseVegetablePricePredictor(model_path, db_connection)
        
        if args.list_vegetables:
            # 列出可用蔬菜
            vegetables = predictor.get_available_vegetables()
            print(f"可用蔬菜ID ({len(vegetables)} 個):")
            for i, vege_id in enumerate(vegetables):
                print(f"  {vege_id}", end='')
                if (i + 1) % 10 == 0:  # 每10個換行
                    print()
            print()
            return
        
        # 顯示使用的參數設定
        logger.info(f"=== 參數設定 ===")
        logger.info(f"使用多線程: {use_multithreading}")
        logger.info(f"最大線程數: {max_workers}")
        logger.info(f"指定蔬菜: {vege_ids if vege_ids else '所有可用蔬菜'}")
        logger.info(f"歷史資料天數: {historical_days}")
        logger.info(f"預測日期範圍: {PREDICT_START_DATE} ~ {PREDICT_END_DATE}")
        
        # 執行預測
        results = predictor.run_prediction_for_vegetables(vege_ids)
        
        print("\n=== 預測結果統計 ===")
        print(f"總蔬菜數: {results['total_vegetables']}")
        print(f"成功預測: {results['successful_predictions']}")
        print(f"預測失敗: {results['failed_predictions']}")
        print(f"存入記錄: {results['total_records_saved']}")
        print(f"更新實際價格: {results['updated_actual_prices']}")
        
    except Exception as e:
        logger.error(f"程式執行失敗: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())