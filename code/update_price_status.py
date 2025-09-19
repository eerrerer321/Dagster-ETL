#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
價格狀態更新程式
==============

更新 price_status 表的 48 筆資料：
- latest_price 取自 daily_avg_price 各 vege_id 的最新 ObsTime 價格
- price_change = (明日第一天預測價 - latest_price) / latest_price * 100
  明日第一天預測價選取邏輯：
    以該 vege_id 的最新 ObsTime 記為 obs_date，
    取 price_predictions 中 target_date = obs_date + 1 天 的紀錄，
    若有多筆，取 predict_date 最大者的 predict_price。
- updated_at 設為 CURRENT_DATE
- 僅更新 price_status 內既有 vege_id，不新增任何資料列。

使用方式:
  python code/update_price_status.py --db postgresql+psycopg2://user:pass@host:port/db --dry-run

注意:
- 若某 vege_id 缺少最新實際價或對應的預測價，將跳過 price_change 計算（設為 NULL），但仍會更新 latest_price（若有）。
"""

# 引入所需的函式庫
import argparse  # 用於解析命令列參數
from dataclasses import dataclass  # 用於建立資料類別
from datetime import date, timedelta  # 用於日期計算
import logging  # 用於記錄程式執行過程
from typing import Dict, Optional  # 用於型別提示

import pandas as pd  # 用於資料處理
from sqlalchemy import create_engine, text  # 用於資料庫連接和SQL查詢

# 設定日誌系統，將訊息輸出格式設為：時間 - 日誌等級 - 訊息內容
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 預設資料庫連接字串
DEFAULT_DB = 'postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres'

@dataclass
class StatusUpdate:
    """
    價格狀態更新資料結構

    屬性:
        vege_id: 蔬菜ID
        latest_price: 最新價格（從daily_avg_price取得）
        price_change: 價格變化百分比（基於預測價格計算）
    """
    vege_id: int
    latest_price: Optional[float]
    price_change: Optional[float]


def load_existing_status_veges(engine) -> pd.DataFrame:
    """
    讀取目前 price_status 既有的 vege_id 清單

    參數:
        engine: SQLAlchemy資料庫引擎

    返回:
        包含所有現有蔬菜ID的DataFrame
    """
    # SQL查詢：取得所有price_status表中的蔬菜ID，並按ID排序
    query = """
        SELECT vege_id
        FROM public.price_status
        ORDER BY vege_id
    """
    # 建立資料庫連接並執行查詢
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def load_latest_actuals(engine) -> pd.DataFrame:
    """
    讀取 daily_avg_price 各 vege_id 的最新 ObsTime 與價格

    參數:
        engine: SQLAlchemy資料庫引擎

    返回:
        包含每個蔬菜最新觀測時間和價格的DataFrame
    """
    # SQL查詢：使用DISTINCT ON取得每個蔬菜ID的最新記錄
    # 按vege_id分組，取ObsTime最新的一筆記錄
    query = """
        SELECT DISTINCT ON (vege_id)
            vege_id,
            "ObsTime" AS obs_date,
            avg_price_per_kg AS latest_price
        FROM public.daily_avg_price
        WHERE avg_price_per_kg IS NOT NULL
        ORDER BY vege_id, "ObsTime" DESC
    """
    # 建立資料庫連接並執行查詢
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn)


def load_first_pred_for_next_day(engine, vege_id: int, target_date: date) -> Optional[float]:
    """
    取得特定蔬菜在目標日期的預測價格

    當有多筆預測記錄時，選擇predict_date最新的那一筆

    參數:
        engine: SQLAlchemy資料庫引擎
        vege_id: 蔬菜ID
        target_date: 目標預測日期

    返回:
        預測價格（float）或None（如果沒有找到預測資料）
    """
    # 建立參數化SQL查詢，防止SQL注入攻擊
    query = text(
        """
        SELECT predict_price
        FROM public.price_predictions
        WHERE vege_id = :vege_id
          AND target_date = :target_date
          AND predict_price IS NOT NULL
        ORDER BY predict_date DESC
        LIMIT 1
        """
    )
    # 執行查詢並傳入參數
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
        'vege_id': int(vege_id),
        'target_date': target_date,
        })
    # 檢查是否有找到資料且價格不是空值
    if len(df) == 0 or pd.isna(df.iloc[0]['predict_price']):
        return None
    return float(df.iloc[0]['predict_price'])


def compute_updates(engine) -> Dict[int, StatusUpdate]:
    """
    計算每個蔬菜的價格更新資料

    僅處理price_status表中既有的蔬菜ID，計算最新價格和價格變化百分比

    參數:
        engine: SQLAlchemy資料庫引擎

    返回:
        字典，key為蔬菜ID，value為StatusUpdate物件
    """
    # 載入現有的蔬菜清單
    status_df = load_existing_status_veges(engine)
    if len(status_df) == 0:
        logger.warning('price_status 無資料可更新，將不進行任何動作。')
        return {}

    # 載入所有蔬菜的最新實際價格資料
    latest_df = load_latest_actuals(engine)
    logger.info(f"最新實際價覆蓋：{len(latest_df)} 筆（不同 vege_id）")

    # 定義內部函數：安全地將各種日期格式轉換為date物件
    def _as_date(v):
        """將不同格式的日期轉換為標準的date物件"""
        import pandas as pd
        from datetime import date as _date
        # 處理pandas Timestamp格式
        if isinstance(v, pd.Timestamp):
            return v.date()
        # 處理已經是date格式的資料
        if isinstance(v, _date):
            return v
        # 後備處理：嘗試從字串解析日期
        try:
            return pd.to_datetime(v).date()
        except Exception:
            return None

    # 建立蔬菜ID到最新價格資訊的對應字典
    latest_map = {}
    for _, r in latest_df.iterrows():
        obs_date = _as_date(r["obs_date"])  # 欄位名來自SQL查詢的AS obs_date
        # 跳過沒有有效日期或價格的記錄
        if obs_date is None or pd.isna(r["latest_price"]):
            continue
        # 儲存蔬菜ID對應的(觀測日期, 最新價格)資訊
        latest_map[int(r.vege_id)] = (obs_date, float(r.latest_price))

    # 初始化更新資料字典和預測成功計數器
    updates: Dict[int, StatusUpdate] = {}
    found_next_pred = 0

    # 遍歷每個需要更新的蔬菜ID
    for _, row in status_df.iterrows():
        vege_id = int(row['vege_id'])
        obs = latest_map.get(vege_id)  # 取得該蔬菜的最新觀測資料
        latest_price = None
        price_change = None

        if obs is not None:
            obs_date, latest_price = obs  # 解包觀測日期和最新價格
            # 計算次日日期作為預測目標日期
            next_day = obs_date + timedelta(days=1)
            # 載入次日的預測價格
            first_pred = load_first_pred_for_next_day(engine, vege_id, next_day)
            # 如果有預測價格且最新價格有效（非空且非零）
            if first_pred is not None and latest_price not in (None, 0):
                try:
                    # 計算價格變化百分比：(預測價-實際價)/實際價 * 100
                    price_change = (first_pred - latest_price) / latest_price * 100.0
                except ZeroDivisionError:
                    # 處理除零錯誤（雖然上面已經檢查過）
                    price_change = None
                else:
                    # 成功計算預測價格變化的蔬菜數量計數
                    found_next_pred += 1

        # 建立該蔬菜的更新資料物件
        updates[vege_id] = StatusUpdate(
            vege_id=vege_id,
            # 將最新價格四捨五入到小數點後兩位，無效值設為None
            latest_price=round(latest_price, 2) if isinstance(latest_price, (int, float)) else None,
            # 將價格變化百分比四捨五入到小數點後兩位，無效值設為None
            price_change=round(price_change, 2) if isinstance(price_change, (int, float)) else None,
        )

    # 記錄找到次日預測價格的蔬菜數量
    logger.info(f"找到次日預測價的 vege_id：{found_next_pred}/{len(status_df)}")
    return updates


def apply_updates(engine, updates: Dict[int, StatusUpdate], dry_run: bool = False) -> int:
    """
    將計算好的更新資料套用到price_status表

    參數:
        engine: SQLAlchemy資料庫引擎
        updates: 包含更新資料的字典
        dry_run: 是否為測試模式（不實際更新資料庫）

    返回:
        成功更新的記錄數量
    """
    # 如果沒有更新資料，直接返回0
    if len(updates) == 0:
        return 0

    # 初始化更新成功的記錄數量
    updated = 0
    # 遍歷每個要更新的蔬菜
    for vege_id, upd in updates.items():
        # 檢查最新價格是否有效，如果缺失則跳過以避免觸發資料庫的NOT NULL約束
        if upd.latest_price is None:
            logger.warning(f"跳過 vege_id={vege_id}：latest_price 缺失，未更新。")
            continue
        # 如果是測試模式，只輸出將要更新的內容但不實際執行
        if dry_run:
            logger.info(f"[DRY-RUN] vege_id={vege_id} latest_price={upd.latest_price} price_change={upd.price_change}")
            continue

        # 建立參數化的UPDATE SQL敘述
        sql = text(
            """
            UPDATE public.price_status
            SET latest_price = :latest_price,
                price_change = :price_change,
                updated_at = CURRENT_DATE
            WHERE vege_id = :vege_id
            """
        )
        # 使用交易现境執行UPDATE操作
        with engine.begin() as conn:
            res = conn.execute(sql, {
                'latest_price': upd.latest_price,
                # 如果price_change為空則設為0.0，以符合資料庫NOT NULL約束
                'price_change': upd.price_change if upd.price_change is not None else 0.0,
                'vege_id': vege_id,
            })
            # 統計實際更新的記錄數量（只計算有匹配到的列）
            if res.rowcount and res.rowcount > 0:
                updated += res.rowcount

    return updated


def main():
    """
    主程式入口點

    設定命令列參數解析，執行價格狀態更新流程
    """
    # 建立命令列參數解析器
    parser = argparse.ArgumentParser(description='更新 price_status 表：維持 48 筆、只更新不新增')
    parser.add_argument('--db', type=str, default=DEFAULT_DB, help='資料庫連接字串')
    parser.add_argument('--dry-run', action='store_true', help='僅顯示將更新的內容，不寫入資料庫')
    # 解析命令列參數
    args = parser.parse_args()

    try:
        # 使用指定的資料庫連接字串建立數據庫引擎
        engine = create_engine(args.db)
        logger.info('資料庫連線建立完成')

        # 計算所有蔬菜的更新資料
        updates = compute_updates(engine)
        if len(updates) == 0:
            logger.info('沒有可更新的內容。')
            return 0

        # 套用更新資料到資料庫
        updated_rows = apply_updates(engine, updates, dry_run=args.dry_run)
        if args.dry_run:
            logger.info('DRY-RUN 完成，未寫入資料庫。')
        else:
            logger.info(f'完成更新，共 {updated_rows} 筆。')
        return 0

    except Exception as e:
        logger.error(f'執行失敗: {e}')
        return 1


# 程式入口點：當作為主程式執行時呼叫main()函數
if __name__ == '__main__':
    raise SystemExit(main())
