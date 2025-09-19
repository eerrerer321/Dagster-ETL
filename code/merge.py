# 資料合併處理程式
# ===================
#
# 功能說明：
# 1. 合併天氣資料和價格資料
# 2. 計算加權天氣特徵
# 3. 依波動性分割資料為高波動和低波動組
# 4. 支援單日和批量日期處理
# 5. 支援多線程並行處理

# 引入必要的函式庫
from sqlalchemy import create_engine, text  # 資料庫連接和SQL查詢
import pandas as pd  # 資料處理和分析
import os  # 作業系統相關功能
import traceback  # 錯誤追蹤
from datetime import datetime, timedelta  # 日期時間處理
from concurrent.futures import ThreadPoolExecutor, as_completed  # 多線程處理


def get_yesterday_date():
    """
    獲取昨天的日期字串

    返回:
        str: 昨天的日期，格式為 YYYY-MM-DD
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def load_weight_data():
    """
    從資料庫載入權重資料

    載入2024年的區域標準化產量資料，用於計算加權天氣特徵

    返回:
        pd.DataFrame: 包含蔬菜ID、城市ID和標準化產量的權重資料
    """
    # 建立資料庫連接
    engine = create_engine(
        "postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"
    )

    # 查詢2024年的權重資料
    df = pd.read_sql(
        "SELECT * FROM yearly_regional_normalized_yields WHERE year = 2024", engine
    )
    return df


def load_weather_data(date_str, engine):
    """
    從資料庫讀取指定日期的天氣資料

    參數:
        date_str: 日期字串，格式為 YYYY-MM-DD
        engine: SQLAlchemy資料庫引擎

    返回:
        pd.DataFrame: 包含天氣資料的DataFrame

    拋出:
        Exception: 當找不到資料或發生錯誤時
    """
    # 建立SQL查詢，擷取指定日期的所有天氣資料
    sql_query = f"""
    SELECT city_id, "ObsTime", "StnPres", "Temperature", "RH", "WS", "Precp", typhoon, typhoon_name
    FROM weather_data
    WHERE DATE("ObsTime") = '{date_str}'
    """

    try:
        # 執行查詢並載入資料
        weather_df = pd.read_sql(sql_query, engine)
        # 檢查是否有找到資料
        if weather_df.empty:
            raise ValueError(f"No weather data found for date: {date_str}")
        return weather_df
    except Exception as e:
        raise Exception(f"Error loading weather data: {e}")


def load_price_data(date_str, engine):
    """
    從資料庫讀取指定日期的蔬菜價格資料

    參數:
        date_str: 日期字串，格式為 YYYY-MM-DD
        engine: SQLAlchemy資料庫引擎

    返回:
        pd.DataFrame: 包含蔬菜價格資料的DataFrame

    拋出:
        Exception: 當找不到資料或發生錯誤時
    """
    # 建立SQL查詢，擷取指定日期的所有蔬菜價格資料
    sql_query = f"""
    SELECT "ObsTime", avg_price_per_kg, vege_id
    FROM daily_avg_price
    WHERE DATE("ObsTime") = '{date_str}'
    """

    try:
        # 執行查詢並載入資料
        price_df = pd.read_sql(sql_query, engine)
        # 檢查是否有找到資料
        if price_df.empty:
            raise ValueError(f"No price data found for date: {date_str}")
        return price_df
    except Exception as e:
        raise Exception(f"Error loading price data: {e}")


def get_vege_reference():
    """單一內建參考表：包含 id、vege_name、market_vege_id、original_trend_seasonal。"""
    vege_name = [
        "菠菜",
        "空心菜",
        "高麗菜",
        "莧菜",
        "芥藍",
        "小白菜",
        "青江菜",
        "大白菜",
        "油菜",
        "大陸妹",
        "紅鳳菜",
        "地瓜葉",
        "水蓮",
        "美生菜",
        "香菜",
        "蘿蔓",
        "九層塔",
        "蔥",
        "山蘇",
        "茭白筍",
        "綠竹筍",
        "玉米筍",
        "蘆筍",
        "芹菜",
        "紅蘿蔔",
        "地瓜",
        "馬鈴薯",
        "薑",
        "蒜",
        "洋蔥",
        "芋頭",
        "山藥",
        "蓮藕",
        "紅蔥頭",
        "茄子",
        "絲瓜",
        "南瓜",
        "櫛瓜",
        "冬瓜",
        "苦瓜",
        "大黃瓜",
        "秋葵",
        "牛番茄",
        "青花菜",
        "花椰菜",
        "辣椒",
        "甜豌豆",
        "四季豆",
    ]
    market_vege_id = [
        "LH1",
        "LF1",
        "LA1",
        "LM1",
        "LK3",
        "LB1",
        "LD1",
        "LC1",
        "LN1",
        "LI1",
        "LQ1",
        "LO1",
        "LT3",
        "LI5",
        "LP1",
        "LI6",
        "LP2",
        "SE1",
        "LX2",
        "SQ1",
        "SH2",
        "FY4",
        "SV2",
        "LG1",
        "SB2",
        "SO1",
        "SC1",
        "SP1",
        "SG5",
        "SD1",
        "SJ1",
        "SU1",
        "SN1",
        "SE5",
        "FI1",
        "FF1",
        "FT1",
        "FF2",
        "FE1",
        "FG1",
        "FC1",
        "FA1",
        "FJ3",
        "FR1",
        "FB1",
        "FV1",
        "FL6",
        "FN1",
    ]
    id_list = [
        1,
        2,
        3,
        4,
        6,
        7,
        8,
        9,
        10,
        11,
        13,
        14,
        15,
        16,
        18,
        19,
        20,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
        49,
        50,
        51,
        52,
        55,
        59,
        60,
    ]
    # 與 id_list 順序對應的波動性分類
    original_trend_seasonal = [
        "H",
        "L",
        "L",
        "L",
        "L",
        "L",
        "L",
        "L",
        "L",
        "H",
        "L",
        "L",
        "H",
        "L",
        "H",
        "H",
        "H",
        "H",
        "H",
        "H",
        "H",
        "L",
        "H",
        "L",
        "L",
        "L",
        "L",
        "L",
        "H",
        "L",
        "L",
        "H",
        "L",
        "H",
        "L",
        "L",
        "L",
        "H",
        "L",
        "L",
        "L",
        "H",
        "L",
        "L",
        "L",
        "H",
        "H",
        "H",
    ]
    return pd.DataFrame(
        {
            "vege_name": vege_name,
            "market_vege_id": market_vege_id,
            "id": id_list,
            "original_trend_seasonal": original_trend_seasonal,
        }
    )


# 已整合至 get_vege_reference()


def split_data_by_volatility(merged_df, clustering_df, yesterday_date, engine):
    """根據original_trend_seasonal分割資料為高波動和低波動"""

    # 確保輸出目錄存在（相對於專案路徑，避免硬編碼磁碟機）
    split_output_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "splitdata")
    )

    os.makedirs(split_output_dir, exist_ok=True)

    # 合併資料以獲得波動性分類
    merged_with_volatility = merged_df.merge(
        clustering_df[["id", "original_trend_seasonal"]], on="id", how="left"
    )

    # 分割為高波動和低波動
    high_volatility_df = merged_with_volatility[
        merged_with_volatility["original_trend_seasonal"] == "H"
    ].drop("original_trend_seasonal", axis=1)
    low_volatility_df = merged_with_volatility[
        merged_with_volatility["original_trend_seasonal"] == "L"
    ].drop("original_trend_seasonal", axis=1)

    # 儲存高波動檔案（輸出前將 id -> vege_id）
    high_volatility_file = (
        f"{split_output_dir}/{yesterday_date}-high-volatility-merged.csv"
    )
    high_volatility_df_renamed = high_volatility_df.rename(columns={"id": "vege_id"})
    high_volatility_df_renamed.to_csv(
        high_volatility_file, index=False, encoding="utf-8-sig"
    )

    # 儲存低波動檔案（輸出前將 id -> vege_id）
    low_volatility_file = (
        f"{split_output_dir}/{yesterday_date}-low-volatility-merged.csv"
    )
    low_volatility_df_renamed = low_volatility_df.rename(columns={"id": "vege_id"})
    low_volatility_df_renamed.to_csv(
        low_volatility_file, index=False, encoding="utf-8-sig"
    )

    print(f"已儲存高波動檔：{high_volatility_file}")
    print(f"高波動筆數：{len(high_volatility_df)}")
    print(f"已儲存低波動檔：{low_volatility_file}")
    print(f"低波動筆數：{len(low_volatility_df)}")

    # 將分割後資料寫入資料庫（附加寫入）
    def _prepare_for_db(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            print("警告：DataFrame 為空，跳過資料庫寫入")
            return df

        out = df.copy()
        print(f"準備寫入資料庫的原始資料：{len(out)} 筆，欄位：{list(out.columns)}")

        # 輸出至 DB 也使用 vege_id 欄位名
        if "id" in out.columns:
            out = out.rename(columns={"id": "vege_id"})
            print("已將 'id' 欄位重命名為 'vege_id'")

        # 將 ObsTime 轉為日期型別（若資料表為 date/timestamp 可相容）
        try:
            out["ObsTime"] = pd.to_datetime(out["ObsTime"]).dt.date
            print(
                f"已轉換 ObsTime 為日期格式，範例值：{out['ObsTime'].iloc[0] if not out.empty else 'N/A'}"
            )
        except Exception as e:
            print(f"警告：ObsTime 轉換失敗：{e}")

        # 檢查必要欄位
        required_columns = ["ObsTime", "vege_id", "avg_price_per_kg"]
        missing_columns = [col for col in required_columns if col not in out.columns]
        if missing_columns:
            raise ValueError(f"缺少必要欄位：{missing_columns}")

        # 檢查數據完整性
        null_counts = out.isnull().sum()
        if null_counts.sum() > 0:
            print(f"警告：發現空值 - {dict(null_counts[null_counts > 0])}")

        print(f"準備寫入資料庫的最終資料：{len(out)} 筆，欄位：{list(out.columns)}")
        return out

    # 準備資料
    high_db = _prepare_for_db(high_volatility_df)
    low_db = _prepare_for_db(low_volatility_df)

    # 逐表寫入並回報結果
    print("\n=== 開始寫入資料庫 ===")

    # 測試資料庫連線
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[OK] 資料庫連線測試成功")
    except Exception as e:
        print(f"[ERROR] 資料庫連線測試失敗：{e}")
        print("請檢查資料庫連線設定")
        return high_volatility_df, low_volatility_df

    # 寫入高波動資料
    if not high_db.empty:
        try:
            rows = len(high_db)
            print(f"正在寫入 high_volatility_merged，資料筆數：{rows}")
            print(
                f"資料範例：{high_db.head(1).to_dict('records') if not high_db.empty else '無'}"
            )

            with engine.begin() as conn:  # 確保事務提交
                high_db.to_sql(
                    "high_volatility_merged",
                    con=conn,
                    if_exists="append",
                    index=False,
                    method=None,
                )

            # 驗證寫入結果
            with engine.connect() as conn:
                obstime_value = (
                    high_db.iloc[0]["ObsTime"]
                    if "ObsTime" in high_db.columns
                    else "unknown"
                )
                count_result = conn.execute(
                    text(
                        'SELECT COUNT(*) FROM high_volatility_merged WHERE "ObsTime" = :obstime_value'
                    ),
                    {"obstime_value": obstime_value},
                )
                actual_count = count_result.scalar()
                print(
                    f"[OK] 已成功寫入資料庫：high_volatility_merged（新增 {rows} 筆，驗證：{actual_count} 筆）"
                )

                if actual_count == 0:
                    print("[ERROR] 驗證失敗：資料庫中未找到剛寫入的資料")

        except Exception as e:
            print(f"[ERROR] 【失敗】寫入資料庫 high_volatility_merged 失敗：{e}")
            print(f"錯誤詳細資訊：{type(e).__name__}")
            traceback.print_exc()
    else:
        print("[WARNING] high_volatility_merged 無資料可寫入")

    # 寫入低波動資料
    if not low_db.empty:
        try:
            rows = len(low_db)
            print(f"正在寫入 low_volatility_merged，資料筆數：{rows}")
            print(
                f"資料範例：{low_db.head(1).to_dict('records') if not low_db.empty else '無'}"
            )

            with engine.begin() as conn:  # 確保事務提交
                low_db.to_sql(
                    "low_volatility_merged",
                    con=conn,
                    if_exists="append",
                    index=False,
                    method=None,
                )

            # 驗證寫入結果
            with engine.connect() as conn:
                obstime_value = (
                    low_db.iloc[0]["ObsTime"]
                    if "ObsTime" in low_db.columns
                    else "unknown"
                )
                count_result = conn.execute(
                    text(
                        'SELECT COUNT(*) FROM low_volatility_merged WHERE "ObsTime" = :obstime_value'
                    ),
                    {"obstime_value": obstime_value},
                )
                actual_count = count_result.scalar()
                print(
                    f"[OK] 已成功寫入資料庫：low_volatility_merged（新增 {rows} 筆，驗證：{actual_count} 筆）"
                )

                if actual_count == 0:
                    print("[ERROR] 驗證失敗：資料庫中未找到剛寫入的資料")

        except Exception as e:
            print(f"[ERROR] 【失敗】寫入資料庫 low_volatility_merged 失敗：{e}")
            print(f"錯誤詳細資訊：{type(e).__name__}")
            traceback.print_exc()
    else:
        print("[WARNING] low_volatility_merged 無資料可寫入")

    print("=== 資料庫寫入完成 ===")

    return high_volatility_df, low_volatility_df


def calculate_weighted_weather(weather_df, weight_df, vege_id):
    """
    計算特定蔬菜的加權天氣特徵

    根據不同地區的產量權重，計算該蔬菜的加權平均天氣數值

    參數:
        weather_df: 天氣資料DataFrame
        weight_df: 權重資料DataFrame
        vege_id: 蔬菜ID

    返回:
        dict: 包含加權天氣特徵的字典，或None（如果沒有權重資料）
    """
    # 定義需要計算加權平均的天氣欄位
    weather_columns = ["StnPres", "Temperature", "RH", "WS", "Precp"]

    # 擷取指定蔬菜的權重資料
    vege_weights = weight_df[weight_df["vege_id"] == vege_id]

    # 如果沒有該蔬菜的權重資料，返回None
    if vege_weights.empty:
        return None

    # 初始化加權值字典
    weighted_values = {}

    for col in weather_columns:
        total_weighted_value = 0
        total_weight = 0

        for _, weight_row in vege_weights.iterrows():
            city_id = weight_row["city_id"]
            weight = weight_row["normalized_yield"]

            city_weather = weather_df[weather_df["city_id"] == city_id]
            if not city_weather.empty:
                weather_value = city_weather[col].iloc[0]
                total_weighted_value += weather_value * weight
                total_weight += weight

        if total_weight > 0:
            weighted_values[col] = round(total_weighted_value / total_weight, 2)
        else:
            weighted_values[col] = 0

    typhoon_value = weather_df["typhoon"].iloc[0] if not weather_df.empty else 0
    weighted_values["typhoon"] = typhoon_value

    return weighted_values


def _build_merged_df(
    date_str: str, engine, weight_df: pd.DataFrame, vege_ref_df: pd.DataFrame
) -> pd.DataFrame:
    """只建立單日 merged_df（不寫檔、不寫 DB）。"""
    weather_df = load_weather_data(date_str, engine)
    price_df = load_price_data(date_str, engine)

    merged_data = []
    for _, price_row in price_df.iterrows():
        vege_id = price_row["vege_id"]

        # 跳過沒有價格的蔬菜
        if (
            pd.isna(price_row["avg_price_per_kg"])
            or price_row["avg_price_per_kg"] == ""
        ):
            continue

        # 檢查蔬菜是否在參考表
        vege_info = vege_ref_df[vege_ref_df["id"] == vege_id]
        if vege_info.empty:
            continue

        # 計算加權天氣
        weighted_weather = calculate_weighted_weather(weather_df, weight_df, vege_id)
        if weighted_weather is not None:
            merged_row = {
                "ObsTime": date_str,
                "avg_price_per_kg": price_row["avg_price_per_kg"],
                "id": vege_id,
                "StnPres": weighted_weather["StnPres"],
                "Temperature": weighted_weather["Temperature"],
                "RH": weighted_weather["RH"],
                "WS": weighted_weather["WS"],
                "Precp": weighted_weather["Precp"],
                "typhoon": weighted_weather["typhoon"],
            }
            merged_data.append(merged_row)

    merged_df = pd.DataFrame(merged_data)
    return merged_df


def merge_for_date(date_str: str, engine):
    """處理單一日期的清整流程，輸出 merged 與分割檔案"""

    print(f"處理日期：{date_str}")

    # 載入共用參考資料
    weight_df = load_weight_data()
    vege_ref_df = get_vege_reference()
    print(f"已載入權重資料：{weight_df.shape}")
    print(f"已載入蔬菜參考表：{vege_ref_df.shape}")

    # 建立單日資料
    merged_df = _build_merged_df(date_str, engine, weight_df, vege_ref_df)
    print(f"單日合併筆數：{len(merged_df)}")

    # 確保輸出目錄存在（相對於專案路徑，避免硬編碼磁碟機）
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{output_dir}/{date_str}-merged.csv"
    # 僅在輸出前將 id -> vege_id，不影響內部運算欄位
    merged_df_renamed = merged_df.rename(columns={"id": "vege_id"})
    merged_df_renamed.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"已儲存合併檔：{output_file}")
    print(f"合併筆數：{len(merged_df)}")

    # 根據波動性分割資料
    print("\n--- 依波動性分割資料 ---")
    high_volatility_df, low_volatility_df = split_data_by_volatility(
        merged_df, vege_ref_df, date_str, engine
    )

    print(f"\n--- 摘要 ---")
    print(f"總共建立檔案：3")
    print(f"- 主合併檔：{len(merged_df)} 筆")
    print(f"- 高波動檔：{len(high_volatility_df)} 筆")
    print(f"- 低波動檔：{len(low_volatility_df)} 筆")

    return merged_df, high_volatility_df, low_volatility_df


def merge_weather_price_data():
    """維持原行為：預設處理昨天"""
    target_date = get_yesterday_date()

    # 建立資料庫連接
    engine = create_engine(
        "postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"
    )

    try:
        return merge_for_date(target_date, engine)
    except Exception as e:
        print(f"合併處理發生錯誤：{e}")
        return None
    finally:
        engine.dispose()


def process_single_date_merge(date_str: str, engine, weight_df: pd.DataFrame, vege_ref_df: pd.DataFrame) -> tuple:
    """處理單一日期的合併處理，返回(date_str, merged_df, success, error_msg)"""
    try:
        print(f"\n--- 處理 {date_str} ---")
        df = _build_merged_df(date_str, engine, weight_df, vege_ref_df)

        if df.empty:
            print(f"[WARNING] {date_str} 無資料，跳過")
            return (date_str, None, False, "無資料")
        
        print(f"[OK] {date_str} 資料合併成功：{len(df)} 筆")
        
        # 依波動性分割當日資料
        merged_with_vol = df.merge(
            vege_ref_df[["id", "original_trend_seasonal"]], on="id", how="left"
        )
        daily_high_df = merged_with_vol[
            merged_with_vol["original_trend_seasonal"] == "H"
        ].drop("original_trend_seasonal", axis=1)
        daily_low_df = merged_with_vol[
            merged_with_vol["original_trend_seasonal"] == "L"
        ].drop("original_trend_seasonal", axis=1)
        
        # 準備寫入資料庫的資料
        def _prepare_daily_db(df: pd.DataFrame) -> pd.DataFrame:
            if df.empty:
                return df
            out = df.copy()
            if "id" in out.columns:
                out = out.rename(columns={"id": "vege_id"})
            try:
                out["ObsTime"] = pd.to_datetime(out["ObsTime"]).dt.date
            except Exception as e:
                print(f"[WARNING] {date_str} ObsTime 轉換失敗：{e}")
            return out

        # 寫入高波動資料
        if not daily_high_df.empty:
            try:
                prepared_high = _prepare_daily_db(daily_high_df)
                with engine.begin() as conn:  # 確保事務提交
                    prepared_high.to_sql(
                        "high_volatility_merged",
                        con=conn,
                        if_exists="append",
                        index=False,
                    )

                # 驗證寫入結果
                with engine.connect() as conn:
                    count_result = conn.execute(
                        text(
                            'SELECT COUNT(*) FROM high_volatility_merged WHERE "ObsTime" = :date_str'
                        ),
                        {"date_str": date_str},
                    )
                    actual_count = count_result.scalar()
                    print(f"  [OK] {date_str} 高波動資料寫入成功：{len(daily_high_df)} 筆（資料庫驗證：{actual_count} 筆）")

                    if actual_count != len(daily_high_df):
                        print(f"  [WARNING] {date_str} 高波動資料寫入數量不符！預期：{len(daily_high_df)}，實際：{actual_count}")

            except Exception as e:
                print(f"  [ERROR] {date_str} 高波動資料寫入失敗：{e}")
                traceback.print_exc()

        # 寫入低波動資料
        if not daily_low_df.empty:
            try:
                prepared_low = _prepare_daily_db(daily_low_df)
                with engine.begin() as conn:  # 確保事務提交
                    prepared_low.to_sql(
                        "low_volatility_merged",
                        con=conn,
                        if_exists="append",
                        index=False,
                    )

                # 驗證寫入結果
                with engine.connect() as conn:
                    count_result = conn.execute(
                        text(
                            'SELECT COUNT(*) FROM low_volatility_merged WHERE "ObsTime" = :date_str'
                        ),
                        {"date_str": date_str},
                    )
                    actual_count = count_result.scalar()
                    print(f"  [OK] {date_str} 低波動資料寫入成功：{len(daily_low_df)} 筆（資料庫驗證：{actual_count} 筆）")

                    if actual_count != len(daily_low_df):
                        print(f"  [WARNING] {date_str} 低波動資料寫入數量不符！預期：{len(daily_low_df)}，實際：{actual_count}")

            except Exception as e:
                print(f"  [ERROR] {date_str} 低波動資料寫入失敗：{e}")
                traceback.print_exc()
        
        return (date_str, df, True, None)
        
    except Exception as e:
        print(f"[ERROR] {date_str} 處理失敗：{e}")
        traceback.print_exc()
        return (date_str, None, False, str(e))

def run_range(start_date: str, end_date: str):
    """處理日期區間（含起訖，格式 YYYY-MM-DD），支援部分日期失敗時繼續處理其他日期"""
    # 解析與驗證日期格式
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    if end_dt < start_dt:
        raise ValueError("END_DATE must be >= START_DATE")

    engine = create_engine(
        "postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"
    )

    # 若只處理單日，沿用單日流程（維持原行為與檔名）
    if start_dt == end_dt:
        try:
            merge_for_date(start_date, engine)
        finally:
            engine.dispose()
        return

    # 區間多日：分為兩階段處理
    # 第一階段：逐日處理並即時寫入資料庫（避免因部分日期失敗影響全部）
    # 第二階段：彙整所有成功資料生成區間檔案
    try:
        print(f"處理日期區間：{start_date} ~ {end_date}")
        weight_df = load_weight_data()
        vege_ref_df = get_vege_reference()
        all_merged = []
        successful_dates = []
        failed_dates = []

        # 測試資料庫連線
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("[OK] 區間處理資料庫連線測試成功")
        except Exception as e:
            print(f"[ERROR] 區間處理資料庫連線測試失敗：{e}")
            print("請檢查資料庫連線設定，將跳過資料庫寫入")
            db_available = False
        else:
            db_available = True

        print("\n=== 第一階段：逐日處理並寫入資料庫 ===")
        
        # 生成所有日期列表
        date_list = []
        cur = start_dt
        while cur <= end_dt:
            date_list.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)

        print(f"[資訊] 共需處理 {len(date_list)} 個日期")
        
        # 決定是否使用多線程（僅在有多個日期且資料庫可用時使用）
        if USE_MULTITHREADING and len(date_list) > 1 and db_available:
            print(f"[多線程] 使用 {MAX_WORKERS} 線程並行處理")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # 提交任務
                future_to_date = {executor.submit(process_single_date_merge, date_str, engine, weight_df, vege_ref_df): date_str 
                                for date_str in date_list}
                
                # 等待完成
                for future in as_completed(future_to_date):
                    date_str = future_to_date[future]
                    try:
                        date_result, merged_df, success, error_msg = future.result()
                        if success and merged_df is not None:
                            all_merged.append(merged_df)
                            successful_dates.append(date_result)
                        else:
                            failed_dates.append((date_result, error_msg if error_msg else "處理失敗"))
                    except Exception as e:
                        print(f"[錯誤] 多線程處理 {date_str} 失敗: {e}")
                        failed_dates.append((date_str, str(e)))
        else:
            print("[單線程] 順序處理")
            
            for date_str in date_list:
                try:
                    date_result, merged_df, success, error_msg = process_single_date_merge(date_str, engine, weight_df, vege_ref_df)
                    if success and merged_df is not None:
                        all_merged.append(merged_df)
                        successful_dates.append(date_result)
                    else:
                        failed_dates.append((date_result, error_msg if error_msg else "處理失敗"))
                except Exception as e:
                    print(f"[錯誤] 處理 {date_str} 失敗: {e}")
                    failed_dates.append((date_str, str(e)))

        print(f"\n=== 第一階段完成 ===")
        print(f"成功處理日期：{len(successful_dates)} 天")
        print(f"失敗日期：{len(failed_dates)} 天")
        if failed_dates:
            print("失敗日期詳情：")
            for date_str, reason in failed_dates:
                print(f"  - {date_str}: {reason}")

        # 第二階段：生成區間彙整檔案
        print(f"\n=== 第二階段：生成區間彙整檔案 ===")
        merged_all = (
            pd.concat(all_merged, ignore_index=True) if all_merged else pd.DataFrame()
        )

        # 寫出合併檔（區間檔名），只改表頭名稱
        output_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data")
        )
        os.makedirs(output_dir, exist_ok=True)
        range_prefix = f"{start_date}-{end_date}"
        merged_path = os.path.join(output_dir, f"{range_prefix}-merged.csv")
        merged_all.rename(columns={"id": "vege_id"}).to_csv(
            merged_path, index=False, encoding="utf-8-sig"
        )
        print(f"已儲存區間合併檔：{merged_path}（{len(merged_all)} 筆）")

        # 依波動性分割（不呼叫單日寫檔函式，避免產生每日檔案）
        merged_with_vol = merged_all.merge(
            vege_ref_df[["id", "original_trend_seasonal"]], on="id", how="left"
        )
        high_df = merged_with_vol[
            merged_with_vol["original_trend_seasonal"] == "H"
        ].drop("original_trend_seasonal", axis=1)
        low_df = merged_with_vol[
            merged_with_vol["original_trend_seasonal"] == "L"
        ].drop("original_trend_seasonal", axis=1)

        split_dir = os.path.join(output_dir, "splitdata")
        os.makedirs(split_dir, exist_ok=True)
        high_path = os.path.join(
            split_dir, f"{range_prefix}-high-volatility-merged.csv"
        )
        low_path = os.path.join(split_dir, f"{range_prefix}-low-volatility-merged.csv")
        high_df.rename(columns={"id": "vege_id"}).to_csv(
            high_path, index=False, encoding="utf-8-sig"
        )
        low_df.rename(columns={"id": "vege_id"}).to_csv(
            low_path, index=False, encoding="utf-8-sig"
        )
        print(f"已儲存區間高波動檔：{high_path}（{len(high_df)} 筆）")
        print(f"已儲存區間低波動檔：{low_path}（{len(low_df)} 筆）")

        # 註：資料庫寫入已在第一階段逐日完成，此處不再重複寫入
        print(f"\n=== 處理完成摘要 ===")
        print(f"成功處理並寫入資料庫的日期：{successful_dates}")
        print(f"總計成功日期：{len(successful_dates)} 天")
        print(f"總計失敗日期：{len(failed_dates)} 天")
        print(f"區間合併檔案已生成：{len(merged_all)} 筆資料")

        # 最終資料庫驗證
        if db_available and successful_dates:
            print(f"\n=== 最終資料庫驗證 ===")
            try:
                with engine.connect() as conn:
                    # 檢查整個區間的資料
                    high_count_result = conn.execute(
                        text(
                            'SELECT COUNT(*) FROM high_volatility_merged WHERE "ObsTime" >= :start_date AND "ObsTime" <= :end_date'
                        ),
                        {"start_date": start_date, "end_date": end_date},
                    )
                    high_total = high_count_result.scalar()

                    low_count_result = conn.execute(
                        text(
                            'SELECT COUNT(*) FROM low_volatility_merged WHERE "ObsTime" >= :start_date AND "ObsTime" <= :end_date'
                        ),
                        {"start_date": start_date, "end_date": end_date},
                    )
                    low_total = low_count_result.scalar()

                    print(f"資料庫中{start_date}~{end_date}期間資料統計：")
                    print(f"- 高波動資料：{high_total} 筆")
                    print(f"- 低波動資料：{low_total} 筆")
                    print(f"- 總計：{high_total + low_total} 筆")

                    # 按日期檢查
                    print(f"\n各日期資料統計：")
                    for date_str in successful_dates[:5]:  # 只顯示前5個日期作為範例
                        high_daily = conn.execute(
                            text(
                                'SELECT COUNT(*) FROM high_volatility_merged WHERE "ObsTime" = :date_str'
                            ),
                            {"date_str": date_str},
                        ).scalar()
                        low_daily = conn.execute(
                            text(
                                'SELECT COUNT(*) FROM low_volatility_merged WHERE "ObsTime" = :date_str'
                            ),
                            {"date_str": date_str},
                        ).scalar()
                        print(
                            f"  {date_str}: 高波動 {high_daily} 筆, 低波動 {low_daily} 筆"
                        )

                    if len(successful_dates) > 5:
                        print(f"  ... （其他 {len(successful_dates)-5} 個日期）")

                    if high_total == 0 and low_total == 0:
                        print("[ERROR] 嚴重警告：資料庫中沒有找到任何指定期間的資料！")
                        print(
                            "可能原因：1. 事務未正確提交 2. 資料表不存在 3. 欄位名稱不匹配"
                        )
                    elif high_total + low_total != len(merged_all):
                        print(
                            f"[WARNING] 資料庫總數 ({high_total + low_total}) 與合併檔案筆數 ({len(merged_all)}) 不符"
                        )
                    else:
                        print("[OK] 資料庫驗證通過，所有資料都已成功寫入")

            except Exception as e:
                print(f"[ERROR] 最終資料庫驗證失敗：{e}")
                traceback.print_exc()

        if successful_dates:
            print("[INFO] 區間處理完成：成功處理的日期資料已即時寫入資料庫")
        if failed_dates:
            print("[WARNING] 部分日期處理失敗，但不影響其他成功日期的資料庫寫入")

    finally:
        engine.dispose()


# ============== 執行參數設定區 ==============
USE_MULTITHREADING = True          # 是否使用多線程 (True/False)
MAX_WORKERS = 10                   # 最大線程數 (建議設為你的CPU線程數-2)

# ============== 日期設定區 ==============
# 修改這裡的日期來指定要下載的範圍
# START_DATE = "2022-01-01"  # 開始日期 (YYYY-MM-DD)
# END_DATE = "2025-08-18"  # 結束日期 (YYYY-MM-DD)，設為 None 則只下載開始日期當天

# 如果要下載昨天的資料，設定為 None
START_DATE = None
END_DATE = None
# =======================================

if __name__ == "__main__":
    if START_DATE and END_DATE:
        run_range(START_DATE, END_DATE)
    elif START_DATE and END_DATE is None:
        # 僅處理 START_DATE 當天
        run_range(START_DATE, START_DATE)
    else:
        # 預設處理昨天
        result = merge_weather_price_data()
