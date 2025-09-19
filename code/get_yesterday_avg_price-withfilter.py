#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
蔬菜價格預測 - 昨日平均價格資料獲取程式
=============================================

功能說明：
1. 自動下載昨天的蔬菜交易價格資料
2. 計算每種蔬果的平均價格
3. 處理缺失資料，從前一天補充
4. 支援多線程並行處理
5. 儲存結果到CSV檔案和資料庫
"""

# 引入系統相關函式庫
import sys  # 系統相關功能
import io   # 輸入輸出處理
import os   # 作業系統介面

# 處理Windows系統的編碼問題，設定標準輸出編碼為UTF-8
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    os.environ["PYTHONIOENCODING"] = "utf-8"

# 引入其他必要的函式庫
import requests  # HTTP請求處理
import json  # JSON資料處理
import pandas as pd  # 資料分析和處理
from datetime import datetime, timedelta  # 日期時間處理
from typing import Optional, Dict, List, Any  # 型別提示
import time  # 時間相關功能
import os  # 作業系統相關功能（重複引入，但保持原程式碼不變）
from sqlalchemy import create_engine  # 資料庫連接
from concurrent.futures import ThreadPoolExecutor, as_completed  # 多線程處理

# 市場代碼到內部蔬菜ID的對照表
# 這個對照表用於將農產品交易API返回的市場代碼轉換為系統內部使用的蔬菜ID
MARKET_TO_ID = {
    "LH1": 1,  # 菠菜
    "LF1": 2,
    "LA1": 3,
    "LM1": 4,
    "LK3": 6,
    "LB1": 7,
    "LD1": 8,
    "LC1": 9,
    "LN1": 10,
    "LI1": 11,
    "LQ1": 13,
    "LO1": 14,
    "LT3": 15,
    "LI5": 16,
    "LP1": 18,
    "LI6": 19,
    "LP2": 20,
    "SE1": 23,
    "LX2": 24,
    "SQ1": 25,
    "SH2": 26,
    "FY4": 27,
    "SV2": 28,
    "LG1": 29,
    "SB2": 31,
    "SO1": 32,
    "SC1": 33,
    "SP1": 34,
    "SG5": 35,
    "SD1": 36,
    "SJ1": 37,
    "SU1": 38,
    "SN1": 39,
    "SE5": 40,
    "FI1": 41,
    "FF1": 42,
    "FT1": 43,
    "FF2": 44,
    "FE1": 45,
    "FG1": 46,
    "FC1": 47,
    "FA1": 49,
    "FJ3": 50,
    "FR1": 51,
    "FB1": 52,
    "FV1": 55,
    "FL6": 59,
    "FN1": 60,
}


class AgriProductsAPI:
    """
    農產品交易行情API客戶端類別

    負責從農委會API獲取農產品交易資料，處理數據格式轉換
    支援多線程並行處理提高效率
    """

    def __init__(self):
        """
        初始化API客戶端

        設定API端點、HTTP Session、多線程參數等配置
        """
        # 農委會農產品交易行情API端點
        self.base_url = "https://data.moa.gov.tw/api/v1/AgriProductsTransType/"
        # 建立持久的HTTP Session
        self.session = requests.Session()
        # 設定請求標頭，模擬瀏覽器請求
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # 多線程處理設定
        self.use_multithreading = True  # 是否啟用多線程
        self.max_workers = 4  # 最大工作線程數

    def get_transaction_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        crop_code: Optional[str] = None,
        crop_name: Optional[str] = None,
        market_name: Optional[str] = None,
        market_code: Optional[str] = None,
        tc_type: Optional[str] = None,
        page: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        從農委會API獲取農產品交易行情資料

        參數:
            start_date: 交易日期(起始) - 格式: YYYY-MM-DD 或 YYY.MM.DD (民國年)
            end_date: 交易日期(結束) - 格式: YYYY-MM-DD 或 YYY.MM.DD (民國年)
            crop_code: 農產品代碼
            crop_name: 農產品名稱
            market_name: 市場名稱
            market_code: 市場代號
            tc_type: 農產品種類代碼
            page: 頁碼控制(用於分頁查詢)

        返回:
            Dict: API回應的JSON資料，包含交易記錄或錯誤訊息
        """

        # 建立查詢參數
        params = {}

        # 轉換日期格式為民國年格式 (YYY.MM.DD)
        if start_date:
            params["Start_time"] = self._convert_to_roc_date(start_date)
        if end_date:
            params["End_time"] = self._convert_to_roc_date(end_date)
        if crop_code:
            params["CropCode"] = crop_code
        if crop_name:
            params["CropName"] = crop_name
        if market_name:
            params["MarketName"] = market_name
        if market_code:
            params["MarketCode"] = market_code
        if tc_type:
            params["TcType"] = tc_type
        if page:
            params["Page"] = page

        try:
            response = self.session.get(
                self.base_url, params=params, timeout=30, verify=False
            )
            response.raise_for_status()

            # 檢查回應內容
            if response.text.strip():
                data = response.json()
                # 確保回應格式正確
                if isinstance(data, dict):
                    if "Data" in data:
                        return data
                    elif data.get("Next") == False and not data.get("Data"):
                        # 正常情況：已到最後一頁，沒有更多資料
                        return {"error": "已到最後一頁", "raw_data": data}
                    else:
                        return {"error": "API回應格式不正確", "raw_data": data}
                else:
                    return {"error": "API回應格式不正確", "raw_data": data}
            else:
                return {"error": "API回應為空"}

        except requests.exceptions.RequestException as e:
            return {"error": f"請求錯誤: {str(e)}"}
        except json.JSONDecodeError as e:
            return {"error": f"JSON解析錯誤: {str(e)}"}

    def _convert_to_roc_date(self, date_str: str) -> str:
        """
        將西元年日期轉換為民國年格式

        Args:
            date_str: 日期字串，格式: YYYY-MM-DD

        Returns:
            民國年格式日期字串: YYY.MM.DD
        """
        try:
            if "." in date_str:
                # 已經是民國年格式
                return date_str

            # 解析西元年日期
            from datetime import datetime

            dt = datetime.strptime(date_str, "%Y-%m-%d")

            # 轉換為民國年 (西元年 - 1911)
            roc_year = dt.year - 1911

            # 格式化為 YYY.MM.DD
            return f"{roc_year:03d}.{dt.month:02d}.{dt.day:02d}"

        except ValueError:
            # 如果轉換失敗，返回原始字串
            return date_str

    def get_all_pages(self, **kwargs) -> List[Dict[str, Any]]:
        """
        獲取所有頁面的資料

        Args:
            **kwargs: 傳遞給get_transaction_data的參數

        Returns:
            所有頁面的資料列表
        """
        all_data = []
        page = "1"  # 從第1頁開始

        while True:
            # 加入頁碼參數
            kwargs["page"] = page

            result = self.get_transaction_data(**kwargs)

            if "error" in result:
                if "已到最後一頁" in result["error"]:
                    print(f"已讀取所有可用頁面，開始處理資料...")
                elif "API回應格式不正確" in result["error"]:
                    print(f"API回應格式異常，但已獲取部分資料，開始處理...")
                else:
                    print(f"錯誤: {result['error']}")
                break

            # 檢查是否有資料
            if "Data" in result and result["Data"]:
                all_data.extend(result["Data"])
                print(
                    f"第{page}頁: 獲取 {len(result['Data'])} 筆資料，累計 {len(all_data)} 筆"
                )
            else:
                print(f"第{page}頁: 沒有資料")
                break

            # 檢查是否還有下一頁
            if result.get("Next") == True:
                page = str(int(page) + 1)
                time.sleep(1)  # 避免請求過於頻繁
            else:
                print("已獲取所有頁面資料")
                break

        return all_data

    def to_dataframe(self, data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        將資料轉換為pandas DataFrame

        Args:
            data: API回應的資料列表

        Returns:
            pandas DataFrame
        """
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)

        # 轉換民國年日期為西元年
        if "TransDate" in df.columns:
            df["TransDate"] = df["TransDate"].apply(self._convert_roc_to_ad_date)
            df["TransDate"] = pd.to_datetime(df["TransDate"], errors="coerce")

        # 轉換價格欄位為數值型態
        price_columns = [
            "Upper_Price",
            "Middle_Price",
            "Lower_Price",
            "Avg_Price",
            "Trans_Quantity",
        ]
        for col in price_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def _convert_roc_to_ad_date(self, roc_date_str: str) -> str:
        """
        將民國年日期轉換為西元年格式

        Args:
            roc_date_str: 民國年日期字串，格式: YYY.MM.DD

        Returns:
            西元年格式日期字串: YYYY-MM-DD
        """
        try:
            if not roc_date_str or roc_date_str == "-":
                return None

            # 解析民國年日期
            parts = roc_date_str.split(".")
            if len(parts) == 3:
                roc_year = int(parts[0])
                month = int(parts[1])
                day = int(parts[2])

                # 轉換為西元年 (民國年 + 1911)
                ad_year = roc_year + 1911

                return f"{ad_year:04d}-{month:02d}-{day:02d}"
            else:
                return roc_date_str

        except (ValueError, AttributeError):
            return roc_date_str

    def get_price_data_for_date(
        self, date_str: str, target_vege_ids: set
    ) -> pd.DataFrame:
        """
        獲取指定日期的蔬菜價格資料

        Args:
            date_str: 日期字串 YYYY-MM-DD
            target_vege_ids: 目標蔬菜ID集合

        Returns:
            包含價格資料的DataFrame
        """
        print(f"正在獲取 {date_str} 的農產品交易資料...")

        # 獲取指定日期的所有農產品交易資料
        all_data = self.get_all_pages(start_date=date_str, end_date=date_str)

        if not all_data:
            print(f"未找到 {date_str} 的交易資料")
            return pd.DataFrame()

        # 轉換為DataFrame
        df = self.to_dataframe(all_data)

        if df.empty:
            print(f"{date_str} 資料轉換失敗或無有效資料")
            return pd.DataFrame()

        # 過濾有效的交易資料（排除休市、無價格資料，並只保留目標蔬菜）
        valid_df = df[
            (df["CropName"] != "休市")
            & (df["Avg_Price"] > 0)
            & (df["Avg_Price"].notna())
            & (df["CropCode"].isin(target_vege_ids))
        ].copy()

        if valid_df.empty:
            print(f"{date_str} 未找到有效的價格資料")
            return pd.DataFrame()

        print(f"{date_str} 找到 {len(valid_df)} 筆有效交易資料")

        # 計算每種農產品的平均價格
        avg_price_df = (
            valid_df.groupby(["CropCode", "CropName"])
            .agg({"Avg_Price": "mean", "Trans_Quantity": "sum"})
            .reset_index()
        )

        return avg_price_df

    def process_single_date_price(self, current_date_str: str) -> str:
        """處理單一日期的價格資料"""
        prev_date_str = (datetime.strptime(current_date_str, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"\n[處理] {current_date_str}")
        
        try:
            result_file = self.save_single_day_avg_price(current_date_str, prev_date_str)
            return result_file
        except Exception as e:
            print(f"[錯誤] 處理 {current_date_str} 發生錯誤: {e}")
            return None

    def save_price_data_for_date_range(self, start_date: str, end_date: str = None):
        """
        獲取指定日期範圍的蔬菜平均價格資料，缺少的從前一天補充，並儲存為CSV檔案
        """
        if end_date is None:
            end_date = start_date

        print(f"[開始] 處理價格資料 ({start_date} 到 {end_date})...")

        # 生成日期範圍
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # 生成所有日期列表
        date_list = []
        current_dt = start_dt
        while current_dt <= end_dt:
            date_list.append(current_dt.strftime("%Y-%m-%d"))
            current_dt += timedelta(days=1)

        print(f"[資訊] 共需處理 {len(date_list)} 個日期")
        
        # 決定是否使用多線程
        if self.use_multithreading and len(date_list) > 1:
            print(f"[多線程] 使用 {self.max_workers} 線程並行處理")
            processed_files = []
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交任務
                future_to_date = {executor.submit(self.process_single_date_price, date_str): date_str 
                                for date_str in date_list}
                
                # 等待完成
                for future in as_completed(future_to_date):
                    date_str = future_to_date[future]
                    try:
                        result = future.result()
                        if result:
                            processed_files.append(result)
                    except Exception as e:
                        print(f"[錯誤] 多線程處理 {date_str} 失敗: {e}")
        else:
            print("[單線程] 順序處理")
            processed_files = []
            
            for date_str in date_list:
                result = self.process_single_date_price(date_str)
                if result:
                    processed_files.append(result)

        print(f"\n[完成] 價格資料處理完成！")
        print(f"成功處理 {len(processed_files)} 個檔案")
        return processed_files

    def save_yesterday_avg_price(self):
        """
        獲取昨天的蔬菜平均價格資料，缺少的從前天補充，並儲存為CSV檔案（保持向後相容）
        """
        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")
        return self.save_price_data_for_date_range(yesterday_str)

    def save_single_day_avg_price(self, target_date_str: str, prev_date_str: str):
        """
        獲取單日的蔬菜平均價格資料，缺少的從前一天補充，並儲存為CSV檔案
        """
        # 定義需要的48種蔬菜vege_id
        target_vege_ids = {
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
        }

        print("=" * 60)
        print(f"開始獲取 {target_date_str} 的48種目標蔬菜價格資料")
        print("=" * 60)

        # 1. 獲取目標日期的資料
        target_data = self.get_price_data_for_date(target_date_str, target_vege_ids)
        target_vege_ids_found = (
            set(target_data["CropCode"].tolist()) if not target_data.empty else set()
        )

        # 2. 找出目標日期缺少的蔬菜
        missing_vege_ids = target_vege_ids - target_vege_ids_found

        if missing_vege_ids:
            print(
                f"\n{target_date_str} 缺少 {len(missing_vege_ids)} 種蔬菜，嘗試從 {prev_date_str} 獲取:"
            )
            for vege_id in sorted(missing_vege_ids):
                print(f"  缺少: {vege_id}")

            # 3. 獲取前一天的資料（只取缺少的蔬菜）
            prev_data = self.get_price_data_for_date(prev_date_str, missing_vege_ids)
            prev_vege_ids_found = (
                set(prev_data["CropCode"].tolist()) if not prev_data.empty else set()
            )

            # 4. 找出前一天也沒有的蔬菜
            still_missing_vege_ids = missing_vege_ids - prev_vege_ids_found
        else:
            prev_data = pd.DataFrame()
            prev_vege_ids_found = set()
            still_missing_vege_ids = set()
            print(f"\n{target_date_str} 已獲得所有48種蔬菜資料！")

        # 5. 建立最終結果DataFrame
        result_rows = []

        # 處理目標日期有資料的蔬菜
        if not target_data.empty:
            for _, row in target_data.iterrows():
                mapped_id = MARKET_TO_ID.get(str(row["CropCode"]))
                if mapped_id is None:
                    print(
                        f"[警告] 找不到對應的 vege_id 對照: {row['CropCode']}，略過該筆"
                    )
                else:
                    result_rows.append(
                        {
                            "ObsTime": target_date_str,
                            "avg_price_per_kg": round(row["Avg_Price"], 2),
                            "vege_id": int(mapped_id),
                            "datasource": "",  # 目標日期的資料，datasource 留空
                        }
                    )

        # 處理前一天補充的蔬菜
        if not prev_data.empty:
            for _, row in prev_data.iterrows():
                mapped_id = MARKET_TO_ID.get(str(row["CropCode"]))
                if mapped_id is None:
                    print(
                        f"[警告] 找不到對應的 vege_id 對照(前一天補充): {row['CropCode']}，略過該筆"
                    )
                else:
                    result_rows.append(
                        {
                            "ObsTime": target_date_str,  # ObsTime仍然是目標日期
                            "avg_price_per_kg": round(row["Avg_Price"], 2),
                            "vege_id": int(mapped_id),
                            "datasource": prev_date_str,  # 資料來源是前一天
                        }
                    )

        # 處理完全沒有資料的蔬菜
        if still_missing_vege_ids:
            print(
                f"\n{prev_date_str} 也沒有資料的蔬菜 ({len(still_missing_vege_ids)} 種):"
            )
            for vege_code in sorted(still_missing_vege_ids):
                print(f"  無資料: {vege_code}")
                mapped_id = MARKET_TO_ID.get(str(vege_code))
                result_rows.append(
                    {
                        "ObsTime": target_date_str,
                        "avg_price_per_kg": None,  # 沒有資料設為 None
                        "vege_id": int(mapped_id) if mapped_id is not None else None,
                        "datasource": "",
                    }
                )

        # 建立結果DataFrame
        result_df = pd.DataFrame(result_rows)

        # 按vege_id排序
        result_df = result_df.sort_values("vege_id").reset_index(drop=True)

        print(f"\n處理結果統計:")
        print(f"  {target_date_str} 有資料: {len(target_vege_ids_found)} 種")
        print(f"  {prev_date_str} 補充: {len(prev_vege_ids_found)} 種")
        print(f"  無資料(Null): {len(still_missing_vege_ids)} 種")
        print(f"  總計: {len(result_df)} 種")

        # 確保data/price資料夾存在（以專案根目錄為基準）
        output_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "data", "price"
        )
        os.makedirs(output_dir, exist_ok=True)

        # 儲存檔案到data/price資料夾
        filename = f"{target_date_str}_AVG_price.csv"
        filepath = os.path.join(output_dir, filename)
        result_df.to_csv(filepath, index=False, encoding="utf-8-sig")

        print(f"\n資料已儲存至 {filepath}")

        # 將結果寫入資料庫（避免重複：相同 ObsTime + vege_id 不重複寫入）
        try:
            # 檢查是否有 psycopg2
            try:
                import psycopg2

                print("[資料庫] 正在連接 PostgreSQL...")
            except ImportError:
                print("[警告] 無法匯入 psycopg2，跳過資料庫寫入")
                print("       請執行: pip install psycopg2-binary")
                print("       如果是 Python 3.13，可能需要等待相容版本")
                return filepath

            engine = create_engine(
                "postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"
            )
            table_name = "daily_avg_price"

            # 讀取已存在的鍵值（同一天已存在的蔬菜代號）
            try:
                existing = pd.read_sql(
                    f'SELECT vege_id FROM {table_name} WHERE "ObsTime" = %(obs)s',
                    con=engine,
                    params={"obs": target_date_str},
                )
                existing_ids = set(
                    pd.to_numeric(existing["vege_id"], errors="coerce")
                    .dropna()
                    .astype(int)
                    .tolist()
                )
                print(f"[資料庫] 發現已存在 {len(existing_ids)} 筆相同日期資料")
            except Exception as db_error:
                print(f"[資料庫] 查詢現有資料失敗：{db_error}")
                existing_ids = set()

            # 準備要寫入的資料（排除已存在鍵值），且僅保留指定欄位
            df_to_insert = result_df.copy()
            # 確保 vege_id 為 int 類型以便與 existing_ids 比較
            df_to_insert["vege_id"] = pd.to_numeric(
                df_to_insert["vege_id"], errors="coerce"
            ).astype("Int64")
            if existing_ids:
                df_to_insert = df_to_insert[~df_to_insert["vege_id"].isin(existing_ids)]
            # 只寫入 ObsTime, avg_price_per_kg, vege_id
            df_to_insert = df_to_insert[["ObsTime", "avg_price_per_kg", "vege_id"]]

            # 將 NaN 轉換為 None 以便寫入資料庫為 NULL
            df_to_insert = df_to_insert.where(pd.notna(df_to_insert), None)

            if len(df_to_insert) > 0:
                df_to_insert.to_sql(table_name, engine, if_exists="append", index=False)
                print(
                    f"[資料庫] 寫入成功：新增 {len(df_to_insert)} 筆，略過 {len(result_df) - len(df_to_insert)} 筆（已存在）"
                )
            else:
                print("[資料庫] 寫入略過：相同日期與蔬菜代號的資料已存在")

            engine.dispose()  # 關閉連接

        except Exception as e:
            print(f"[資料庫] 寫入失敗：{e}")
            print("         資料已儲存至 CSV 檔案，可稍後手動匯入資料庫")

        return filepath


def main():
    """主程式 - 獲取蔬菜平均價格資料"""
    # ============== 執行參數設定區 ==============
    USE_MULTITHREADING = True          # 是否使用多線程 (True/False)
    MAX_WORKERS = 10                   # 最大線程數 (建議設為你的CPU線程數-2)
    
    # ============== 日期設定區 ==============
    # 修改這裡的日期來指定要下載的範圍
    # START_DATE = "2025-07-24"  # 開始日期 (YYYY-MM-DD)
    # END_DATE = "2025-08-01"  # 結束日期 (YYYY-MM-DD)，設為 None 則只下載開始日期當天

    # 如果要下載昨天的資料，設定為 None
    START_DATE = None
    END_DATE = None
    # =======================================

    print("=" * 60)
    print("蔬菜價格預測 - 平均價格資料獲取程式")
    print("=" * 60)

    # 建立API客戶端
    api = AgriProductsAPI()
    api.use_multithreading = USE_MULTITHREADING
    api.max_workers = MAX_WORKERS

    try:
        if START_DATE is not None:
            # 使用指定的日期範圍
            start_date = START_DATE
            end_date = END_DATE if END_DATE else start_date
            print(f"目標日期範圍: {start_date} 到 {end_date}")

            # 執行資料獲取和處理
            processed_files = api.save_price_data_for_date_range(start_date, end_date)

            print(f"\n成功處理 {len(processed_files)} 個檔案:")
            for filepath in processed_files:
                print(f"- {filepath}")
        else:
            # 預設模式：獲取昨天的資料
            yesterday = datetime.now() - timedelta(days=1)
            print(
                f"目標日期: {yesterday.strftime('%Y-%m-%d')} ({yesterday.strftime('%A')})"
            )

            # 執行資料獲取和處理
            processed_files = api.save_yesterday_avg_price()

            # 檢查生成的檔案（以專案根目錄為基準）
            output_dir = os.path.join(
                os.path.dirname(os.path.dirname(__file__)), "data", "price"
            )
            filename = f"{yesterday.strftime('%Y-%m-%d')}_AVG_price.csv"
            filepath = os.path.join(output_dir, filename)
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                print(f"\n檔案資訊:")
                print(f"檔案名稱: {filename}")
                print(f"檔案路徑: {filepath}")
                print(f"檔案大小: {file_size:,} bytes")

                # 讀取並顯示統計資訊
                try:
                    import pandas as pd

                    df = pd.read_csv(filepath)

                    # 統計資料來源（以是否有價格與 datasource 是否為空為準）
                    valid_mask = df["avg_price_per_kg"].notna()
                    yesterday_mask = valid_mask & df["datasource"].isna()
                    day_before_mask = valid_mask & df["datasource"].notna()
                    yesterday_count = int(yesterday_mask.sum())
                    day_before_count = int(day_before_mask.sum())
                    null_count = int(df["avg_price_per_kg"].isna().sum())

                    print(f"\n最終資料統計:")
                    print(f"- 總蔬菜數量: {len(df)} 種")
                    print(f"- 昨天資料: {yesterday_count} 種")
                    print(f"- 前天補充: {day_before_count} 種")
                    print(f"- 無資料(Null): {null_count} 種")

                    print(f"\n資料預覽 (前5筆):")
                    preview_df = df.head(5).copy()
                    preview_df["datasource"] = preview_df["datasource"].fillna("")
                    print(preview_df.to_string(index=False))

                    print(f"\n欄位資訊:")
                    print(f"- ObsTime: 觀測時間")
                    print(f"- avg_price_per_kg: 平均價格 (元/公斤)")
                    print(f"- vege_id: 蔬果代號")
                    print(f"- datasource: 資料來源日期 (空白=當天，有日期=補充來源)")

                except Exception as e:
                    print(f"讀取檔案預覽時發生錯誤: {e}")

    except Exception as e:
        print(f"程式執行錯誤: {str(e)}")
        return 1

    print("\n" + "=" * 60)
    print("程式執行完成！")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
