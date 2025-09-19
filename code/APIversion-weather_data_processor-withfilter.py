#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
農委會自動氣象站資料處理程式
=============================

功能說明：
1. 獲取前一天的自動氣象站資料
2. 按縣市分組計算平均值
3. 儲存為CSV檔案並寫入資料庫
4. 支援多線程並行處理提高效率
5. 具備颱風警報檢查功能
"""

# 引入系統相關函式庫
import sys  # 系統相關功能
import io   # 輸入輸出處理
import os   # 作業系統介面
from sqlalchemy import create_engine  # 資料庫連接

# 處理Windows系統的編碼問題，設定標準輸出編碼為UTF-8
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
    os.environ["PYTHONIOENCODING"] = "utf-8"

# 引入其他必要的函式庫
import requests  # HTTP請求處理
import pandas as pd  # 資料分析和處理
from datetime import datetime, timedelta  # 日期時間處理
from typing import Dict, List, Any  # 型別提示
import json  # JSON資料處理
from requests.adapters import HTTPAdapter  # HTTP適配器
from urllib3.util.retry import Retry  # 重試機制
from concurrent.futures import ThreadPoolExecutor, as_completed  # 多線程處理


class WeatherDataProcessor:
    """
    氣象資料處理器類別

    負責從農委會API獲取氣象資料，進行處理計算後儲存到檔案和資料庫
    具備颱風警報檢查功能和多線程處理能力
    """

    def __init__(self):
        """
        初始化氣象資料處理器

        設定API端點、檔案路徑、多線程參數等基本配置
        """
        # 農委會自動氣象站資料API端點
        self.api_url = "https://data.moa.gov.tw/api/v1/AutoWeatherStationType/"

        # 設定輸出目錄：使用相對路徑指向專案根目錄下的data/weather資料夾
        self.output_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "data", "weather"
        )

        # 颱風警報相關API端點
        self.typhoon_api_url = "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/get_warning_typhoon"
        self.typhoon_main_url = (
            "https://rdc28.cwa.gov.tw/TDB/public/warning_typhoon_list/"
        )

        # 多線程處理設定
        self.use_multithreading = True  # 是否啟用多線程
        self.max_workers = 4  # 最大工作線程數

        # 城市名稱到城市代碼的對應表
        # 用於將API回傳的完整城市名稱轉換為資料庫使用的簡化代碼
        self.city_mapping = {
            "南投縣": "NTO",
            "臺中市": "TXG",
            "台中市": "TXG",  # API可能使用簡體字，保持相容性
            "臺北市": "TPE",
            "台北市": "TPE",  # API可能使用簡體字，保持相容性
            "臺南市": "TNN",
            "台南市": "TNN",  # API可能使用簡體字，保持相容性
            "臺東縣": "TTT",
            "台東縣": "TTT",  # API可能使用簡體字，保持相容性
            "嘉義市": "CYI",
            "嘉義縣": "CYQ",
            "基隆市": "KEE",
            "宜蘭縣": "ILN",
            "屏東縣": "PIF",
            "彰化縣": "CHA",
            "新北市": "NTP",
            "新竹市": "HSZ",
            "新竹縣": "HSC",
            "桃園市": "TAO",
            "澎湖縣": "PEN",
            "花蓮縣": "HUA",
            "苗栗縣": "MIA",
            "金門縣": "KIN",
            "雲林縣": "YUN",
            "高雄市": "KHH",
            "連江縣": "LCC",
        }

    def create_scraper_session(self):
        """
        建立模擬瀏覽器的HTTP Session

        設定重試機制和常用的瀏覽器Headers，提高網站爬取成功率

        返回:
            requests.Session: 配置好的Session物件
        """
        # 建立新的HTTP Session
        session = requests.Session()

        # 設定重試策略：當遇到特定錯誤時自動重試
        retry_strategy = Retry(
            total=3,  # 最多重試3次
            backoff_factor=1,  # 重試間隔倍數
            status_forcelist=[429, 500, 502, 503, 504],  # 需要重試的HTTP狀態碼
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        # 將重試策略套用到HTTP和HTTPS連接
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 設定常用的瀏覽器Headers，模擬真實的瀏覽器請求
        session.headers.update(
            {
                # 模擬Chrome瀏覽器的User-Agent
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                # 接受的內容類型
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                # 接受的語言：繁體中文優先
                "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
                # 接受的編碼方式
                "Accept-Encoding": "gzip, deflate, br",
                # 保持連接活躍
                "Connection": "keep-alive",
                # 自動升級不安全請求
                "Upgrade-Insecure-Requests": "1",
            }
        )

        return session

    def check_yesterday_typhoon_warning(self, target_date=None):
        """檢查指定日期是否有颱風警報"""

        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)
        elif isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d")

        print(f"[檢查] {target_date.strftime('%Y-%m-%d')} 是否有颱風警報")

        return self.scrape_typhoon_warning_history(target_date, days_range=0)

    def scrape_typhoon_warning_history(self, target_date=None, days_range=1):
        """爬取指定日期前N天的颱風警報歷史資料"""

        if target_date is None:
            target_date = datetime.now()
        elif isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d")

        query_date = target_date - timedelta(days=days_range)
        print(f"[查詢] {query_date.strftime('%Y-%m-%d')} 的颱風警報資料")

        # 建立session
        session = self.create_scraper_session()

        try:
            # 第一步：訪問主頁面建立session
            print("[步驟1] 訪問主頁面建立session...")

            response = session.get(self.typhoon_main_url, timeout=30, verify=False)
            print(f"   主頁面狀態碼: {response.status_code}")

            if response.status_code != 200:
                print("[錯誤] 無法訪問主頁面")
                return None

            # 第二步：POST請求取得警報資料
            all_warnings = []
            target_year = target_date.year

            # POST參數
            post_data = {"year": str(target_year)}

            print(f"[步驟2] 嘗試POST請求...")
            print(f"   參數: {post_data}")

            # 更新headers
            session.headers.update(
                {
                    "Referer": self.typhoon_main_url,
                    "X-Requested-With": "XMLHttpRequest",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                }
            )

            try:
                response = session.post(
                    self.typhoon_api_url, data=post_data, timeout=30, verify=False
                )
                print(f"   狀態碼: {response.status_code}")

                if response.status_code == 200:
                    response_text = response.text.strip()

                    # 檢查是否為錯誤訊息
                    if "No direct script access allowed" in response_text:
                        print("   [錯誤] 防護機制阻擋")
                        return None

                    # 嘗試解析JSON（處理BOM）
                    try:
                        # 移除BOM (Byte Order Mark)
                        clean_text = response_text
                        if clean_text.startswith("\ufeff"):
                            clean_text = clean_text[1:]

                        data = json.loads(clean_text)
                        print("   [成功] 取得JSON資料！")
                        print(
                            f"   [資料] 筆數: {len(data) if isinstance(data, list) else '1 (dict)'}"
                        )

                        # 處理資料
                        if isinstance(data, list) and data:
                            all_warnings.extend(data)
                            print(f"   [成功] 加入 {len(data)} 筆警報資料")

                    except json.JSONDecodeError as e:
                        print(f"   [錯誤] JSON解析錯誤: {e}")
                        return None

            except requests.exceptions.RequestException as e:
                print(f"   [錯誤] 請求錯誤: {e}")
                return None

            # 處理結果
            if all_warnings:
                print(f"\n[成功] 取得 {len(all_warnings)} 筆颱風警報資料")

                # 篩選指定日期範圍的資料
                filtered_warnings = self.filter_warnings_by_date(
                    all_warnings, target_date, days_range
                )

                if filtered_warnings:
                    print(f"\n🚨 找到 {len(filtered_warnings)} 個颱風警報！")

                    for warning in filtered_warnings:
                        typhoon_name = warning.get("cht_name", "N/A")
                        eng_name = warning.get("eng_name", "N/A")
                        sea_start = warning.get("sea_start_datetime", "N/A")
                        sea_end = warning.get("sea_end_datetime", "N/A")

                        print(f"[警報] 颱風: {typhoon_name} ({eng_name})")
                        print(f"   海警期間: {sea_start} ~ {sea_end}")

                    return {
                        "has_warning": True,
                        "warnings": filtered_warnings,
                        "summary": f"颱風警報: {', '.join([w.get('cht_name', 'N/A') for w in filtered_warnings])}",
                    }
                else:
                    query_date = target_date - timedelta(days=days_range)
                    print(f"\n[正常] {query_date.strftime('%Y-%m-%d')} 沒有颱風警報")

                    return {
                        "has_warning": False,
                        "warnings": [],
                        "summary": f"{query_date.strftime('%Y-%m-%d')} 無颱風警報",
                    }
            else:
                print("[錯誤] 無法取得颱風警報資料")
                return {
                    "has_warning": False,
                    "warnings": [],
                    "summary": "無法查詢颱風警報資料",
                }

        except Exception as e:
            print(f"[錯誤] 爬蟲錯誤: {e}")

        finally:
            session.close()

        return None

    def filter_warnings_by_date(self, warnings, target_date, days_range):
        """篩選指定日期範圍內的警報"""
        if days_range == 0:
            query_date = target_date
        else:
            query_date = target_date - timedelta(days=days_range)

        filtered = []
        for warning in warnings:
            # 檢查警報是否在查詢日期當天有效
            sea_start = warning.get("sea_start_datetime")
            sea_end = warning.get("sea_end_datetime")

            if sea_start:
                try:
                    start_dt = datetime.strptime(sea_start[:19], "%Y-%m-%d %H:%M:%S")

                    # 處理結束時間為空的情況（進行中的颱風警報）
                    if sea_end and sea_end.strip():
                        end_dt = datetime.strptime(sea_end[:19], "%Y-%m-%d %H:%M:%S")
                    else:
                        # 如果結束時間為空，假設警報仍在進行中，設定一個未來時間
                        end_dt = datetime.now() + timedelta(days=30)  # 假設最多30天

                    # 檢查查詢日期是否在警報期間內
                    query_start = query_date.replace(hour=0, minute=0, second=0)
                    query_end = query_date.replace(hour=23, minute=59, second=59)

                    # 如果警報期間與查詢日期有重疊，就算符合
                    if not (end_dt < query_start or start_dt > query_end):
                        filtered.append(warning)

                except Exception as e:
                    continue

        return filtered

    def get_weather_data(
        self, start_date: str, end_date: str = None
    ) -> List[Dict[str, Any]]:
        """
        從農委會API獲取指定日期範圍的氣象資料

        參數:
            start_date: 開始日期 (YYYY-MM-DD 格式)
            end_date: 結束日期 (可選，預設與開始日期相同)

        返回:
            List[Dict]: 氣象站資料列表
        """
        # 如果沒有指定結束日期，則與開始日期相同
        if end_date is None:
            end_date = start_date

        print(f"[獲取] 正在獲取 {start_date} 到 {end_date} 的氣象資料...")

        # 設定查詢參數
        params = {"Start_time": start_date, "End_time": end_date}

        try:
            # 發送HTTP GET請求到農委會API
            response = requests.get(
                self.api_url, params=params, timeout=60, verify=False
            )
            # 檢查HTTP狀態碼，如果錯誤則拋出例外
            response.raise_for_status()

            # 解析JSON回應
            data = response.json()
            if "Data" in data:
                weather_data = data["Data"]
                print(f"[成功] 獲取 {len(weather_data)} 筆氣象資料")
                return weather_data
            else:
                print("[錯誤] API回應中沒有Data欄位")
                return []

        except requests.exceptions.RequestException as e:
            print(f"[錯誤] API請求失敗: {e}")
            return []
        except json.JSONDecodeError as e:
            print(f"[錯誤] JSON解析失敗: {e}")
            return []

    def get_yesterday_weather_data(self) -> List[Dict[str, Any]]:
        """獲取前一天的氣象資料（保持向後相容）"""
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        return self.get_weather_data(yesterday)

    def filter_valid_stations(
        self, weather_data: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """過濾出有效的氣象站資料（有完整數據的站點）並排除指定城市"""
        valid_data = []
        # 定義要排除的城市名稱
        excluded_city_names = {
            "嘉義市",
            "基隆市",
            "新竹市",
            "澎湖縣",
            "金門縣",
            "連江縣",
        }

        for record in weather_data:
            # 檢查是否有城市資訊和必要的氣象數據
            city = record.get("CITY", "")
            temp = record.get("TEMP")
            pres = record.get("PRES")

            # 過濾條件：有城市資訊且有基本氣象數據，且不在排除清單中
            if (
                city
                and temp is not None
                and pres is not None
                and city not in excluded_city_names
            ):
                valid_data.append(record)

        print(
            f"[過濾] 有效氣象站資料: {len(valid_data)} 筆（已排除 {len(excluded_city_names)} 個指定城市）"
        )
        return valid_data

    def calculate_city_averages(
        self, weather_data: List[Dict[str, Any]], target_date: str = None
    ) -> pd.DataFrame:
        """計算各縣市的氣象資料平均值"""
        print("[計算] 正在計算各縣市平均值...")

        # 轉換為DataFrame
        df = pd.DataFrame(weather_data)

        # 轉換數值欄位
        numeric_fields = ["TEMP", "HUMD", "PRES", "WDSD", "H_24R"]
        for field in numeric_fields:
            df[field] = pd.to_numeric(df[field], errors="coerce")

        # 按城市分組計算平均值
        city_averages = (
            df.groupby("CITY")
            .agg(
                {
                    "PRES": "mean",  # StnPres
                    "TEMP": "mean",  # Temperature
                    "HUMD": "mean",  # RH
                    "WDSD": "mean",  # WS
                    "H_24R": "mean",  # Precp
                }
            )
            .round(2)
        )

        # 重新命名欄位
        city_averages.columns = ["StnPres", "Temperature", "RH", "WS", "Precp"]

        # 重設索引，讓CITY成為一般欄位
        city_averages.reset_index(inplace=True)

        # 將城市名稱轉換為城市代號
        city_averages["city_id"] = city_averages["CITY"].map(self.city_mapping)

        # 移除沒有對應代號的城市
        city_averages = city_averages.dropna(subset=["city_id"])

        # 加入觀測時間
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
        else:
            # 將 YYYY-MM-DD 格式轉換為 YYYY/MM/DD 格式
            target_date = target_date.replace("-", "/")
        city_averages["ObsTime"] = target_date

        # 檢查颱風狀況（使用指定日期）
        if target_date:
            check_date = target_date.replace("/", "-")  # 轉換回 YYYY-MM-DD 格式
            typhoon_result = self.check_yesterday_typhoon_warning(check_date)
        else:
            typhoon_result = self.check_yesterday_typhoon_warning()

        if typhoon_result and typhoon_result["has_warning"]:
            city_averages["typhoon"] = "1"
            # 取第一個颱風的英文名稱
            first_typhoon = typhoon_result["warnings"][0]
            city_averages["typhoon_name"] = first_typhoon.get("eng_name", "")
        else:
            city_averages["typhoon"] = "0"
            city_averages["typhoon_name"] = ""

        # 重新排列欄位順序
        city_averages = city_averages[
            [
                "city_id",
                "ObsTime",
                "StnPres",
                "Temperature",
                "RH",
                "WS",
                "Precp",
                "typhoon",
                "typhoon_name",
            ]
        ]

        print(f"[完成] {len(city_averages)} 個縣市的平均值計算")

        # 顯示處理的城市
        print("處理的城市:")
        for _, row in city_averages.iterrows():
            print(
                f"  {row['city_id']}: 溫度 {row['Temperature']:.1f}°C, 濕度 {row['RH']:.1f}%"
            )

        return city_averages

    def save_to_csv(self, data: pd.DataFrame, target_date: str = None) -> str:
        """儲存資料為CSV檔案"""
        # 確保輸出目錄存在
        os.makedirs(self.output_dir, exist_ok=True)

        # 產生檔案名稱
        if target_date is None:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        else:
            # 將 YYYY-MM-DD 格式轉換為 YYYYMMDD 格式
            date_str = target_date.replace("-", "")
        filename = f"daily_weather_{date_str}.csv"
        filepath = os.path.join(self.output_dir, filename)

        try:
            data.to_csv(filepath, index=False, encoding="utf-8-sig")
            print(f"[儲存] 資料已儲存至: {filepath}")
            return filepath
        except Exception as e:
            print(f"[錯誤] 儲存檔案失敗: {e}")
            raise

    def process_single_date(self, date_str: str) -> str:
        """處理單一日期的氣象資料"""
        print(f"\n[處理] {date_str}")
        
        try:
            # 1. 獲取指定日期的氣象資料
            weather_data = self.get_weather_data(date_str)
            if not weather_data:
                print(f"[警告] 無法獲取 {date_str} 的氣象資料，跳過此日期")
                return None

            # 2. 過濾有效的氣象站資料
            valid_data = self.filter_valid_stations(weather_data)
            if not valid_data:
                print(f"[警告] {date_str} 沒有有效的氣象站資料，跳過此日期")
                return None

            # 3. 計算各縣市平均值
            city_averages = self.calculate_city_averages(valid_data, date_str)
            if city_averages.empty:
                print(f"[警告] {date_str} 無法計算縣市平均值，跳過此日期")
                return None

            # 4. 顯示結果摘要
            print(f"\n[摘要] {date_str} 處理結果:")
            print(f"   處理縣市數: {len(city_averages)}")
            print(f"   平均溫度範圍: {city_averages['Temperature'].min():.1f}°C - {city_averages['Temperature'].max():.1f}°C")
            print(f"   平均濕度範圍: {city_averages['RH'].min():.1f}% - {city_averages['RH'].max():.1f}%")

            # 5. 儲存為CSV
            filepath = self.save_to_csv(city_averages, date_str)

            # 6. 寫入資料庫（避免重複：相同 ObsTime + city_id 不重複寫入）
            try:
                # 檢查是否有 psycopg2
                try:
                    import psycopg2

                    print(f"[資料庫] 正在連接 PostgreSQL ({date_str})...")

                    engine = create_engine(
                        "postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"
                    )
                    table_name = "weather_data"

                    # 讀取已存在鍵值（同一天已存在的 city_id）
                    obs_time = date_str.replace("-", "/")
                    try:
                        existing = pd.read_sql(
                            f'SELECT city_id FROM {table_name} WHERE "ObsTime" = %(obs)s',
                            con=engine,
                            params={"obs": obs_time},
                        )
                        existing_ids = set(existing["city_id"].astype(str))
                        print(f"[資料庫] 發現已存在 {len(existing_ids)} 筆相同日期資料 ({date_str})")
                    except Exception as db_error:
                        print(f"[資料庫] 查詢現有資料失敗：{db_error}")
                        existing_ids = set()

                    # 準備要寫入的資料（排除已存在鍵值）
                    df_to_insert = city_averages.copy()
                    df_to_insert["city_id"] = df_to_insert["city_id"].astype(str)
                    if existing_ids:
                        df_to_insert = df_to_insert[~df_to_insert["city_id"].isin(existing_ids)]

                    # 將 NaN 轉為 None，以便寫入為 NULL
                    df_to_insert = df_to_insert.where(pd.notna(df_to_insert), None)

                    if len(df_to_insert) > 0:
                        df_to_insert.to_sql(table_name, engine, if_exists="append", index=False)
                        print(f"[資料庫] 寫入成功 ({date_str})：新增 {len(df_to_insert)} 筆，略過 {len(city_averages) - len(df_to_insert)} 筆（已存在）")
                    else:
                        print(f"[資料庫] 寫入略過 ({date_str})：相同日期與 city_id 的資料已存在")

                    engine.dispose()  # 關閉連接

                except ImportError:
                    print(f"[警告] 無法匯入 psycopg2，跳過資料庫寫入 ({date_str})")

            except Exception as e:
                print(f"[資料庫] 寫入失敗 ({date_str})：{e}")

            return filepath

        except Exception as e:
            print(f"[錯誤] 處理 {date_str} 發生錯誤: {e}")
            return None

    def process_weather_data(self, start_date: str = None, end_date: str = None):
        """主要處理流程"""
        if start_date is None:
            # 如果沒指定日期，預設為昨天
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        if end_date is None:
            end_date = start_date

        print(f"[開始] 處理氣象資料 ({start_date} 到 {end_date})...")
        print("=" * 50)

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
                future_to_date = {executor.submit(self.process_single_date, date_str): date_str 
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
                result = self.process_single_date(date_str)
                if result:
                    processed_files.append(result)

        print(f"\n[完成] 氣象資料處理完成！")
        print(f"成功處理 {len(processed_files)} 個檔案")
        print("=" * 50)

        return processed_files


def main():
    """主程式"""
    # ============== 執行參數設定區 ==============
    USE_MULTITHREADING = True          # 是否使用多線程 (True/False)
    MAX_WORKERS = 10                   # 最大線程數 (建議設為你的CPU線程數-2)
    
    # ============== 日期設定區 ==============
    # 修改這裡的日期來指定要下載的範圍
    # START_DATE = "2022-01-01"  # 開始日期 (YYYY-MM-DD)
    # END_DATE = "2025-08-13"  # 結束日期 (YYYY-MM-DD)，設為 None 則只下載開始日期當天

    # 如果要下載昨天的資料，設定為 None
    START_DATE = None
    END_DATE = None
    # =======================================

    processor = WeatherDataProcessor()
    processor.use_multithreading = USE_MULTITHREADING
    processor.max_workers = MAX_WORKERS

    if START_DATE is None:
        # 預設模式：下載昨天的資料
        processor.process_weather_data()
    else:
        # 指定日期模式
        processor.process_weather_data(START_DATE, END_DATE)


if __name__ == "__main__":
    main()
