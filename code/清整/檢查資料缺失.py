#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
天氣資料完整性檢查工具
功能：
1. 檢查每個日期是否都有16個縣市的天氣資料
2. 識別缺失的縣市資料
3. 生成詳細的完整性報告
4. 統計資料覆蓋率
"""

import sys
import io
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from collections import defaultdict

# 設定標準輸出編碼為UTF-8，解決Windows cp950編碼問題
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

class WeatherDataCompletenessChecker:
    """天氣資料完整性檢查器"""
    
    def __init__(self, connection_string="postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"):
        """初始化"""
        self.connection_string = connection_string
        self.table_name = "weather_data"
        self.engine = None
        
        # 定義完整的16個縣市列表
        self.expected_cities = {
            'NTO', 'CYQ', 'ILN', 'PIF', 'CHA', 'NTP', 'HSC', 'TAO', 
            'TXG', 'TPE', 'TNN', 'TTT', 'HUA', 'MIA', 'YUN', 'KHH'
        }
        
        # 縣市代碼到名稱的對應
        self.city_names = {
            'NTO': '南投縣', 'CYQ': '嘉義縣', 'ILN': '宜蘭縣', 'PIF': '屏東縣',
            'CHA': '彰化縣', 'NTP': '新北市', 'HSC': '新竹縣', 'TAO': '桃園市',
            'TXG': '臺中市', 'TPE': '臺北市', 'TNN': '臺南市', 'TTT': '臺東縣',
            'HUA': '花蓮縣', 'MIA': '苗栗縣', 'YUN': '雲林縣', 'KHH': '高雄市'
        }

        # 價格資料設定
        self.price_table = "daily_avg_price"
        self.price_expected_count = 48  # 每日應有 48 種蔬菜
        self.price_date_candidates = ["ObsTime", "date", "obs_date", "Date", "obs_time"]
        self.price_id_candidates = ["vege_id", "id", "vegetable_id"]
        
    def connect_database(self):
        """連接資料庫"""
        try:
            print("[連接] 正在連接資料庫...")
            self.engine = create_engine(self.connection_string)
            # 測試連接
            with self.engine.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("SELECT 1"))
            print("[成功] 資料庫連接成功")
            return True
        except Exception as e:
            print(f"[錯誤] 資料庫連接失敗: {e}")
            return False
    
    def get_date_range(self):
        """獲取資料庫中的日期範圍"""
        try:
            query = f'''
            SELECT 
                MIN("ObsTime") as min_date,
                MAX("ObsTime") as max_date,
                COUNT(DISTINCT "ObsTime") as total_dates
            FROM "{self.table_name}"
            '''
            
            result = pd.read_sql(query, self.engine)
            min_date = result['min_date'].iloc[0]
            max_date = result['max_date'].iloc[0]
            total_dates = result['total_dates'].iloc[0]
            
            print(f"[範圍] 資料日期範圍: {min_date} ~ {max_date}")
            print(f"[統計] 總共有 {total_dates} 個不同日期的資料")
            
            return min_date, max_date, total_dates
            
        except Exception as e:
            print(f"[錯誤] 獲取日期範圍失敗: {e}")
            return None, None, 0
    
    def check_daily_completeness(self):
        """檢查每日資料完整性"""
        print(f"\n[檢查] 正在檢查每日資料完整性...")
        
        try:
            # 查詢每個日期的縣市資料數量
            query = f'''
            SELECT 
                "ObsTime",
                COUNT(*) as city_count,
                STRING_AGG(city_id, ', ' ORDER BY city_id) as cities
            FROM "{self.table_name}"
            GROUP BY "ObsTime"
            ORDER BY "ObsTime"
            '''
            
            daily_data = pd.read_sql(query, self.engine)
            
            print(f"[資料] 共查詢到 {len(daily_data)} 個日期的資料")
            
            # 分析完整性
            complete_dates = []
            incomplete_dates = []
            missing_cities_summary = defaultdict(int)
            
            for _, row in daily_data.iterrows():
                obs_time = row['ObsTime']
                city_count = row['city_count']
                cities = set(row['cities'].split(', ')) if row['cities'] else set()
                
                if city_count == 16:
                    complete_dates.append(obs_time)
                else:
                    missing_cities = self.expected_cities - cities
                    incomplete_dates.append({
                        'date': obs_time,
                        'city_count': city_count,
                        'cities': cities,
                        'missing_cities': missing_cities
                    })
                    
                    # 統計每個城市的缺失次數
                    for city in missing_cities:
                        missing_cities_summary[city] += 1
            
            return daily_data, complete_dates, incomplete_dates, missing_cities_summary
            
        except Exception as e:
            print(f"[錯誤] 檢查日期完整性失敗: {e}")
            return None, [], [], {}
    
    def generate_report(self, daily_data, complete_dates, incomplete_dates, missing_cities_summary):
        """生成詳細報告"""
        print(f"\n" + "=" * 80)
        print("天氣資料完整性檢查報告")
        print("=" * 80)
        print(f"檢查時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        total_dates = len(daily_data)
        complete_count = len(complete_dates)
        incomplete_count = len(incomplete_dates)
        completeness_rate = (complete_count / total_dates * 100) if total_dates > 0 else 0
        
        print(f"\n📊 總體統計:")
        print(f"   總日期數: {total_dates}")
        print(f"   完整日期數: {complete_count} (16縣市)")
        print(f"   不完整日期數: {incomplete_count}")
        print(f"   完整率: {completeness_rate:.2f}%")
        
        if incomplete_dates:
            print(f"\n⚠️  不完整日期詳情:")
            print("=" * 80)
            print(f"{'日期':<12} {'縣市數':<6} {'缺失縣市':<20} {'缺失縣市名稱'}")
            print("=" * 80)
            
            for item in incomplete_dates:
                date_str = str(item['date'])[:10]
                missing_cities_str = ', '.join(sorted(item['missing_cities']))
                missing_names = ', '.join([self.city_names[city] for city in sorted(item['missing_cities'])])
                print(f"{date_str:<12} {item['city_count']:<6} {missing_cities_str:<20} {missing_names}")
            
            print("=" * 80)
            
            # 統計最常缺失的縣市
            if missing_cities_summary:
                print(f"\n📈 縣市缺失統計:")
                print("=" * 50)
                print(f"{'縣市代碼':<8} {'縣市名稱':<10} {'缺失次數':<8} {'缺失率'}")
                print("=" * 50)
                
                for city, count in sorted(missing_cities_summary.items(), key=lambda x: x[1], reverse=True):
                    missing_rate = (count / total_dates * 100) if total_dates > 0 else 0
                    city_name = self.city_names.get(city, '未知')
                    print(f"{city:<8} {city_name:<10} {count:<8} {missing_rate:.1f}%")
                
                print("=" * 50)
        
        # 顯示最近的日期狀況
        print(f"\n📅 最近日期狀況 (最新10天):")
        print("=" * 60)
        print(f"{'日期':<12} {'縣市數':<6} {'狀態':<10} {'缺失縣市'}")
        print("=" * 60)
        
        recent_data = daily_data.tail(10)
        for _, row in recent_data.iterrows():
            date_str = str(row['ObsTime'])[:10]
            city_count = row['city_count']
            status = "✅ 完整" if city_count == 16 else f"❌ 不完整"
            
            if city_count == 16:
                missing_str = "-"
            else:
                cities = set(row['cities'].split(', ')) if row['cities'] else set()
                missing_cities = self.expected_cities - cities
                missing_str = ', '.join(sorted(missing_cities))
            
            print(f"{date_str:<12} {city_count:<6} {status:<10} {missing_str}")
        
        print("=" * 60)
    
    def check_specific_date_range(self, start_date=None, end_date=None):
        """檢查特定日期範圍"""
        if start_date and end_date:
            print(f"\n[特定範圍] 檢查 {start_date} 到 {end_date} 的資料...")
            
            query = f'''
            SELECT 
                "ObsTime",
                COUNT(*) as city_count,
                STRING_AGG(city_id, ', ' ORDER BY city_id) as cities
            FROM "{self.table_name}"
            WHERE "ObsTime" >= %(start_date)s AND "ObsTime" <= %(end_date)s
            GROUP BY "ObsTime"
            ORDER BY "ObsTime"
            '''
            
            try:
                range_data = pd.read_sql(query, self.engine, params={
                    'start_date': start_date,
                    'end_date': end_date
                })
                
                print(f"[結果] 指定範圍內共有 {len(range_data)} 個日期")
                
                for _, row in range_data.iterrows():
                    date_str = str(row['ObsTime'])[:10]
                    city_count = row['city_count']
                    
                    if city_count == 16:
                        print(f"  {date_str}: ✅ 完整 ({city_count}/16)")
                    else:
                        cities = set(row['cities'].split(', ')) if row['cities'] else set()
                        missing_cities = self.expected_cities - cities
                        missing_str = ', '.join(sorted(missing_cities))
                        print(f"  {date_str}: ❌ 不完整 ({city_count}/16) 缺失: {missing_str}")
                
            except Exception as e:
                print(f"[錯誤] 查詢特定範圍失敗: {e}")
    
    def suggest_fixes(self, incomplete_dates):
        """建議修復方案"""
        if not incomplete_dates:
            return
            
        print(f"\n🔧 修復建議:")
        print("=" * 50)
        
        # 分析缺失模式
        pattern_analysis = {}
        for item in incomplete_dates:
            missing_tuple = tuple(sorted(item['missing_cities']))
            if missing_tuple not in pattern_analysis:
                pattern_analysis[missing_tuple] = []
            pattern_analysis[missing_tuple].append(item['date'])
        
        print(f"發現 {len(pattern_analysis)} 種不同的缺失模式:")
        
        for i, (missing_cities, dates) in enumerate(pattern_analysis.items(), 1):
            missing_names = ', '.join([self.city_names[city] for city in missing_cities])
            print(f"\n模式 {i}: 缺失 {missing_names} ({len(missing_cities)}個縣市)")
            print(f"  影響日期數: {len(dates)}")
            print(f"  日期範圍: {min(dates)} ~ {max(dates)}")
            
            if len(dates) <= 5:
                print(f"  具體日期: {', '.join([str(d)[:10] for d in dates])}")
        
        print(f"\n建議:")
        print(f"1. 檢查天氣資料收集程式是否正常運行")
        print(f"2. 確認API回應是否包含所有縣市資料")
        print(f"3. 檢查資料庫寫入邏輯是否有錯誤")
        print(f"4. 對於歷史缺失資料，可考慮重新執行對應日期的資料收集")
    
    def close_connection(self):
        """關閉資料庫連接"""
        if self.engine:
            self.engine.dispose()
            print("[關閉] 資料庫連接已關閉")

    # ===================== 新增：價格完整性檢查 =====================
    def _detect_column(self, table: str, candidates):
        """從資料表欄位中尋找第一個存在的候選欄位名"""
        from sqlalchemy import text
        try:
            sql = text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :tbl
                """
            )
            df = pd.read_sql(sql, self.engine, params={"tbl": table})
            cols = set(df["column_name"].tolist())
            for c in candidates:
                if c in cols:
                    return c
        except Exception as e:
            print(f"[警告] 偵測 {table} 欄位失敗：{e}")
        return None

    def check_price_completeness(self, start_date: str, end_date: str):
        """檢查 daily_avg_price 在指定日期區間的每日筆數是否達到 48，並輸出分組 TXT 報告。"""
        print("\n[檢查] 正在檢查價格資料完整性 (daily_avg_price)...")

        # 偵測日期與蔬菜ID欄位
        date_col = self._detect_column(self.price_table, self.price_date_candidates)
        id_col = self._detect_column(self.price_table, self.price_id_candidates)

        if not date_col or not id_col:
            print(f"[錯誤] 無法偵測 {self.price_table} 的日期或蔬菜ID欄位，請確認資料表 schema。")
            return False

        print(f"[偵測] 使用日期欄位：{date_col}，蔬菜ID欄位：{id_col}")

        # 查詢區間內各日期的蔬菜筆數
        try:
            # 對資料表與欄位加上雙引號，避免大小寫/保留字問題
            query = (
                "SELECT CAST(\"{dc}\" AS DATE) AS dt, "
                "COUNT(DISTINCT \"{ic}\") AS cnt "
                "FROM \"{tbl}\" "
                "WHERE CAST(\"{dc}\" AS DATE) BETWEEN %(start)s AND %(end)s "
                "GROUP BY CAST(\"{dc}\" AS DATE) "
                "ORDER BY dt"
            ).format(dc=date_col, ic=id_col, tbl=self.price_table)
            df = pd.read_sql(query, self.engine, params={"start": start_date, "end": end_date})
            # 正規化 dt 欄位為 date 物件
            if not df.empty:
                try:
                    df["dt"] = pd.to_datetime(df["dt"]).dt.date
                except Exception:
                    pass
        except Exception as e:
            print(f"[錯誤] 查詢價格資料失敗：{e}")
            return False

        # 生成完整日期列表，補齊缺漏日期（視為 0 筆）
        try:
            sdt = datetime.strptime(start_date, "%Y-%m-%d").date()
            edt = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            print("[錯誤] 日期格式需為 YYYY-MM-DD")
            return False

        all_dates = []
        cur = sdt
        while cur <= edt:
            all_dates.append(cur)
            cur += timedelta(days=1)

        cnt_map = {row.dt if not hasattr(row.dt, 'date') else row.dt: int(row.cnt) for _, row in df.iterrows()}
        grouped = {}
        for d in all_dates:
            c = cnt_map.get(d, 0)
            grouped.setdefault(c, []).append(d)

        # 輸出 TXT 報告
        folder = os.path.dirname(os.path.abspath(__file__))
        out_path = os.path.join(folder, f"價格資料完整性_{start_date}_{end_date}.txt")

        try:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(f"價格資料完整性檢查報告（{start_date} ~ {end_date}）\n")
                f.write("=" * 60 + "\n\n")

                # 先寫 48 筆齊全，再寫其他分組（由高到低）
                full_days = grouped.get(self.price_expected_count, [])
                f.write(f"{self.price_expected_count}筆齊全資料: {len(full_days)}天\n\n")

                for cnt in sorted([k for k in grouped.keys() if k != self.price_expected_count], reverse=True):
                    dates = grouped[cnt]
                    if not dates:
                        continue
                    # 日期列表
                    dates_str = ", ".join([d.strftime("%Y-%m-%d") for d in dates])
                    f.write(f"{cnt}筆資料: {dates_str}\n\n")

            print(f"[完成] 已輸出價格完整性報告：{out_path}")
            # 同時印出摘要到 console
            print(f"{self.price_expected_count}筆齊全資料: {len(full_days)}天")
            for cnt in sorted([k for k in grouped.keys() if k != self.price_expected_count], reverse=True):
                print(f"{cnt}筆資料: {len(grouped[cnt])}天")
            return True
        except Exception as e:
            print(f"[錯誤] 輸出報告失敗：{e}")
            return False

def main():
    """主程式"""
    print("=" * 80)
    print("天氣資料完整性檢查工具")
    print("=" * 80)
    
    # 創建檢查器
    checker = WeatherDataCompletenessChecker()
    
    try:
        # 連接資料庫
        if not checker.connect_database():
            return 1
        
        # 獲取日期範圍
        min_date, max_date, total_dates = checker.get_date_range()
        if total_dates == 0:
            print("[警告] 資料庫中沒有天氣資料")
            return 1
        
        # 檢查每日完整性
        daily_data, complete_dates, incomplete_dates, missing_cities_summary = checker.check_daily_completeness()
        
        if daily_data is None:
            return 1
        
        # 生成報告
        checker.generate_report(daily_data, complete_dates, incomplete_dates, missing_cities_summary)
        
        # 提供修復建議（天氣資料）
        checker.suggest_fixes(incomplete_dates)

        # 新增：執行價格資料完整性檢查（固定範圍：2022-01-01 ~ 2025-08-13）
        print("\n" + "=" * 80)
        print("價格資料完整性檢查（daily_avg_price）")
        print("=" * 80)
        checker.check_price_completeness("2022-01-01", "2025-08-13")
        
        # 詢問是否要檢查特定日期範圍（僅在互動式終端執行）
        if sys.stdin.isatty():
            print(f"\n" + "=" * 80)
            while True:
                check_range = input("\n是否要檢查特定日期範圍? (y/n): ").strip().lower()
                if check_range in ['n', 'no']:
                    break
                elif check_range in ['y', 'yes']:
                    try:
                        start_input = input("請輸入開始日期 (YYYY-MM-DD): ").strip()
                        end_input = input("請輸入結束日期 (YYYY-MM-DD): ").strip()
                        
                        # 驗證日期格式
                        start_date = datetime.strptime(start_input, '%Y-%m-%d').date()
                        end_date = datetime.strptime(end_input, '%Y-%m-%d').date()
                        
                        checker.check_specific_date_range(start_date, end_date)
                        break
                        
                    except ValueError:
                        print("[錯誤] 日期格式不正確，請使用 YYYY-MM-DD 格式")
                    except Exception as e:
                        print(f"[錯誤] 檢查特定範圍時發生錯誤: {e}")
                        break
                else:
                    print("請輸入 y 或 n")
        else:
            print("\n[略過] 非互動環境，跳過特定日期範圍詢問。")
    
    except KeyboardInterrupt:
        print("\n[中斷] 程式被用戶中斷")
        return 1
    except Exception as e:
        print(f"[錯誤] 程式執行錯誤: {e}")
        return 1
    finally:
        checker.close_connection()
    
    print("\n" + "=" * 80)
    print("檢查完成！")
    print("=" * 80)
    return 0

if __name__ == "__main__":
    exit(main())