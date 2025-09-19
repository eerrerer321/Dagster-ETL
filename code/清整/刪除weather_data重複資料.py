#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
天氣資料庫重複資料清理工具
功能：
1. 檢測 weather_data 表中的重複資料
2. 保留最新插入的資料（基於 id 或時間戳）
3. 安全刪除重複資料
4. 提供詳細的清理報告
"""

import sys
import io
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# 設定標準輸出編碼為UTF-8，解決Windows cp950編碼問題
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

class WeatherDataCleaner:
    """天氣資料清理器"""
    
    def __init__(self, connection_string="postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"):
        """初始化"""
        self.connection_string = connection_string
        self.table_name = "weather_data"
        self.engine = None
        
    def connect_database(self):
        """連接資料庫"""
        try:
            print("[連接] 正在連接資料庫...")
            self.engine = create_engine(self.connection_string)
            # 測試連接
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            print("[成功] 資料庫連接成功")
            return True
        except Exception as e:
            print(f"[錯誤] 資料庫連接失敗: {e}")
            return False
    
    def analyze_duplicates(self):
        """分析重複資料"""
        print(f"\n[分析] 正在分析 {self.table_name} 表的重複資料...")
        
        try:
            # 查詢總資料筆數
            total_query = f'SELECT COUNT(*) as total FROM "{self.table_name}"'
            total_df = pd.read_sql(total_query, self.engine)
            total_count = total_df['total'].iloc[0]
            print(f"[統計] 總資料筆數: {total_count:,}")
            
            # 查詢重複資料（基於 ObsTime + city_id 的組合）
            duplicate_query = f'''
            SELECT 
                "ObsTime", 
                city_id, 
                COUNT(*) as duplicate_count
            FROM "{self.table_name}" 
            GROUP BY "ObsTime", city_id 
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC, "ObsTime", city_id
            '''
            
            duplicate_df = pd.read_sql(duplicate_query, self.engine)
            
            if duplicate_df.empty:
                print("[結果] 沒有發現重複資料")
                return None
            
            print(f"[發現] {len(duplicate_df)} 組重複資料:")
            print("\n重複資料統計:")
            print("=" * 80)
            print(f"{'日期':<12} {'城市代號':<8} {'重複筆數':<8} {'多餘筆數':<8}")
            print("=" * 80)
            
            total_duplicates = 0
            for _, row in duplicate_df.iterrows():
                excess_count = row['duplicate_count'] - 1  # 扣掉要保留的一筆
                total_duplicates += excess_count
                obs_time_str = str(row['ObsTime'])[:10]  # 只取日期部分
                print(f"{obs_time_str:<12} {row['city_id']:<8} {row['duplicate_count']:<8} {excess_count:<8}")
            
            print("=" * 80)
            print(f"需要刪除的多餘資料總數: {total_duplicates:,} 筆")
            
            return duplicate_df
            
        except Exception as e:
            print(f"[錯誤] 分析重複資料失敗: {e}")
            return None
    
    def preview_deletion(self, duplicate_df):
        """預覽將要刪除的資料"""
        print(f"\n[預覽] 將要刪除的資料詳情:")
        
        for _, row in duplicate_df.head(5).iterrows():  # 只顯示前5組作為示例
            obs_time = row['ObsTime']
            city_id = row['city_id']
            
            # 查詢該組合的所有記錄
            detail_query = f'''
            SELECT 
                ctid,
                "ObsTime",
                city_id,
                "StnPres",
                "Temperature",
                "RH",
                typhoon,
                typhoon_name
            FROM "{self.table_name}" 
            WHERE "ObsTime" = %(obs_time)s AND city_id = %(city_id)s
            ORDER BY ctid
            '''
            
            try:
                detail_df = pd.read_sql(detail_query, self.engine, params={
                    'obs_time': obs_time,
                    'city_id': city_id
                })
                
                obs_time_str = str(obs_time)[:10]  # 只顯示日期部分
                print(f"\n{obs_time_str} - {city_id} (共 {len(detail_df)} 筆):")
                print(f"  保留: {detail_df.iloc[-1]['ctid']} (最後插入的記錄)")
                for i in range(len(detail_df) - 1):
                    print(f"  刪除: {detail_df.iloc[i]['ctid']}")
                    
            except Exception as e:
                print(f"  查詢詳情失敗: {e}")
    
    def clean_duplicates(self, duplicate_df, dry_run=True):
        """清理重複資料"""
        action = "預覽模式" if dry_run else "執行刪除"
        print(f"\n[{action}] 開始清理重複資料...")
        
        deleted_count = 0
        errors = []
        
        try:
            with self.engine.begin() as conn:  # 使用交易
                for _, row in duplicate_df.iterrows():
                    obs_time = row['ObsTime']
                    city_id = row['city_id']
                    duplicate_count = row['duplicate_count']
                    
                    # 刪除除了最後一筆以外的所有重複資料
                    # 使用ROW_NUMBER()來標識要刪除的記錄
                    delete_query = f'''
                    DELETE FROM "{self.table_name}" 
                    WHERE ctid IN (
                        SELECT ctid FROM (
                            SELECT ctid, 
                                   ROW_NUMBER() OVER (ORDER BY ctid) as rn
                            FROM "{self.table_name}" 
                            WHERE "ObsTime" = :obs_time AND city_id = :city_id
                        ) t
                        WHERE rn < :keep_row_number
                    )
                    '''
                    
                    keep_row_number = duplicate_count  # 保留最後一筆（行號最大的）
                    
                    if not dry_run:
                        try:
                            result = conn.execute(text(delete_query), {
                                'obs_time': obs_time,
                                'city_id': city_id,
                                'keep_row_number': keep_row_number
                            })
                            deleted_count += result.rowcount
                            obs_time_str = str(obs_time)[:10]
                            print(f"[刪除] {obs_time_str} - {city_id}: 刪除 {result.rowcount} 筆")
                        except Exception as e:
                            error_msg = f"{obs_time} - {city_id}: {e}"
                            errors.append(error_msg)
                            print(f"[錯誤] {error_msg}")
                    else:
                        will_delete = duplicate_count - 1  # 刪除筆數 = 總筆數 - 1 (保留的)
                        deleted_count += will_delete
                        obs_time_str = str(obs_time)[:10]
                        print(f"[預覽] {obs_time_str} - {city_id}: 將刪除 {will_delete} 筆")
            
            if not dry_run:
                print(f"\n[完成] 成功刪除 {deleted_count:,} 筆重複資料")
                if errors:
                    print(f"[警告] {len(errors)} 個錯誤:")
                    for error in errors:
                        print(f"  - {error}")
            else:
                print(f"\n[預覽完成] 預計刪除 {deleted_count:,} 筆重複資料")
                
        except Exception as e:
            print(f"[錯誤] 清理過程發生錯誤: {e}")
            if not dry_run:
                print("[回滾] 交易已回滾，沒有資料被刪除")
    
    def verify_cleanup(self):
        """驗證清理結果"""
        print(f"\n[驗證] 驗證清理結果...")
        
        try:
            # 重新檢查重複資料
            duplicate_query = f'''
            SELECT 
                COUNT(*) as duplicate_groups
            FROM (
                SELECT "ObsTime", city_id
                FROM "{self.table_name}"
                GROUP BY "ObsTime", city_id
                HAVING COUNT(*) > 1
            ) AS duplicates
            '''
            
            result = pd.read_sql(duplicate_query, self.engine)
            remaining_duplicates = result['duplicate_groups'].iloc[0]
            
            if remaining_duplicates == 0:
                print("[成功] ✅ 所有重複資料已清理完成")
            else:
                print(f"[警告] ⚠️ 仍有 {remaining_duplicates} 組重複資料")
            
            # 查詢最終資料筆數
            total_query = f'SELECT COUNT(*) as total FROM "{self.table_name}"'
            total_df = pd.read_sql(total_query, self.engine)
            final_count = total_df['total'].iloc[0]
            print(f"[統計] 清理後總資料筆數: {final_count:,}")
            
        except Exception as e:
            print(f"[錯誤] 驗證過程失敗: {e}")
    
    def close_connection(self):
        """關閉資料庫連接"""
        if self.engine:
            self.engine.dispose()
            print("[關閉] 資料庫連接已關閉")

def main():
    """主程式"""
    print("=" * 80)
    print("天氣資料庫重複資料清理工具")
    print("=" * 80)
    print(f"執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 創建清理器
    cleaner = WeatherDataCleaner()
    
    try:
        # 連接資料庫
        if not cleaner.connect_database():
            return 1
        
        # 分析重複資料
        duplicate_df = cleaner.analyze_duplicates()
        if duplicate_df is None:
            return 0
        
        # 預覽將要刪除的資料
        cleaner.preview_deletion(duplicate_df)
        
        # 詢問用戶是否要執行清理
        print("\n" + "=" * 80)
        print("⚠️  警告：以下操作將永久刪除重複的資料！")
        print("建議：在執行前先備份資料庫")
        print("=" * 80)
        
        while True:
            choice = input("\n請選擇操作:\n1. 僅預覽 (不執行刪除)\n2. 執行清理 (永久刪除重複資料)\n3. 退出\n請輸入選擇 (1/2/3): ").strip()
            
            if choice == '1':
                # 預覽模式
                cleaner.clean_duplicates(duplicate_df, dry_run=True)
                break
            elif choice == '2':
                # 確認執行
                confirm = input("\n確定要執行刪除操作嗎？這個操作不可逆！(yes/no): ").strip().lower()
                if confirm in ['yes', 'y']:
                    cleaner.clean_duplicates(duplicate_df, dry_run=False)
                    cleaner.verify_cleanup()
                else:
                    print("[取消] 操作已取消")
                break
            elif choice == '3':
                print("[退出] 程式結束")
                break
            else:
                print("請輸入有效的選擇 (1, 2, 或 3)")
        
    except KeyboardInterrupt:
        print("\n[中斷] 程式被用戶中斷")
        return 1
    except Exception as e:
        print(f"[錯誤] 程式執行錯誤: {e}")
        return 1
    finally:
        cleaner.close_connection()
    
    print("\n" + "=" * 80)
    print("清理工具執行完成！")
    print("=" * 80)
    return 0

if __name__ == "__main__":
    exit(main())