from sqlalchemy import create_engine, text

# 建立資料庫連接
engine = create_engine(
    "postgresql+psycopg2://lorraine:0000@111.184.52.4:9527/postgres"
)

print("=== 檢查資料庫內容 ===")

try:
    with engine.connect() as conn:
        # 檢查高波動資料表
        high_count = conn.execute(text("SELECT COUNT(*) FROM high_volatility_merged WHERE \"ObsTime\" >= '2025-07-16' AND \"ObsTime\" <= '2025-08-12'")).scalar()
        print(f"高波動資料表中 2025-07-16 到 2025-08-12 期間的資料筆數: {high_count}")
        
        # 檢查低波動資料表  
        low_count = conn.execute(text("SELECT COUNT(*) FROM low_volatility_merged WHERE \"ObsTime\" >= '2025-07-16' AND \"ObsTime\" <= '2025-08-12'")).scalar()
        print(f"低波動資料表中 2025-07-16 到 2025-08-12 期間的資料筆數: {low_count}")
        
        print(f"總計: {high_count + low_count} 筆")
        
        # 檢查各日期的資料筆數
        print("\n各日期資料統計:")
        dates = ['2025-07-16', '2025-07-17', '2025-07-19', '2025-07-20', '2025-08-08', '2025-08-12']
        for date in dates:
            high_daily = conn.execute(text("SELECT COUNT(*) FROM high_volatility_merged WHERE \"ObsTime\" = :date"), {"date": date}).scalar()
            low_daily = conn.execute(text("SELECT COUNT(*) FROM low_volatility_merged WHERE \"ObsTime\" = :date"), {"date": date}).scalar()
            print(f"  {date}: 高波動 {high_daily} 筆, 低波動 {low_daily} 筆, 總計 {high_daily + low_daily} 筆")
            
        # 檢查2025-08-08的重複資料問題
        print(f"\n=== 檢查 2025-08-08 資料詳情 ===")
        result = conn.execute(text("SELECT vege_id, COUNT(*) as cnt FROM high_volatility_merged WHERE \"ObsTime\" = '2025-08-08' GROUP BY vege_id HAVING COUNT(*) > 1")).fetchall()
        if result:
            print("發現重複的高波動資料:")
            for row in result:
                print(f"  vege_id {row[0]}: {row[1]} 筆重複")
        else:
            print("高波動資料沒有重複")
            
        result = conn.execute(text("SELECT vege_id, COUNT(*) as cnt FROM low_volatility_merged WHERE \"ObsTime\" = '2025-08-08' GROUP BY vege_id HAVING COUNT(*) > 1")).fetchall()
        if result:
            print("發現重複的低波動資料:")
            for row in result:
                print(f"  vege_id {row[0]}: {row[1]} 筆重複")
        else:
            print("低波動資料沒有重複")

except Exception as e:
    print(f"檢查資料庫時發生錯誤: {e}")
finally:
    engine.dispose()