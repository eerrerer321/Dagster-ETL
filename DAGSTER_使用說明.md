# Dagster 定時任務使用說明

## 概述
您的 Dagster 系統已經設定為每天早上 6:00 自動執行兩個 Python 程式：
1. `APIversion-weather_data_processor-withfilter.py` - 氣象資料處理
2. `get_yesterday_avg_price-withfilter.py` - 蔬菜價格資料處理

## 啟動 Dagster

### 方法一：使用批次檔案（推薦）
```bash
雙擊 start_dagster.bat
```

### 方法二：手動啟動
```bash
# 啟動虛擬環境
dagster_venv\Scripts\activate.bat

# 啟動 Dagster
dagster dev -m dagster_project.definitions
```

## 使用 Dagster UI

1. 啟動後，瀏覽器會自動開啟 http://127.0.0.1:3000
2. 在左側選單中點擊 "Assets" 查看您的資料資產
3. 在左側選單中點擊 "Jobs" 查看任務
4. 在左側選單中點擊 "Schedules" 查看和管理排程

## 修改執行時間

### 當前設定
- **執行時間**: 每天早上 6:00
- **Cron 表達式**: `0 6 * * *`

### 如何修改時間

編輯 `dagster_project/definitions.py` 檔案，找到第 22 行：

```python
daily_data_schedule = ScheduleDefinition(
    job=daily_data_job,
    cron_schedule="0 6 * * *",  # 修改這一行
    name="daily_data_schedule",
    description="Run daily data processing at 6 AM",
)
```

### Cron 表達式格式
```
分鐘 小時 日 月 星期
*    *   *  *  *
```

### 常用時間設定範例

| 時間 | Cron 表達式 | 說明 |
|------|-------------|------|
| 每天早上 6:00 | `0 6 * * *` | 目前設定 |
| 每天早上 8:00 | `0 8 * * *` | 改為早上 8 點 |
| 每天下午 2:00 | `0 14 * * *` | 下午 2 點（24小時制） |
| 每天晚上 10:00 | `0 22 * * *` | 晚上 10 點 |
| 每天早上 7:30 | `30 7 * * *` | 早上 7 點 30 分 |
| 每週一早上 6:00 | `0 6 * * 1` | 只在週一執行 |
| 每月 1 號早上 6:00 | `0 6 1 * *` | 每月第一天執行 |

### 修改步驟

1. 停止 Dagster（按 Ctrl+C）
2. 編輯 `dagster_project/definitions.py`
3. 修改 `cron_schedule` 的值
4. 儲存檔案
5. 重新啟動 Dagster

### 範例：改為每天早上 8:00 執行

```python
daily_data_schedule = ScheduleDefinition(
    job=daily_data_job,
    cron_schedule="0 8 * * *",  # 改為早上 8:00
    name="daily_data_schedule",
    description="Run daily data processing at 8 AM",  # 更新描述
)
```

## 啟用/停用排程

1. 在 Dagster UI 中點擊 "Schedules"
2. 找到 "daily_data_schedule"
3. 點擊右側的開關來啟用或停用排程

## 手動執行任務

如果您想立即執行任務而不等待排程：

1. 在 Dagster UI 中點擊 "Jobs"
2. 找到 "daily_data_job"
3. 點擊 "Launch Run" 按鈕

## 查看執行記錄

1. 在 Dagster UI 中點擊 "Runs"
2. 查看所有任務的執行歷史和狀態
3. 點擊任何一個執行記錄查看詳細日誌

## 故障排除

### 如果排程沒有執行
1. 確認排程已啟用（在 Schedules 頁面檢查）
2. 檢查系統時間是否正確
3. 查看 Runs 頁面是否有錯誤訊息

### 如果任務執行失敗
1. 在 Runs 頁面點擊失敗的執行記錄
2. 查看錯誤日誌
3. 確認資料來源 API 是否正常
4. 檢查網路連線

## 注意事項

1. **保持 Dagster 運行**: 排程只有在 Dagster 服務運行時才會執行
2. **時區**: 時間基於系統本地時區
3. **資料儲存**: 處理後的資料會儲存在 `data/weather` 和 `data/price` 資料夾中
4. **備份**: 建議定期備份重要的資料檔案
