# 蔬菜價格預測系統說明文件

## 系統概述

本系統是一個完整的蔬菜價格預測平台，包含資料收集、處理、預測和狀態更新等功能模組。系統自動從政府開放資料平台獲取氣象和農產品交易資料，結合機器學習模型進行價格預測。

## 檔案結構

```
dagster_test/
├── code/
│   ├── update_price_status.py                           # 價格狀態更新
│   ├── APIversion-weather_data_processor-withfilter.py  # 氣象資料處理
│   ├── get_yesterday_avg_price-withfilter.py           # 蔬菜價格資料獲取
│   ├── merge.py                                         # 資料合併處理
│   └── predict_model/
│       ├── H_vegetable_price_predictor.py               # 高波動蔬菜預測
│       └── L_vegetable_price_predictor.py               # 低波動蔬菜預測
└── README.md                                            # 本說明文件
```

## 各檔案功能詳解

### 1. APIversion-weather_data_processor-withfilter.py
**功能**：農委會自動氣象站資料處理程式

**主要動作**：
- 從農委會API獲取前一天的自動氣象站資料
- 按縣市分組計算氣象數據平均值（溫度、濕度、氣壓、風速、降雨量）
- 檢查颱風警報狀態
- 將城市名稱轉換為標準化城市代碼
- 支援多線程並行處理提高效率
- 儲存結果到CSV檔案和PostgreSQL資料庫的weather_data表

**輸出**：
- CSV檔案：`data/weather/daily_weather_YYYYMMDD.csv`
- 資料庫表：`weather_data`

### 2. get_yesterday_avg_price-withfilter.py
**功能**：蔬菜價格資料獲取程式

**主要動作**：
- 從農委會農產品交易行情API獲取前一天的蔬菜交易資料
- 處理48種目標蔬菜的價格資料
- 計算每種蔬菜的日平均價格
- 處理缺失資料：若當天無資料則從前一天補充
- 將市場代碼轉換為內部蔬菜ID
- 支援多線程批量處理
- 儲存結果到CSV檔案和PostgreSQL資料庫

**輸出**：
- CSV檔案：`data/price/YYYY-MM-DD_AVG_price.csv`
- 資料庫表：`daily_avg_price`

### 3. merge.py
**功能**：資料合併處理程式

**主要動作**：
- 從資料庫載入天氣資料和價格資料
- 載入區域產量權重資料
- 根據各地區產量權重計算每種蔬菜的加權天氣特徵
- 合併天氣和價格資料生成完整的特徵資料集
- 根據蔬菜波動性分割資料為高波動和低波動兩組
- 支援單日處理和批量日期範圍處理
- 支援多線程並行處理

**輸出**：
- CSV檔案：`data/YYYY-MM-DD-merged.csv`
- 分割檔案：`data/splitdata/YYYY-MM-DD-high-volatility-merged.csv`
- 分割檔案：`data/splitdata/YYYY-MM-DD-low-volatility-merged.csv`
- 資料庫表：`high_volatility_merged`, `low_volatility_merged`

### 4. H_vegetable_price_predictor.py
**功能**：高波動蔬菜價格預測器

**主要動作**：
- 載入預訓練的XGBoost模型（models_high_next7.pkl）
- 從`high_volatility_merged`表獲取歷史資料
- 建立時間特徵、天氣特徵和價格特徵
- 使用滑動視窗方式預測未來7天價格
- 支援批量日期範圍預測
- 支援多線程並行處理（按日期或按蔬菜）
- 自動更新實際價格和MAPE準確度指標

**輸出**：
- 資料庫表：`price_predictions`（包含predict_date, target_date, predict_price等欄位）

### 5. L_vegetable_price_predictor.py
**功能**：低波動蔬菜價格預測器

**主要動作**：
- 載入預訓練的XGBoost模型（models_low_next7.pkl）
- 從`low_volatility_merged`表獲取歷史資料
- 建立時間特徵、天氣特徵和價格特徵
- 使用滑動視窗方式預測未來7天價格
- 支援批量日期範圍預測
- 支援多線程並行處理
- 自動更新實際價格和MAPE準確度指標

**輸出**：
- 資料庫表：`price_predictions`

### 6. update_price_status.py
**功能**：價格狀態更新程式

**主要動作**：
- 從`daily_avg_price`表獲取各蔬菜的最新實際價格
- 從`price_predictions`表獲取次日預測價格
- 計算價格變化百分比：(預測價-實際價)/實際價 × 100
- 更新`price_status`表的48筆蔬菜狀態記錄
- 支援乾跑模式（--dry-run）進行測試
- 僅更新既有記錄，不新增資料

**輸出**：
- 資料庫表：`price_status`（更新latest_price, price_change, updated_at欄位）

## 系統執行流程

### 日常自動化流程
1. **資料收集階段**
   ```bash
   python code/APIversion-weather_data_processor-withfilter.py
   python code/get_yesterday_avg_price-withfilter.py
   ```

2. **資料處理階段**
   ```bash
   python code/merge.py
   ```

3. **預測階段**
   ```bash
   python code/predict_model/H_vegetable_price_predictor.py
   python code/predict_model/L_vegetable_price_predictor.py
   ```

4. **狀態更新階段**
   ```bash
   python code/update_price_status.py
   ```

### 批量歷史資料處理
各程式都支援批量處理模式，可修改程式內的日期設定進行歷史資料補齊。

## 資料庫表結構

- **weather_data**：氣象資料表
- **daily_avg_price**：日均價格表
- **high_volatility_merged**：高波動蔬菜合併資料表
- **low_volatility_merged**：低波動蔬菜合併資料表
- **price_predictions**：價格預測結果表
- **price_status**：蔬菜價格狀態表
- **yearly_regional_normalized_yields**：年度區域標準化產量表（權重資料）

## 技術特色

- **多線程並行處理**：所有程式都支援多線程，可大幅提升處理效率
- **錯誤處理機制**：完整的例外處理和日誌記錄
- **資料完整性檢查**：自動檢查重複資料，避免重複寫入
- **彈性配置**：支援參數調整和不同執行模式
- **資料補齊機制**：自動處理缺失資料
- **準確度追蹤**：自動計算和更新預測準確度指標

## 注意事項

1. 所有程式都需要PostgreSQL資料庫連接
2. 氣象和農產品API需要網路連接
3. 預測模型檔案需要放在predict_model目錄下
4. 建議按順序執行各程式以確保資料完整性
5. 首次執行前請確認資料庫表結構已建立