# 蔬菜價格預測器使用說明

這是一個基於XGBoost機器學習模型的蔬菜價格預測工具，從資料庫獲取歷史資料並將預測結果直接存入資料庫。

## 檔案說明

- `models_high_next7.pkl` - 預訓練的XGBoost模型檔案
- `vegetable_price_predictor.py` - 主要預測程式（從資料庫讀取/寫入）
- `README.md` - 使用說明文件

## 系統需求

```bash
pip install pandas numpy xgboost scikit-learn sqlalchemy psycopg2-binary
```

## 資料庫結構

### 1. 歷史資料來源表 (high_volatility_merged)
```sql
CREATE TABLE public.high_volatility_merged (
    "ObsTime" date NOT NULL,
    avg_price_per_kg numeric(10, 2) NULL,
    vege_id int4 NOT NULL,
    "StnPres" numeric(10, 2) NULL,
    "Temperature" numeric(10, 2) NULL,
    "RH" numeric(10, 2) NULL,
    "WS" numeric(10, 2) NULL,
    "Precp" numeric(10, 2) NULL,
    typhoon int4 NULL
);
```

### 2. 預測結果存儲表 (price_predictions)
```sql
CREATE TABLE public.price_predictions (
    id serial4 NOT NULL,
    vege_id int4 NULL,
    predict_date date NOT NULL,    -- 執行預測的日期
    target_date date NOT NULL,     -- 預測目標日期
    predict_price numeric(10, 2) NOT NULL,
    actual_price numeric(10, 2) NULL,
    mape numeric(5, 2) NULL,
    CONSTRAINT price_predictions_pkey PRIMARY KEY (id),
    CONSTRAINT price_predictions_vege_id_target_date_key UNIQUE (vege_id, target_date)
);
```

## 使用方法

### 1. 基本使用
```bash
# 為所有可用蔬菜進行預測（使用今天的日期）
python H_vegetable_price_predictor.py
python L_vegetable_price_predictor.py

# 指定特定蔬菜進行預測
python H_vegetable_price_predictor.py --vege_ids 1,15,18
python L_vegetable_price_predictor.py --vege_ids 2,3,4

# 列出所有可用的蔬菜ID
python H_vegetable_price_predictor.py --list_vegetables
python L_vegetable_price_predictor.py --list_vegetables

# 使用更長的歷史資料（預設180天）
python H_vegetable_price_predictor.py --historical_days 120

# 多線程加速處理（預設開啟，4線程）
python H_vegetable_price_predictor.py --max_workers 8

# 關閉多線程功能，使用單線程處理
python H_vegetable_price_predictor.py --no_multithreading
```

### 2. 多線程加速功能 ⚡

**自動多線程：**
- 當指定多個預測日期時，程式自動使用多線程並行處理
- 每個線程處理一個完整的預測日期（包含該日期的所有蔬菜預測）
- 單一日期時自動使用單線程模式

**效能提升：**
```bash
# 範例：處理26天的預測（2025-08-03到2025-08-28）
# 單線程模式：約需 130-260 分鐘
python H_vegetable_price_predictor.py --no_multithreading

# 多線程模式（4線程）：約需 35-65 分鐘（約4倍加速）
python H_vegetable_price_predictor.py --max_workers 4

# 多線程模式（8線程）：約需 20-40 分鐘（約6-8倍加速）
python H_vegetable_price_predictor.py --max_workers 8
```

**多線程參數：**
```bash
# 預設使用4線程
python H_vegetable_price_predictor.py

# 自訂線程數
python H_vegetable_price_predictor.py --max_workers 6

# 關閉多線程（適用於資源受限的環境）
python H_vegetable_price_predictor.py --no_multithreading
```

**適用場景：**
- ✅ **批量補齊歷史資料**：多日期範圍自動啟用多線程
- ✅ **大量日期預測**：顯著提升處理速度
- ✅ **定期執行**：單一日期自動使用單線程
- ⚠️ **資源管理**：可根據伺服器性能調整線程數

### 3. 日期設定功能
程式碼頂部有日期設定區，可以指定執行預測的日期或日期區間：

```python
# ============== 日期設定區 ==============
# 修改這裡的日期來指定執行預測的日期範圍（對應到price_predictions表的predict_date欄位）
# 
# 批量補齊歷史預測資料：
# PREDICT_START_DATE = "2025-08-20"  # 開始日期 (YYYY-MM-DD)
# PREDICT_END_DATE = "2025-08-25"    # 結束日期 (YYYY-MM-DD)
# 
# 補齊單一日期：
# PREDICT_START_DATE = "2025-08-25"  # 指定日期 (YYYY-MM-DD)
# PREDICT_END_DATE = "2025-08-25"    # 同一日期 (YYYY-MM-DD)

# 如果要使用今天的日期執行預測，設定為 None
PREDICT_START_DATE = None
PREDICT_END_DATE = None
# =======================================
```

**使用場景：**

1. **日常執行**：
   ```python
   PREDICT_START_DATE = None
   PREDICT_END_DATE = None
   # 使用今天的日期執行預測
   ```

2. **補齊單一日期**：
   ```python
   PREDICT_START_DATE = "2025-08-25"
   PREDICT_END_DATE = "2025-08-25"
   # 只補齊8月25日的預測
   ```

3. **批量補齊歷史資料**：
   ```python
   PREDICT_START_DATE = "2025-08-20"
   PREDICT_END_DATE = "2025-08-25"
   # 補齊8月20日到8月25日的所有預測（6天）
   ```

**預測邏輯：**
- 預測基準日期：指定日期的前一天（歷史資料截止日期）
- 預測範圍：從指定日期開始的未來7天
- 批量執行：程式會自動遍歷日期範圍內的每一天

### 4. 自定義資料庫連接
```bash
python H_vegetable_price_predictor.py --db "postgresql+psycopg2://user:password@host:port/database"
```

### 5. 實際使用範例

**範例1：補齊過去一週的預測（2025-08-20到2025-08-26）**
1. 編輯 `H_vegetable_price_predictor.py`：
   ```python
   PREDICT_START_DATE = "2025-08-20"
   PREDICT_END_DATE = "2025-08-26"
   ```
2. 執行程式（多線程加速）：
   ```bash
   # 使用6線程加速處理
   python H_vegetable_price_predictor.py --historical_days 120 --max_workers 6
   ```
3. 執行結果：會產生 7天 × 19個蔬菜 × 7筆預測 = 931筆預測記錄
4. 預期耗時：約 8-15 分鐘（相比單線程的 35-70 分鐘）

**範例2：補齊特定日期（2025-08-25）**
1. 編輯 `L_vegetable_price_predictor.py`：
   ```python
   PREDICT_START_DATE = "2025-08-25"
   PREDICT_END_DATE = "2025-08-25"
   ```
2. 執行程式：
   ```bash
   python L_vegetable_price_predictor.py --vege_ids 2,3,4
   ```
3. 執行結果：會產生 1天 × 3個蔬菜 × 7筆預測 = 21筆預測記錄

**範例3：日常自動執行（使用今天日期）**
1. 保持預設設定：
   ```python
   PREDICT_START_DATE = None
   PREDICT_END_DATE = None
   ```
2. 透過Dagster每日6:10自動執行，或手動執行：
   ```bash
   python H_vegetable_price_predictor.py
   python L_vegetable_price_predictor.py
   ```

## 功能特點

### 1. 自動資料獲取
- 從 `high_volatility_merged` 表自動獲取歷史價格和天氣資料
- 預設使用最近90天的資料進行預測（可調整）
- 自動過濾出有足夠歷史資料的蔬菜

### 2. 預測功能
- 每個蔬菜預測未來7天的價格
- 使用預訓練的XGBoost模型
- 包含時間特徵、天氣特徵和價格特徵

### 3. 資料庫存儲
- 預測結果直接存入 `price_predictions` 表
- 自動處理重複資料（相同蔬菜和目標日期）
- 預測日期設為執行程式的當天日期

### 4. 實際價格更新
- 自動檢查過去3天的預測記錄
- 如果已有實際價格資料，自動更新 `actual_price`
- 自動計算並更新 MAPE (平均絕對百分比誤差)

## 輸出結果

程式執行後會顯示：
```
=== 預測結果統計 ===
總蔬菜數: 50
成功預測: 48
預測失敗: 2
存入記錄: 336  (48個蔬菜 × 7天)
更新實際價格: 12
```

## 歷史資料需求

**建議歷史資料長度：**

- **最少需求**：20天有效交易資料（模型才會執行預測）
- **建議使用**：180-360筆有效記錄（約6-12個月）
- **最佳效果**：360筆以上（獲得更穩定的特徵）

**注意**：程式自動獲取最新的 N 筆有效記錄，而非固定天數，能自動適應市場休息日。

**資料需求原因：**
- 30天滾動平均特徵（調整為最少10天）
- 各種滯後特徵（lag 1, 3, 7, 14, 30天）
- 波動率計算（調整為更靈活的最小期數）
- 季節性特徵需要更長期的觀察

## 缺失資料處理機制

### 1. 天氣資料補值邏輯
程式使用前向/後向填充來處理天氣資料的缺失值：

**填充邏輯：**
```python
# 前向填充 (Forward Fill)：用前一天的天氣資料填補
df[col] = df[col].ffill()  

# 後向填充 (Backward Fill)：如果前面沒有資料，用後一天的資料填補
df[col] = df[col].bfill()
```

**適用原理：**
- 天氣條件通常變化較為緩慢和連續
- 相鄰日期的天氣條件具有高度相關性
- 即使缺少某天的天氣資料，使用前一天的資料是合理的近似

**填充順序：**
1. 先執行前向填充（ffill）：用前一個有效值填補後面的 NaN
2. 再執行後向填充（bfill）：用後一個有效值填補前面剩餘的 NaN

**示例：**
```
原始溫度資料:    [25.0, NaN, NaN, 23.5, 24.0]
步驟1-前向填充:  [25.0, 25.0, 25.0, 23.5, 24.0]
步驟2-後向填充:  [25.0, 25.0, 25.0, 23.5, 24.0] (無變化)

開頭缺失情況:    [NaN, NaN, 23.5, 24.0, NaN]
步驟1-前向填充:  [NaN, NaN, 23.5, 24.0, 24.0]
步驟2-後向填充:  [23.5, 23.5, 23.5, 24.0, 24.0]
```

**實際應用場景：**
- 週末無氣象觀測記錄 → 使用週五的天氣資料
- 設備故障導致某日資料缺失 → 使用相鄰日期的資料
- 颱風天停止觀測 → 使用前後可用的天氣資料

**各天氣指標的處理：**
- 溫度、氣壓、濕度：前後向填充（變化緩慢）
- 降雨量：填充為0可能更合理，但程式統一使用填充邏輯
- 颱風指標：0/1值也使用相同邏輯處理

### 2. 價格資料補值機制

**滾動窗口調整：**
- 移動平均的最小期數設為窗口大小的 1/3
- 例：7天移動平均最少需要 3 天資料即可計算
- 波動率計算：7天窗口最少 3 天，14天窗口最少 5 天

**特徵工程適應：**
- 比較特徵（如 `y_above_ma7`）的 NaN 值填充為 0
- 確保即使部分資料缺失，特徵工程仍能正常運作

### 3. 市場休息日處理

**資料獲取策略：**
- 不使用固定天數限制（如90天），改為獲取最新的 N 筆有效記錄
- 如果記錄不足，自動擴大查詢範圍
- 確保獲得足夠的有效交易日資料

**最低資料需求調整：**
- 降低最低資料需求從 30 天改為 20 天
- 考慮週末和節假日的影響
- 特徵數量要求從固定 10 個降為總特徵數的一半

### 4. 完整補值流程

```
資料預處理流程：

原始資料
    ↓
天氣資料數值化 (pd.to_numeric)
    ↓
天氣資料填充 (ffill → bfill)
    ↓
建立價格特徵 (調整最小期數)
    ↓
填充比較特徵的 NaN → 0
    ↓
最終特徵矩陣 (fillna(0))
    ↓
XGBoost預測
```

**容錯機制：**
- 如果整行特徵都為 NaN 或 0，跳過該預測日
- 如果可用特徵少於要求，降低閾值繼續預測
- 個別蔬菜預測失敗不影響其他蔬菜處理

## 注意事項

1. **資料庫連接**：請確保資料庫連接字串正確且有讀寫權限
2. **歷史資料充足性**：蔬菜需要至少20天的有效價格資料才會進行預測（考慮市場休息日）
3. **重複執行**：相同蔬菜和目標日期的預測會被更新而非重複插入
4. **實際價格更新**：程式會自動更新過去3天有實際價格的預測記錄
5. **錯誤處理**：個別蔬菜預測失敗不會影響其他蔬菜的處理
6. **缺失資料處理**：程式能自動處理市場休息日和部分資料缺失的情況

## 程式執行流程

1. **日期設定**：檢查全域日期設定，決定預測執行日期
2. **載入模型**：載入預訓練的XGBoost模型
3. **連接資料庫**：連接到PostgreSQL資料庫
4. **查詢蔬菜清單**：查詢可用的蔬菜ID（資料庫中有資料且模型中有對應模型的）
5. **對每個蔬菜執行預測**：
   - 從對應的資料表獲取歷史資料（高波動：`high_volatility_merged`，低波動：`low_volatility_merged`）
   - 進行特徵工程（時間特徵、天氣特徵、價格特徵）
   - 以指定日期的前一天為基準，使用XGBoost模型預測未來7天價格
   - 將預測結果存入 `price_predictions` 表
6. **更新實際價格**：檢查並更新過去3天預測記錄的實際價格和MAPE（從 `daily_avg_price` 表獲取）

## 錯誤排除

- **連接錯誤**：檢查資料庫連接字串和網路連接
- **權限錯誤**：確保資料庫用戶有讀寫權限
- **資料不足**：某些蔬菜可能因歷史資料不足而跳過預測
- **模型載入失敗**：檢查 `models_high_next7.pkl` 檔案是否存在且完整

## 定期執行建議

建議設定每日定時執行此程式，以：
- 獲得最新的預測結果
- 更新已有實際價格的預測記錄準確度
- 保持預測資料的時效性

```bash
# 範例：每日上午8點執行
# 在 crontab 中添加：
# 0 8 * * * /path/to/python /path/to/vegetable_price_predictor.py
```