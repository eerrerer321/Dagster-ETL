"""Dagster assets for daily data processing"""

import sys
import os
from datetime import datetime, timedelta

from dagster import asset, AssetExecutionContext

# Add the project root to Python path to import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 由於檔案名稱包含連字號，無法直接導入，使用 importlib 動態載入
import importlib.util

# Load weather processor
weather_spec = importlib.util.spec_from_file_location(
    "weather_processor",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "APIversion-weather_data_processor-withfilter.py")
)
weather_module = importlib.util.module_from_spec(weather_spec)
weather_spec.loader.exec_module(weather_module)
WeatherDataProcessor = weather_module.WeatherDataProcessor

# Load price processor
price_spec = importlib.util.spec_from_file_location(
    "price_processor",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "get_yesterday_avg_price-withfilter.py")
)
price_module = importlib.util.module_from_spec(price_spec)
price_spec.loader.exec_module(price_module)
AgriProductsAPI = price_module.AgriProductsAPI


@asset(group_name="daily_data")
def weather_data(context: AssetExecutionContext) -> str:
    """Process yesterday's weather data and save to CSV"""
    context.log.info("Starting weather data processing...")

    try:
        import subprocess
        import sys
        from datetime import datetime, timedelta

        # 使用子進程執行原始程式，避免編碼問題
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "APIversion-weather_data_processor-withfilter.py")

        # 設定環境變數
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONLEGACYWINDOWSSTDIO'] = '1'

        # 執行程式
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )

        # 記錄輸出
        if result.stdout:
            context.log.info(f"Weather processor stdout: {result.stdout}")
        if result.stderr:
            context.log.warning(f"Weather processor stderr: {result.stderr}")

        # 檢查執行結果
        if result.returncode == 0:
            # 檢查輸出檔案是否存在（注意：氣象檔名使用 YYYYMMDD 而非 YYYY-MM-DD）
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'weather')
            filename = f"daily_weather_{yesterday}.csv"
            filepath = os.path.join(output_dir, filename)

            if os.path.exists(filepath):
                context.log.info(f"Weather data processed successfully: {filepath}")
                return filepath
            else:
                raise Exception("Weather data file was not created")
        else:
            raise Exception(f"Weather processor failed with return code {result.returncode}")

    except Exception as e:
        context.log.error(f"Error processing weather data: {str(e)}")
        raise


@asset(group_name="daily_data")
def vegetable_price_data(context: AssetExecutionContext) -> str:
    """Process yesterday's vegetable price data and save to CSV"""
    context.log.info("Starting vegetable price data processing...")

    try:
        import subprocess
        import sys
        from datetime import datetime, timedelta

        # 使用子進程執行原始程式，避免編碼問題
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "get_yesterday_avg_price-withfilter.py")

        # 設定環境變數
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONLEGACYWINDOWSSTDIO'] = '1'

        # 執行程式
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )

        # 記錄輸出
        if result.stdout:
            context.log.info(f"Price processor stdout: {result.stdout}")
        if result.stderr:
            context.log.warning(f"Price processor stderr: {result.stderr}")

        # 檢查執行結果
        if result.returncode == 0:
            # 檢查輸出檔案是否存在
            yesterday = datetime.now() - timedelta(days=1)
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'price')
            filename = f"{yesterday.strftime('%Y-%m-%d')}_AVG_price.csv"
            filepath = os.path.join(output_dir, filename)

            if os.path.exists(filepath):
                context.log.info(f"Vegetable price data processed successfully: {filepath}")
                return filepath
            else:
                raise Exception("Vegetable price data file was not created")
        else:
            raise Exception(f"Price processor failed with return code {result.returncode}")

    except Exception as e:
        context.log.error(f"Failed to process vegetable price data: {str(e)}")
        raise


@asset(group_name="post_process")
def merge_data(context: AssetExecutionContext) -> str:
    """Run merge.py to merge yesterday's weather and price data into a single CSV"""
    context.log.info("Starting merge data post-process...")

    try:
        import subprocess
        import sys
        from datetime import datetime, timedelta

        # 使用子進程執行原始程式，避免編碼問題
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "merge.py")

        # 設定環境變數
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONLEGACYWINDOWSSTDIO'] = '1'

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )

        if result.stdout:
            context.log.info(f"Merge stdout: {result.stdout}")
        if result.stderr:
            context.log.warning(f"Merge stderr: {result.stderr}")

        if result.returncode == 0:
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            filename = f"{yesterday}-merged.csv"
            filepath = os.path.join(output_dir, filename)

            if os.path.exists(filepath):
                context.log.info(f"Merge completed successfully: {filepath}")
                return filepath
            else:
                raise Exception("Merged file was not created")
        else:
            raise Exception(f"merge.py failed with return code {result.returncode}")

    except Exception as e:
        context.log.error(f"Failed to run merge.py: {str(e)}")
        raise


@asset(group_name="predictions")
def high_volatility_predictions(context: AssetExecutionContext) -> str:
    """Run H_vegetable_price_predictor.py to predict prices for high volatility vegetables"""
    context.log.info("Starting high volatility vegetable price predictions...")

    try:
        import subprocess
        import sys

        # 使用子進程執行高波動預測程式
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "predict_model", "H_vegetable_price_predictor.py")

        # 設定環境變數
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONLEGACYWINDOWSSTDIO'] = '1'

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            cwd=os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "predict_model")
        )

        if result.stdout:
            context.log.info(f"High volatility predictor stdout: {result.stdout}")
        if result.stderr:
            context.log.warning(f"High volatility predictor stderr: {result.stderr}")

        if result.returncode == 0:
            context.log.info("High volatility predictions completed successfully")
            return "High volatility predictions completed"
        else:
            raise Exception(f"High volatility predictor failed with return code {result.returncode}")

    except Exception as e:
        context.log.error(f"Failed to run high volatility predictor: {str(e)}")
        raise


@asset(group_name="predictions")
def low_volatility_predictions(context: AssetExecutionContext) -> str:
    """Run L_vegetable_price_predictor.py to predict prices for low volatility vegetables"""
    context.log.info("Starting low volatility vegetable price predictions...")

    try:
        import subprocess
        import sys

        # 使用子進程執行低波動預測程式
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "predict_model", "L_vegetable_price_predictor.py")

        # 設定環境變數
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONLEGACYWINDOWSSTDIO'] = '1'

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            cwd=os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "predict_model")
        )

        if result.stdout:
            context.log.info(f"Low volatility predictor stdout: {result.stdout}")
        if result.stderr:
            context.log.warning(f"Low volatility predictor stderr: {result.stderr}")

        if result.returncode == 0:
            context.log.info("Low volatility predictions completed successfully")
            return "Low volatility predictions completed"
        else:
            raise Exception(f"Low volatility predictor failed with return code {result.returncode}")

    except Exception as e:
        context.log.error(f"Failed to run low volatility predictor: {str(e)}")
        raise


@asset(group_name="status_update")
def update_price_status_data(context: AssetExecutionContext) -> str:
    """Run update_price_status.py to update price status table"""
    context.log.info("Starting price status update...")

    try:
        import subprocess
        import sys

        # 使用子進程執行價格狀態更新程式
        script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "code", "update_price_status.py")

        # 設定環境變數
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        env['PYTHONLEGACYWINDOWSSTDIO'] = '1'

        # 執行程式（不使用 --dry-run，直接更新資料庫）
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            env=env,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )

        if result.stdout:
            context.log.info(f"Price status update stdout: {result.stdout}")
        if result.stderr:
            context.log.warning(f"Price status update stderr: {result.stderr}")

        if result.returncode == 0:
            context.log.info("Price status update completed successfully")
            return "Price status update completed"
        else:
            raise Exception(f"Price status update failed with return code {result.returncode}")

    except Exception as e:
        context.log.error(f"Failed to run price status update: {str(e)}")
        raise