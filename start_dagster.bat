@echo off
chcp 65001
echo 啟動 Dagster 定時任務系統...
echo.

cd /d "%~dp0"

echo 檢查虛擬環境...
if not exist "dagster_venv\Scripts\activate.bat" (
    echo 錯誤：找不到虛擬環境，請先運行 setup.py
    pause
    exit /b 1
)

echo 啟動虛擬環境...
call dagster_venv\Scripts\activate.bat

echo 設定編碼環境變數...
set PYTHONIOENCODING=utf-8
set PYTHONLEGACYWINDOWSSTDIO=1
set LANG=zh_TW.UTF-8
set LC_ALL=zh_TW.UTF-8

echo 啟動 Dagster...
echo 網頁界面將在 http://127.0.0.1:3000 開啟
echo 按 Ctrl+C 停止服務
echo.

dagster dev -m dagster_project.definitions

pause
