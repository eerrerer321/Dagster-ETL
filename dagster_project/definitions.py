"""Dagster definitions and schedules"""

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

from dagster_project.assets import weather_data, vegetable_price_data, merge_data, high_volatility_predictions, low_volatility_predictions, update_price_status_data

# Define a job that runs both assets
daily_data_job = define_asset_job(
    name="daily_data_job",
    selection=AssetSelection.groups("daily_data"),
    description="Daily job to process weather and vegetable price data"
)

# Define a schedule to run every day at 6 AM Taiwan time
daily_data_schedule = ScheduleDefinition(
    job=daily_data_job,
    cron_schedule="0 6 * * *",  # Every day at 6:00 AM
    execution_timezone="Asia/Taipei",
    name="daily_data_schedule",
    description="Run daily data processing at 6 AM",
)

# Define a job and schedule for merge post-process at 6:05 AM
merge_job = define_asset_job(
    name="merge_job",
    selection=AssetSelection.groups("post_process"),
    description="Daily job to merge weather and vegetable price data",
)

merge_schedule = ScheduleDefinition(
    job=merge_job,
    cron_schedule="5 6 * * *",  # Every day at 6:05 AM
    execution_timezone="Asia/Taipei",
    name="merge_schedule",
    description="Run merge post-process at 6:05 AM",
)

# Define a job and schedule for predictions at 6:10 AM
predictions_job = define_asset_job(
    name="predictions_job",
    selection=AssetSelection.groups("predictions"),
    description="Daily job to run vegetable price predictions",
)

predictions_schedule = ScheduleDefinition(
    job=predictions_job,
    cron_schedule="10 6 * * *",  # Every day at 6:10 AM
    execution_timezone="Asia/Taipei",
    name="predictions_schedule",
    description="Run vegetable price predictions at 6:10 AM",
)

# Define a job and schedule for price status update at 6:15 AM
status_update_job = define_asset_job(
    name="status_update_job",
    selection=AssetSelection.groups("status_update"),
    description="Daily job to update price status table",
)

status_update_schedule = ScheduleDefinition(
    job=status_update_job,
    cron_schedule="15 6 * * *",  # Every day at 6:15 AM
    execution_timezone="Asia/Taipei",
    name="status_update_schedule",
    description="Run price status update at 6:15 AM",
)

# Combine all definitions
defs = Definitions(
    assets=[weather_data, vegetable_price_data, merge_data, high_volatility_predictions, low_volatility_predictions, update_price_status_data],
    jobs=[daily_data_job, merge_job, predictions_job, status_update_job],
    schedules=[daily_data_schedule, merge_schedule, predictions_schedule, status_update_schedule],
)