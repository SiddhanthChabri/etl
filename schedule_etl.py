"""
ETL Scheduler for Retail Data Warehouse
Automates ETL execution with scheduling, error handling, retry logic, and email notifications
Author: [Your Name]
Date: February 2026
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import time
import logging
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import sys
import os

# Import your ETL modules
try:
    from run_etl import main as run_etl_pipeline
    from logger_config import setup_logger
except ImportError as e:
    print(f"Error importing ETL modules: {e}")
    sys.exit(1)

# Setup logger
logger = setup_logger('etl_scheduler')

# ========================
# CONFIGURATION
# ========================

class SchedulerConfig:
    """Configuration for ETL Scheduler"""
    
    # Schedule Settings (CRON Format)
    # Run daily at 2:00 AM
    ETL_SCHEDULE_CRON = "0 2 * * *"  # minute hour day month day_of_week
    
    # Alternative schedules (comment/uncomment as needed):
    # ETL_SCHEDULE_CRON = "0 */6 * * *"  # Every 6 hours
    # ETL_SCHEDULE_CRON = "0 0 * * 0"    # Every Sunday at midnight
    # ETL_SCHEDULE_CRON = "*/30 * * * *" # Every 30 minutes (for testing)
    
    # Retry Settings
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 300  # 5 minutes
    EXPONENTIAL_BACKOFF = True  # 5min, 10min, 20min
    
    # Email Notification Settings
    ENABLE_EMAIL_NOTIFICATIONS = True
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    SENDER_EMAIL = "your_email@gmail.com"
    SENDER_PASSWORD = "your_app_password"  # Use App Password for Gmail
    RECIPIENT_EMAILS = ["recipient1@example.com", "recipient2@example.com"]
    
    # Job Store (for persistent scheduling)
    USE_PERSISTENT_STORE = False  # Set True for production
    DATABASE_URL = "sqlite:///etl_jobs.sqlite"  # For job persistence
    
    # Timezone
    TIMEZONE = "Asia/Kolkata"  # IST


# ========================
# EMAIL NOTIFICATION FUNCTIONS
# ========================

def send_email_notification(subject, body, is_html=False):
    """
    Send email notification
    
    Args:
        subject (str): Email subject
        body (str): Email body content
        is_html (bool): Whether body contains HTML
    """
    if not SchedulerConfig.ENABLE_EMAIL_NOTIFICATIONS:
        logger.info("Email notifications disabled")
        return
    
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = SchedulerConfig.SENDER_EMAIL
        msg['To'] = ', '.join(SchedulerConfig.RECIPIENT_EMAILS)
        msg['Subject'] = subject
        
        # Attach body
        mime_type = 'html' if is_html else 'plain'
        msg.attach(MIMEText(body, mime_type))
        
        # Connect and send
        server = smtplib.SMTP(SchedulerConfig.SMTP_SERVER, SchedulerConfig.SMTP_PORT)
        server.starttls()
        server.login(SchedulerConfig.SENDER_EMAIL, SchedulerConfig.SENDER_PASSWORD)
        server.send_message(msg)
        server.quit()
        
        logger.info(f"Email sent successfully: {subject}")
        
    except Exception as e:
        logger.error(f"Failed to send email notification: {e}")


def send_success_email(duration_seconds, records_processed=None):
    """Send success notification email"""
    duration_mins = duration_seconds / 60
    
    subject = "✅ ETL Job Completed Successfully"
    body = f"""
    <html>
        <body>
            <h2 style="color: green;">ETL Job Completed Successfully</h2>
            <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Duration:</strong> {duration_mins:.2f} minutes ({duration_seconds:.1f} seconds)</p>
            <p><strong>Status:</strong> All ETL processes completed without errors</p>
            {f'<p><strong>Records Processed:</strong> {records_processed}</p>' if records_processed else ''}
            <hr>
            <p style="color: gray; font-size: 12px;">Automated message from Retail DW ETL Scheduler</p>
        </body>
    </html>
    """
    send_email_notification(subject, body, is_html=True)


def send_failure_email(error_message, retry_count=0):
    """Send failure notification email"""
    subject = f"❌ ETL Job Failed (Attempt {retry_count + 1}/{SchedulerConfig.MAX_RETRIES})"
    body = f"""
    <html>
        <body>
            <h2 style="color: red;">ETL Job Failed</h2>
            <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Error:</strong></p>
            <pre style="background-color: #f4f4f4; padding: 10px; border-radius: 5px;">{error_message}</pre>
            <p><strong>Retry Attempt:</strong> {retry_count + 1} of {SchedulerConfig.MAX_RETRIES}</p>
            <hr>
            <p style="color: gray; font-size: 12px;">Automated message from Retail DW ETL Scheduler</p>
        </body>
    </html>
    """
    send_email_notification(subject, body, is_html=True)


# ========================
# ETL EXECUTION WITH RETRY LOGIC
# ========================

def execute_etl_with_retry():
    """
    Execute ETL pipeline with retry logic and exponential backoff
    
    Returns:
        bool: True if successful, False otherwise
    """
    retry_count = 0
    
    while retry_count <= SchedulerConfig.MAX_RETRIES:
        try:
            logger.info(f"{'='*60}")
            logger.info(f"Starting ETL execution (Attempt {retry_count + 1}/{SchedulerConfig.MAX_RETRIES + 1})")
            logger.info(f"{'='*60}")
            
            start_time = time.time()
            
            # Execute your main ETL pipeline
            run_etl_pipeline()
            
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"{'='*60}")
            logger.info(f"ETL completed successfully in {duration:.2f} seconds")
            logger.info(f"{'='*60}")
            
            # Send success notification
            send_success_email(duration)
            
            return True
            
        except Exception as e:
            logger.error(f"ETL execution failed (Attempt {retry_count + 1}): {str(e)}")
            
            retry_count += 1
            
            if retry_count <= SchedulerConfig.MAX_RETRIES:
                # Calculate delay with exponential backoff
                if SchedulerConfig.EXPONENTIAL_BACKOFF:
                    delay = SchedulerConfig.RETRY_DELAY_SECONDS * (2 ** (retry_count - 1))
                else:
                    delay = SchedulerConfig.RETRY_DELAY_SECONDS
                
                logger.warning(f"Retrying in {delay} seconds...")
                
                # Send failure notification (but not final failure yet)
                send_failure_email(str(e), retry_count - 1)
                
                time.sleep(delay)
            else:
                logger.critical(f"ETL failed after {SchedulerConfig.MAX_RETRIES} retries")
                
                # Send final failure notification
                send_failure_email(f"FINAL FAILURE after {SchedulerConfig.MAX_RETRIES} retries:\n{str(e)}", 
                                  retry_count - 1)
                
                return False
    
    return False


# ========================
# SCHEDULER EVENT LISTENERS
# ========================

def job_executed_listener(event):
    """Callback when job executes successfully"""
    logger.info(f"Job {event.job_id} executed successfully at {datetime.now()}")


def job_error_listener(event):
    """Callback when job encounters error"""
    logger.error(f"Job {event.job_id} raised {event.exception}")
    logger.error(f"Traceback: {event.traceback}")


# ========================
# SCHEDULER INITIALIZATION
# ========================

def initialize_scheduler():
    """
    Initialize and configure APScheduler
    
    Returns:
        BackgroundScheduler: Configured scheduler instance
    """
    logger.info("Initializing ETL Scheduler...")
    
    # Create scheduler with optional persistent job store
    if SchedulerConfig.USE_PERSISTENT_STORE:
        from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
        jobstores = {
            'default': SQLAlchemyJobStore(url=SchedulerConfig.DATABASE_URL)
        }
        scheduler = BackgroundScheduler(jobstores=jobstores, timezone=SchedulerConfig.TIMEZONE)
    else:
        scheduler = BackgroundScheduler(timezone=SchedulerConfig.TIMEZONE)
    
    # Add event listeners
    scheduler.add_listener(job_executed_listener, EVENT_JOB_EXECUTED)
    scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
    
    # Add the ETL job with cron trigger
    scheduler.add_job(
        execute_etl_with_retry,
        trigger=CronTrigger.from_crontab(SchedulerConfig.ETL_SCHEDULE_CRON),
        id='retail_etl_job',
        name='Retail Data Warehouse ETL',
        replace_existing=True,
        max_instances=1,  # Prevent concurrent runs
        misfire_grace_time=3600  # Allow 1 hour grace period if missed
    )
    
    logger.info(f"ETL job scheduled with cron: {SchedulerConfig.ETL_SCHEDULE_CRON}")
    
    return scheduler


# ========================
# MANUAL TRIGGER FUNCTION
# ========================

def run_etl_now():
    """Manually trigger ETL execution (for testing)"""
    logger.info("Manual ETL execution triggered")
    execute_etl_with_retry()


# ========================
# MAIN EXECUTION
# ========================

def main():
    """Main entry point for scheduler"""
    logger.info("="*60)
    logger.info("RETAIL DATA WAREHOUSE ETL SCHEDULER")
    logger.info("="*60)
    logger.info(f"Starting scheduler at {datetime.now()}")
    logger.info(f"Schedule: {SchedulerConfig.ETL_SCHEDULE_CRON}")
    logger.info(f"Timezone: {SchedulerConfig.TIMEZONE}")
    logger.info(f"Max Retries: {SchedulerConfig.MAX_RETRIES}")
    logger.info(f"Email Notifications: {'Enabled' if SchedulerConfig.ENABLE_EMAIL_NOTIFICATIONS else 'Disabled'}")
    logger.info("="*60)
    
    # Initialize scheduler
    scheduler = initialize_scheduler()
    
    # Print next scheduled run times
    jobs = scheduler.get_jobs()
    for job in jobs:
        logger.info(f"Job: {job.name}")
        logger.info(f"Next run: {job.next_run_time}")
    
    # Start scheduler
    scheduler.start()
    logger.info("Scheduler started successfully!")
    logger.info("Press Ctrl+C to exit")
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(60)  # Check every minute
            
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown(wait=True)
        logger.info("Scheduler shut down gracefully")


if __name__ == "__main__":
    # Uncomment ONE of the following:
    
    # 1. Run scheduler (production mode)
    main()
    
    # 2. Run ETL immediately (testing mode)
    # run_etl_now()
