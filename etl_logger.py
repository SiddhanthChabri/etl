import logging
import traceback
from datetime import datetime
from sqlalchemy import text
from db_connection import engine
import socket
import sys

class ETLLogger:
    """Professional ETL logging with database persistence"""
    
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.batch_id = None
        self.start_time = None
        self.server_name = socket.gethostname()
        
        # Setup file logging with UTF-8 encoding
        logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(
                f'etl_{datetime.now().strftime("%Y%m%d")}.log',
                encoding='utf-8'  # Add this
            ),
            logging.StreamHandler(sys.stdout)
            ]
        )

        
        self.logger = logging.getLogger(pipeline_name)
    
    def start_batch(self, batch_name):
        """Start a new ETL batch execution"""
        self.start_time = datetime.now()
        
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO etl_batch_log 
                    (batch_name, pipeline_name, start_time, status, server_name)
                    VALUES (:batch_name, :pipeline, :start_time, 'RUNNING', :server)
                    RETURNING batch_id
                """),
                {
                    "batch_name": batch_name,
                    "pipeline": self.pipeline_name,
                    "start_time": self.start_time,
                    "server": self.server_name
                }
            )
            self.batch_id = result.fetchone()[0]
        
        self.logger.info(f"🚀 Batch started: {batch_name} (ID: {self.batch_id})")
        return self.batch_id
    
    def end_batch(self, status='SUCCESS', records_read=0, records_inserted=0, 
                  records_updated=0, records_rejected=0, error_message=None):
        """End the current batch execution"""
        end_time = datetime.now()
        execution_time = (end_time - self.start_time).total_seconds()
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE etl_batch_log 
                    SET end_time = :end_time,
                        status = :status,
                        records_read = :read,
                        records_inserted = :inserted,
                        records_updated = :updated,
                        records_rejected = :rejected,
                        error_message = :error,
                        execution_time_seconds = :exec_time
                    WHERE batch_id = :batch_id
                """),
                {
                    "end_time": end_time,
                    "status": status,
                    "read": records_read,
                    "inserted": records_inserted,
                    "updated": records_updated,
                    "rejected": records_rejected,
                    "error": error_message,
                    "exec_time": execution_time,
                    "batch_id": self.batch_id
                }
            )
        
        if status == 'SUCCESS':
            self.logger.info(f"✅ Batch completed successfully in {execution_time:.2f}s")
        else:
            self.logger.error(f"❌ Batch failed: {error_message}")
    
    def log_step(self, step_name, step_type='TRANSFORM'):
        """Context manager for logging individual ETL steps"""
        return ETLStepLogger(self.batch_id, step_name, step_type, self.logger)
    
    def log_error(self, error_type, table_name, error_message, 
                  record_id=None, error_details=None, severity='ERROR'):
        """Log an error to database and file"""
        stack = traceback.format_exc()
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO etl_error_log 
                    (batch_id, error_type, error_severity, table_name, 
                     record_id, error_message, error_details, stack_trace)
                    VALUES (:batch_id, :type, :severity, :table, 
                            :record_id, :message, :details, :stack)
                """),
                {
                    "batch_id": self.batch_id,
                    "type": error_type,
                    "severity": severity,
                    "table": table_name,
                    "record_id": record_id,
                    "message": str(error_message),
                    "details": error_details,
                    "stack": stack
                }
            )
        
        self.logger.error(f"❌ Error in {table_name}: {error_message}")
    
    def log_data_quality(self, table_name, check_name, records_checked, 
                         records_passed, records_failed, failure_details=None):
        """Log data quality check results"""
        pass_percentage = (records_passed / records_checked * 100) if records_checked > 0 else 0
        status = 'PASSED' if pass_percentage >= 95 else 'WARNING' if pass_percentage >= 90 else 'FAILED'
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO data_quality_log 
                    (batch_id, table_name, check_name, check_type, records_checked,
                     records_passed, records_failed, pass_percentage, status, failure_details)
                    VALUES (:batch_id, :table, :check, 'VALIDATION', :checked,
                            :passed, :failed, :percentage, :status, :details)
                """),
                {
                    "batch_id": self.batch_id,
                    "table": table_name,
                    "check": check_name,
                    "checked": records_checked,
                    "passed": records_passed,
                    "failed": records_failed,
                    "percentage": pass_percentage,
                    "status": status,
                    "details": failure_details
                }
            )
        
        icon = "✅" if status == "PASSED" else "⚠️" if status == "WARNING" else "❌"
        self.logger.info(f"{icon} Quality Check [{table_name}]: {pass_percentage:.1f}% passed")
    
    def quarantine_record(self, source_system, table_name, rejection_reason, raw_data):
        """Send rejected record to quarantine"""
        import json
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO data_quarantine 
                    (batch_id, source_system, table_name, rejection_reason, raw_data)
                    VALUES (:batch_id, :source, :table, :reason, :data)
                """),
                {
                    "batch_id": self.batch_id,
                    "source": source_system,
                    "table": table_name,
                    "reason": rejection_reason,
                    "data": json.dumps(raw_data)
                }
            )
        
        self.logger.warning(f"⚠️ Record quarantined from {table_name}: {rejection_reason}")


class ETLStepLogger:
    """Context manager for logging individual ETL steps"""
    
    def __init__(self, batch_id, step_name, step_type, logger):
        self.batch_id = batch_id
        self.step_name = step_name
        self.step_type = step_type
        self.logger = logger
        self.step_id = None
        self.start_time = None
        self.records_processed = 0
    
    def __enter__(self):
        self.start_time = datetime.now()
        
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO etl_step_log 
                    (batch_id, step_name, step_type, start_time, status)
                    VALUES (:batch_id, :step, :type, :start_time, 'RUNNING')
                    RETURNING step_id
                """),
                {
                    "batch_id": self.batch_id,
                    "step": self.step_name,
                    "type": self.step_type,
                    "start_time": self.start_time
                }
            )
            self.step_id = result.fetchone()[0]
        
        self.logger.info(f"▶️  Step started: {self.step_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        execution_time = (end_time - self.start_time).total_seconds()
        status = 'FAILED' if exc_type else 'SUCCESS'
        error_message = str(exc_val) if exc_val else None
        
        with engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE etl_step_log 
                    SET end_time = :end_time,
                        status = :status,
                        records_processed = :records,
                        error_message = :error,
                        execution_time_seconds = :exec_time
                    WHERE step_id = :step_id
                """),
                {
                    "end_time": end_time,
                    "status": status,
                    "records": self.records_processed,
                    "error": error_message,
                    "exec_time": execution_time,
                    "step_id": self.step_id
                }
            )
        
        if status == 'SUCCESS':
            self.logger.info(f"✅ Step completed: {self.step_name} ({execution_time:.2f}s)")
        else:
            self.logger.error(f"❌ Step failed: {self.step_name} - {error_message}")
        
        return False  # Don't suppress exceptions
    
    def update_records_processed(self, count):
        """Update the count of records processed in this step"""
        self.records_processed = count
