from sqlalchemy import text
from db_connection import engine
from datetime import datetime

class WatermarkManager:
    """Manages ETL watermarks for incremental loading with multi-source support"""
    
    @staticmethod
    def get_last_watermark(table_name, source_system=None):
        """Retrieve the last loaded timestamp for a table and optional source"""
        with engine.connect() as conn:
            if source_system:
                result = conn.execute(
                    text("""
                        SELECT last_loaded_timestamp, last_loaded_date, last_invoice_number,
                               records_processed, records_rejected
                        FROM etl_watermark 
                        WHERE table_name = :tname AND source_system = :src
                    """),
                    {"tname": table_name, "src": source_system}
                ).fetchone()
            else:
                result = conn.execute(
                    text("""
                        SELECT last_loaded_timestamp, last_loaded_date, last_invoice_number,
                               records_processed, records_rejected
                        FROM etl_watermark 
                        WHERE table_name = :tname
                        ORDER BY last_loaded_timestamp DESC
                        LIMIT 1
                    """),
                    {"tname": table_name}
                ).fetchone()
            
            if result:
                return {
                    'timestamp': result[0],
                    'date': result[1],
                    'invoice': result[2],
                    'records_processed': result[3] or 0,
                    'records_rejected': result[4] or 0
                }
            return None
    
    @staticmethod
    def update_watermark(table_name, source_system=None, new_timestamp=None, 
                        new_date=None, invoice_number=None, 
                        records_processed=0, records_rejected=0):
        """Update watermark after successful load"""
        with engine.begin() as conn:
            if source_system:
                conn.execute(
                    text("""
                        UPDATE etl_watermark 
                        SET last_loaded_timestamp = COALESCE(:ts, last_loaded_timestamp),
                            last_loaded_date = COALESCE(:dt, last_loaded_date),
                            last_invoice_number = COALESCE(:inv, last_invoice_number),
                            records_processed = records_processed + :proc,
                            records_rejected = records_rejected + :rej,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE table_name = :tname AND source_system = :src
                    """),
                    {
                        "ts": new_timestamp,
                        "dt": new_date,
                        "inv": invoice_number,
                        "tname": table_name,
                        "src": source_system,
                        "proc": records_processed,
                        "rej": records_rejected
                    }
                )
            else:
                conn.execute(
                    text("""
                        UPDATE etl_watermark 
                        SET last_loaded_timestamp = COALESCE(:ts, last_loaded_timestamp),
                            last_loaded_date = COALESCE(:dt, last_loaded_date),
                            last_invoice_number = COALESCE(:inv, last_invoice_number),
                            records_processed = records_processed + :proc,
                            records_rejected = records_rejected + :rej,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE table_name = :tname
                    """),
                    {
                        "ts": new_timestamp,
                        "dt": new_date,
                        "inv": invoice_number,
                        "tname": table_name,
                        "proc": records_processed,
                        "rej": records_rejected
                    }
                )
        
        print(f"✅ Watermark updated for {table_name}" + 
              (f" [{source_system}]" if source_system else "") +
              f": +{records_processed} records, {records_rejected} rejected")
