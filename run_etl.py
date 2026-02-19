import sys
import os
from datetime import datetime
from logger_config import setup_logger
import time

# Setup logger
logger = setup_logger('ETL_Orchestrator')

class ETLOrchestrator:
    """
    Master ETL orchestrator that runs all ETL scripts in sequence
    """
    
    def __init__(self):
        self.start_time = datetime.now()
        self.steps_completed = []
        self.steps_failed = []
        self.step_timings = {}
    
    def run_step(self, step_name, script_path):
        """
        Run a single ETL step
        """
        logger.info("="*60)
        logger.info(f"STEP: {step_name}")
        logger.info("="*60)
        
        step_start = time.time()
        
        try:
            # Check if script exists
            if not os.path.exists(script_path):
                raise FileNotFoundError(f"Script not found: {script_path}")
            
            logger.info(f"Executing: {script_path}")
            
            # Execute the script
            with open(script_path, 'r', encoding='utf-8') as file:
                script_content = file.read()
                exec(script_content, {'__name__': '__main__'})
            
            # Calculate duration
            duration = time.time() - step_start
            self.step_timings[step_name] = duration
            
            logger.info(f"✓ {step_name} completed successfully in {duration:.2f} seconds")
            self.steps_completed.append(step_name)
            return True
            
        except Exception as e:
            duration = time.time() - step_start
            self.step_timings[step_name] = duration
            
            logger.error(f"✗ {step_name} FAILED after {duration:.2f} seconds")
            logger.error(f"Error: {str(e)}")
            self.steps_failed.append(step_name)
            return False
    
    def run_pipeline(self):
        """
        Run the complete ETL pipeline
        """
        logger.info("\n" + "="*60)
        logger.info("ETL PIPELINE EXECUTION STARTED")
        logger.info("="*60)
        logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60 + "\n")
        
        # Define ETL steps in sequence
        etl_steps = [
            ("Load Store Dimension", "load_dimensions.py"),
            ("Load Time Dimension", "load_dimensions.py"),  # Adjust if separate
            ("Load Customer SCD", "load_customer_scd.py"),
            ("Load Product Dimension", "load_product.py"),
            ("Load Fact Sales", "load_fact_sales.py")
        ]
        
        # Track overall success
        all_success = True
        
        # Execute each step
        for step_num, (step_name, script_path) in enumerate(etl_steps, 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"EXECUTING STEP {step_num}/{len(etl_steps)}: {step_name}")
            logger.info(f"{'='*60}\n")
            
            success = self.run_step(step_name, script_path)
            
            if not success:
                all_success = False
                logger.error(f"\n{'='*60}")
                logger.error(f"PIPELINE STOPPED AT: {step_name}")
                logger.error(f"{'='*60}\n")
                break
            
            # Small delay between steps
            time.sleep(1)
        
        # Generate final report
        self.generate_report(all_success)
        
        return all_success
    
    def generate_report(self, success):
        """
        Generate final execution report
        """
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        logger.info("\n" + "="*60)
        logger.info("ETL PIPELINE EXECUTION REPORT")
        logger.info("="*60)
        
        # Overall status
        if success:
            logger.info("STATUS: ✓✓✓ SUCCESS ✓✓✓")
        else:
            logger.error("STATUS: ✗✗✗ FAILED ✗✗✗")
        
        logger.info("="*60)
        
        # Timing information
        logger.info(f"Start Time:  {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End Time:    {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration:    {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        
        logger.info("="*60)
        
        # Steps summary
        logger.info(f"Total Steps:      {len(self.steps_completed) + len(self.steps_failed)}")
        logger.info(f"Steps Completed:  {len(self.steps_completed)}")
        logger.info(f"Steps Failed:     {len(self.steps_failed)}")
        
        logger.info("="*60)
        
        # Completed steps
        if self.steps_completed:
            logger.info("COMPLETED STEPS:")
            for step in self.steps_completed:
                duration = self.step_timings.get(step, 0)
                logger.info(f"  ✓ {step} ({duration:.2f}s)")
        
        # Failed steps
        if self.steps_failed:
            logger.info("\nFAILED STEPS:")
            for step in self.steps_failed:
                duration = self.step_timings.get(step, 0)
                logger.error(f"  ✗ {step} ({duration:.2f}s)")
        
        logger.info("="*60)
        
        # Performance breakdown
        if self.step_timings:
            logger.info("\nPERFORMANCE BREAKDOWN:")
            sorted_steps = sorted(self.step_timings.items(), key=lambda x: x[1], reverse=True)
            for step, duration in sorted_steps:
                percentage = (duration / total_duration) * 100
                logger.info(f"  {step}: {duration:.2f}s ({percentage:.1f}%)")
        
        logger.info("="*60)
        
        # Recommendations
        if not success:
            logger.info("\nRECOMMENDATIONS:")
            logger.info("  1. Check the error logs above for detailed error messages")
            logger.info("  2. Verify database connections and credentials")
            logger.info("  3. Ensure all required files and dependencies exist")
            logger.info("  4. Check data quality issues if validation failed")
            logger.info("="*60)


def main():
    """
    Main entry point for ETL orchestrator
    """
    try:
        orchestrator = ETLOrchestrator()
        success = orchestrator.run_pipeline()
        
        if success:
            logger.info("\n✓✓✓ ETL PIPELINE COMPLETED SUCCESSFULLY! ✓✓✓\n")
            sys.exit(0)
        else:
            logger.error("\n✗✗✗ ETL PIPELINE FAILED! ✗✗✗\n")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\n⚠ ETL Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n✗ Unexpected error in ETL orchestrator: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
