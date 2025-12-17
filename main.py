#!/usr/bin/env python3
"""
Main scheduler script that runs all scrapers every hour.
Executes scrapers sequentially and handles errors gracefully.
"""

import time
import schedule
from datetime import datetime
import sys
import traceback

# Import all scraper modules
import digital_health
import contract_finder
import find_tender
import htn_co


def run_scraper(scraper_name, scraper_function):
    """
    Run a single scraper with error handling and logging.
    """
    print("\n" + "=" * 80)
    print(f"🚀 Starting {scraper_name}")
    print(f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    try:
        scraper_function()
        print(f"✅ {scraper_name} completed successfully")
    except Exception as e:
        print(f"❌ {scraper_name} failed with error:")
        print(f"Error: {str(e)}")
        traceback.print_exc()
        print(f"Continuing with next scraper...")
    
    print("=" * 80)


def run_all_scrapers():
    """
    Run all scrapers sequentially.
    """
    print("\n" + "🔄" * 40)
    print(f"Starting scraper run at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🔄" * 40 + "\n")
    
    start_time = time.time()
    
    # Run each scraper
    run_scraper("Digital Health", digital_health.main)
    time.sleep(5)  # Small delay between scrapers
    
    run_scraper("Contract Finder", contract_finder.main)
    time.sleep(5)
    
    run_scraper("Find Tender", find_tender.main)
    time.sleep(5)
    
    run_scraper("HTN.co", htn_co.main)
    
    # Calculate total time
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)
    
    print("\n" + "🎉" * 40)
    print(f"All scrapers completed!")
    print(f"Total time: {minutes}m {seconds}s")
    print(f"Next run scheduled in 1 hour")
    print("🎉" * 40 + "\n")


def main():
    """
    Main function that sets up the schedule and runs continuously.
    """
    print("=" * 80)
    print("📊 SCRAPER SCHEDULER STARTED")
    print("=" * 80)
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Schedule: Every 1 hour")
    print("Press Ctrl+C to stop")
    print("=" * 80 + "\n")
    
    # Run immediately on start
    print("Running initial scrape...")
    run_all_scrapers()
    
    # Schedule to run every hour
    schedule.every(1).hours.do(run_all_scrapers)
    
    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n\n" + "=" * 80)
        print("🛑 Scheduler stopped by user")
        print("=" * 80)
        sys.exit(0)


if __name__ == "__main__":
    main()
