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
import notifier
import db

# Import all scraper modules
import digital_health
import contract_finder
import find_tender
import htn_co
import startups_co
import ukri
import eu_startups
import businesscloud
import htworld
import energyvoice
import marineindustrynews
import themanufacturer
import prnewswire
import ukdefencejournal
import consultancy_eu
import consultancy_uk
import erp_today
import computable_nl
import capgemini
import oracle
import deloitte
import homes_england
import bidstats
import huntingdonshire
import planning_inspectorate
import eastcambs
import greater_cambridge
import cambridge_news
import companies_house
import thedrum
import businesswire
import marketingweek
import prolificnorth
import thegrocer


def run_scraper(scraper_name, scraper_function, scraper_module=None):
    """
    Run a single scraper with error handling and logging.
    For single-company scrapers, pass the module so the subscription
    is_active check can be performed before running.
    """
    # Check subscription status for single-company scrapers
    if scraper_module is not None:
        scraper_id = getattr(scraper_module, 'SCRAPER_ID', None)
        company_id = getattr(scraper_module, 'COMPANY_ID', None)
        if scraper_id is not None and company_id is not None:
            if not db.is_subscription_active(scraper_id, company_id):
                print(f"\n⏭️  Skipping {scraper_name} — subscription is inactive")
                return

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
        notifier.notify_error(scraper_name, e)
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

    # Load subscription statuses from DB once for the entire run
    print("🔍 Loading company-scraper subscription statuses...")
    db.load_active_subscriptions()
    print()

    # Run each scraper
    run_scraper("Digital Health", digital_health.main, digital_health)
    time.sleep(5)  # Small delay between scrapers
    
    run_scraper("Contract Finder", contract_finder.main)  # multi-company: checked internally
    time.sleep(5)

    run_scraper("Find Tender", find_tender.main)  # multi-company: checked internally
    time.sleep(5)

    run_scraper("HTN.co", htn_co.main, htn_co)
    time.sleep(5)

    run_scraper("Startups.co", startups_co.main, startups_co)
    time.sleep(5)

    run_scraper("UKRI", ukri.main, ukri)
    time.sleep(5)

    run_scraper("EU-Startups", eu_startups.main, eu_startups)
    time.sleep(5)

    run_scraper("BusinessCloud", businesscloud.main, businesscloud)
    time.sleep(5)

    run_scraper("HT World", htworld.main, htworld)
    time.sleep(5)

    run_scraper("Energy Voice", energyvoice.main, energyvoice)
    time.sleep(5)

    run_scraper("Marine Industry News", marineindustrynews.main, marineindustrynews)
    time.sleep(5)

    run_scraper("The Manufacturer", themanufacturer.main, themanufacturer)
    time.sleep(5)

    run_scraper("PR Newswire UK", prnewswire.main)  # multi-company: checked internally
    time.sleep(5)

    run_scraper("UK Defence Journal", ukdefencejournal.main, ukdefencejournal)
    time.sleep(5)

    run_scraper("Consultancy EU", consultancy_eu.main)  # multi-company: checked internally
    time.sleep(5)

    run_scraper("Consultancy UK", consultancy_uk.main)  # multi-company: checked internally
    time.sleep(5)

    run_scraper("ERP Today", erp_today.main, erp_today)
    time.sleep(5)

    run_scraper("Computable NL", computable_nl.main, computable_nl)
    time.sleep(5)

    run_scraper("Capgemini", capgemini.main, capgemini)
    time.sleep(5)

    run_scraper("Oracle", oracle.main, oracle)
    time.sleep(5)

    run_scraper("Deloitte", deloitte.main, deloitte)
    time.sleep(5)

    run_scraper("Homes England", homes_england.main, homes_england)
    time.sleep(5)

    run_scraper("Bidstats", bidstats.main, bidstats)
    time.sleep(5)

    run_scraper("Huntingdonshire", huntingdonshire.main, huntingdonshire)
    time.sleep(5)

    run_scraper("Planning Inspectorate", planning_inspectorate.main, planning_inspectorate)
    time.sleep(5)

    run_scraper("East Cambs", eastcambs.main, eastcambs)
    time.sleep(5)

    run_scraper("Greater Cambridge", greater_cambridge.main, greater_cambridge)
    time.sleep(5)

    run_scraper("Cambridge News", cambridge_news.main, cambridge_news)
    time.sleep(5)

    run_scraper("Companies House", companies_house.main)  # multi-company: checked internally
    time.sleep(5)

    run_scraper("The Drum", thedrum.main, thedrum)
    time.sleep(5)

    run_scraper("Business Wire", businesswire.main, businesswire)
    time.sleep(5)

    run_scraper("Marketing Week", marketingweek.main, marketingweek)
    time.sleep(5)

    run_scraper("Prolific North", prolificnorth.main, prolificnorth)
    time.sleep(5)

    run_scraper("The Grocer", thegrocer.main, thegrocer)
    time.sleep(5)
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
    print("Schedule: Every 2 hour")
    print("Press Ctrl+C to stop")
    print("=" * 80 + "\n")
    
    # Run immediately on start
    print("Running initial scrape...")
    run_all_scrapers()
    
    # Schedule to run every hour
    schedule.every(5).hours.do(run_all_scrapers)
    
    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(120)  # Check every minute
    except KeyboardInterrupt:
        print("\n\n" + "=" * 80)
        print("🛑 Scheduler stopped by user")
        print("=" * 80)
        sys.exit(0)


if __name__ == "__main__":
    main()
