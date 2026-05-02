# Scrapers → Company Map

Quick reference for which scraper belongs to which company.

---

## Solo Search

> Digital health, tech startups, NHS / ICB procurement

| ID | File | Source |
|----|------|--------|
| 1 | `businesscloud.py` | https://businesscloud.co.uk |
| 3 | `digital_health.py` | https://digitalhealth.net |
| 4 | `eu_startups.py` | https://eu-startups.com |
| 5 | `find_tender.py` | https://www.findtender.service.gov.uk |
| 6 | `htworld.py` | https://htworld.co.uk |
| 7 | `htn_co.py` | https://htn.co.uk |
| 8 | `startups_co.py` | https://startups.co.uk |
| 11 | `ukri.py` | https://www.ukri.org |

---

## Arden Executive

> Defence, manufacturing, aerospace, marine, energy

| ID | File | Source |
|----|------|--------|
| 9 | `themanufacturer.py` | https://www.themanufacturer.com |
| 10 | `ukdefencejournal.py` | https://ukdefencejournal.org.uk |
| 12 | `marineindustrynews.py` | https://www.marineindustrynews.co.uk |
| 13 | `energyvoice.py` | https://www.energyvoice.com |
| 14 | `prnewswire.py` | https://www.prnewswire.co.uk |

---

## ERP Recruit

> ERP and consulting industry news

| ID | File | Source |
|----|------|--------|
| 5 | `find_tender.py` | https://www.find-tender.service.gov.uk |
| 14 | `prnewswire.py` | https://www.prnewswire.co.uk |
| 15 | `consultancy_eu.py` | https://www.consultancy.eu |
| 16 | `consultancy_uk.py` | https://www.consultancy.uk |
| 17 | `erp_today.py` | https://erp.today |
| 18 | `computable_nl.py` | https://computable.nl |
| 19 | `capgemini.py` | https://www.capgemini.com (UK/IE/BE/LU/NL) |
| 20 | `oracle.py` | https://www.oracle.com/nl/news/ |
| 21 | `deloitte.py` | https://www.deloitte.com (UK/NL/BE/IE/LU) |
| 29 | `companies_house.py` | https://www.gov.uk/search/all (Companies House news) |

---

## PLEA (Landscape Architecture)

> Landscape design, public realm, biodiversity, grounds maintenance procurement

| ID | File | Source |
|----|------|--------|
| 2 | `contract_finder.py` | https://www.contractsfinder.service.gov.uk |
| 5 | `find_tender.py` | https://www.find-tender.service.gov.uk |
| 22 | `homes_england.py` | https://www.gov.uk/search/all (housing news) |
| 23 | `bidstats.py` | https://bidstats.uk (landscape/grounds tenders, current month) |
| 24 | `huntingdonshire.py` | https://publicaccess.huntingdonshire.gov.uk (planning applications) |
| 25 | `planning_inspectorate.py` | https://www.gov.uk (Planning Inspectorate appeals) |
| 26 | `eastcambs.py` | https://eastcambs.gov.uk (planning applications) |
| 27 | `greater_cambridge.py` | https://applications.greatercambridgeplanning.org (planning applications) |
| 28 | `cambridge_news.py` | https://cambridge-news.co.uk (planning/property news via Mantis API) |

---

## Headliners

> Marketing, communications, PR, brand and creative industry news

| ID | File | Source |
|----|------|--------|
| 2 | `contract_finder.py` | https://www.contractsfinder.service.gov.uk (marketing/comms/PR, £50k+) |
| 14 | `prnewswire.py` | https://www.prnewswire.co.uk |
| 29 | `companies_house.py` | https://www.gov.uk/search/all (Companies House news) |
| 30 | `thedrum.py` | https://www.thedrum.com |
| 31 | `businesswire.py` | https://www.businesswire.com |
| 32 | `marketingweek.py` | https://www.marketingweek.com |
| 33 | `prolificnorth.py` | https://www.prolificnorth.co.uk |
| 34 | `thegrocer.py` | https://www.thegrocer.co.uk |

---


{
  "companies_house": "Leadership Hire",
  "the_drum": "Contract Wins",
  "business_wire_uk": "Funding & Investment",
  "pr_week": "Leadership Hire",
  "campaign": "Leadership Hire",
  "marketing_week": "Leadership Hire",
  "contract_finder": "Contract Wins",
  "prolific_north": "Leadership Hire",
  "the_grocer": "Product Launches",
  "pr_newswire_uk": "Funding & Investment"
}


## Both Companies

> Runs for multiple companies with different keywords/stages

| ID | File | Source |
|----|------|--------|
| 2 | `contract_finder.py` | https://www.contractsfinder.service.gov.uk |

### Contract Finder CPV codes

**Solo Search (Digital Health / IT)**
`72000000, 72200000, 72500000, 72600000, 51600000, 48000000, 48180000, 72300000, 75123000`

**Arden Executive (Defence / Aerospace / Marine / Energy)**
`35300000, 35600000, 34700000, 34500000, 45251000, 45262300, 71300000, 42900000`

---

## Company IDs (Supabase)

| Company | UUID |
|---------|------|
| Solo Search | `234f37eb-1147-43fb-89c1-9812e0824e1f` |
| Arden Executive | `c5d7d2eb-189d-49dc-89e1-2af375d0b3ce` |
| ERP Recruit | `81ef7ff7-c548-42d8-8b22-b339b26d08ac` |
| PLEA | *(add UUID here)* |
| Headliners | `16adda91-be84-461a-b6e2-dc81e76cc2c6` |
