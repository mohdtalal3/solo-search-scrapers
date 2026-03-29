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
| 15 | `consultancy_eu.py` | https://www.consultancy.eu |
| 16 | `consultancy_uk.py` | https://www.consultancy.uk |
| 17 | `erp_today.py` | https://erp.today |

---

## Both Companies

> Runs twice — once per company with different CPV codes and keywords

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
