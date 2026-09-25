"""Standalone test: decode a Google News link. Usage: python3 test_decoder.py [gnews_url]"""
import os
import sys

from dotenv import load_dotenv
from googlenewsdecoder import gnewsdecoder

load_dotenv()

url = sys.argv[1] if len(sys.argv) > 1 else (
    "https://news.google.com/rss/articles/CBMieEFVX3lxTE82MkZwQmxmdWNMZldrcmQycjJNb3MzemNLNEhOY01YSFdWV1VLOGtrTlh1UWc1RlZxQjV5Qk9yYjlmaUNEQTE3TS1PVFZjTnBzNVl4SWFFMzV0LVVlNlMzTDJaaFhXcUlOdHkxalVEaFE0VU1VMU1Veg?oc=5"
)

proxy = os.getenv("SCRAPER_PROXY")
# GB/EU exit IPs hit Google's consent wall — use a US exit for decoding
if proxy:
    proxy = proxy.replace("__cr.gb", "__cr.us")
print(f"Proxy:  {proxy or 'none'}")
print(f"Input:  {url}")
result = gnewsdecoder(url, proxy=proxy)

if result.get("success"):
    print(f"Decoded: {result['decoded_url']}")
else:
    print(f"Failed:  {result.get('message')}")
