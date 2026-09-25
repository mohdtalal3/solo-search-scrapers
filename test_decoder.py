"""Standalone test: decode a Google News link. Usage: python3 test_decoder.py [gnews_url]"""
import sys

from googlenewsdecoder import gnewsdecoder

url = sys.argv[1] if len(sys.argv) > 1 else (
    "https://news.google.com/rss/articles/CBMieEFVX3lxTE82MkZwQmxmdWNMZldrcmQycjJNb3MzemNLNEhOY01YSFdWV1VLOGtrTlh1UWc1RlZxQjV5Qk9yYjlmaUNEQTE3TS1PVFZjTnBzNVl4SWFFMzV0LVVlNlMzTDJaaFhXcUlOdHkxalVEaFE0VU1VMU1Veg?oc=5"
)

print(f"Input:  {url}")
result = gnewsdecoder(url)

if result.get("success"):
    print(f"Decoded: {result['decoded_url']}")
else:
    print(f"Failed:  {result.get('message')}")
