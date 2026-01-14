from seleniumbase import SB
import json

URL = (
    "https://www.htworld.co.uk/wp-json/wp/v2/posts"
    "?per_page=100&page=1&orderby=date&order=desc"
)

with SB(uc=True, headless=True) as sb:
    # Open the WP REST API endpoint like a real browser
    sb.open(URL)

    # Small wait to ensure response is fully loaded
    sb.sleep(2)

    # WP JSON is rendered as plain text in the browser
    raw_json = sb.get_text("body")
    print(raw_json)
    posts = json.loads(raw_json)

    for post in posts:
        print({
            "title": post["title"]["rendered"],
            "url": post["link"],
            "timestamp": post["date_gmt"],
        })
