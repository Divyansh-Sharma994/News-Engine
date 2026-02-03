"""
Google News Fetcher (Previously named GDELT Fetcher)
This file searches the internet (via Google News) to find article links.
It's like the "Search Engine" part of the robot.
"""

import requests
import feedparser
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict
import random
import time

# This is the main function we use to find news.
def fetch_gdelt_simple(keyword: str, days: int = 7, max_articles: int = 5000) -> List[Dict]:
    """
    Search for news articles about a 'keyword'.
    It looks at news from the last 'days' days.
    """
    
    articles = []
    
    # We pretend to be different browsers (Chrome, Mac, Linux) so Google doesn't block us.
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]
    
    # This small function fetches one single RSS feed link.
    async def fetch_rss_async(url):
        try:
            # Pick a random browser identity
            headers = {'User-Agent': random.choice(user_agents)}
            # We wait up to 30 seconds for Google to reply.
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        # If success, read the text and parse it as an RSS feed
                        content = await response.text()
                        feed = feedparser.parse(content) # feedparser understands RSS format
                        return feed.entries
                    else:
                        return []
        except Exception as e:
            # If generic error, just return empty list
            return []
        
    # This function creates MANY different search URLs to get the most results.
    async def fetch_massive_sources():
        base_query = requests.utils.quote(keyword)
        
        # We try searching for the keyword in many different ways
        queries = [
            f"{base_query}", 
            f'"{keyword}"', # Exact match (keeps words together)
            f"{base_query}%20news", 
            f"{base_query}%20market",
            f"{base_query}%20industry",
            f"{base_query}%20report",
        ]
        
        # We look for news in these countries (US, UK, India, Australia, Canada, Singapore)
        regions = [
            "US:en", "GB:en", "IN:en", "AU:en", "CA:en", "SG:en"
        ]
        
        urls = []
        # For every query variation, pick 4 random countries to search in.
        for q in queries:
            selected_regions = random.sample(regions, min(len(regions), 4))
            for region in selected_regions: 
                hl = "en-" + region.split(':')[0] # Language (e.g., en-US)
                gl = region.split(':')[0]         # Country (e.g., US)
                ceid = region                     # Region ID
                
                # Create the Google News RSS URL
                # This is the "magic" URL that asks Google for news
                url = f"https://news.google.com/rss/search?q={q}%20when%3A{days}d&hl={hl}&gl={gl}&ceid={ceid}"
                urls.append(url)
        
        # We search 10 URLs at a time so we don't crash our internet
        batch_size = 10
        all_results = []
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            tasks = [fetch_rss_async(url) for url in batch]
            
            # 'asyncio.gather' runs all 10 tasks in parallel!
            results = await asyncio.gather(*tasks)
            all_results.extend(results)
            # Sleep for a tiny bit to be polite to the server
            await asyncio.sleep(0.1)
            
        return all_results
    
    # START THE SEARCH!
    all_entries_lists = asyncio.run(fetch_massive_sources())
    
    seen_titles = set()
    
    # Process all the results we got back
    for entries in all_entries_lists:
        if entries:
            for entry in entries:
                title = entry.get('title', '')
                
                # --- Deduplication (Removing copies) ---
                # Sometimes we get the exact same article from US and UK feeds.
                # We check if we've already seen this title (ignoring spaces and case).
                if len(title) > 20:
                    # Check start and end of title
                    norm_title = (title[:20] + title[-20:]).lower().replace(" ", "")
                else:
                    norm_title = title.lower().replace(" ", "")
                
                # If we haven't seen this title before, add it!
                if title and norm_title not in seen_titles:
                    seen_titles.add(norm_title)
                    
                    # Clean up the HTML from the description
                    raw_description = entry.get('summary', '')
                    soup = BeautifulSoup(raw_description, 'html.parser')
                    clean_description = soup.get_text(separator=' ', strip=True)
                    
                    articles.append({
                        'title': title,
                        'description': clean_description if clean_description else 'No description',
                        'source': entry.get('source', {}).get('title', 'Unknown'),
                        'link': entry.get('link', ''), # This link will be encrypted by Google (we fix it later)
                        'published': entry.get('published', '')
                    })
                    
                    # Stop if we have enough articles
                    if len(articles) >= max_articles:
                        return articles
    
    return articles
