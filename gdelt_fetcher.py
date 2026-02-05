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
from datetime import datetime, timedelta
import random
import time
import re
from aiohttp_socks import ProxyConnector
from stem import Signal
from stem.control import Controller



# ---------------------------------------------------------
# SMART EXPANSION MAP
# We search for these related topics to get MORE articles
# ---------------------------------------------------------
SECTOR_TOPICS = {
    "Finance": ["stocks", "banking", "economy", "investment", "fintech", "market", "trading", "crypto", "dividend", "revenue", "fiscal", "quarterly", "merger", "acquisition"],
    "Tech & AI": ["artificial intelligence", "startup", "cybersecurity", "software", "innovation", "gadgets", "cloud computing", "machine learning", "robotics", "semiconductor", "big data", "saas", "hardware"],
    "Health": ["medicine", "healthcare", "pharma", "wellness", "medical", "biotech", "hospital", "clinical trial", "vaccine", "genomic", "telemedicine", "digital health"],
    "Sustainability": ["climate change", "green energy", "renewable", "carbon", "environment", "esg", "solar", "wind", "electric vehicle", "circular economy", "biodiversity", "net zero"],
    "Education": ["schools", "universities", "edtech", "learning", "students", "campus", "curriculum", "literacy", "higher education", "vocational", "scholarship"],
    "Sports": ["cricket", "football", "olympics", "tournament", "championship", "league", "athlete", "sponsorship", "world cup", "transfer", "record"],
    "Startups": ["funding", "unicorn", "venture capital", "entrepreneur", "ipo", "acquisition", "seed round", "series a", "accelerator", "incubator", "scalability"],
    "Lifestyle": ["fashion", "travel", "food", "luxury", "trends", "culture", "design", "wellness", "real estate", "architecture", "gastronomy", "influencer"]
}

# This is the main function we use to find news.
def fetch_gdelt_simple(keyword: str, days: int = 7, max_articles: int = 50000, progress_callback=None, target_regions: List[str] = None, sector_context: str = None, use_tor: bool = False, saturation_mode: bool = False) -> List[Dict]:
    """
    Search for news articles about a 'keyword'.
    It looks at news from the last 'days' days.
    
    If use_tor is True, it will route requests through Tor (127.0.0.1:9150).
    If saturation_mode is True, it uses extreme slicing to hit 700+ articles.
    """
    def renew_tor_identity(control_port=9151):
        """
        Signals Tor for a New Identity (Change IP).
        Tor Browser Control Port is usually 9151.
        Tor Service Control Port is usually 9051.
        """
        try:
            with Controller.from_port(port=control_port) as controller:
                controller.authenticate()
                controller.signal(Signal.NEWNYM)
                # Short sleep to allow the circuit to be rebuilt
                time.sleep(2)
                print("✅ Tor identity renewed successfully.")
        except Exception as e:
            print(f"❌ Failed to renew Tor identity: {e}")
    
    articles = []
    
    # Default to all if None provided (mostly for backward compatibility)
    if not target_regions:
        target_regions = ["IN:en", "US:en", "GB:en", "AU:en", "CA:en", "SG:en"]
    
    # We pretend to be many different browsers (Chrome, Mac, Linux, Safari, Firefox)
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 14.2; rv:122.0) Gecko/20100101 Firefox/122.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1',
        'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Mobile Safari/537.36'
    ]
    
    # Random headers to smooth traffic
    def get_random_headers():
        return {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.8', 'en-IN,en;q=0.9']),
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
    
    # This small function fetches one single RSS feed link with RETRY logic.
    async def fetch_rss_async(url, connector=None):
        max_retries = 3
        base_delay = 5
        
        for attempt in range(max_retries + 1):
            try:
                # Pick a random browser identity and headers
                headers = get_random_headers()
                
                # We wait up to 30 seconds for Google to reply.
                async with aiohttp.ClientSession(connector=connector) as session:
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            # If success, read the text and parse it as an RSS feed
                            content = await response.text()
                            feed = feedparser.parse(content) 
                            return feed.entries
                        
                        elif response.status in [429, 503]:
                            # Rate limited! AGGRESSIVE COOLDOWN
                            if attempt < max_retries:
                                # If using Tor, try to renew identity
                                if use_tor:
                                    print("🌀 Tor: Requesting New Identity (IP Rotation)...")
                                    renew_tor_identity()
                                
                                # Exponential backoff with a massive base
                                delay = 10 * (2 ** attempt) + random.uniform(2.0, 5.0)
                                print(f"⚠️ Rate limited (429/503). Entering Deep Sleep for {delay:.1f}s...")
                                await asyncio.sleep(delay)
                                continue
                            else:
                                print(f"❌ Failed after {max_retries} retries: {url}")
                                return []
                        else:
                            # Other error (404, etc), don't retry
                            return []
            except Exception as e:
                # Network error? Retry nicely.
                if attempt < max_retries:
                     delay = base_delay * (2 ** attempt)
                     await asyncio.sleep(delay)
                else:
                    return []
        return []

    # This function creates different search URLs to get deep results.
    async def fetch_resilient_sources(progress_callback=None):
        base_query = requests.utils.quote(keyword)
        
        # 1. High-Impact Queries
        if saturation_mode:
            # Ultra-wide variations for Saturation Mode
            variations = [
                "", "news", "report", "breaking", "update", "latest", 
                "analysis", "forecast", "trends", "market", "sector", "industry"
            ]
            queries = []
            for var in variations:
                if var: queries.append(f"{base_query}%20{var}")
                else: queries.append(f"{base_query}")
                queries.append(f'"{keyword}"%20{var}' if var else f'"{keyword}"')
        else:
            queries = [
                f"{base_query}", 
                f'"{keyword}"', 
                f"{base_query}%20news",
                f"{base_query}%20report"
            ]
        
        # 2. SMART EXPANSION (If a sector is provided or detected)
        # Check if we should use sector_context
        # If sector_input was "CUSTOM", main.py might have classified it already
        effective_sector = sector_context
        
        if effective_sector and effective_sector in SECTOR_TOPICS:
            related_topics = SECTOR_TOPICS[effective_sector]
            print(f"🧠 Smart Expansion Active for '{effective_sector}': Adding {len(related_topics)} related topics...")
            
            for topic in related_topics:
                safe_topic = requests.utils.quote(topic)
                if keyword.lower() == effective_sector.lower():
                    queries.append(safe_topic)
                else:
                    queries.append(f"{base_query}%20{safe_topic}")
        
        # BALANCED CONCURRENCY for Resilience
        # Increased from 5 to 10 for better speed while maintaining stability
        semaphore = asyncio.Semaphore(10)
        
        # OMEGA TOR CONNECTOR
        connector = None
        if use_tor:
            print("🛡️ Tor Mode: Routing through 127.0.0.1:9150")
            connector = ProxyConnector.from_url("socks5://127.0.0.1:9150")
        
        urls = []
        for i in range(days):
            date_start = datetime.now() - timedelta(days=i+1)
            date_end = datetime.now() - timedelta(days=i)
            
            # Format dates for Google News (after:YYYY-MM-DD before:YYYY-MM-DD)
            # This ensures each chunk looks at a unique window
            start_str = date_start.strftime("%Y-%m-%d")
            end_str = date_end.strftime("%Y-%m-%d")
            
            # We slice the day into chunks. 
            # Google News doesn't support hour-level 'before/after' in RSS, 
            # but we can use different regional parameters or permutations to maximize coverage.
            time_filter = f"%20after%3A{start_str}%20before%3A{end_str}"
            
            for q in queries:
                for region in target_regions:
                    hl = "en-" + region.split(':')[0]
                    gl = region.split(':')[0]
                    ceid = region
                    
                    # We keep 'chunk' as a parameter to differentiate cache-busting if any, 
                    # but the primary fix is the inclusion of the 'before' filter.
                    url = f"https://news.google.com/rss/search?q={q}{time_filter}&hl={hl}&gl={gl}&ceid={ceid}"
                    urls.append(url)
        
        # Deduplicate URLs just in case
        urls = list(dict.fromkeys(urls))
        
        # Stats for progress
        total_tasks = len(urls)
        completed_tasks = 0
        
        async def fetch_with_semaphore(url):
            nonlocal completed_tasks
            async with semaphore:
                # MANDATORY JITTERED DELAY
                await asyncio.sleep(random.uniform(0.5, 2.0)) # Slightly faster jitter
                
                results = await fetch_rss_async(url, connector=connector)
                
                # Update Progress
                completed_tasks += 1
                if progress_callback:
                    try:
                        progress_callback(completed_tasks, total_tasks)
                    except: pass
                
                return results

        print(f"📡 Launching Resilient Search with {len(urls)} URLs (Speed Optimized)...")
        tasks = [fetch_with_semaphore(url) for url in urls]
        # Using return_exceptions=True to ensure one failure doesn't kill the whole process
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_results = []
        for res in results:
            if isinstance(res, list):
                all_results.extend(res)
            elif isinstance(res, Exception):
                print(f"⚠️ Task failed with exception: {res}")
        return all_results
    
    # START THE SEARCH!
    all_entries_lists = asyncio.run(fetch_resilient_sources(progress_callback))
    
    seen_titles = set()
    
    # Process all the results we got back
    for entry in all_entries_lists:
        title = entry.get('title', '').strip()
        source = entry.get('source', {}).get('title', 'Unknown')
        
        # --- Deduplication ---
        # We use a tuple of (normalized_title, source) to catch exact same 
        # articles from the same source while allowing the same headline 
        # from different outlets.
        norm_title = re.sub(r'\s+', ' ', title).lower()
        dedup_key = (norm_title, source)
        
        if title and dedup_key not in seen_titles:
            seen_titles.add(dedup_key)
            
            # Clean description
            raw_description = entry.get('summary', '')
            soup = BeautifulSoup(raw_description, 'html.parser')
            clean_description = soup.get_text(separator=' ', strip=True)
            
            # Remove "and more »" which Google News often appends
            clean_description = re.sub(r'\s*and more\s*»', '', clean_description)
            
            articles.append({
                'title': title,
                'description': clean_description if clean_description else 'No description',
                'source': source,
                'link': entry.get('link', ''),
                'published': entry.get('published', '')
            })
            
            # Stop if max reached
            if len(articles) >= max_articles:
                break
    
    return articles
