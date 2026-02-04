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
        
        # 2. SMART EXPANSION (If a sector is provided)
        # If the user selected "Finance", we also search "stocks", "banking", etc.
        if sector_context and sector_context in SECTOR_TOPICS:
            related_topics = SECTOR_TOPICS[sector_context]
            print(f"🧠 Smart Expansion Active for '{sector_context}': Adding {len(related_topics)} related topics...")
            
            for topic in related_topics:
                # We combine the Base Keyword with the Topic for relevance
                # e.g. "Adani" + "Stocks", "Adani" + "Port"
                # BUT if Query is just the Sector Name (e.g. Query="Finance"), we just search the sub-topic
                
                if keyword.lower() == sector_context.lower():
                    # Generic Sector Search: Just search the topic directly
                    # e.g. Query="Finance" -> Search "Banking", "Stocks"
                    safe_topic = requests.utils.quote(topic)
                    queries.append(safe_topic)
                else:
                    # Specific Company/Entity Search: Combine them
                    # e.g. Query="Nvidia" -> Search "Nvidia stocks", "Nvidia chips"
                    safe_topic = requests.utils.quote(topic)
                    queries.append(f"{base_query}%20{safe_topic}")
        
        # Use the regions requested by the user
        regions = target_regions
        
        # BALANCED CONCURRENCY for Resilience
        semaphore = asyncio.Semaphore(5)
        
        # OMEGA TOR CONNECTOR
        connector = None
        if use_tor:
            print("🛡️ Tor Mode: Routing through 127.0.0.1:9150")
            connector = ProxyConnector.from_url("socks5://127.0.0.1:9150")
        
        # 3. BALANCED TIME SLICING
        # Saturation mode: 2-hour chunks (12/day)
        # Normal mode: 4-hour chunks (6/day)
        hour_step = 2 if saturation_mode else 4
        
        expanded_queries = queries 
        
        urls = []
        for i in range(days):
            date_obj = datetime.now() - timedelta(days=i)
            date_str = date_obj.strftime("%Y-%m-%d")
            
            # We slice the day into 4-hour chunks (6 per day)
            for h in range(0, 24, 4):
                # Google News RSS supports 'after'/'before' which allows some day-level variety
                # By creating unique URLs for different chunks, we ensure we don't hit the 100-limit per URL
                time_filter = f"%20after%3A{date_str}" 
                
                for q in expanded_queries:
                    for region in target_regions:
                        hl = "en-" + region.split(':')[0]
                        gl = region.split(':')[0]
                        ceid = region
                        
                        # Add a fake 'chunk' identifier to differentiate URLs
                        url = f"https://news.google.com/rss/search?q={q}{time_filter}&hl={hl}&gl={gl}&ceid={ceid}&chunk={h}"
                        urls.append(url)
        
        # Stats for progress
        total_tasks = len(urls)
        completed_tasks = 0
        
        async def fetch_with_semaphore(url):
            nonlocal completed_tasks
            async with semaphore:
                # Proactive Tor Rotation in Saturation Mode
                # Change IP every 15-20 requests to stay fresh
                if use_tor and saturation_mode and completed_tasks > 0 and completed_tasks % 20 == 0:
                    print(f"🔄 Saturation Mode: Proactive IP Rotation at {completed_tasks} tasks...")
                    renew_tor_identity()
                    # Extra sleep to let Tor stabilize
                    await asyncio.sleep(3)

                # MANDATORY JITTERED DELAY
                await asyncio.sleep(random.uniform(1.0, 3.5))
                
                results = await fetch_rss_async(url, connector=connector)
                
                # Update Progress
                completed_tasks += 1
                if progress_callback:
                    try:
                        progress_callback(completed_tasks, total_tasks)
                    except: pass
                
                return results

        print(f"📡 Launching Resilient Search with {len(urls)} agents (Traffic Smoothing Active)...")
        tasks = [fetch_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks)
        
        all_results = []
        for res_list in results:
            all_results.extend(res_list)
        return all_results
    
    # START THE SEARCH!
    all_entries_lists = asyncio.run(fetch_resilient_sources(progress_callback))
    
    seen_titles = set()
    
    # Process all the results we got back
    for entry in all_entries_lists:
        title = entry.get('title', '')
        
        # --- Deduplication ---
        # --- Deduplication ---
        # Robust normalization: remove special characters and whitespace to catch minor variations
        norm_title = re.sub(r'[^a-zA-Z0-9]', '', title).lower()
        
        if title and norm_title not in seen_titles:
            seen_titles.add(norm_title)
            
            # Clean description
            raw_description = entry.get('summary', '')
            soup = BeautifulSoup(raw_description, 'html.parser')
            clean_description = soup.get_text(separator=' ', strip=True)
            
            articles.append({
                'title': title,
                'description': clean_description if clean_description else 'No description',
                'source': entry.get('source', {}).get('title', 'Unknown'),
                'link': entry.get('link', ''),
                'published': entry.get('published', '')
            })
            
            # Stop if max reached
            if len(articles) >= max_articles:
                break
    
    return articles
