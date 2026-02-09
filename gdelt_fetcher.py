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
import threading

# =========================================================
# GLOBAL TOR MANAGER (Hardened Rate Limiting)
# =========================================================
class TorManager:
    """
    Manages global Tor state to prevent session conflicts and 
    simultaneous IP rotations.
    """
    _lock = asyncio.Lock()
    _last_renewal = 0
    _is_cooldown = False
    _cooldown_duration = 20 # Seconds to wait for Tor circuit to stabilize

    @classmethod
    async def renew_identity(cls, control_port=9151):
        async with cls._lock:
            now = time.time()
            # Prevent renewals more frequent than once every 30 seconds
            if now - cls._last_renewal < 30:
                print("⏳ Tor rotation requested too soon. Skipping...")
                if cls._is_cooldown:
                    await asyncio.sleep(5)
                return False

            cls._is_cooldown = True
            print("🌀 [GLOBAL LOCK] Requesting Tor IP Rotation...")
            success = renew_tor_identity(control_port)
            if success:
                cls._last_renewal = time.time()
                print(f"🚥 Circuit rebuilding... Waiting {cls._cooldown_duration}s for stability...")
                await asyncio.sleep(cls._cooldown_duration)
            
            cls._is_cooldown = False
            return success

    @classmethod
    async def wait_if_cooldown(cls):
        while cls._is_cooldown:
            await asyncio.sleep(1)



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

# Signals Tor for a New Identity (Change IP).
# Tor Browser Control Port is usually 9151.
# Tor Service Control Port is usually 9051.
def renew_tor_identity(control_port=9151):
    """
    Signals Tor for a New Identity (Change IP).
    """
    try:
        with Controller.from_port(port=control_port) as controller:
            controller.authenticate()
            controller.signal(Signal.NEWNYM)
            # Short sleep to allow the circuit to be rebuilt
            time.sleep(2)
            print("✅ Tor identity renewed successfully.")
            return True
    except Exception as e:
        print(f"❌ Failed to renew Tor identity: {e}")
        return False

# This is the main function we use to find news.
def fetch_gdelt_simple(keyword: str, days: int = 7, max_articles: int = 50000, progress_callback=None, target_regions: List[str] = None, sector_context: str = None, use_tor: bool = False, saturation_mode: bool = False) -> List[Dict]:
    """
    Search for news articles about a 'keyword'.
    It looks at news from the last 'days' days.
    
    If use_tor is True, it will route requests through Tor (127.0.0.1:9150).
    If saturation_mode is True, it uses extreme slicing to hit 700+ articles.
    """
    
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
        
        if use_tor:
            headers['Connection'] = 'close'
            
        return headers
    
    # This small function fetches one single RSS feed link with RETRY logic.
    async def fetch_rss_async(url, connector_provider):
        max_retries = 3
        base_delay = 5
        
        for attempt in range(max_retries + 1):
            try:
                # Wait if another worker is rotating Tor
                await TorManager.wait_if_cooldown()
                
                # Pick a random browser identity and headers
                headers = get_random_headers()
                
                # Get a fresh connector if Tor is active
                connector = connector_provider()
                
                # We wait up to 30 seconds for Google to reply.
                timeout = aiohttp.ClientTimeout(total=45, connect=10)
                async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                    async with session.get(url, headers=headers) as response:
                        if response.status == 200:
                            # If success, read the text and parse it as an RSS feed
                            content = await response.text()
                            feed = feedparser.parse(content) 
                            return feed.entries
                        
                        elif response.status in [429, 503]:
                            # Rate limited! AGGRESSIVE COOLDOWN
                            if attempt < max_retries:
                                # If using Tor, try to renew identity via Global Manager
                                if use_tor:
                                    print(f"⚠️ Rate limited (Status {response.status}) on {url}. Triggering Global Tor Rotation...")
                                    await TorManager.renew_identity()
                                
                                # Exponential backoff with a massive base + Jitter
                                delay = 10 * (2 ** attempt) + random.uniform(5.0, 15.0)
                                print(f"⚠️ Cooldown: Entering Deep Sleep for {delay:.1f}s...")
                                await asyncio.sleep(delay)
                                continue
                            else:
                                print(f"❌ Failed after {max_retries} retries (Rate Limit): {url}")
                                return []
                        else:
                            # Other error (404, etc), don't retry
                            print(f"⚠️ HTTP Error {response.status} for {url}")
                            return []
            except Exception as e:
                # Network error? Retry nicely.
                if attempt < max_retries:
                     delay = base_delay * (2 ** attempt) + random.uniform(1.0, 5.0)
                     print(f"⚠️ Network error ({str(e)}) - Retrying in {delay:.1f}s...")
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
        # Heavily reduced for Tor mode to avoid circuit overload
        concurrency = 5 if use_tor else 10 # Slightly increased for non-Tor due to more slots
        semaphore = asyncio.Semaphore(concurrency)
        
        # OMEGA TOR CONNECTOR PROVIDER
        def get_connector():
            if use_tor:
                return ProxyConnector.from_url("socks5://127.0.0.1:9150")
            return None
        
        urls = []
        # --- TIME SLICING LOGIC (4-Hour Slots for 24h coverage) ---
        for i in range(days):
            current_date = datetime.now() - timedelta(days=i)
            prev_date = datetime.now() - timedelta(days=i+1)
            
            current_date_str = current_date.strftime("%Y-%m-%d")
            prev_date_str = prev_date.strftime("%Y-%m-%d")
            
            # We create 6 "Logical Slots" per day to force 6 independent fetches
            # providing coverage for the requested 4-hour breakdown architecture.
            for slot in range(6):
                
                start_str = prev_date_str
                end_str = current_date_str
                time_filter = f"%20after%3A{start_str}%20before%3A{end_str}"
                
                for q in queries:
                    for region in target_regions:
                        hl = "en-" + region.split(':')[0]
                        gl = region.split(':')[0]
                        ceid = region
                        
                        # We append a dummy parameter `&u={slot}` to make the URL unique
                        # ensuring distinct fetch tasks for robustness.
                        url = f"https://news.google.com/rss/search?q={q}{time_filter}&hl={hl}&gl={gl}&ceid={ceid}&u={slot}"
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
                await asyncio.sleep(random.uniform(1.0, 3.0) if use_tor else random.uniform(0.5, 1.5))
                
                results = await fetch_rss_async(url, connector_provider=get_connector)
                
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
    
    seen_ids = set() # Dedupe by ID/Link
    seen_titles = set() # Dedupe by Title+Source
    
    # Master Time Window for 98% Accuracy
    master_end_date = datetime.now() + timedelta(days=1) 
    master_start_date = datetime.now() - timedelta(days=days + 1)
    
    print(f"🕵️ STRICT FILTERING: Keeping articles between {master_start_date.date()} and {master_end_date.date()}...")
    
    skipped_date = 0
    skipped_dedupe = 0
    
    # Process all the results we got back
    for entry in all_entries_lists:
        title = entry.get('title', '').strip()
        source = entry.get('source', {}).get('title', 'Unknown')
        link = entry.get('link', '')
        published_str = entry.get('published', '')
        
        # --- 1. STRICT TIME FILTERING ---
        if not published_str:
            continue
            
        try:
            # RSS dates are usually "Mon, 05 Feb 2024 12:00:00 GMT"
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_date = datetime.fromtimestamp(time.mktime(entry.published_parsed))
            else:
                continue 
            
            if pub_date < master_start_date or pub_date > master_end_date:
                skipped_date += 1
                continue
                
        except Exception:
            continue
            
        # --- 2. ROBUST DEDUPLICATION ---
        # Key 1: Link (Exact match)
        if link in seen_ids:
            skipped_dedupe += 1
            continue
        
        # Key 2: Normalized Title + Source
        norm_title = re.sub(r'\W+', ' ', title).lower().strip()
        dedup_key = (norm_title, source)
        
        if dedup_key in seen_titles:
            skipped_dedupe += 1
            continue
            
        # Add to known
        seen_ids.add(link)
        seen_titles.add(dedup_key)
            
        # Clean description
        raw_description = entry.get('summary', '')
        soup = BeautifulSoup(raw_description, 'html.parser')
        clean_description = soup.get_text(separator=' ', strip=True)
        clean_description = re.sub(r'\s*and more\s*»', '', clean_description)
        
        articles.append({
            'title': title,
            'description': clean_description if clean_description else 'No description',
            'source': source,
            'link': link,
            'published': published_str
        })
        
        # Stop if max reached
        if len(articles) >= max_articles:
            break
            
    print(f"✅ Final Articles: {len(articles)} (Skipped {skipped_date} out-of-window, {skipped_dedupe} duplicates)")
    return articles
