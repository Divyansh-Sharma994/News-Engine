"""
Article Content Scraper (Async) - High Accuracy
This file visits website links and reads the text for us.
Think of this as a "Digital Reader Robot".
"""

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import re
import json
import random
import ssl
import requests 
from urllib.parse import urlparse, parse_qs
from aiohttp_socks import ProxyConnector
from aiohttp import DummyCookieJar, TCPConnector, ClientResponseError
try:
    from gdelt_fetcher import renew_tor_identity, TorManager
except ImportError:
    # Failsafe if imported in a way that doesn't allow direct import
    class TorManager:
        @classmethod
        async def renew_identity(cls): renew_tor_identity()
        @classmethod
        async def wait_if_cooldown(cls): pass

# --- AIOHTTP LIMIT HACK ---
# Yahoo Finance sends massive headers. aiohttp defaults to 8190 bytes.
# We must increase this globally to avoid "Header value is too long" errors.
# We access the internal http_parser to change defaults.
from aiohttp import http_parser
http_parser.MAX_LINE_SIZE = 65536
http_parser.MAX_FIELD_SIZE = 65536
http_parser.MAX_HEADERS = 65536

# --- GOOGLE NEWS DECODER ---
# What is this?
# Google News gives us "encrypted" links (like news.google.com/Cahd...).
# If we click them, they redirect us. But our robot needs to know the REAL link
# (like msn.com/article) BEFORE it visits, so it doesn't get blocked.
async def decode_google_news_url(session, url):
    """
    Decodes a 'news.google.com' URL to the actual source URL using a special API.
    """
    try:
        # We mimic multiple modern browsers to avoid detection
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        headers = {
            'User-Agent': random.choice(user_agents),
        }
        
        # 1. Ask Google for the page
        async with session.get(url, headers=headers, allow_redirects=True) as resp:
            text = await resp.text()

        # 2. Extract the hidden code (called 'data-p') from the page HTML
        soup = BeautifulSoup(text, 'lxml')
        c_wiz = soup.select_one('c-wiz[data-p]')
        
        if not c_wiz:
            # If we can't find the code, maybe it's already a real link?
            if "news.google.com" not in str(resp.url):
                return str(resp.url)
            return url

        data_p = c_wiz.get('data-p')
        
        # 3. Prepare a "secret message" to send to Google's backend API
        # We replace some characters to match the format Google expects.
        obj = json.loads(data_p.replace('%.@.', '["garturlreq",'))
        
        payload = {
            'f.req': json.dumps([[['Fbv4je', json.dumps(obj[:-6] + obj[-2:]), 'null', 'generic']]])
        }
        
        api_headers = {
            'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
            'user-agent': headers['User-Agent'],
        }

        # 4. Send the message to the 'batchexecute' API
        async with session.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            headers=api_headers,
            data=payload
        ) as api_resp:
            api_text = await api_resp.text()

        # 5. Read the reply. It's messy, so we clean it up.
        cleaned_text = api_text.replace(")]}'", "").strip()
        array_data = json.loads(cleaned_text)
        
        # The real URL is hidden deep inside the response list.
        main_array_string = array_data[0][2]
        inner_array = json.loads(main_array_string)
        real_url = inner_array[1]
        
        return real_url

    except Exception as e:
        # If anything goes wrong, just return the original URL and hope for the best.
        print(f"⚠️ Failed to decode Google News URL: {e}")
        return url


# This function goes to a single website link and reads the FULL text.
async def scrape_article_content_async(session, url, use_tor=False):
    """
    Go to a website and read the entire article.
    Also checks if the article requires a subscription.
    """
    try:
        # Wait if another worker is rotating Tor
        if use_tor:
            await TorManager.wait_if_cooldown()

        # STEP 1: If it's a Google link, decode it first!
        if "news.google.com" in url:
            decoded_url = await decode_google_news_url(session, url)
            if decoded_url != url:
                url = decoded_url
        
        # headers = "Identity Card". We show this to websites so they let us in.
        # We mimic multiple modern browsers to avoid detection
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]

        headers = {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://news.google.com/', # We say "Google sent us!"
        }
        
        if use_tor:
            headers['Connection'] = 'close'
        
        # STEP 2: Download the page
        # We wait up to 30 seconds.
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30), allow_redirects=True) as response:
            if response.status in [429, 503] and use_tor:
                print(f"⚠️ Rate limited on {url}. Triggering Global Tor Rotation...")
                await TorManager.renew_identity()
                # We don't retry here to keep it simple
            
            if response.status != 200:
                # 401/403 means "Access Denied" (Paywall)
                if response.status in [401, 403]:
                    return {"full_text": "", "summary": "", "is_paywall": True}
                return None
            
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml') # BeautifulSoup makes the HTML readable
            
            # --- PAYWALL DETECTION ---
            # We look for specific words that mean "You need to pay".
            paywall_keywords = [
                "subscription required", "subscribe now", "already a subscriber", 
                "log in to continue", "read the full article", "premium content", 
                "register to continue", "you have reached your limit"
            ]
            
            text_lower = soup.get_text().lower()
            is_paywall = False
            for keyword in paywall_keywords:
                if keyword in text_lower[:1000]: # Check top of page
                    is_paywall = True
                    break
            
            # --- CLEANING THE PAGE (Aggressive) ---
            # Remove ads, menus, popups, and other junk.
            # We add more specific junk tags and classes.
            for noise in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe", "button", "input", "textarea", "select", "option", "ads", "noscript", "svg", "figure", "figcaption"]):
                noise.decompose()

            # Remove elements with "bad" class/id names
            # This is a heuristic to kill sidebars, popups, and sticky headers that survive tag cleaning
            bad_patterns = re.compile(r"ad-|ads|promo|subscribe|popup|cookie|menu|sidebar|social|share|comment|newsletter|related", re.IGNORECASE)
            for tag in soup.find_all(attrs={"class": bad_patterns}) + soup.find_all(attrs={"id": bad_patterns}):
                tag.decompose()
            
            # --- FINDING THE ARTICLE TEXT (Smart Logic) ---
            # 1. Try to find the <article> tag (Standard HTML5)
            article_tag = soup.find('article')
            
            # 2. Try to find common "main content" divs
            content_div = soup.find('div', class_=re.compile(r"content|body|article|story|main", re.IGNORECASE))
            
            target = None
            if article_tag:
                target = article_tag
            elif content_div:
                target = content_div
            else:
                 # 3. Last Resort Heuristic: Find the element with the most paragraph text
                 # We look for parents of <p> tags and see which one contains the most text.
                 parents = {}
                 for p in soup.find_all('p'):
                    text = p.get_text(strip=True)
                    if len(text) > 50: # Only count substantial paragraphs
                        parent = p.parent
                        if parent not in parents:
                            parents[parent] = 0
                        parents[parent] += len(text)
                 
                 # Pick the parent with the most text
                 if parents:
                     target = max(parents, key=parents.get)
                 else:
                     target = soup.body

            if not target: target = soup
            
            # Collect all paragraphs from the best container
            paragraphs = []
            # Get all text, but ensure nice spacing
            for p in target.find_all(['p', 'h2', 'h3', 'li']):
                # Simple filter: don't include copyright footers or tiny text
                text = p.get_text(separator=' ', strip=True)
                
                # Stronger filter for "Read more", "Click here", etc.
                if len(text) > 30 and "copyright" not in text.lower() and "all rights reserved" not in text.lower():
                    paragraphs.append(text)
            
            # Join them
            full_text = '\n\n'.join(paragraphs)
            
            # FAILSAFE: If the "Smart" logic found nothing (maybe it's a div-soup website)
            # Try just grabbing all text from the body if it's not too huge
            if len(full_text) < 200:
                all_text = soup.get_text(separator='\n\n', strip=True)
                # If the raw text isn't massive (garbage), use it
                if len(all_text) > 200 and len(all_text) < 50000:
                     full_text = all_text

            # Clean up massive whitespace blocks
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            
            # Create a short summary (first 3 paragraphs)
            if len(paragraphs) > 0:
                summary = ' '.join(paragraphs[:3])
            else:
                summary = full_text[:400] + "..." if len(full_text) > 400 else full_text
            
            # Final Check for Paywalls
            if len(full_text) < 500 and ("subscribe" in text_lower or "login" in text_lower or "register" in text_lower):
                is_paywall = True

            return {
                "full_text": full_text,
                "summary": summary,
                "is_paywall": is_paywall
            }
    
    except Exception as e:
        # Fallback for "Header value is too long" (common with Yahoo)
        # aiohttp fails, so we try 'requests' (synchronous) in a thread
        if "Header value is too long" in str(e) or "Got more than" in str(e):
            try:
                # Run sync requests in thread
                loop = asyncio.get_event_loop()
                def fetch_sync():
                    return requests.get(url, headers=headers, timeout=30, verify=False)
                
                response = await loop.run_in_executor(None, fetch_sync)
                
                if response.status_code == 200:
                    html = response.text
                    soup = BeautifulSoup(html, 'lxml')
                    
                    # --- PAYWALL DETECTION (Duplicate logic) ---
                    paywall_keywords = [
                        "subscription required", "subscribe now", "already a subscriber", 
                        "log in to continue", "read the full article", "premium content", 
                        "register to continue", "you have reached your limit"
                    ]
                    text_lower = soup.get_text().lower()
                    is_paywall = False
                    for keyword in paywall_keywords:
                        if keyword in text_lower[:1000]: 
                            is_paywall = True
                            break
                    
                    # --- CLEANING THE PAGE (Aggressive) ---
                    for noise in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe", "button", "input", "textarea", "select", "option", "ads", "noscript", "svg", "figure", "figcaption"]):
                        noise.decompose()
                    
                    bad_patterns = re.compile(r"ad-|ads|promo|subscribe|popup|cookie|menu|sidebar|social|share|comment|newsletter|related", re.IGNORECASE)
                    for tag in soup.find_all(attrs={"class": bad_patterns}) + soup.find_all(attrs={"id": bad_patterns}):
                        tag.decompose()
                    
                    # --- FINDING THE ARTICLE TEXT (Simple Fallback) ---
                    paragraphs = []
                    for p in soup.find_all(['p', 'h2', 'h3']):
                        text = p.get_text(separator=' ', strip=True)
                        if len(text) > 30:
                            paragraphs.append(text)
                    
                    full_text = '\n\n'.join(paragraphs[:500]) # Limit to avoid huge processing
                    if not full_text:
                        full_text = soup.get_text(separator='\n\n', strip=True)[:5000]
                        
                    summary = full_text[:400] + "..." if len(full_text) > 400 else full_text
                    
                    return {
                        "full_text": full_text,
                        "summary": summary,
                        "is_paywall": is_paywall
                    }
            except Exception as e2:
                 print(f"⚠️ Requests fallback failed for {url}: {e2}")

        # If scraping fails, we ignore it safely.
        print(f"⚠️ Scraping failed for {url}: {e}")
        return None

# This function updates our list of articles with the detailed info
async def enhance_articles_async(articles, limit=None, progress_callback=None, use_tor=False):
    """
    Process articles to get full content.
    This runs 'scrape_article_content_async' for MANY articles at once.
    """
    # Deduplicate links before processing
    unique_targets = []
    seen_links = set()
    for article in (articles[:limit] if limit else articles):
        link = article.get('link')
        if link and link not in seen_links:
            seen_links.add(link)
            unique_targets.append(article)
    
    targets = unique_targets
    total = len(targets)
    completed = 0
    
    # Use DummyCookieJar to discard cookies (prevents "Header value too long" errors from Yahoo)
    jar = DummyCookieJar()
    
    # Reduced concurrency for Tor mode to avoid circuit overload
    concurrency = 5 if use_tor else 20
    semaphore = asyncio.Semaphore(concurrency)

    async def sem_scrape(session, url):
        async with semaphore:
            result = await scrape_article_content_async(session, url, use_tor=use_tor)
            
            nonlocal completed
            completed += 1
            if progress_callback:
                try:
                    progress_callback(completed, total)
                except:
                    pass
            return result

    # OMEGA TOR CONNECTOR PROVIDER
    def get_connector():
        # Create unverified SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        if use_tor:
            # Pass ssl context to ProxyConnector if supported, otherwise let it be
            # aiohttp_socks typically handles this via the `ssl` param in request or connector
            # For ProxyConnector, we often pass it to the ClientSession or the connector constructor
            return ProxyConnector.from_url("socks5://127.0.0.1:9150", ssl=ssl_context)
        
        # Standard TCP connector without SSL verification
        return TCPConnector(ssl=False)

    # We use a context manager to ensure the session is closed, 
    # but we might need to recreate it if Tor rotates.
    # However, for simplicity, we'll keep one session per enhancement run
    # and let individual scrapes wait for cooldown.
    connector = get_connector()
    async with aiohttp.ClientSession(cookie_jar=jar, connector=connector) as session:
        tasks = []
        for article in targets:
            tasks.append(sem_scrape(session, article['link']))
        
        results = await asyncio.gather(*tasks)
        
        for i, result in enumerate(results):
            original_description = targets[i].get('description', '')
            
            if result and len(result.get('full_text', '')) > 100:
                # Success!
                targets[i]['full_text'] = result['full_text']
                targets[i]['summary'] = result['summary']
                targets[i]['is_paywall'] = result['is_paywall']
            else:
                # FALLBACK: If scraping failed or returned empty text
                # Use the RSS description we already have!
                fallback_msg = f"⚠️ Could not scrape full content automatically.\n\n**Summary from Source:**\n{original_description}"
                
                targets[i]['full_text'] = fallback_msg
                targets[i]['summary'] = original_description
                targets[i]['is_paywall'] = False
        
    return targets
