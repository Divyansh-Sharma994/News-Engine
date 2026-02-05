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
from urllib.parse import urlparse, parse_qs

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

    except Exception:
        # If anything goes wrong, just return the original URL and hope for the best.
        return url


# This function goes to a single website link and reads the FULL text.
async def scrape_article_content_async(session, url):
    """
    Go to a website and read the entire article.
    Also checks if the article requires a subscription.
    """
    try:
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
        
        # STEP 2: Download the page
        # We wait up to 30 seconds.
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30), allow_redirects=True) as response:
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
            
            # --- CLEANING THE PAGE ---
            # Remove ads, menus, popups, and other junk.
            for noise in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe", "button", "ads", "noscript", "svg"]):
                noise.decompose()
            
            # --- FINDING THE ARTICLE TEXT (Smart Logic) ---
            # 1. Try to find the <article> tag (Standard HTML5)
            article_tag = soup.find('article')
            if article_tag:
                 target = article_tag
            else:
                 # 2. Heuristic: Find the element with the most paragraph text
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
                if len(text) > 30 and "copyright" not in text.lower():
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
    
    except Exception:
        # If scraping fails, we ignore it safely.
        return None

# This function updates our list of articles with the detailed info
async def enhance_articles_async(articles, limit=None, progress_callback=None):
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
    
    jar = aiohttp.CookieJar(unsafe=True)
    semaphore = asyncio.Semaphore(20)

    async def sem_scrape(session, url):
        async with semaphore:
            result = await scrape_article_content_async(session, url)
            
            nonlocal completed
            completed += 1
            if progress_callback:
                try:
                    progress_callback(completed, total)
                except:
                    pass
            return result

    async with aiohttp.ClientSession(cookie_jar=jar) as session:
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
