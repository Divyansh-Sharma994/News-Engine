import os
import streamlit as st
import pandas as pd
from io import BytesIO
import asyncio
# import aiohttp # Not used directly here
# import re # Not used directly here
# app2.py
from advanced_ner_extractor import extract_top_companies, load_ner_model  # <--- Updated import
# Import our helper tools (which we wrote in other files)
from gdelt_fetcher import fetch_gdelt_simple
from article_scraper import enhance_articles_async
from sector_classifier import classify_sector

@st.cache_resource(show_spinner=False)
def get_ner_pipeline():
    """
    Cached function to load the NER model once across the app session.
    """
    model, available = load_ner_model()
    return model

# --- PAGE SETUP ---
# This configures the browser tab title and layout
st.set_page_config(page_title="News Intelligence", layout="wide", initial_sidebar_state="collapsed")

# --- CUSTOM STYLING (CSS) ---
# We hide the default Streamlit menu to make it look like a real app.
st.markdown("""
<style>
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
</style>
""", unsafe_allow_html=True)

# --- THEME CONTROL ---
# This remembers if you like Dark Mode or Light Mode.
if 'theme' not in st.session_state:
    st.session_state.theme = 'dark'

col_theme = st.columns([0.95, 0.05])
with col_theme[1]:
    # The toggle button for theme
    if st.button("🌓" if st.session_state.theme == 'dark' else "🌞", help="Toggle theme"):
        st.session_state.theme = 'light' if st.session_state.theme == 'dark' else 'dark'
        st.rerun()

# Apply Light Mode colors if selected
if st.session_state.theme == 'light':
    st.markdown("""
    <style>
        .stApp { background-color: #FFFFFF; color: #000000; }
        .stMarkdown, .stText, h1, h2, h3 { color: #000000 !important; }
    </style>
    """, unsafe_allow_html=True)

# --- APP HEADER ---
if os.path.exists("Mavericks logo.png"):
    st.image("Mavericks logo.png", width=150)
st.title("📰 News Search Engine")
st.caption("Enter a keyword to find the latest news articles with full content previews.")

# Initialize our "memory" to store articles
if "articles" not in st.session_state:
    st.session_state.articles = []

# --- INPUT SECTION (Search Bar) ---
    
# --- INPUT SECTION (Search Bar) ---

# Map friendly names to Google News codes
REGION_MAP = {
    "India 🇮🇳": "IN:en",
    "USA 🇺🇸": "US:en",
    "UK 🇬🇧": "GB:en",
    "Australia 🇦🇺": "AU:en",
    "Canada 🇨🇦": "CA:en",
    "Singapore 🇸🇬": "SG:en",
    "New Zealand 🇳🇿": "NZ:en",
    "Ireland 🇮🇪": "IE:en",
    "South Africa 🇿🇦": "ZA:en",
    "Philippines 🇵🇭": "PH:en",
    "Malaysia 🇲🇾": "MY:en",
    "Pakistan 🇵🇰": "PK:en",
    "Hong Kong 🇭🇰": "HK:en",
    "UAE 🇦🇪": "AE:en",
    "Europe 🇪🇺": "EU:en",
    "Global 🌐": "WORLD:en"
}

# Layout: [Sector/Keyword (2)] [Region (1)] [Days (1)]
col1, col2, col3 = st.columns([2, 1, 1])

with col1:
    sector_input = st.selectbox(
        "📂 Select Sector", 
        ["Lifestyle", "Sustainability", "Tech & AI", "Health", "Finance", "Education", "Sports", "Startups", "CUSTOM"],
        index=2 # Default to Tech & AI
    )
    
    if sector_input == "CUSTOM":
        custom_keyword = st.text_input("🔍 Enter Custom Sector/Keyword", help="Type your topic")
        query = custom_keyword
    else:
        query = sector_input

with col2:
    # Moved from Sidebar to Main UI for visibility
    selected_region_names = st.multiselect(
        "🌍 Regions",
        options=list(REGION_MAP.keys()),
        default=["India 🇮🇳"],
        help="Select which countries to source news from."
    )
    
    # Convert names to codes
    selected_region_codes = [REGION_MAP[name] for name in selected_region_names]
    
    if not selected_region_codes:
        selected_region_codes = list(REGION_MAP.values())

with col3:
    duration = st.number_input("📅 Days back", min_value=1, max_value=3650, value=7)

st.markdown("---")

# --- CONFIGURATION DEFAULTS ---
use_tor = False
saturation_mode = False

# --- SEARCH ACTION ---

# --- SEARCH ACTION ---
# This runs when you click the big red button
if st.button("🚀 Find News Articles", type="primary", use_container_width=True):
    # --- CUSTOM LOADER ---
    # Increased size: Ratio 2:5, Width 250
    col_img, col_txt = st.columns([2, 5])
    with col_img:
        if os.path.exists("loader.jpg"):
            st.image("loader.jpg", width=250)
    with col_txt:
        st.markdown("### 🛡️ Resilient Deep Search Active...")
        st.markdown("We are conducting a deep, stable search. To avoid being blocked, we are processing queries with traffic-smoothing delays. Please stay with us... ⏳")
        # Initialize Progress Bar immediately here to show 0%
        main_progress = st.progress(0, text="0% complete - Initializing Omega Strategy...")

    # --- INTERNAL CLASSIFICATION (For Custom Keywords) ---
    if sector_input == "CUSTOM" and query:
        # Get API key from secrets
        try:
            gemini_key = st.secrets.get("general", {}).get("GEMINI_API_KEY")
        except:
            gemini_key = None
        
        # Classify using Hybrid approach (Gemini > SBERT > Keywords)
        classified_sector = classify_sector(query, api_key=gemini_key)
        
        # Store for display later
        st.session_state.classified_sector = classified_sector
        print(f"DEBUG: Hybrid Classification for '{query}': {classified_sector}")

    else:
        # Reset if not custom
        st.session_state.classified_sector = None

    
    # --- PROGRESSIVE LOADING STATUS ---
    with st.status("🤖 AI Agent is working...", expanded=True) as status:
        
        # STEP 1: FIND LINKS
        # Fake a small progress update to show activity
        main_progress.progress(10, text="10% complete - Initializing Traffic Smoothing...")
        status.write(f"🔍 Resilient Mode: Slicing {duration} days into 4h-chunks for stable high-volume retrieval...")
        
        # Callback to update the UI during the Search Phase (10% -> 50%)
        def search_progress_handler(completed, total):
            pct = 10 + int((completed / total) * 40) # Scale 0-100 to 10-50
            msg = f"{pct}% - Searching: Agent {completed}/{total} active..."
            main_progress.progress(pct, text=msg)
            # We can't update status.write too fast or it flickers, so we just update bar
            
        # We ask for up to 50000 links
        raw_articles = fetch_gdelt_simple(
            query, 
            days=duration, 
            max_articles=50000, 
            progress_callback=search_progress_handler,
            target_regions=selected_region_codes,
            sector_context=st.session_state.get('classified_sector') if sector_input == "CUSTOM" else sector_input,
            use_tor=use_tor,
            saturation_mode=saturation_mode
        )
        
        # Jump to 50% after finding links
        main_progress.progress(50, text=f"50% complete - Found {len(raw_articles)} links... Processing...")
        
        if not raw_articles:
            status.update(label="❌ No news found!", state="error", expanded=False)
            st.error("No news found for this keyword. Please try another.")
            st.session_state.articles = []
        else:
            status.write(f"✅ Found {len(raw_articles)} links from around the web.")
            
            # STEP 2: READ CONTENT
            status.write(f"📖 Visiting all {len(raw_articles)} websites to extract content...")
            
            # This little function updates the main progress bar
            def update_progress(current, total):
                # We map the scraping progress (0-100%) to the remaining main progress (50-100%)
                scrape_percent = (current / total)
                total_percent = int(50 + (scrape_percent * 50))
                
                main_progress.progress(total_percent, text=f"{total_percent}% complete - Reading article {current}/{total}")
                
                # Update text every few items inside the status box too
                if current % 10 == 0 or current == total:
                     status.update(label=f"📖 Reading articles... ({int(scrape_percent*100)}%)")
            
            # RUN THE SCRAPER! (This visits all sites)
            try:
                # DEBUG: Check raw articles count
                st.write(f"DEBUG: Starting scrape for {len(raw_articles)} articles...")
                
                enhanced_articles = asyncio.run(enhance_articles_async(
                    raw_articles, 
                    limit=None, 
                    progress_callback=update_progress
                ))
                
                # DEBUG: Check result count
                st.write(f"DEBUG: Scrape finished. Enhanced {len(enhanced_articles)} articles.")
                
                main_progress.progress(100, text="100% complete - Done!")
                
                st.session_state.articles = enhanced_articles
                st.session_state.last_query = query
                
                # Collapse the status box when done
                status.update(label="✅ All Done! Articles ready.", state="complete", expanded=False)
            except Exception as e:
                st.error(f"CRITICAL ERROR during scraping: {e}")
                status.update(label="❌ Error during scraping", state="error")

# --- DISPLAY RESULTS ---
# --- DISPLAY RESULTS ---
if st.session_state.articles:
    
    # Check if we have a classified sector to show
    if st.session_state.get('classified_sector'):
        st.info(f"🧠 **AI Internal Context:** Classified as '{st.session_state.classified_sector}'")
        
 
    # ==============================================================
    # 🆕 NEW SECTION: MARKET INTELLIGENCE DASHBOARD
    # ==============================================================
    st.markdown("---")
    st.subheader("📊 Market Intelligence: Top Mentions")
    
    # Create a container for the analysis so it looks distinct
    with st.container():
        # We use a spinner because NER can take 1-2 seconds
        with st.spinner("🔍 Analyzing top brands and entities..."):
            
            # 1. Run the extraction tool
            # We use the current query context to help the extractor
            
            # Load the model (cached)
            ner_model = get_ner_pipeline()
            

            top_companies = extract_top_companies(
                st.session_state.articles, 
                st.session_state.get('last_query', query),
                top_n=5,
                ner_model=ner_model
            )
            
            if top_companies:
                # 2. Display Top 3 as Big Metrics + Total Articles on the right
                cols = st.columns(len(top_companies[:3]) + 1)
                for idx, company in enumerate(top_companies[:3]):
                    with cols[idx]:
                        st.metric(
                            label=f"#{idx+1} {company['name']}", 
                            value=f"{company['dominance_score']:.1f}", 
                            delta=f"{company['mentions']} mentions"
                        )
                
                # Show Total Articles in the right-most column
                with cols[-1]:
                    st.metric(
                        label="📄 Total Coverage", 
                        value=len(st.session_state.articles),
                        delta="Total Articles Found",
                        delta_color="normal"
                    )
                
                # 3. Detailed Breakdown in an Expander
                with st.expander(f"📉 View Full Leaderboard ({len(top_companies)} companies detected)"):
                    # Convert to a clean DataFrame for display
                    df_companies = pd.DataFrame(top_companies)
                    
                    # Select and rename columns for a cleaner table
                    display_df = df_companies[[
                        'rank', 'name', 'mentions', 'articles', 'dominance_score', 'avg_involvement'
                    ]].rename(columns={
                        'rank': 'Rank',
                        'name': 'Entity Name',
                        'mentions': 'Mentions',
                        'articles': 'Articles',
                        'dominance_score': 'Dominance Score',
                        'avg_involvement': 'Avg. Involvement (%)'
                    })
                    
                    st.dataframe(
                        display_df, 
                        hide_index=True, 
                        use_container_width=True,
                        column_config={
                            "Dominance Score": st.column_config.ProgressColumn(
                                "Dominance", 
                                format="%.1f", 
                                min_value=0, 
                                max_value=100
                            ),
                            "Avg. Involvement (%)": st.column_config.NumberColumn(
                                "Involvement",
                                format="%.1f%%"
                            )
                        }
                    )
            else:
                st.caption("ℹ️ No dominant corporate entities detected in this batch.")
                
    st.markdown("---")
    # ==============================================================
    # END NEW SECTION
    # ==============================================================

    st.subheader(f"📋 Results for '{st.session_state.get('last_query', query)}'")
    # --- SORTING OPTIONS ---
    col_sort1, col_sort2 = st.columns([1, 4])
    with col_sort1:
        sort_order = st.radio("Sort by Date:", ["Newest First ⬇️", "Oldest First ⬆️"], index=0)
    
    # Sort the articles based on selection
    try:
        reverse_sort = True if "Newest" in sort_order else False
        
        # Helper function to normalize timestamps (handles both tz-aware and tz-naive)
        def get_sortable_timestamp(article):
            if not article.get('published'):
                return pd.Timestamp.min.tz_localize(None)  # Make tz-naive
            
            timestamp = pd.to_datetime(article['published'], errors='coerce')
            
            if timestamp is pd.NaT:
                return pd.Timestamp.min.tz_localize(None)
            
            # Convert to timezone-naive for consistent comparison
            if timestamp.tz is not None:
                timestamp = timestamp.tz_localize(None)
            
            return timestamp
        
        st.session_state.articles.sort(
            key=get_sortable_timestamp, 
            reverse=reverse_sort
        )
    except Exception as e:
        st.warning(f"Could not sort articles: {e}")
    # --- SCROLLABLE CONTAINER ---
    # A box with fixed height (800px) so you can scroll inside it.
    with st.container(height=800):
        for i, article in enumerate(st.session_state.articles):
            title = article['title']
            source = article['source']
            summary = article.get('summary', 'No summary available.')
            full_text = article.get('full_text', '')
            is_paywall = article.get('is_paywall', False)
            link = article['link']
            published = article['published']
            
            # --- ARTICLE CARD ---
            with st.container():
                # Title fits?
                st.markdown(f"### {i+1}. [{title}]({link})")
                st.caption(f"**Source:** {source} | **Published:** {published}")
                
                # DROPDOWN: "Read Full Article Content"
                # Everything inside here is hidden until clicked
                with st.expander("📖 Read Full Article Content"):
                    # 1. Summary
                    st.markdown("#### Summary")
                    st.info(summary)
                    
                    # 2. Full Text
                    st.markdown("#### Full Article")
                    if is_paywall:
                        st.warning("🔒 **Subscription Required**: This article seems to be behind a paywall.")
                    
                    if full_text:
                        st.write(full_text)
                    else:
                        st.warning("⚠️ Could not extract full text.")

                st.markdown(f"🔗 [**Go to Valid Original Article**]({link})")
                st.markdown("---")

    # --- DOWNLOAD BUTTONS ---
    df = pd.DataFrame(st.session_state.articles)
    
    # Prepare Excel file
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='News')
    buffer.seek(0)
    
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        st.download_button(
            label="📥 Download as Excel",
            data=buffer,
            file_name=f"news_{st.session_state.get('last_query', 'results')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    with col_dl2:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"news_{st.session_state.get('last_query', 'results')}.csv",
            mime="text/csv"
        )
