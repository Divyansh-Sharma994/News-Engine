import asyncio
from gdelt_fetcher import fetch_gdelt_simple
# import logging

# logging.basicConfig(level=logging.INFO)

def test_search():
    print("Testing GDELT Fetcher...")
    try:
        articles = fetch_gdelt_simple(
            "Artificial Intelligence", 
            days=2, 
            max_articles=10, 
            use_tor=False 
        )
        print(f"✅ Found {len(articles)} articles.")
        
        # Test simplified NER
        from advanced_ner_extractor import extract_top_companies, load_ner_model
        model, _ = load_ner_model()
        top = extract_top_companies(articles, "Artificial Intelligence", top_n=5, ner_model=model)
        
        print("\n🏆 Top Brands (Simpliifed - Mentions Only):")
        for company in top:
            print(f"- {company['name']}: {company['mentions']} mentions")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_search()
