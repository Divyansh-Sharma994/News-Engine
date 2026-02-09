"""
Production-Grade NER-based Entity Extraction for News Intelligence
Implements strict company/organization filtering with dominance-based ranking
"""

import re
from collections import Counter, defaultdict
from typing import List, Dict, Tuple, Set
import warnings
warnings.filterwarnings('ignore')

# Use transformers-based NER (optional - fallback to pattern-based)
def load_ner_model():
    """
    Factory function to load the NER model safely.
    Returns: (pipeline, available_bool)
    """
    try:
        from transformers import pipeline
        # Aggregation strategy simple merges sub-tokens (B-ORG, I-ORG) into one entity
        model = pipeline("ner", model="dslim/bert-base-NER", aggregation_strategy="simple")
        print("✅ Transformers NER loaded successfully")
        return model, True
    except Exception as e:
        print(f"⚠️ Transformers not available, using pattern-based extraction: {e}")
        return None, False

# Default global state - initially None
ner_pipeline = None
NER_AVAILABLE = False


class AdvancedNERExtractor:
    """
    Production-grade entity extractor with strict company/organization filtering
    """
    
    def __init__(self, ner_instance=None):
        # Use passed instance if available, otherwise fallback to global (which is likely None now)
        self.ner = ner_instance if ner_instance else ner_pipeline
        
        # STRICT: Publishers and news outlets to exclude
        self.excluded_publishers = {
            'reuters', 'bloomberg', 'cnbc', 'cnn', 'bbc', 'forbes', 'techcrunch',
            'times', 'post', 'guardian', 'journal', 'news', 'press', 'media',
            'tribune', 'herald', 'gazette', 'chronicle', 'observer', 'telegraph',
            'associated press', 'ap news', 'afp', 'pti', 'ani', 'ians'
        }
        
        # STRICT: Generic terms that are NOT companies
        self.generic_terms = {
            'government', 'police', 'court', 'hospital', 'university', 'school',
            'company', 'corporation', 'industry', 'market', 'sector', 'department',
            'ministry', 'office', 'bureau', 'agency', 'service', 'center',
            'institute', 'foundation', 'trust', 'group', 'team', 'committee',
            'council', 'board', 'commission', 'authority', 'people', 'public',
            'officials', 'sources', 'experts', 'analysts', 'investors', 'customers'
        }
        
        # STRICT: Location/country indicators
        self.location_indicators = {
            'india', 'indian', 'us', 'usa', 'uk', 'china', 'chinese', 'japan',
            'america', 'american', 'europe', 'european', 'asia', 'asian',
            'delhi', 'mumbai', 'bangalore', 'london', 'new york', 'beijing',
            'tokyo', 'singapore', 'dubai', 'california', 'texas'
        }
        
        # Known company suffixes for validation
        self.company_suffixes = {
            'inc', 'corp', 'ltd', 'llc', 'co', 'group', 'holdings', 'technologies',
            'systems', 'solutions', 'services', 'industries', 'enterprises',
            'international', 'global', 'motors', 'energy', 'pharma', 'labs'
        }
        
        # Main actor position indicators (headline structure)
        self.main_actor_positions = ['start', 'subject']  # First 3 words or subject position
    
    def _is_valid_company_name(self, entity: str) -> bool:
        """
        STRICT validation: Is this truly a company/organization name?
        """
        entity_lower = entity.lower().strip()
        
        # Rule 1: Exclude publishers
        if any(pub in entity_lower for pub in self.excluded_publishers):
            return False
        
        # Rule 2: Exclude generic terms (single word)
        if ' ' not in entity_lower and entity_lower in self.generic_terms:
            return False
        
        # Rule 3: Exclude locations
        if entity_lower in self.location_indicators:
            return False
        
        # Rule 4: Must be capitalized (proper noun)
        if not entity[0].isupper():
            return False
        
        # Rule 5: Minimum length
        if len(entity) < 2:
            return False
        
        # Rule 6: Cannot be all uppercase (likely acronym without context)
        if entity.isupper() and len(entity) < 3:
            return False
        
        # Rule 7: Cannot contain only numbers
        if re.match(r'^[\d\s\-\/]+$', entity):
            return False
        
        return True
    
    
    # Involvement scoring removed for simplicity as per user request
    def _calculate_involvement_score(self, entity: str, headline: str, position: int, total_words: int) -> float:
        return 1.0 # Default value since we only care about mentions now
    
    def extract_entities_ner(self, articles: List[Dict], progress_callback=None) -> Dict[str, Dict]:
        """
        Extract entities using NER with strict filtering
        Returns: {entity_name: {mentions, involvement_scores, headlines}}
        """
        entity_data = defaultdict(lambda: {
            'mentions': 0,
            'involvement_scores': [],
            'headlines': [],
            'article_count': 0,
            'sources': set()
        })
        
        total_articles = len(articles)
        
        for idx, article in enumerate(articles):
            # Update progress
            if progress_callback:
                try:
                    progress_callback(idx + 1, total_articles)
                except:
                    pass

            headline = article.get('title', '')
            source = article.get('source', 'Unknown')
            
            # --- ENHANCEMENT: Include Article Content ---
            # We construct a "rich text" for analysis: Title + Summary + First 1000 chars of body
            # This ensures we capture mentions even if they aren't in the headline
            
            full_text = article.get('full_text', '')
            summary = article.get('summary', '')
            
            # Combine available text components
            # Priority: Title > Summary > Body start
            text_parts = [headline]
            
            if summary and len(summary) > 10:
                text_parts.append(summary)
            
            # If full text is available and different from summary, add the rest
            if full_text and len(full_text) > 50 and full_text != summary:
                text_parts.append(full_text)
                
            combined_text = ". ".join(text_parts)
            
            if not combined_text or len(combined_text) < 10:
                continue
            
            # CHUNKING LOGIC FOR LONG ARTICLES
            # We split the text into chunks of ~2000 characters to avoid model limits
            # and ensure we catch entities throughout the entire article.
            chunk_size = 2000
            chunks = [combined_text[i:i+chunk_size] for i in range(0, len(combined_text), chunk_size)]
            
            # Use NER or fallback to pattern-based
            all_entities = []
            for i, chunk in enumerate(chunks):
                if self.ner:
                    chunk_entities = self._extract_with_transformers(chunk)
                else:
                    chunk_entities = self._extract_with_patterns(chunk)
                
                # Adjust positions for chunks after the first one
                offset = i * chunk_size
                adjusted_entities = [(entity, pos + offset) for entity, pos in chunk_entities]
                all_entities.extend(adjusted_entities)
            
            entities = all_entities
            
            # Track article-level uniqueness for article_count
            seen_in_article = set()
            
            for entity_text, position in entities:
                # STRICT: Validate company/organization
                if not self._is_valid_company_name(entity_text):
                    continue
                
                # Normalization for keying
                entity_key = entity_text
                
                # Calculate involvement score (simplified)
                involvement = 1.0
                
                # Update stats
                entity_data[entity_key]['mentions'] += 1
                entity_data[entity_key]['involvement_scores'].append(involvement)
                entity_data[entity_key]['headlines'].append(headline)
                
                # Only add headline if not already there to save memory
                if headline not in entity_data[entity_key]['headlines']:
                    entity_data[entity_key]['headlines'].append(headline)
                entity_data[entity_key]['sources'].add(source)

                # Increment article_count only once per entity per article
                if entity_key not in seen_in_article:
                    entity_data[entity_key]['article_count'] += 1
                    seen_in_article.add(entity_key)
        
        return dict(entity_data)
    
    def _extract_with_transformers(self, text: str) -> List[Tuple[str, int]]:
        """Extract ORG entities using transformers NER"""
        try:
            results = self.ner(text)
            entities = []
            
            for item in results:
                # STRICT: Only ORG entities
                if item['entity_group'] == 'ORG':
                    entity_text = item['word'].strip()
                    # Estimate position
                    position = len(text[:item['start']].split())
                    entities.append((entity_text, position))
            
            return entities
        except:
            return self._extract_with_patterns(text)
    
    def _extract_with_patterns(self, text: str) -> List[Tuple[str, int]]:
        """Fallback: Pattern-based extraction"""
        entities = []
        words = text.split()
        
        i = 0
        while i < len(words):
            # Look for capitalized sequences (2-4 words)
            if words[i][0].isupper():
                entity_words = [words[i]]
                j = i + 1
                
                while j < len(words) and j < i + 4:
                    if words[j][0].isupper() or words[j].lower() in self.company_suffixes:
                        entity_words.append(words[j])
                        j += 1
                    else:
                        break
                
                if len(entity_words) >= 1:
                    entity = ' '.join(entity_words)
                    entities.append((entity, i))
                    i = j
                else:
                    i += 1
            else:
                i += 1
        
        return entities
    
    def rank_by_dominance(self, entity_data: Dict[str, Dict], total_articles: int) -> List[Dict]:
        """
        Rank entities purely by MENTION COUNT (Frequency).
        Simple logic: More mentions = Higher rank.
        """
        ranked = []
        
        for entity, data in entity_data.items():
            mentions = data['mentions']
            article_count = data['article_count']
            
            # NOISE REMOVAL: Minimum threshold
            # If very few mentions, ignore unless it's a tiny dataset
            if mentions < 2 and total_articles > 10:
                continue
            
            ranked.append({
                'name': entity,
                'mentions': mentions,
                'articles': article_count,
                # 'dominance_score' kept for compatibility but equals mentions
                'dominance_score': mentions, 
                'entity_type': 'company'
            })
        
        # Sort by mentions (descending)
        ranked.sort(key=lambda x: x['mentions'], reverse=True)
        
        # Add ranks
        for i, item in enumerate(ranked, 1):
            item['rank'] = i
        
        return ranked


def extract_top_companies(articles: List[Dict], query: str, top_n: int = 10, ner_model=None, progress_callback=None) -> List[Dict]:
    """
    Main function: Extract top trending companies/organizations
    
    Args:
        articles: List of article dictionaries with 'title', 'source'
        query: Search query (for context)
        top_n: Number of top entities to return
        ner_model: Optional pre-loaded NER pipeline
        progress_callback: Optional function(current, total)
    
    Returns:
        List of top N companies ranked by dominance
    """
    if not articles:
        return []
    
    extractor = AdvancedNERExtractor(ner_instance=ner_model)
    
    # Step 1: Extract entities with NER
    entity_data = extractor.extract_entities_ner(articles, progress_callback=progress_callback)
    
    # Step 2: Rank them
    ranked_entities = extractor.rank_by_dominance(entity_data, len(articles))
    
    # Step 3: Filter out the query itself (if it appears)
    # e.g. if searching for "NVIDIA", we don't want NVIDIA to be #1 result usually, 
    # or maybe we do? Let's keep it but maybe flag it? 
    # Usually users want to see *related* entities.
    # For now, we return everything.
    
    return ranked_entities[:top_n]

