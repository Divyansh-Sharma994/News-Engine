"""
Hybrid Sector Classifier – 100 Sectors
Priority: Gemini → SBERT → Keyword Fallback
Optimized for accuracy + speed
"""

from sentence_transformers import SentenceTransformer
import numpy as np
import re

# ==============================
# GLOBALS (Lazy Loaded)
# ==============================
_sbert_model = None
_sector_embeddings = None


def get_sbert_model():
    global _sbert_model
    if _sbert_model is None:
        _sbert_model = SentenceTransformer("all-MiniLM-L6-v2")
    return _sbert_model


# ==============================
# 100 SECTORS
# ==============================
ALL_SECTORS = [
    "Politics","Government","Policy","Diplomacy","Law","Judiciary","Courts",
    "Elections","Administration","Regulation","Business","Economy","Finance",
    "Banking","Insurance","Investment","StockMarket","Startup","Corporate",
    "Acquisition","Manufacturing","Industry","Trade","Export","Import","MSME",
    "Logistics","Retail","Wholesale","Inflation","Technology",
    "ArtificialIntelligence","MachineLearning","DataScience","Cybersecurity",
    "Blockchain","Software","Hardware","Cloud","Internet","Media","Journalism",
    "SocialMedia","DigitalMarketing","Advertising","PublicRelations","Content",
    "Influencer","Telecom","Broadcasting","Healthcare","PublicHealth","Pharma",
    "Biotechnology","MedicalDevices","Hospitals","MentalHealth","Nutrition",
    "Fitness","Disease","Education","University","School","EdTech",
    "SkillDevelopment","Research","Exams","Students","Training","Career",
    "Environment","ClimateChange","Sustainability","RenewableEnergy","OilGas",
    "Electricity","Water","Waste","Wildlife","EnvironmentalPolicy","Agriculture",
    "Agritech","Farming","Crops","Livestock","Fisheries","FoodProcessing",
    "RuralDevelopment","Irrigation","Farmer","Society","Culture","Lifestyle",
    "Fashion","Entertainment","Film","Music","Sports","Tourism","Spirituality"
]


# ==============================
# SECTOR DESCRIPTIONS
# ==============================
SECTOR_DESCRIPTIONS = {
    "Politics": "political parties governance elections leaders",
    "Government": "state central administration ministries departments",
    "Policy": "public policy regulations frameworks decisions",
    "Diplomacy": "foreign relations ambassadors treaties",
    "Law": "laws acts legislation legal system",
    "Judiciary": "judges courts justice system",
    "Courts": "court cases trials verdicts",
    "Elections": "voting campaigns democracy",
    "Administration": "bureaucracy civil services governance",
    "Regulation": "compliance rules authorities",

    "Business": "business companies enterprises",
    "Economy": "economy gdp inflation growth recession",
    "Finance": "finance money markets capital",
    "Banking": "banks loans deposits credit",
    "Insurance": "insurance risk coverage policies",
    "Investment": "investments stocks bonds funds",
    "StockMarket": "shares trading exchange indices",
    "Startup": "startups entrepreneurship innovation",
    "Corporate": "corporate firms organizations",
    "Acquisition": "mergers acquisitions takeovers",

    "Manufacturing": "factories production industrial goods",
    "Industry": "industrial sector plants",
    "Trade": "trade commerce buying selling",
    "Export": "exports foreign trade",
    "Import": "imports foreign goods",
    "MSME": "small medium enterprises",
    "Logistics": "supply chain transport warehousing",
    "Retail": "retail consumer sales",
    "Wholesale": "bulk distribution",
    "Inflation": "price rise cost of living",

    "Technology": "technology innovation digital",
    "ArtificialIntelligence": "AI automation intelligence",
    "MachineLearning": "ML models training",
    "DataScience": "data analytics statistics",
    "Cybersecurity": "cyber security hacking threats",
    "Blockchain": "blockchain crypto web3",
    "Software": "software applications coding",
    "Hardware": "devices electronics computers",
    "Cloud": "cloud computing servers",
    "Internet": "web online networks",

    "Media": "news media publishing",
    "Journalism": "journalism reporting news",
    "SocialMedia": "social platforms facebook twitter",
    "DigitalMarketing": "seo online ads marketing",
    "Advertising": "advertising promotions ads",
    "PublicRelations": "PR brand reputation",
    "Content": "content creation articles videos",
    "Influencer": "influencers creators",
    "Telecom": "telecom mobile networks",
    "Broadcasting": "tv radio broadcasting",

    "Healthcare": "health medical care",
    "PublicHealth": "public health prevention",
    "Pharma": "pharmaceutical drugs medicine",
    "Biotechnology": "biotech genetics research",
    "MedicalDevices": "medical equipment devices",
    "Hospitals": "hospitals clinics",
    "MentalHealth": "mental health psychology",
    "Nutrition": "diet nutrition wellness",
    "Fitness": "exercise gym health",
    "Disease": "disease illness epidemic",

    "Education": "education learning teaching",
    "University": "universities higher education",
    "School": "schools students teachers",
    "EdTech": "online education platforms",
    "SkillDevelopment": "skills training vocational",
    "Research": "academic research studies",
    "Exams": "tests examinations",
    "Students": "students learners",
    "Training": "training programs courses",
    "Career": "jobs employment growth",

    "Environment": "environment nature ecology",
    "ClimateChange": "global warming climate",
    "Sustainability": "sustainable green eco",
    "RenewableEnergy": "solar wind clean energy",
    "OilGas": "oil gas fossil fuels",
    "Electricity": "power grid utilities",
    "Water": "water resources",
    "Waste": "waste recycling pollution",
    "Wildlife": "animals biodiversity",
    "EnvironmentalPolicy": "environment regulations",

    "Agriculture": "farming agriculture crops",
    "Agritech": "agricultural technology",
    "Farming": "farms cultivation",
    "Crops": "harvest crop production",
    "Livestock": "cattle dairy animals",
    "Fisheries": "fishing aquaculture",
    "FoodProcessing": "food manufacturing",
    "RuralDevelopment": "rural villages growth",
    "Irrigation": "water irrigation",
    "Farmer": "farmers agriculture workers",

    "Society": "social issues community",
    "Culture": "culture heritage traditions",
    "Lifestyle": "lifestyle living habits",
    "Fashion": "fashion clothing style",
    "Entertainment": "entertainment movies shows",
    "Film": "cinema filmmaking",
    "Music": "music songs artists",
    "Sports": "sports games athletes",
    "Tourism": "travel tourism",
    "Spirituality": "religion faith beliefs"
}


# ==============================
# PRECOMPUTE SBERT EMBEDDINGS
# ==============================
def build_sector_embeddings():
    global _sector_embeddings
    if _sector_embeddings is not None:
        return _sector_embeddings

    model = get_sbert_model()
    texts = [
        f"{sector}. {SECTOR_DESCRIPTIONS.get(sector, sector)}"
        for sector in ALL_SECTORS
    ]
    embeddings = model.encode(texts, normalize_embeddings=True)
    _sector_embeddings = dict(zip(ALL_SECTORS, embeddings))
    return _sector_embeddings


# ==============================
# GEMINI CLASSIFIER
# ==============================
def classify_with_gemini(keyword: str, api_key: str):
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        model = genai.GenerativeModel("gemini-1.5-flash")

        prompt = f"""
Classify the keyword into ONE most relevant sector.
Return ONLY the exact sector name from this list:

{", ".join(ALL_SECTORS)}

Keyword: {keyword}
"""

        response = model.generate_content(
            prompt,
            generation_config={"temperature": 0.0, "max_output_tokens": 16}
        )

        result = response.text.strip()
        normalized = re.sub(r"[^a-zA-Z]", "", result).lower()

        for sector in ALL_SECTORS:
            if normalized == re.sub(r"[^a-zA-Z]", "", sector).lower():
                return sector

        return None

    except Exception:
        return None


# ==============================
# SBERT CLASSIFIER
# ==============================
def classify_with_sbert(keyword: str):
    model = get_sbert_model()
    sector_embeddings = build_sector_embeddings()

    keyword_emb = model.encode(
        keyword.lower().strip(),
        normalize_embeddings=True
    )

    scores = {
        sector: float(np.dot(keyword_emb, emb))
        for sector, emb in sector_embeddings.items()
    }

    best_sector = max(scores, key=scores.get)
    if scores[best_sector] >= 0.28:
        return best_sector

    return None


# ==============================
# KEYWORD FALLBACK
# ==============================
KEYWORD_MAP = {
    "ai": "ArtificialIntelligence",
    "startup": "Startup",
    "election": "Elections",
    "court": "Courts",
    "hospital": "Hospitals",
    "disease": "Disease",
    "climate": "ClimateChange",
    "farming": "Agriculture",
    "sports": "Sports",
    "music": "Music",
    "film": "Film"
}

def classify_with_keywords(keyword: str):
    text = keyword.lower()
    for k, v in KEYWORD_MAP.items():
        if k in text:
            return v
    return "Business"


# ==============================
# FINAL CONTROLLER
# ==============================
def classify_sector(keyword: str, api_key: str = None) -> str:
    if not keyword or not keyword.strip():
        return "Business"

    if api_key:
        res = classify_with_gemini(keyword, api_key)
        if res:
            return res

    res = classify_with_sbert(keyword)
    if res:
        return res

    return classify_with_keywords(keyword)
