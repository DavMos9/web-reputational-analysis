from dotenv import load_dotenv
import os

load_dotenv()

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
NEWS_API_KEY    = os.getenv("NEWS_API_KEY")
GUARDIAN_API_KEY = os.getenv("GUARDIAN_API_KEY")
NYT_API_KEY     = os.getenv("NYT_API_KEY")

# HuggingFace token — opzionale. Propagato nell'ambiente affinché transformers
# lo rilevi automaticamente (aumenta il rate limit per il download del modello).
_hf_token = os.getenv("HF_TOKEN")
if _hf_token:
    os.environ["HF_TOKEN"] = _hf_token
