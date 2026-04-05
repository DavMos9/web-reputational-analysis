import requests


GOOGLE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"


def search_google(
    query: str,
    api_key: str,
    cx: str,
    max_results: int = 10,
    start: int = 1,
    timeout: int = 10
) -> dict:
    """Esegue una query sulla Google Custom Search API."""

    params = {
        "q": query,
        "key": api_key,
        "cx": cx,
        "num": max_results,
        "start": start
    }

    try:
        response = requests.get(GOOGLE_ENDPOINT, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        return {
            "source": "google",
            "query": query,
            "items": data.get("items", [])
        }

    except requests.exceptions.RequestException as exc:
        return {
            "source": "google",
            "query": query,
            "items": [],
            "error": str(exc)
        }