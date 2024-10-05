import requests

TMDB_API_KEY = '14137c24b9825da634b9dbf967068220'
TMDB_BASE_URL = 'https://api.themoviedb.org/3/search/movie'

def fetch_tmdb_data(movie_title, timeout=None):
    params = {
        'api_key': TMDB_API_KEY,
        'query': movie_title,
        'language': 'en-US',
        'page': 1
    }

    try:
        response = requests.get(TMDB_BASE_URL, params=params, timeout=timeout)
        response.raise_for_status()  
        data = response.json()
        
        if data['results']:
            return data['results'][0] 
        return None
    except requests.exceptions.RequestException as e:
        print(f"TMDb API request failed: {e}")
        return None
