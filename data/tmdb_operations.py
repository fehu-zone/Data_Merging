import time
import requests
import pandas as pd

TMDB_API_KEY = '14137c24b9825da634b9dbf967068220'
TMDB_BASE_URL_SEARCH = 'https://api.themoviedb.org/3/search/movie'
TMDB_BASE_URL_DETAILS = 'https://api.themoviedb.org/3/movie'

REQUEST_TIMEOUT = 10  
REQUESTS_PER_MINUTE = 30  
BATCH_SIZE = 100  

csv_file = 'data/imdb_top_1000.csv'
df = pd.read_csv(csv_file)

df.columns = df.columns.str.strip()

movie_column = 'Series_Title'

# Yeni sütunlar ekle
df['tmdb_id'] = None
df['tmdb_vote_average'] = None
df['tmdb_release_date'] = None
df['tmdb_original_language'] = None
df['tmdb_popularity'] = None
df['tmdb_genres'] = None
df['tmdb_budget'] = None
df['tmdb_revenue'] = None
df['tmdb_runtime'] = None
df['tmdb_status'] = None

def fetch_tmdb_data(movie_title, timeout=None):
    search_params = {
        'api_key': TMDB_API_KEY,
        'query': movie_title,
        'language': 'en-US',
        'page': 1
    }

    try:
        response = requests.get(TMDB_BASE_URL_SEARCH, params=search_params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        
        if data['results']:
            movie_id = data['results'][0]['id']
            return fetch_tmdb_details(movie_id, timeout)  # Film detaylarını al
        return None
    except requests.exceptions.RequestException as e:
        print(f"TMDb API request failed: {e}")
        return None

def fetch_tmdb_details(movie_id, timeout=None):
    details_url = f"{TMDB_BASE_URL_DETAILS}/{movie_id}"
    params = {'api_key': TMDB_API_KEY, 'language': 'en-US'}
    
    try:
        response = requests.get(details_url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"TMDb details request failed for ID {movie_id}: {e}")
        return None

def rate_limit(last_request_time, requests_count):
    if requests_count >= REQUESTS_PER_MINUTE:
        elapsed_time = time.time() - last_request_time
        wait_time = 60 - elapsed_time
        if wait_time > 0:
            print(f"Rate limit reached, waiting for {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        return 0, time.time() 
    return requests_count, last_request_time

last_request_time = time.time()
requests_count = 0

for index, row in df.iterrows():
    movie_title = row[movie_column]
    
    requests_count, last_request_time = rate_limit(last_request_time, requests_count)

    try:
        tmdb_data = fetch_tmdb_data(movie_title, timeout=REQUEST_TIMEOUT)
        requests_count += 1  
    except requests.exceptions.Timeout:
        print(f"Request for {movie_title} has timed out.")
        continue 

    if tmdb_data:
        df.at[index, 'tmdb_id'] = tmdb_data.get('id', None)
        df.at[index, 'tmdb_vote_average'] = tmdb_data.get('vote_average', None)
        
        release_date = tmdb_data.get('release_date')
        if release_date:
            df.at[index, 'tmdb_release_date'] = release_date
            df.at[index, 'Released_Year'] = release_date[:4]
        else:
            df.at[index, 'tmdb_release_date'] = None
            df.at[index, 'Released_Year'] = None
        
        df.at[index, 'tmdb_original_language'] = tmdb_data.get('original_language', None)
        df.at[index, 'tmdb_popularity'] = tmdb_data.get('popularity', None)

        genre_ids = tmdb_data.get('genres', [])
        df.at[index, 'tmdb_genres'] = ', '.join([genre['name'] for genre in genre_ids]) if genre_ids else 'N/A'
        
        # Detaylı sorgudan çekilen veriler
        df.at[index, 'tmdb_budget'] = tmdb_data.get('budget', None)
        df.at[index, 'tmdb_revenue'] = tmdb_data.get('revenue', None)
        df.at[index, 'tmdb_runtime'] = tmdb_data.get('runtime', None)
        df.at[index, 'tmdb_status'] = tmdb_data.get('status', None)

        # Eksik veri kontrolü
        if tmdb_data.get('budget') is None:
            print(f"{movie_title} için bütçe bilgisi eksik.")
        if tmdb_data.get('revenue') is None:
            print(f"{movie_title} için gelir bilgisi eksik.")
        if tmdb_data.get('runtime') is None:
            print(f"{movie_title} için süre bilgisi eksik.")
        if tmdb_data.get('status') is None:
            print(f"{movie_title} için durum bilgisi eksik.")

    if (index + 1) % BATCH_SIZE == 0:
        df.to_csv('data/updated_imdb_top_1000.csv', index=False)
        print(f"{index + 1} movies processed, file saved.")

df.drop(columns=['Poster_Link'], inplace=True, errors='ignore')

print("Updated DataFrame columns:", df.columns.tolist())

output_file = 'data/updated_imdb_top_1000_v2.csv'
df.to_csv(output_file, index=False)
print(f"All data has been successfully updated and saved in {output_file}")
