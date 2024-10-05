import time
import requests
import pandas as pd
from data.tmdb_operations import fetch_tmdb_data

REQUEST_TIMEOUT = 10  
REQUESTS_PER_MINUTE = 30  
BATCH_SIZE = 100  

csv_file = 'data/imdb_top_1000.csv'
df = pd.read_csv(csv_file)

df.columns = df.columns.str.strip()

movie_column = 'Series_Title'

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

def rate_limit(last_request_time, requests_count):
    if requests_count >= REQUESTS_PER_MINUTE:
        elapsed_time = time.time() - last_request_time
        wait_time = 60 - elapsed_time
        if wait_time > 0:
            print(f"Rate limit reached, {wait_time:.2f} waiting for seconds...")
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
        print(f"{movie_title} The request for has timed out.")
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

        genre_ids = tmdb_data.get('genre_ids', [])
        df.at[index, 'tmdb_genres'] = ', '.join(map(str, genre_ids)) if genre_ids else 'N/A'
        
        df.at[index, 'tmdb_budget'] = tmdb_data.get('budget', None)
        df.at[index, 'tmdb_revenue'] = tmdb_data.get('revenue', None)
        df.at[index, 'tmdb_runtime'] = tmdb_data.get('runtime', None)
        df.at[index, 'tmdb_status'] = tmdb_data.get('status', None)

    if (index + 1) % BATCH_SIZE == 0:
        df.to_csv('data/updated_imdb_top_1000.csv', index=False)
        print(f"{index + 1}movie processed, file saved.")

# I deleted the poster link column. You may want to delete it if you wish. Delete this line of code if you want
df.drop(columns=['Poster_Link'], inplace=True, errors='ignore')

# Sütunları kontrol et
print("Updated DataFrame columns:", df.columns.tolist())

output_file = 'data/updated_imdb_top_1000_v2.csv'  # new filename
df.to_csv(output_file, index=False)
print(f"All data has been successfully updated and saved in {output_file}")

