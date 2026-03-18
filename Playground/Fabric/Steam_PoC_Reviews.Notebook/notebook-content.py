# Fabric notebook source


# CELL ********************

import requests
import json
import time # We need to sleep between requests so Steam doesn't get mad

app_id = "105600" # Terraria
base_url = f"https://store.steampowered.com/appreviews/{app_id}?json=1"

# Steam uses '*' to mean "give me the very first page"
params = {
    "filter": "recent",
    "num_per_page": 100,
    "cursor": "*" 
}

my_tracer_reviews = []
max_pages = 3 # Hard limit so we don't sit here all day

for page in range(max_pages):
    print(f"Fetching page {page + 1}...")
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        
        # Grab the reviews array from the response
        reviews = data.get("reviews", [])
        my_tracer_reviews.extend(reviews)
        
        # Grab the cursor for the next page
        next_cursor = data.get("cursor")
        print(f" -> Grabbed {len(reviews)} reviews. Next cursor is: {next_cursor[:10]}...")
        
        # If no cursor is returned, or we got 0 reviews, we hit the end!
        if not next_cursor or len(reviews) == 0:
            print("No more reviews to fetch.")
            break
            
        # Update our parameters dictionary with the new cursor for the next loop iteration
        params["cursor"] = next_cursor
        
        # Sleep for 1.5 seconds to respect the API rate limits
        time.sleep(1.5)
    else:
        print(f"Failed! Status Code: {response.status_code}")
        break

print(f"\nSuccess! We collected {len(my_tracer_reviews)} reviews for our tracer bullet.")

