"""
    This class is use to webscrapp adzuna jobs
"""

import sys
import time
import requests
from bs4 import BeautifulSoup as bs_adzuna
import pandas as pd


"""
    availables countries are au, be, br, ca, ch, de, es, fr, gb, in, it, mx, nl, nz, pl, sg, us, za
    us as United States, gb as Great Britain, at as Autralia and so on
"""

countries = ['au','be','br','ca','ch','de','es','fr','gb','in','it','mx','nl','nz','pl','sg','us','za']


#get data and create data frame
for country in countries:

    df = pd.DataFrame()
    print(country)

    error_page = 0
    error_type = 200

    for page in range(1,100):
        print(page)
        time.sleep(2)


        """
            Adzuna limits the number of requests by key id for 24H.
            Reason why we use two key id in order to get more jobs possible in less time. 
        """

        #first key id
        #url = "https://api.adzuna.com/v1/api/jobs/"+str(country)+"/search/"+str(page)+"?app_id=5af6dd44&app_key=9eaaa1ee41c2d62124d0b345d43499ff&results_per_page=100"
                
        #second key id
        url = "https://api.adzuna.com/v1/api/jobs/"+str(country)+"/search/"+str(page)+"?app_id=203b447b&app_key=d5b8b476bf953c4fefe2d21abf95bd92&results_per_page=100"


        #print(url)


        page_adzuna_new = requests.get(url)

        if page_adzuna_new.status_code == 200:
            try:
                new_data = page_adzuna_new.json()
                recs_new = new_data['results']
                df_new = pd.json_normalize(recs_new)
                df = pd.concat([df, df_new]).reset_index(drop=True)
                df["source"] = "adzuna"
                df['country']=  country
            except ValueError:
                print("Response is not valid JSON:", page_adzuna_new.text)
        else:
            print(f"Error: {page_adzuna_new.status_code}, Response: {page_adzuna_new.text}")
            error_page +=1

            error_type = page_adzuna_new.status_code

            """
                if we have more than 10 pages error, it seems like we don't get any data for that country.
                In this case, we just change the country and try to get data from another one.
            """
            if (error_page == 10): 
                print("Switching Country")
                countries_iter = iter(countries)         # Convert country list to iterator
                country = next(countries_iter, None)      # Get the next country item or None if at the end
                print(country)
                error_page = 0
                df = pd.DataFrame()
                break
            
            """
                if we have 429 error, it seems like we have exceeded our limit for the day.
                In this case, we stop the webscrapping script.
            """
            if error_type == 429: sys.exit()

            
    #save raw data
    df.to_pickle('outputs/raw/jobs_adzuna_'+country+'_nov.pkl')



    
print("/////////////////////////////////////")
print(df.head(5))
#print(df.columns)
#print(df['company.display_name'].head(5))


"""
Next Step for AirFlow

params "created": "2024-05-14T16:53:36Z"
day: 2024-05-14
params = {
    "created_date": day  # Replace 'created_date' with the actual parameter name
}

# Send the GET request
response = requests.get(api_url, params=params)
"""