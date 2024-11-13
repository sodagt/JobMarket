
"""
Cette classe nous permet de webscrapper les sociétés/entreprises du site LinkedIn.

"""

#import sys
#sys.path.append('../')
import os
import time

import pandas as pd
import pickle



print("///////////////////////////////////// START COMPANY LINK")

company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities,company_links = [],[],[],[],[],[],[],[],[],[]



#pickle list to get companies links 
#with open("outputs/raw/company_links.pkl","rb") as file:
#    company_links_pkl = pickle.load(file)

#company_links_distinct = list(set(company_links))
#company_links_distinct = company_links_pkl 

company_links_distinct_df = pd.read_pickle("outputs/raw/company_links.pkl") 
#company_links_distinct_df = pd.read_pickle("outputs/raw/jobs_linkedin_nov_5.pkl") 

#company_links_distinct=company_links_distinct_df.values.tolist()
company_links_distinct=company_links_distinct_df[company_links_distinct_df.columns[0]].tolist()
#company_links_distinct=company_links_distinct_df[company_links_distinct_df.columns[12]].drop_duplicates().tolist()



print(type(company_links_distinct))

print(company_links_distinct[0])
print(company_links_distinct[4].split('?')[0]+ '/about/')




#helper before appendibg
def append_if_exists(data, target_list):
    if data:  # Checks if data is not None and not empty
        target_list.append(data)


#get more information from company link

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# Replace with your LinkedIn credentials
#username = os.getenv("$LINKEDIN_USERNAME")
#password = os.getenv("$LINKEDIN_PASSWORD")

username = "oadnanimak@gmail.com"
password = "ProjetFormationDE#2024"

print(username)
print(password)

# Setup Chrome driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Open LinkedIn login page
driver.get("https://www.linkedin.com/login")

# Find the username and password fields and fill them in
driver.find_element(By.ID, "username").send_keys(username)
driver.find_element(By.ID, "password").send_keys(password)

# Submit the login form
driver.find_element(By.XPATH, "//button[@type='submit']").click()

# Wait for login to complete (adjust as necessary)
print("SLEEP BEFORE COMPANY LINK")
time.sleep(5)

for company_link in company_links_distinct[:4]:
        

        print("/////// Linkedin Companies LInks /////// ")

        append_if_exists(company_link,company_links)


        #print(company_link)

        #parts = company_link.split('?')
        #print(parts)
        #company_link_about = ''.join(parts[0]) + '/about/'
        company_link_about = company_link.split('?')[0] + '/about/'

        print(company_link_about)

        # Navigate to the company page (replace with the URL of the company you want)        
        driver.get(company_link_about)


        #get company info

        # Wait for the page to load (adjust time as necessary)
        time.sleep(7)

        # Extract the company name (this may need to be adjusted depending on the page structure)
        try:
            #print(driver.current_url)
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            # Wait for the company page to be visible
            #time.sleep(5)
           

            #Helper function to check if an element exists and get its text
            def get_text_if_exists(by, value,attribute = 'text'):
                    try:
                        element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((by, value)))
                        if attribute == 'text':
                            return element.text if element else None
                        else:
                            return element.get_attribute(attribute)

                #try:
                    #element = driver.find_element(by, value)
                    #return element.text if element else None
                    except:
                        return None  # Return None or a default value if the element doesn't exist
                
            name = get_text_if_exists(By.TAG_NAME, 'h1',attribute='title')
            #print("company_name")
            #print(name)
            #company_name.append(name)
            append_if_exists(name,company_name)

            # Extract the description
            description = get_text_if_exists(By.CSS_SELECTOR, 'p.break-words')
            #company_description.append(description)
            append_if_exists(description,company_description)
            #print(description)

            # Find and extract each piece of information
            website = get_text_if_exists(By.CSS_SELECTOR, 'a[rel="noopener noreferrer"]')
            #company_website.append(website)
            append_if_exists(website,company_website)
            #print(website)

            verified_date = get_text_if_exists(By.XPATH, '//dt[h3[text()="Verified page"]]/following-sibling::dd')
            #print(verified_date)

            industry = get_text_if_exists(By.XPATH, '//dt[h3[text()="Industry"]]/following-sibling::dd')
            #company_industries.append(industry)
            append_if_exists(industry,company_industries)
            #print(industry)

            size = get_text_if_exists(By.XPATH, '//dt[h3[text()="Company size"]]/following-sibling::dd')
            #company_size.append(size)
            append_if_exists(size,company_size)
            #print(size)

            linkedIn_members = get_text_if_exists(By.XPATH, '//dt[h3[text()="Company size"]]/following-sibling::dd[2]/span')
            #company_associated_members.append(linkedIn_members)
            append_if_exists(linkedIn_members,company_associated_members)
            #print(linkedIn_members)

            location = get_text_if_exists(By.XPATH, '//dt[h3[text()="Headquarters"]]/following-sibling::dd')
            #company_location.append(location)
            append_if_exists(location,company_location)
            #print(location)
            
            creation_date = get_text_if_exists(By.XPATH, '//dt[h3[text()="Founded"]]/following-sibling::dd')
            company_creation_date.append(creation_date)
            append_if_exists(creation_date,company_creation_date)
            #print(creation_date)

            specialties = get_text_if_exists(By.XPATH, '//dt[h3[text()="Specialties"]]/following-sibling::dd')
            company_specialities.append(specialties)
            append_if_exists(specialties,company_specialities)
            #print(specialties)

        
        except Exception as e:
            print(f"Error extracting company name: {e}")

 
        finally: #allow time to see the data in the console
            time.sleep(3)



# Close the browser window after use
driver.quit()
    


liste_company = [company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities,company_links]
df_linkedin_company_scrapping = pd.DataFrame(list(zip(company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities,company_links)),
                                          columns=["company_name","company_description","company_website","company_industries","company_size","company_associated_members","company_location","company_creation_date","company_specialities","company_links"]).reset_index(drop=True)
df_linkedin_company_scrapping["source"] = "linkedin"
df_linkedin_company_scrapping['ingest_date'] = pd.Timestamp.now()


print("/////////////////////////////////////")
df_linkedin_company_scrapping.shape
print(df_linkedin_company_scrapping.head(5))

#Save raw data
df_linkedin_company_scrapping.to_pickle('outputs/raw/companies_linkedin__new.pkl')
