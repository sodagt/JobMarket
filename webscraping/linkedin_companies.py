"""
    This class is use to webscrapp linkedin companies
"""

#import sys
#sys.path.append('../')
import os
import time

import pandas as pd
import pickle

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities,company_links = [],[],[],[],[],[],[],[],[],[]



print("///////////////////////////////////// START GET COMPANY LINK")

company_links_distinct_df = pd.read_pickle("outputs/raw/company_links.pkl") 
company_links_distinct=company_links_distinct_df[company_links_distinct_df.columns[0]].tolist()


#helper before appendibg (check the type of data)
def append_if_exists(data, target_list):
    if data:  # Checks if data is not None and not empty
        target_list.append(data)



# Use our LinkedIn credentials
#username = os.getenv("$LINKEDIN_USERNAME")
#password = os.getenv("$LINKEDIN_PASSWORD")
username = "oadnanimak@gmail.com"
password = "ProjetFormationDE#2024"


# Setup Chrome driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Open LinkedIn login page
driver.get("https://www.linkedin.com/login")

# Find the username and password fields and fill them in
driver.find_element(By.ID, "username").send_keys(username)
driver.find_element(By.ID, "password").send_keys(password)

# Submit the login form
driver.find_element(By.XPATH, "//button[@type='submit']").click()

# Wait for login to complete
print("SLEEP BEFORE COMPANY LINK")
time.sleep(5)

#get more information from company link
for company_link in company_links_distinct[:4]:
        

        print("/////// Linkedin Companies LInks /////// ")

        append_if_exists(company_link,company_links)

        #go to the about page to get more information about the company
        company_link_about = company_link.split('?')[0] + '/about/'

        # Navigate to the company page       
        driver.get(company_link_about)


        #get company info

        # Wait for the page to load
        time.sleep(7)

        # Extract the company name
        try:
            #print(driver.current_url)
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC

            # Wait for the company page to be visible
            time.sleep(5)
           

            #Helper function to check if an element exists and get its text
            def get_text_if_exists(by, value,attribute = 'text'):
                    try:
                        element = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((by, value)))
                        if attribute == 'text':
                            return element.text if element else None
                        else:
                            return element.get_attribute(attribute)

                    except:
                        return None  # Return None if the element doesn't exist
                

            """
                Find and extract each piece of information
            """
            
            # Extract the company name
            name = get_text_if_exists(By.TAG_NAME, 'h1',attribute='title')
            #print("company_name")
            #print(name)
            #company_name.append(name)
            append_if_exists(name,company_name)

            # Extract the description
            description = get_text_if_exists(By.CSS_SELECTOR, 'p.break-words')
            append_if_exists(description,company_description)

            website = get_text_if_exists(By.CSS_SELECTOR, 'a[rel="noopener noreferrer"]')
            append_if_exists(website,company_website)

            verified_date = get_text_if_exists(By.XPATH, '//dt[h3[text()="Verified page"]]/following-sibling::dd')

            industry = get_text_if_exists(By.XPATH, '//dt[h3[text()="Industry"]]/following-sibling::dd')
            append_if_exists(industry,company_industries)

            size = get_text_if_exists(By.XPATH, '//dt[h3[text()="Company size"]]/following-sibling::dd')
            append_if_exists(size,company_size)

            linkedIn_members = get_text_if_exists(By.XPATH, '//dt[h3[text()="Company size"]]/following-sibling::dd[2]/span')
            append_if_exists(linkedIn_members,company_associated_members)

            location = get_text_if_exists(By.XPATH, '//dt[h3[text()="Headquarters"]]/following-sibling::dd')
            append_if_exists(location,company_location)
            
            creation_date = get_text_if_exists(By.XPATH, '//dt[h3[text()="Founded"]]/following-sibling::dd')
            #company_creation_date.append(creation_date)
            append_if_exists(creation_date,company_creation_date)

            specialties = get_text_if_exists(By.XPATH, '//dt[h3[text()="Specialties"]]/following-sibling::dd')
            #company_specialities.append(specialties)
            append_if_exists(specialties,company_specialities)


        except Exception as e:
            print(f"Error extracting company name: {e}")

 
        finally: #allow time to see the data in the console
            time.sleep(3)



# Close the browser window after use
driver.quit()
    

print("///////////////////////////////////// START CONVERTING COMPANIES to DF")
liste_company = [company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities,company_links]
df_linkedin_company_scrapping = pd.DataFrame(list(zip(company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities,company_links)),
                                          columns=["company_name","company_description","company_website","company_industries","company_size","company_associated_members","company_location","company_creation_date","company_specialities","company_links"]).reset_index(drop=True)
df_linkedin_company_scrapping["source"] = "linkedin"
df_linkedin_company_scrapping['ingest_date'] = pd.Timestamp.now()
print("///////////////////////////////////// END CONVERTING COMPANIES to DF")


print("///////////////////////////////////// START PIKCLING COMPANIES")

#Save raw data
df_linkedin_company_scrapping.to_pickle('outputs/raw/companies_linkedin__new.pkl')

print("///////////////////////////////////// START PIKCLING COMPANIES")

