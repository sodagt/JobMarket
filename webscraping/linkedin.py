"""
Cette classe nous permet de webscrapper les données du site LinkedIn.

"""

#import sys
#sys.path.append('../')
import os
import time

import pandas as pd
import pickle

import requests
from bs4 import BeautifulSoup as bs_linkedin 

#from pyspark.sql import SparkSession
#from pyspark.sql.functions import lit

#from configs.conf import es
#from common.utils import insert_data_elk


url_linkedin ='https://www.linkedin.com/jobs/search?trk=guest_homepage-basic_guest_nav_menu_jobs&position='

url_linkedin_salary_range = 'https://www.linkedin.com/jobs/search?keywords=&location=United%20States&geoId=103644278&f_TPR=&f_SB2=1&position='

url_linkedin_product_manager = 'https://www.linkedin.com/jobs/search?keywords=product%20manager&location=%C3%89tats-Unis&geoId=&trk=public_jobs_jobs-search-bar_search-submit&position='

"""
job_titles,job_companies,job_locations,job_links, jobs_posted_times, company_links  = [],[],[],[],[],[]

max_page_number = 150


#get macro information from main page

def linkedin_scraper(webpage, page_number):
    #next_page = webpage + str(page_number)
    #print(str(next_page))
    #response = requests.get(str(next_page))
    #soup = bs_linkedin(response.content,'html.parser')
    #jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
        
    print("page_number")

    for page_number in range (0,max_page_number):
        page_number = page_number + 1

        print(page_number)

        next_page = webpage + str(page_number)
        #print("current_page")
        #print(str(next_page))
        response = requests.get(str(next_page))
        #linkedin_scraper(webpage, page_number)
        #print(response)
        
        soup_linkedin = bs_linkedin(response.content,'lxml')
        jobs_linkedin = soup_linkedin.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
        #print(jobs_linkedin)


        for job in jobs_linkedin:
            job_title = job.find('h3', class_='base-search-card__title').text.strip()
            job_titles.append(job_title)
            #print("job_title")
            #print(job_title)

            job_company = job.find('h4', class_='base-search-card__subtitle').text.strip()
            job_companies.append(job_company)

            job_location =  job.find('span', class_='job-search-card__location').text.strip()
            job_locations.append(job_location)
            
            job_link =  job.find('a', class_='base-card__full-link')['href']
            job_links.append(job_link)

            #get posted time
            job_posted_time = job.find('time',class_='job-search-card__listdate')
            job_posted_time_new = job.find('time',class_='job-search-card__listdate--new')

            if job_posted_time is not None :
                job_posted_time = job.find('time',class_='job-search-card__listdate')['datetime']
            elif job_posted_time_new is not None : 
                job_posted_time = job.find('time',class_='job-search-card__listdate--new')['datetime']

            #print(job_posted_time)
            jobs_posted_times.append(job_posted_time)

            #print(job_title)
            #print(job_company)


print("SLEEP BEFORE POSITION linkedin_scraper")
time.sleep(3)

print("START POSITION linkedin_scraper")
for position in range(10,15):
    print("***************** POSITION "+str(position))
    linkedin_scraper(url_linkedin+str(position)+'&pageNum=', 0)


print(job_titles[1])
print(job_companies[7])
print(job_locations[3])
print(job_links[10])
print(jobs_posted_times[27])


time.sleep(10)
print("STARTING JOB LINK")


#exploring job link

salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries =[],[],[],[],[],[],[]

#def linkedin_scraper_job_link(job_links):
#get more information from job link
for job_link in job_links:
        linkedin_jobs_link = requests.get(job_link)
        soup_jobs_link = bs_linkedin(linkedin_jobs_link.content,"lxml")
        #print(soup_jobs_link)

        salary=""
        salary_min= ""
        salary_max= ""

        #get salary info
        salary_info_block = soup_jobs_link.find('span',class_='main-job-card__salary-info block my-0.5 mx-0')
        salary_compensation = soup_jobs_link.find('div',class_='salary compensation__salary')
        salary_card_info = soup_jobs_link.find('span',class_='aside-job-card__salary-info')

        
        #get company link
        company_link =  soup_jobs_link.find('a', class_='topcard__org-name-link')
        #company_links.append(company_link)
        if company_link is not None:
            #print("Company Link:", company_link)
            company_links.append(company_link['href'])

        #else:
         #   print("No link found with the specified class.")


        
        if salary_info_block is not None: 
            salary = salary_info_block.text.strip().replace('\n','').replace('\t','')
            salary_min = salary.split('-')[0]
            salary_max = salary.split('-')[1]

        elif salary_compensation is not None: 
            salary = salary_compensation.text.strip().replace('\n','').replace('\t','')
            salary_min = salary.split('-')[0]
            salary_max = salary.split('-')[1]

        elif salary_card_info is not None: 
            salary = salary_card_info.text.strip().replace('\n','').replace('\t','')
            salary_min = salary.split('-')[0]
            salary_max = salary.split('-')[1]


        salaries.append(salary)
        salaries_min.append(salary_min)
        salaries_max.append(salary_max)

        #get job criteria

                
        job_criteria = soup_jobs_link.find_all('li',class_='description__job-criteria-item')

        for criteria in job_criteria:

            if (criteria.find('h3').text.strip() == 'Seniority level') : 
                seniority_level = criteria.find_next('span',class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip().replace('\n','')
                seniority_levels.append(seniority_level)

            if (criteria.find('h3').text.strip() == 'Employment type') : 
                employment_type = criteria.find_next('span',class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip().replace('\n','')
                #print(employment_type)
                employment_types.append(employment_type)

            if (criteria.find('h3').text.strip() == 'Job function') : 
                job_function = criteria.find_next('span',class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip().replace('\n','')
                #print(job_function)
                job_functions.append(job_function)

            if (criteria.find('h3').text.strip() == 'Industries') : 
                job_industry = criteria.find_next('span',class_='description__job-criteria-text description__job-criteria-text--criteria').text.strip().replace('\n','')
                #print(job_industry)
                job_industries.append(job_industry)



time.sleep(10)
print("STARTING JOB DF")


liste = [job_titles,job_companies,job_locations,job_links, salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries, jobs_posted_times, company_links]

print("/////////////////////////////////////")
print(liste[0])

df_linkedin_scrapping = pd.DataFrame(list(zip(job_titles,job_companies,job_locations,job_links,salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries, jobs_posted_times, company_links)), 
                       columns=["title", "company", "location","link","salary","salary_min","salary_max","seniority_level","employment_type","job_function","job_industry","posted_time","company_link"]).reset_index(drop=True)
                                 
df_linkedin_scrapping["source"] = "linkedin"
df_linkedin_scrapping['ingest_date'] = pd.Timestamp.now()



df_linkedin_scrapping.shape

print("/////////////////////////////////////")
#print(type(df_linkedin_scrapping))
print(df_linkedin_scrapping.head(5))

#Save raw data
df_linkedin_scrapping.to_pickle('outputs/raw/jobs_linkedin_nov_15.pkl')

print("///////////////////////////////////// END PIKCLING JOBS")

#pickle company_links list to get it after for company's DF 
distinct_company_links = list(set(company_links))
company_links_df = pd.DataFrame(distinct_company_links)
company_links_df.to_pickle("outputs/raw/company_links_nov_15.pkl")

#with open("outputs/raw/company_links.pkl","wb") as file:
#    pickle.dump(distinct_company_links,file)

print("///////////////////////////////////// END PIKCLING COMPANIES LINKS")

"""






#"""

print("///////////////////////////////////// START COMPANY LINK")

company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities = [],[],[],[],[],[],[],[],[]



#pickle list to get companies links 
#with open("outputs/raw/company_links.pkl","rb") as file:
#    company_links_pkl = pickle.load(file)

#company_links_distinct = list(set(company_links))
#company_links_distinct = company_links_pkl 

company_links_distinct_df = pd.read_pickle("outputs/raw/company_links_nov_15.pkl") 
#company_links_distinct=company_links_distinct_df.values.tolist()
company_links_distinct=company_links_distinct_df[company_links_distinct_df.columns[0]].tolist()



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
username = os.getenv("$LINKEDIN_USERNAME")
password = os.getenv("$LINKEDIN_PASSWORD")
print(username)
print(password)

# Setup Chrome driver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Open LinkedIn login page
driver.get("https://www.linkedin.com/login")

# Find the username and password fields and fill them in
driver.find_element(By.ID, "username").send_keys("oadnanimak@gmail.com")
driver.find_element(By.ID, "password").send_keys("ProjetFormationDE#2024")

# Submit the login form
driver.find_element(By.XPATH, "//button[@type='submit']").click()

# Wait for login to complete (adjust as necessary)
print("SLEEP BEFORE COMPANY LINK")
time.sleep(5)

for company_link in company_links_distinct:
        

        print("/////// Linkedin Companies LInks /////// ")


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
    


liste_company = [company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities]
df_linkedin_company_scrapping = pd.DataFrame(list(zip(company_name,company_description,company_website,company_industries,company_size,company_associated_members,company_location,company_creation_date,company_specialities)),
                                          columns=["company_name","company_description","company_website","company_industries","company_size","company_associated_members","company_location","company_creation_date","company_specialities"]).reset_index(drop=True)
df_linkedin_company_scrapping["source"] = "linkedin"
df_linkedin_company_scrapping['ingest_date'] = pd.Timestamp.now()


print("/////////////////////////////////////")
df_linkedin_company_scrapping.shape
print(df_linkedin_company_scrapping.head(5))

#Save raw data
df_linkedin_company_scrapping.to_pickle('outputs/raw/companies_linkedin_nov_15.pkl')

#"""






#from elasticsearch import Elasticsearch


#Write df in ElasticSearch

#Create index in needed
#if not es.indices.exists(index="bigdata-linkedin"):
#    es.indices.create(index="bigdata-linkedin")



''' 
#insert_data_elk(df_linkedin_scrapping)


# Search document
res = es.search(index="bigdata-linkedin", body={"query": {"match_all": {}}})
print(res)
'''


