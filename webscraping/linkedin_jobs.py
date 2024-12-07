"""
    This class is use to webscrapp linkedin offers
"""

#import sys
#sys.path.append('../')
import os
import time

import pandas as pd
import pickle

import requests
from bs4 import BeautifulSoup as bs_linkedin 


url_linkedin ='https://www.linkedin.com/jobs/search?trk=guest_homepage-basic_guest_nav_menu_jobs&position='

url_linkedin_salary_range = 'https://www.linkedin.com/jobs/search?keywords=&location=United%20States&geoId=103644278&f_TPR=&f_SB2=1&position='

url_linkedin_product_manager = 'https://www.linkedin.com/jobs/search?keywords=product%20manager&location=%C3%89tats-Unis&geoId=&trk=public_jobs_jobs-search-bar_search-submit&position='


job_titles,job_companies,job_locations,job_links, jobs_posted_times, company_links  = [],[],[],[],[],[]

max_page_number = 150
max_position_number = 50


#get macro information from main page
def linkedin_scraper(webpage, page_number):

 
    print("page_number")

    for page_number in range (0,max_page_number):
        page_number = page_number + 1

        print(page_number)

        next_page = webpage + str(page_number)
       
        response = requests.get(str(next_page))
        
        soup_linkedin = bs_linkedin(response.content,'lxml')
        jobs_linkedin = soup_linkedin.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')


        for job in jobs_linkedin:
            job_title = job.find('h3', class_='base-search-card__title').text.strip()
            job_titles.append(job_title)

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

            jobs_posted_times.append(job_posted_time)



print("SLEEP BEFORE POSITION linkedin_scraper in page")
time.sleep(3)

print("START POSITION linkedin_scraper")
for position in range(1,max_position_number):
    print("***************** POSITION "+str(position))
    linkedin_scraper(url_linkedin+str(position)+'&pageNum=', 0)



"""
    Exploring job link

    We get more information about the job in job_link. Example: salaries, industries,..
    Let's webscrapp the job_link !
"""


print("SLEEP BEFORE JOB LINK")
time.sleep(10)

salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries =[],[],[],[],[],[],[]

print("START JOB LINK")
for job_link in job_links:
        
        linkedin_jobs_link = requests.get(job_link)
        soup_jobs_link = bs_linkedin(linkedin_jobs_link.content,"lxml")

        salary=""
        salary_min= ""
        salary_max= ""

        #get salary info
        salary_info_block = soup_jobs_link.find('span',class_='main-job-card__salary-info block my-0.5 mx-0')
        salary_compensation = soup_jobs_link.find('div',class_='salary compensation__salary')
        salary_card_info = soup_jobs_link.find('span',class_='aside-job-card__salary-info')

        
        #get company link
        company_link =  soup_jobs_link.find('a', class_='topcard__org-name-link')
        if company_link is not None:
            company_links.append(company_link['href'])

        
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
print("END JOB LINK")



print("SLEEP BEFORE WRITTING DF")
time.sleep(10)


print("///////////////////////////////////// START CONVERTING JOBS to DF")
liste = [job_titles,job_companies,job_locations,job_links, salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries, jobs_posted_times, company_links]

df_linkedin_scrapping = pd.DataFrame(list(zip(job_titles,job_companies,job_locations,job_links,salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries, jobs_posted_times, company_links)), 
                       columns=["title", "company", "location","link","salary","salary_min","salary_max","seniority_level","employment_type","job_function","job_industry","posted_time","company_link"]).reset_index(drop=True)
                                 
df_linkedin_scrapping["source"] = "linkedin"
df_linkedin_scrapping['ingest_date'] = pd.Timestamp.now()
print("///////////////////////////////////// END CONVERTING JOBS to DF")




print("///////////////////////////////////// START PIKCLING JOBS")

#Save raw data
df_linkedin_scrapping.to_pickle('outputs/raw/jobs_linkedin_nov_15.pkl')

print("///////////////////////////////////// END PIKCLING JOBS")




print("///////////////////////////////////// START PIKCLING COMPANIES LINKS")

#pickle company_links list to optimize processing time for company's DF 
distinct_company_links = list(set(company_links))
company_links_df = pd.DataFrame(distinct_company_links)
company_links_df.to_pickle("outputs/raw/company_links_nov_15.pkl")

print("///////////////////////////////////// END PIKCLING COMPANIES LINKS")
