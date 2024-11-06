"""
Cette classe nous permet de webscrapper les données du site LinkedIn.

"""

#import sys
#sys.path.append('../')

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


job_titles,job_companies,job_locations,job_links, jobs_posted_times, company_links  = [],[],[],[],[],[]

max_page_number = 1


#get macro information from main page

def linkedin_scraper(webpage, page_number):
    next_page = webpage + str(page_number)
    #print(str(next_page))
    response = requests.get(str(next_page))
    #soup = bs_linkedin(response.content,'html.parser')
    #jobs = soup.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
        
    if page_number < max_page_number:
        page_number = page_number + 1
        linkedin_scraper(webpage, page_number)
        #print(response)
        #print(page_number)
        soup_linkedin = bs_linkedin(response.content,'lxml')
        jobs_linkedin = soup_linkedin.find_all('div', class_='base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card')
        #print(jobs_linkedin)

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

            #print(job_posted_time)
            jobs_posted_times.append(job_posted_time)

            #print(job_title)
            #print(job_company)


for position in range(1,6):
    #print(position)
    linkedin_scraper(url_linkedin+str(position)+'&pageNum=', 0)


print(job_titles[1])
print(job_companies[7])
print(job_locations[3])
print(job_links[10])
print(jobs_posted_times[27])




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
                #print(seniority_level)
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



company_name,company_description,company_website,company_industries,company_size,company_location,company_type,company_creation_date,company_specialities = [],[],[],[],[],[],[],[],[]


#get more information from company link

for company_link in company_links:
        
        #linkedin_company_link = requests.get(company_link)
        url_company='https://www.linkedin.com/company/computercore?trk=public_jobs_topcard-org-name'
        linkedin_company_link = requests.get(url_company)
        soup_company_link = bs_linkedin(linkedin_company_link.content,"lxml")
        #company_name, company_website, description, publication_date, creation_date, nb_employee, size
        #logo, location, industries, parity_women parity_men, avg_age_employees, ca, source, insta/face


        #get company info

        #name_test = soup_company_link.find('h1',class_='top-card-layout__title')
        #if name_test is not None:
        print( soup_company_link)
        name = soup_company_link.find('h1',class_='top-card-layout__title')
        print("company link, name :" + name)
        #.text.strip()
        '''
        company_name.append(name)


       # description_test = soup_company_link.find('p',class_='break-words')
       # if description_test is not None:
        description = soup_company_link.find('p',class_='break-words').text.strip()
        company_description.append(description)


        website_test = soup_company_link.find('dt', class_='front-sans')
       # if description_test is not None:
        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Website') : 
                website = soup_company_link.find('a',class_='link-no-visited-state')['href']
                company_website.append(website)


        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Industry') : 
                industries = soup_company_link.find('dd',class_='front-sans').text.strip()
                company_industries.append(industries)


        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Company size') : 
                size = soup_company_link.find('dd',class_='front-sans').text.strip()
                company_size.append(size)


        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Headquarters') : 
                location = soup_company_link.find('dd',class_='front-sans').text.strip()
                company_location.append(location)


        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Type') : 
                type = soup_company_link.find('dd',class_='front-sans').text.strip()
                company_type.append(type)


        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Founded') : 
                creation_date = soup_company_link.find('dd',class_='front-sans').text.strip()
                company_creation_date.append(creation_date)


        if (soup_company_link.find('dt',class_='front-sans').text.strip() == 'Specialities') : 
                specialities = soup_company_link.find('dd',class_='front-sans').text.strip()
                company_specialities.append(specialities)

        '''



    


#Create dataframa

#spark = SparkSession.builder.appName("linkedin").getOrCreate()


liste = [job_titles,job_companies,job_locations,job_links, salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries, jobs_posted_times, company_links]

print("/////////////////////////////////////")
print(liste[0])

df_linkedin_scrapping = pd.DataFrame(list(zip(job_titles,job_companies,job_locations,job_links,salaries, salaries_min, salaries_max, seniority_levels, employment_types, job_functions, job_industries, jobs_posted_times, company_links)), 
                       columns=["title", "company", "location","link","salary","salary_min","salary_max","seniority_level","employment_type","job_function","job_industry","posted_time","company_link"]).reset_index(drop=True)
                                 
df_linkedin_scrapping["source"] = "linkedin"
df_linkedin_scrapping['ingest_date'] = pd.Timestamp.now()



df_linkedin_scrapping.shape


liste_company = [company_name,company_description,company_website,company_industries,company_size,company_location,company_type,company_creation_date,company_specialities]
df_linkedin_company_scrapping = pd.DataFrame(list(zip(company_name,company_description,company_website,company_industries,company_size,company_location,company_type,company_creation_date,company_specialities)),
                                          columns=["company_name","company_description","company_website","company_industries","company_size","company_location","company_type","company_creation_date","company_specialities"]).reset_index(drop=True)
df_linkedin_company_scrapping["source"] = "linkedin"
df_linkedin_company_scrapping['ingest_date'] = pd.Timestamp.now()

print("/////////////////////////////////////")
#print(type(df_linkedin_scrapping))
print(df_linkedin_scrapping.head(5))

print(df_linkedin_company_scrapping.head(5))



from elasticsearch import Elasticsearch


#Write df in ElasticSearch

#Create index in needed
#if not es.indices.exists(index="bigdata-linkedin"):
#    es.indices.create(index="bigdata-linkedin")


#Save raw data
#df_linkedin_scrapping.to_pickle('outputs/raw/jobs_linkedin_nov.pkl')

df_linkedin_company_scrapping.to_pickle('outputs/raw/companies_linkedin_nov.pkl')

''' 
#insert_data_elk(df_linkedin_scrapping)


# Search document
res = es.search(index="bigdata-linkedin", body={"query": {"match_all": {}}})
print(res)
'''


