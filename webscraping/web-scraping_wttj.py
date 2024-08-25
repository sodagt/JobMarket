#!/usr/bin/env python
# coding: utf-8

''' Script permettant de récupérer les différentes compagnies et les jobs dans le site welcometothejungle
version Aout 2024
'''


# Import librairies

from selenium import webdriver #Webdriver de Selenium qui permet de contrôler un navigateur
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import requests 
import pandas as pd
import json 
import pickle



# Initialisations variables


#chrome_options = Options()
#chrome_options.add_argument("--headless")
driver = webdriver.Chrome()
#driver = webdriver.Chrome(options=chrome_options)
companies_list=[]
links_list = []
list_of_jobs=[]
baseurl='https://www.welcometothejungle.com/en/companies/' 
wait_time=10
infos_of_companies=[]
errors_company=[]
wait = WebDriverWait(driver, wait_time)


# Récupération de toutes les compagnies dans la liste companies_list

init_companies = baseurl+"?page=1&aroundQuery="
driver.get(init_companies)
page_elements_comp = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//a[@class="sc-dUOoGL bVfajw"]')))

max_page_company=page_elements_comp[-1].text

for page_comp in range (1,int(max_page_company)+1 ):
    current_url = baseurl+"?page="+ str(page_comp) +"&aroundQuery="
    driver.get(current_url)
    wait = WebDriverWait(driver, wait_time)
    div_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="sc-1fnor8f-3 reqyj"]')))
    for div in div_elements:
        #print ( div)
        comp_element = div.find_element(By.TAG_NAME, 'a')
        href_comp = comp_element.get_attribute('href')
        companies_list.append(href_comp)




#Infos des entreprises 

def get_infos_company(url_company):
    ''' Cette fonction récupère toutes les informations associées à une entreprise sur la page de welcome to the jungle
    version2 du  24 Aout 2024'''

    dictionnary_of_infos = {}
    facebook_link = None
    instagram_link = None
    linkedin_link = None
    twitter_link = None
    creation_date = None
    nb_collab = None
    parity_women = None
    parity_men = None
    avg_age = None
    ca=None
    elts_presentation = []
    contents=[]
    dictionnary_of_infos["url_company"] = url_company.split('?')[0]
    try:
        facebook_link_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="social-network-facebook"]')))
        facebook_link = facebook_link_element.get_attribute('href')
        dictionnary_of_infos["facebook_link"] = facebook_link
    except TimeoutException:
        dictionnary_of_infos["facebook_link"] = facebook_link

    try:
        instagram_link_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="social-network-instagram"]')))
        instagram_link = instagram_link_element.get_attribute('href')
        dictionnary_of_infos["instagram_link"] = instagram_link
    except TimeoutException:
        dictionnary_of_infos["instagram_link"] = instagram_link

    try:
        linkedin_link_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="social-network-linkedin"]')))
        linkedin_link = linkedin_link_element.get_attribute('href')
        dictionnary_of_infos["linkedin_link"] = linkedin_link
    except TimeoutException:
        dictionnary_of_infos["linkedin_link"] = linkedin_link

    try:
        twitter_link_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="social-network-twitter"]')))
        twitter_link = twitter_link_element.get_attribute('href')
        dictionnary_of_infos["twitter_link_element"] = twitter_link
    except TimeoutException:
        dictionnary_of_infos["twitter_link_element"] = twitter_link

    try:
        div_presentation = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='sc-1tacsq-1 vg684j-2 vnfuG QVIoI']")))
        for div in div_presentation:
            elts_presentation.append(div.text)
        dictionnary_of_infos["contents"] = elts_presentation
    except TimeoutException:
        dictionnary_of_infos["contents"] = contents
        
    try:
        creation_date_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="stats-creation-year"]')))
        creation_date = creation_date_element.text
        dictionnary_of_infos["creation_date"] = creation_date
    except TimeoutException:
        dictionnary_of_infos["creation_date"] = creation_date

    try:
        nb_collab_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="stats-nb-employees"]')))
        nb_collab = nb_collab_element.text
        dictionnary_of_infos["nb_employee"] = nb_collab
    except TimeoutException:
        dictionnary_of_infos["nb_employee"] =  nb_collab  

    try:
        parity_women_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="stats-parity-women"]')))
        parity_women = parity_women_element.text
        dictionnary_of_infos["parity_women"] = parity_women
    except TimeoutException:
        dictionnary_of_infos["parity_women"] =  parity_women  

    try:
        parity_men_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="stats-parity-men"]')))
        parity_men = parity_men_element.text
        dictionnary_of_infos["parity_men"] = parity_men
    except TimeoutException:
        dictionnary_of_infos["parity_men"] =  parity_men  

    try:
        avg_age_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="stats-average-age"]')))
        avg_age = avg_age_element.text
        dictionnary_of_infos["avg_age"] = avg_age
    except TimeoutException:
        dictionnary_of_infos["avg_age"] =  avg_age  

    try:
        CA_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'span[data-testid="stats-revenue"]')))
        ca = CA_element.text
        dictionnary_of_infos["ca"] = ca
    except TimeoutException:
        dictionnary_of_infos["ca"] =  ca  

    try:
        comp_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//h1[@class='sc-jhlPcU gXnsKh']")))
        name_company=comp_elements[0].text
        dictionnary_of_infos["name_company"]=name_company
    except TimeoutException:
        try:
            comp_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//h1[@class='sc-ha-dNcR cGNEVu']")))
            name_company=comp_elements[0].text
            dictionnary_of_infos["name_company"]=name_company
        except TimeoutException:
            dictionnary_of_infos["name_company"]=None

    try:
        domain_location_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//p[@class='sc-gvZAcH kkPqQa sc-ffZAAA dfCtXz wui-text']")))
        domain=domain_location_elements[0].text
        site=domain_location_elements[1].text
        dictionnary_of_infos["domain"]=domain
        dictionnary_of_infos["location"]=site
    except TimeoutException:
        try:
            domain_location_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//p[@class='sc-gvZAcH kkPqQa sc-eiQriw VXDZR wui-text']")))
            domain=domain_location_elements[0].text
            site=domain_location_elements[1].text
            dictionnary_of_infos["domain"]=domain
            dictionnary_of_infos["location"]=site
        except TimeoutException:
            dictionnary_of_infos["domain"]=None
            dictionnary_of_infos["location"]=None      
                                    
    try:
        website_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a.sc-hgRRfv.jOCZOZ.sc-emIrwa.bwttAi')))
        company_website = website_element.get_attribute('href')
        dictionnary_of_infos["company_website"]=company_website
    except TimeoutException:
        dictionnary_of_infos["company_website"]=None


    return dictionnary_of_infos





#récupération des infos des compagnies et en même temps des url des jobs associés 
for company in companies_list:
    #print ('boucle')
    driver.get(company)
    try: 
        infos_comp=get_infos_company(company)
        infos_of_companies.append(infos_comp) 
        url_company_job=company.split('?')[0]+"/jobs"
        #liens des jobs à trouver
        init=url_company_job+"?page=1"
        driver.get(init)
        # Définir 1 comme valeur par défaut pour max_page_jobs pour gérer le cas 
        max_page_jobs = 1
        try : #On utilise un try except pour pouvoir sauter les pages qui n'ont pas la même structure html pour éviter  une erreur    
            page_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, '//a[@class="sc-jtskMo jlbDae"]')))
            max_page_jobs=page_elements[-1].text
        except Exception as e:
            print(f"Impossible de trouver le nombre de pages pour {company}, exécution sur la page 1. Erreur: {e}")

        for page in range (1,int(max_page_jobs)+1 ):
            url = url_company_job+"?page="+str(page)
            driver.get(url)
            wait = WebDriverWait(driver, wait_time)
            div_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//div[@class='sc-1gjh7r6-7 dLavcG']")))
            for div in div_elements:
                a_element = div.find_element(By.TAG_NAME, 'a')
                href_value = a_element.get_attribute('href')
                links_list.append(href_value)
    except Exception as e: 
            errors_company.append({company: e})
            print(f"Erreur pour l'entreprise {company}: {e}")

#création du dataframe à partir de la liste
companies_wttj=pd.DataFrame(infos_of_companies)
links_df = pd.DataFrame(links_list, columns=["Job Link"])

#sauvegarde du dataframe
companies_wttj.to_pickle('companis_wttj_aout.pkl')
links_df.to_pickle('job_links_wttj_aout.pkl')

driver.quit()  # Fermeture du navigateur après avoir traité toutes les entreprises





# Fonction qui recupère pour un lien les informations du job

def get_infos(url_scrap):
    '''Fonction qui recupère pour une page donnée les informations qui nous intéressent'''
    page = requests.get(url_scrap)
    soup = bs(page.text, "lxml")
    divs = soup.find_all('div', class_='sc-bOhtcR eDrxLt')
    # Parcourez les divs pour extraire les informations spécifiques
    for div in divs:
        text = div.get_text(strip=True)
        if 'contract' in div.i['name']:
            type_contract = text.strip()
        elif 'salary' in div.i['name']:
            salary = text.strip()#[-1].strip()
        elif 'remote' in div.i['name']:
            remote = text.strip()
    #extraire le reste des infos du json 
    fiche_json = soup.find('script', type='application/ld+json')
    if fiche_json:
        json_data = json.loads(fiche_json.string)
        if 'experienceRequirements' in json_data:
            monthsOfExperience = json_data['experienceRequirements'].get('monthsOfExperience', 'Not specified')
        else:
            monthsOfExperience = 'Not specified'
        title=json_data['title']
        contents=json_data['description']
        date_public=json_data['datePosted']
        employmentType=json_data['employmentType']
        addressCountry=json_data['jobLocation'][0]['address']['addressCountry']
        postalCode=json_data['jobLocation'][0]['address']['postalCode']
        streetAddress=json_data['jobLocation'][0]['address']['streetAddress']
        addressLocality=json_data['jobLocation'][0]['address']['addressLocality']
        addressRegion=json_data['jobLocation'][0]['address']['addressRegion']
        industry=json_data['industry']
        company=json_data['hiringOrganization']['name']
        try:
            logo=json_data['hiringOrganization']['logo']
        except:
            logo=''
        type_company=json_data['hiringOrganization']['@type']
        company=json_data['hiringOrganization']['name']
        site_company=json_data['hiringOrganization']['sameAs']
        qualifications=json_data['qualifications']
        validThrough=json_data['validThrough']  

    dictionnary_of_infos = {"title": title, "company": company,"type_company":type_company, "site_company":site_company, "type_contract": type_contract, "salary": salary, "contents": contents, "date_public": date_public,
    "monthsOfExperience": monthsOfExperience,  "employmentType":employmentType, "remote":remote,  "qualifications":qualifications,  "validThrough":validThrough,"logo": logo, "addressCountry": addressCountry, "postalCode": postalCode,"addressLocality":addressLocality ,
      "streetAddress": streetAddress,"industry": industry, "addressRegion":addressRegion, "link_job": url_scrap} 
   
    return dictionnary_of_infos





# Création dataframe des jobs

for url_job in links_list:
    infos_job=get_infos(url_job)
    list_of_jobs.append(infos_job) 

jobs_wttj=pd.DataFrame(list_of_jobs)
#sauvegarde df
jobs_wttj.to_pickle('jobs_wttj_aout.pkl')



#sauvegarde des listes créées
with open('companies_list_wttj_aout.pkl', 'wb') as fichier:
    pickle.dump(companies_list, fichier)

with open('links_job_wttj_aout.pkl', 'wb') as fichier:
    pickle.dump(links_list, fichier)

with open('infos_of_companies_wttj_aout.pkl', 'wb') as fichier:
    pickle.dump(infos_of_companies, fichier)

with open('list_of_jobs_wttj_aout.pkl', 'wb') as fichier:
    pickle.dump(list_of_jobs, fichier)





