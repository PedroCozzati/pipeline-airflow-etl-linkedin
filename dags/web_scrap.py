from http.client import IncompleteRead
import re
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import requests
from controllers.web_connection import WebConnection
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc
import time

option = webdriver.ChromeOptions()
option.add_argument("--start-maximized")
driver = webdriver.Chrome(options=option)

# option = webdriver.EdgeOptions()
# driver = webdriver.Edge(options=option)

estado = "São Paulo,"
cidade = "São Paulo,"
pais = "Brazil"
trabalho = "Desenvolvedor"
distance = "40"

max_pages = 90

connection = WebConnection(
    driver=driver,
    url="https://www.linkedin.com/jobs/",
    keyword=trabalho,
    estado=estado,
    cidade=cidade,
    pais=pais,
    distance=distance,
)

data = connection.start_connection()
soup = BeautifulSoup(data)

title_list = []
company_list = []
location_list = []
time_opened_list = []
link_list = []
description_list = []
application_list = []
cleaned_jobs_infos = []
experience_level_list = []
job_type_list = []
role_list = []
sector_list = []

def cleaning_job_info(input, output):
    job_infos_aux = str(input).split("\n")
    for info in job_infos_aux:
        if info.strip():
            output.append(info.strip())
            
def smooth_scroll(driver):
    current_position = driver.execute_script("return window.pageYOffset;")
    target = current_position - 900 
    for i in range(current_position, target, -10):
        driver.execute_script(f"window.scrollTo(0, {i});")
        time.sleep(0.01) 
        
def get_info():
    pagina = 0
    end_page = False
    
    have_more_jobs_button = True
    wait = WebDriverWait(driver, 10)
    
    while have_more_jobs_button and pagina <= max_pages:
        
       
        time.sleep(4)
       
        jobs = driver.find_elements(By.CSS_SELECTOR, "div[data-row]")
        print(len(jobs))
        
        
        #EVITAR RATE LIMIT DA API 
        # if len(jobs) > 400 and len(jobs) % 400 == 0:
        #     time.sleep(100)
        
        have_more_jobs_button = driver.find_element(
                By.CSS_SELECTOR, "button.infinite-scroller__show-more-button"
            )
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        smooth_scroll(driver)
        
        time.sleep(2)

        print("Atualmente na página: " + str(pagina))

       
        try:
            button_click = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.infinite-scroller__show-more-button")))
            
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            time.sleep(4)
            button_click.click()
            time.sleep(10)
            time.sleep(4)
        except:
            print("Loader automático...")
        finally:
            pagina = pagina + 1

        time.sleep(2)

    for job in jobs:
        time.sleep(2)
        title = job.find_element(By.TAG_NAME, "h3").text.strip()
        time.sleep(2)
        company = job.find_element(By.TAG_NAME, "h4").text.strip()
        time.sleep(2)
        location = job.find_element(
            By.CSS_SELECTOR, "span.job-search-card__location"
        ).text.strip()
        time_opened = job.find_element(By.CSS_SELECTOR, "time").text.strip()
        time.sleep(2)
        link = job.find_element(By.TAG_NAME, "a").get_attribute("href")

        try:
            time.sleep(2)
            job_details = requests.get(link)
        except IncompleteRead as e:
            job_details = e.partial
            
            
        job_details_soup = BeautifulSoup(job_details.text, "html.parser")
        # Timer necessário para carregar a nova pagina a tempo da informação aparecer
        time.sleep(5)

        # Descrição da vaga
        if job_details_soup.select_one("div.description__text.description__text--rich"):
            job_details_description = (
                job_details_soup.select_one(
                    "div.description__text.description__text--rich"
                )
                .select_one("section")
                .select_one("div")
                .get_text()
                .strip()
            )
        else:
            job_details_description = "NA"

        # Pegando informação de numero de aplicações da vaga
        if job_details_soup.find("figcaption", class_="num-applicants__caption"):
            job_applications = (
                job_details_soup.find("figcaption", class_="num-applicants__caption")
                .get_text()
                .strip()
            )
        else:
            job_applications="NA"

        # Informações gerais da vaga (nivel de experiencia, tipo de emprego, função, etc)
        if job_details_soup.find("ul", class_="description__job-criteria-list"):
            job_infos = (
                job_details_soup.find("ul", class_="description__job-criteria-list")
                .get_text()
                .strip()
            )
        else:
            job_infos ="NA"

        cleaning_job_info(job_infos, cleaned_jobs_infos)

        print(cleaned_jobs_infos)

        title_list.append(title)
        company_list.append(company)
        location_list.append(location)
        time_opened_list.append(time_opened)
        link_list.append(link)
        description_list.append(job_details_description)
        application_list.append(job_applications)

        try:
            experience_level_list.append(cleaned_jobs_infos[1])
        except:
            experience_level_list.append("NA")
            
        try:
            job_type_list.append(cleaned_jobs_infos[3])
        except:
            job_type_list.append("NA")
            
        try:
            role_list.append(cleaned_jobs_infos[5])
        except:
            role_list.append("NA")

        try:
            sector_list.append(cleaned_jobs_infos[7])
        except:
            sector_list.append("NA")
            
        cleaned_jobs_infos.clear()

    driver.close()


get_info()

df = pd.DataFrame.from_dict(
    {
        "title": title_list,
        "company": company_list,
        "location": location_list,
        "time_opened": time_opened_list,
        "link": link_list,
        "applications": application_list,
        "experience_level": experience_level_list,
        "job_type": job_type_list,
        "role": role_list,
        "sectors": sector_list,
        "description": description_list,
    }
)

df.to_csv("results/linkedin_jobs.csv", sep=",", index=False)
