from datetime import datetime
from http.client import IncompleteRead
import os
import re
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import requests
from sqlalchemy import create_engine
from web_connect import web_connection
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time


def web_scrap_linkedin():
    options = webdriver.FirefoxOptions()
    options.accept_insecure_certs = True
    driver = webdriver.Remote(
        command_executor="http://selenium-container:4444/wd/hub", options=options
    )
    
    try:
        driver.set_page_load_timeout(40)
        
        max_pages=50

        estado = "São Paulo,"
        cidade = "São Paulo,"
        pais = "Brazil"
        trabalho = "Desenvolvedor"
        distance = "40"

        connection = web_connection.WebConnection(
            url="https://www.linkedin.com/jobs/",
            driver=driver,
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
        job_list = []

        def cleaning_job_info(input, output):
            job_infos_aux = str(input).split("\n")
            for info in job_infos_aux:
                if info.strip():
                    output.append(info.strip())
        def smooth_scroll(driver):
            current_position = driver.execute_script("return window.pageYOffset;")
            target = current_position - 900
            for i in range(current_position, target, -100):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(0.01)

        def get_info():
            wait = WebDriverWait(driver, 3)
            pagina = 0
            have_more_jobs_button = True
            print("ok") if driver.find_elements(
                By.CSS_SELECTOR, "div[data-row]"
            ) else driver.quit()
    
            while pagina <= max_pages:
                jobs = driver.find_elements(By.CSS_SELECTOR, "div[data-row]")
                print(len(jobs))
                have_more_jobs_button = driver.find_element(
                                    By.CSS_SELECTOR, "button.infinite-scroller__show-more-button"
                                )
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                smooth_scroll(driver)

                print("Atualmente na página: " + str(pagina))

                try:
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    button_click = wait.until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, "button.infinite-scroller__show-more-button")
                        )
                    )
                    button_click.click()
                except:
                    print("Loader automático...")
                finally:
                    pagina = pagina + 1

            for job in jobs:
                title = job.find_element(By.TAG_NAME, "h3").text.strip()
                company = job.find_element(By.TAG_NAME, "h4").text.strip()
                location = job.find_element(
                    By.CSS_SELECTOR, "span.job-search-card__location"
                ).text.strip()
                time_opened = job.find_element(By.CSS_SELECTOR, "time").text.strip()
                link = job.find_element(By.TAG_NAME, "a").get_attribute("href")
                job_id = int(link.split("?")[0].rsplit("-", 1)[-1])

                max_attempts = 20
                attempt = 0
                job_details_description = None

                while attempt < max_attempts and not job_details_description:
                    time.sleep(1)
                    try:
                        job_details = requests.get(link)
                    except IncompleteRead as e:
                        job_details = e.partial

                    job_details_soup = BeautifulSoup(job_details.text, "html.parser")

                    class_regex = re.compile(r"description")
                    
                    job_details_description_element = job_details_soup.find(
                        "div", attrs={"class": class_regex}
                    )

                    job_applications_element = (
                        job_details_soup.find("figcaption", class_="num-applicants__caption")
                    )
                    
                    job_details_element = job_details_soup.find("ul", class_="description__job-criteria-list")
                    
                    if job_details_element:
                        job_infos = (
                            job_details_soup.find(
                                "ul", class_="description__job-criteria-list"
                            )
                            .get_text()
                            .strip()
                        )
                    else:
                        attempt+=1
                    
                    if job_applications_element:
                        job_applications =job_details_soup.select_one("figcaption.num-applicants__caption").get_text().strip()
                    else:
                        attempt+=1
                        
                    if job_details_description_element:
                        job_details_description = (
                            job_details_description_element.select_one("section")
                            .select_one("div")
                            .get_text()
                            .strip()
                        )
                    else:
                        attempt += 1

                if job_details_description:
                    print("desc ok")
                else:
                    job_details_description ='NA'
                    job_applications ='NA'
                    job_infos="NA"
                
                cleaning_job_info(job_infos, cleaned_jobs_infos)
                print(job_infos)

                title_list.append(title)
                company_list.append(company)
                location_list.append(location)
                time_opened_list.append(time_opened)
                link_list.append(link)
                description_list.append(job_details_description)
                job_list.append(job_id)
                application_list.append(job_applications)
                
                try:
                    experience_level_list.append(cleaned_jobs_infos[1])
                except:
                    experience_level_list.append("NA")

                cleaned_jobs_infos.clear()
                
        get_info()

        driver.quit()
        
        df = pd.DataFrame.from_dict(
            {
                "title": title_list,
                "job_id": job_list,
                "register_date": str(datetime.now().strftime("%Y-%m-%d")),
                "company": company_list,
                "location": location_list,
                "time_opened": time_opened_list,
                "link": link_list,
                "applications": application_list,
                "experience_level": experience_level_list,
                "job_type":"NA",
                "role": "NA",
                "sectors": "NA",
                "description": description_list,
            }
        )

        destination_file = "../source/dados_raw.db"

        current_directory = os.path.dirname(__file__)

        destination_path = os.path.join(current_directory, destination_file)

        disk_engine = create_engine(f"sqlite:///{destination_path}")
        df.to_sql("vaga", disk_engine, if_exists="replace")
        
    except:
        driver.quit()
