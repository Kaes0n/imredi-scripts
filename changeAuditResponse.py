import time
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from requests.exceptions import RequestException
from multiprocessing import Process, Queue
import os, sys


# Настройка логирования
logging.basicConfig(filename='audit_log.log', level=logging.INFO, 
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Функция для загрузки данных из Excel файла
def load_excel_data(file_path):
    df = pd.read_excel(file_path, engine='openpyxl')
    return df

# Функция для обработки аудитов
def process_audit(driver, audit_id, blocks_and_fields):
    try:
        print(f"Processing audit ID {audit_id}")
        logging.info(f"Processing audit ID {audit_id}")
        
        # Открываем страницу смены статуса
        change_status_url = f"https://imredi.yourCompanyName.ru/admin/task/taskhaspoint/{audit_id}/change_status/2/"
        
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:               
                driver.get(change_status_url)
                if "502" not in driver.title:
                    break
                else:
                    print(f"Received status code 502. Retrying... (Attempt {retry_count + 1})")
                    logging.warning(f"Received status code 502. Retrying... (Attempt {retry_count + 1})")
                    retry_count += 1
                    time.sleep(5)  # Подождем 5 секунд перед повторной попыткой
            except RequestException as e:
                print(f"Error checking status code: {e}. Retrying... (Attempt {retry_count + 1})")
                logging.error(f"Error checking status code: {e}. Retrying... (Attempt {retry_count + 1})")
                retry_count += 1
                time.sleep(5)

        if retry_count == max_retries:
            print(f"Failed to load page after {max_retries} attempts. Skipping this audit.")
            logging.error(f"Failed to load page after {max_retries} attempts. Skipping this audit.")
            return

        # Ждем и кликаем на кнопку подтверждения
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and @value='Да, я уверен']"))
        ).click()

        # Открываем страницу аудита
        audit_url = f"https://imredi.yourCompanyName.ru/web-client/audit/{audit_id}/"
        driver.get(audit_url)
        time.sleep(10)
        # Обрабатываем каждый блок и поле
        for block_name, field_names in blocks_and_fields.items():
            # Ждем полной загрузки страницы
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, f"//span[contains(@class, 'MuiTab-wrapper') and text()='{block_name}']"))
            )

            # Кликаем на элемент с block_name
            block_element = driver.find_element(By.XPATH, f"//span[contains(@class, 'MuiTab-wrapper') and text()='{block_name}']")
            driver.execute_script("arguments[0].scrollIntoView(true);", block_element)
            time.sleep(3)
            # Пробуем кликнуть с помощью JavaScript
            driver.execute_script("arguments[0].click();", block_element)
            
            
            # Обрабатываем каждое поле в блоке
            for field_name in field_names:
                try:
                    time.sleep(3)
                    # Прокручиваем страницу
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    
                    driver.execute_script("window.scrollTo(0, 0);")
                    

                    div_elements = WebDriverWait(driver, 20).until(
                        EC.presence_of_all_elements_located((By.CLASS_NAME, 'jss276'))
                    )
                    for div in div_elements:
                        try:
                            field_element = div.find_element(By.XPATH, f".//p[contains(text(), '{field_name}')]")
                            checkbox = div.find_element(By.XPATH, ".//input[@type='checkbox']")
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", checkbox)
                            

                            driver.execute_script("arguments[0].click();", checkbox)
                            print(f"Clicked on checkbox for field '{field_name}' at block '{block_name}'")
                            logging.info(f"Clicked on checkbox for field '{field_name}' at block '{block_name}'")
                            break
                        except NoSuchElementException:
                            continue
                except TimeoutException:
                    print(f"Element with field name '{field_name}' not found due to timeout.")
                    logging.warning(f"Element with field name '{field_name}' not found due to timeout.")
        time.sleep(3)
        # Завершение аудита (оставляем без изменений)
        bathroom_element = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//span[contains(@class, 'MuiTab-wrapper') and text()='Санузел магазина']"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", bathroom_element)
        
        bathroom_element.click()
        

        finish_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//span[contains(text(), 'Завершить аудит')]"))
        )
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", finish_button)
        
        driver.execute_script("arguments[0].click();", finish_button)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[@role='dialog']//p[contains(text(), 'Завершить аудит?')]"))
        )
        time.sleep(5)
        confirm_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[.//span[text()='Завершить']]"))
        )
        driver.execute_script("arguments[0].click();", confirm_button)

        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.XPATH, "//div[@role='dialog']"))
        )

        print(f"Audit ID {audit_id} processed and finalized successfully.")
        logging.info(f"Audit ID {audit_id} processed and finalized successfully.")
        
    except (NoSuchElementException, TimeoutException, WebDriverException) as e:
        print(f"Error processing audit ID {audit_id}: {e}, {block_name}, {field_names}")
        logging.error(f"Error processing audit ID {audit_id}: {e}, {block_name}, {field_names}")
        raise

# Основная функция
def setup_logging(process_id):
    log_filename = f'11.07 часть 2 audit_log_process_{process_id}.log'
    logging.basicConfig(filename=log_filename, level=logging.INFO, 
                        format='%(asctime)s:%(levelname)s:%(message)s')
    return logging.getLogger()

def process_chunk(chunk, username, password, q, process_id):
    logger = setup_logging(process_id)
    
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)

    try:
        # Вход на сайт
        login_url = "https://imredi.yourCompanyName.ru"
        driver.get(login_url)
        
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys(password, Keys.RETURN)
        
        WebDriverWait(driver, 10).until(EC.url_changes(login_url))

        total_audits = len(chunk['audit_id'].unique())
        current_audit_count = 0

        for audit_id in chunk['audit_id'].unique():
            current_audit_count += 1
            print(f"Process {process_id}: Processing audit {current_audit_count}/{total_audits}: ID {audit_id}")
            logger.info(f"Processing audit {current_audit_count}/{total_audits}: ID {audit_id}")

            audit_data = chunk[chunk['audit_id'] == audit_id].groupby('block_name')['field_name'].apply(list).to_dict()

            try:
                process_audit(driver, audit_id, audit_data)
            except Exception as e:
                print(f"Process {process_id}: Failed to process audit ID {audit_id}: {e}")
                logger.error(f"Failed to process audit ID {audit_id}: {e}")
                continue

        q.put((current_audit_count, total_audits, process_id))
    finally:
        driver.quit()

def main():
    if len(sys.argv) != 2:
        print("Использование: python отменаНарушений.py <число_экземпляров>")
        return

    num_instances = int(sys.argv[1])
    excel_file = 'Отменить_нарушения_в_аудите.xlsx'
    username = ""
    password = ""

    df = load_excel_data(excel_file)

    # Разделение DataFrame на части
    chunk_size = len(df) // num_instances
    chunks = [df[i:i+chunk_size] for i in range(0, len(df), chunk_size)]

    # Использование многопроцессорности
    processes = []
    q = Queue()

    for i, chunk in enumerate(chunks):
        p = Process(target=process_chunk, args=(chunk, username, password, q, i))
        processes.append(p)
        p.start()

    # Ожидание завершения всех процессов
    for p in processes:
        p.join()

    # Сбор результатов
    total_processed = 0
    total_audits = 0

    while not q.empty():
        processed, total, process_id = q.get()
        total_processed += processed
        total_audits += total
        print(f"Process {process_id} completed: processed {processed}/{total} audits")

    print(f"All audits processed. Total processed: {total_processed}/{total_audits}")

    # Объединение логов
    combined_log = 'Отменить_нарушения_в_аудите.log'
    with open(combined_log, 'w') as outfile:
        for i in range(num_instances):
            log_file = f'Отменить_нарушения_в_аудите_{i}.log'
            if os.path.exists(log_file):
                with open(log_file, 'r') as infile:
                    outfile.write(infile.read())
                os.remove(log_file)  # Удаляем отдельные файлы логов после объединения

    print(f"Combined log file created: {combined_log}")

if __name__ == "__main__":
    main()
