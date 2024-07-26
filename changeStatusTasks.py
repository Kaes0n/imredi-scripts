import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import traceback, time, sys
from multiprocessing import Process, Queue

# Чтение файла и преобразование URL
def read_and_transform_urls(file_path):
    try:
        df = pd.read_excel(file_path)
        df['media_data'] = df['media_data'].apply(lambda x: x.replace('change/#media', 'change_status/4'))
        return df['media_data'].tolist()
    except Exception as e:
        print(f"Ошибка при чтении и преобразовании файла: {e}")
        traceback.print_exc()
        return []

# Вход на сайт
def login(driver, username, password):
    try:
        driver.get("https://imredi.yourCompanyName.ru/admin/login/")
        driver.find_element(By.NAME, "username").send_keys(username)
        driver.find_element(By.NAME, "password").send_keys(password)
        driver.find_element(By.NAME, "password").send_keys(Keys.RETURN)

        # Проверка успешности входа
        WebDriverWait(driver, 0.1).until(
            EC.url_changes("https://imredi.yourCompanyName.ru/admin/login/")
        )
    except TimeoutException:
        print("Ошибка: Неверный логин или пароль.")
        return False
    except NoSuchElementException as e:
        print(f"Ошибка при попытке входа: {e}")
        traceback.print_exc()
        return False
    except WebDriverException as e:
        print(f"Ошибка WebDriver: {e}")
        traceback.print_exc()
        return False
    return True

# Открытие URL и изменение статуса
def process_urls(driver, urls):
    successful_changes = 0
    failed_changes = 0
    failed_urls = []

    # Открытие первых 10 URL в новых вкладках
    for i in range(0, len(urls), 10):
        open_urls = urls[i:i + 10]

        # Открываем 10 вкладок сразу без ожидания загрузки
        for url in open_urls:
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])
            driver.get(url)
            print(f"Открываю URL: {url}")

        time.sleep(1)  # Даем время для загрузки страниц

        # Переходим к каждой вкладке по очереди и обрабатываем её содержимое
        for url_index, url in enumerate(open_urls):
            driver.switch_to.window(driver.window_handles[url_index + 1])
            try:
                # Проверка наличия элемента перед использованием WebDriverWait
                if not driver.find_elements(By.CSS_SELECTOR, "input.btn.btn-danger"):
                    print(f"Ошибка: Не найдены элементы на странице для URL: {url}")
                    failed_changes += 1
                    failed_urls.append(url)
                    continue
                
                # Ожидание появления элемента и взаимодействие с ним
                button = WebDriverWait(driver, 0.1).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input.btn.btn-danger"))
                )
                button.click()

                WebDriverWait(driver, 0.1).until(
                    EC.url_contains("change")
                )
                print(f"Статус успешно изменен для URL: {url}")
                successful_changes += 1
            except TimeoutException:
                print(f"Ошибка: Превышено время ожидания для URL: {url}")
                failed_changes += 1
                failed_urls.append(url)
            except WebDriverException as e:
                print(f"Ошибка WebDriver при обработке URL: {url} - {e}")
                traceback.print_exc()
                failed_changes += 1
                failed_urls.append(url)
            except Exception as e:
                print(f"Неожиданная ошибка при обработке URL: {url} - {e}")
                traceback.print_exc()
                failed_changes += 1
                failed_urls.append(url)


        # Закрытие всех вкладок кроме первой
        while len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
            driver.close()

        driver.switch_to.window(driver.window_handles[0])

    return successful_changes, failed_changes, failed_urls

# Запись URL с ошибками в файл
def save_failed_urls(failed_urls, file_path):
    df = pd.DataFrame(failed_urls, columns=["Failed URLs"])
    df.to_excel(file_path, index=False)

def process_chunk(chunk, username, password, q, process_id):
    try:
        driver = webdriver.Chrome()
        driver.implicitly_wait(0.1)

        if not login(driver, username, password):
            print(f"Процесс {process_id}: Ошибка входа: Неверный логин или пароль.")
            return

        urls = chunk['media_data'].tolist()
        successful_changes, failed_changes, failed_urls = process_urls(driver, urls)
        q.put((successful_changes, failed_changes, failed_urls, process_id))
    finally:
        driver.quit()

def main():
    if len(sys.argv) != 2:
        print("Использование: python changestatus.py <число_экземпляров>")
        return

    num_instances = int(sys.argv[1])
    file_path = 'файл_с_задачими.xlsx'
    failed_urls_file = 'failed_urls.xlsx'
    username = ''
    password = ''

    # Чтение и преобразование DataFrame
    df = pd.read_excel(file_path)
    df['media_data'] = df['media_data'].apply(lambda x: x.replace('change/#media', 'change_status/4'))

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
    total_successful = 0
    total_failed = 0
    all_failed_urls = []

    while not q.empty():
        successful, failed, failed_urls, process_id = q.get()
        total_successful += successful
        total_failed += failed
        all_failed_urls.extend(failed_urls)
        print(f"Процесс {process_id}: Успешно изменено {successful}, не удалось изменить {failed}")

    print(f"Всего успешно изменено: {total_successful}")
    print(f"Всего не удалось изменить: {total_failed}")

    if all_failed_urls:
        save_failed_urls(all_failed_urls, failed_urls_file)
        print(f"URL с ошибками сохранены в файл {failed_urls_file}")

if __name__ == "__main__":
    main()