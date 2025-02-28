import time
import random
import json
import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException

with open("config.json", "r", encoding="utf-8") as config_file:
    config = json.load(config_file)

logging_enabled = config.get("logging_enabled", True)
logging_level = config.get("logging_level", "INFO").upper()
save_to_csv = config.get("save_to_csv", "zoon_data_all_full.csv")

if logging_enabled:
    logging.basicConfig(
        level=getattr(logging, logging_level, logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("logs.txt", encoding="utf-8"),
        ]
    )
else:
    logging.disable(logging.CRITICAL)

logging.info("Парсинг запущен")

def random_sleep(a=1, b=3):
    delay = random.uniform(a, b)
    logging.debug(f"Ожидание {delay:.2f} секунд")
    time.sleep(delay)

options = webdriver.ChromeOptions()
options.add_argument("--ignore-certificate-errors")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--disable-extensions")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--incognito")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36")
options.add_argument("--headless=new")  

service = Service('C:\\Program Files\\Google\\Chrome\\Application\\chromedriver.exe')
driver = webdriver.Chrome(service=service, options=options)
driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

base_url = "https://zoon.ru/msk/"
logging.info(f"Открываем страницу {base_url}")
driver.get(base_url)
random_sleep(2, 4)

def get_categories():
    try:
        menu_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "header-switcher-icon")))
        menu_button.click()
        logging.info("Нажали на кнопку меню")
        random_sleep(2, 4)

        categories = {}
        category_elements = driver.find_elements(By.CLASS_NAME, "navigation-nav-link")
        for category in category_elements:
            category_name = category.text.strip()
            category_link = category.get_attribute("href")
            categories[category_name] = category_link

        logging.info(f"Найдено {len(categories)} категорий")
        return categories

    except Exception as e:
        logging.error(f"Ошибка при получении категорий: {e}")
        return {}

def click_show_more():
    try:
        button = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.js-next-page")))
        if not button.is_displayed():
            logging.warning("Кнопка 'Показать ещё' скрыта, завершаем парсинг")
            return False
        driver.execute_script("arguments[0].scrollIntoView();", button)
        random_sleep(1, 2)
        try:
            button.click()
        except ElementClickInterceptedException:
            driver.execute_script("arguments[0].click();", button)
        logging.info("Нажали 'Показать ещё'")
        return True
    except (TimeoutException, NoSuchElementException):
        logging.warning("Кнопка 'Показать ещё' не найдена, возможно, все данные загружены")
        return False
    except StaleElementReferenceException:
        logging.warning("Кнопка устарела, пробуем снова")
        return click_show_more()

def parse_category(category_name, category_url):
    logging.info(f"Парсим категорию: {category_name} ({category_url})")
    driver.get(category_url)
    random_sleep(3, 5)

    data = []
    max_items = 10000

    while True:
        items = driver.find_elements(By.CLASS_NAME, "minicard-item__info")
        print(f"В категории '{category_name}' загружено {len(items)} записей")
        logging.info(f"В категории '{category_name}' загружено {len(items)} записей")

        if len(items) >= max_items:
            logging.info("Достигли 10 000 записей, завершаем")
            break

        if not click_show_more():
            break

        random_sleep(2, 5)

    for item in items:
        try:
            title = item.find_element(By.TAG_NAME, "a").text
            link = item.find_element(By.TAG_NAME, "a").get_attribute("href")

            try:
                address_block = item.find_element(By.XPATH, ".//following-sibling::address")
                address_place = address_block.find_element(By.CLASS_NAME, "address").text if address_block.find_elements(By.CLASS_NAME, "address") else "Нет данных"
                metro_name = address_block.find_element(By.CLASS_NAME, "metro").text if address_block.find_elements(By.CLASS_NAME, "metro") else "Нет данных"
                metro_time = address_block.find_element(By.CLASS_NAME, "distance").text if address_block.find_elements(By.CLASS_NAME, "distance") else "Нет данных"
            except NoSuchElementException:
                logging.warning(f"Адрес отсутствует для {title}")
                address_place, metro_name, metro_time = "Нет данных", "Нет данных", "Нет данных"

            try:
                rating_div = item.find_element(By.CLASS_NAME, "z-stars--12")
                rating = rating_div.get_attribute("style").split("--rating:")[1].split(";")[0].strip()
            except (NoSuchElementException, IndexError):
                rating = "Нет рейтинга"

            try:
                reviews = item.find_element(By.CLASS_NAME, "comments").text.split()[0]
            except NoSuchElementException:
                reviews = "Нет отзывов"

            try:
                features_block = item.find_element(By.CLASS_NAME, "minicard-item__features")
                features = ", ".join([el.text for el in features_block.find_elements(By.TAG_NAME, "a") + features_block.find_elements(By.TAG_NAME, "span") if el.text.strip()])
            except NoSuchElementException:
                features = "Нет подкатегорий"

            data.append({
                "Название": title,
                "Ссылка": link,
                "Адрес места": address_place,
                "Метро": metro_name,
                "Время до метро": metro_time,
                "Рейтинг": rating,
                "Отзывы": reviews,
                "Подкатегории": features,
            })
        except Exception as e:
            logging.error(f"Ошибка при парсинге элемента {title}: {e}")


    return data

categories = get_categories()
all_data = []

for name, url in categories.items():
        all_data.extend(parse_category(name, url))
        logging.info("Даем паузу перед следующей категорией")
        random_sleep(2, 5)



driver.quit()

df = pd.DataFrame(all_data)
df.to_csv(save_to_csv, index=False, encoding="utf-8-sig")
logging.info(f"Сохранено {len(df)} записей в {save_to_csv}")
