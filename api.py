import json
import logging
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import csv

with open("config.json", "r", encoding="utf-8") as config_file:
    config = json.load(config_file)

logging_enabled = config.get("logging_enabled", True)
logging_level = config.get("logging_level", "INFO").upper()
save_to_csv = config.get("save_to_csv", "zoon_data_api_full.csv")

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

logging.info("API запущен")

ACCESS_TOKEN = "vk1.a.eS55JrL4tMoVVp9oOb38m6X3agCMRtr2dxogMo_USsSGmpJMz91d48Gsw-D25EPAmEUDlJvR2U6TCTgvNxQ0OpEBMVRr71UWUJ0ojznrf2EWLt6MN7gJD39W9e_QL1xMaBVzTdycszYNX7U4LnQrczte86Gr-XF6Lb426sZXTTLnmsU89kE3FJZY_uJ7kfwT"

API_VERSION = "5.131"
BASE_URL = "https://api.vk.com/method/"

file_path = "zoon_data_all_full_.csv"
log_file = "logs.csv"

df = pd.read_csv(file_path)
business_names = df["Название"].dropna().unique().tolist()

aggregated_results = []
six_months_ago = datetime.now() - timedelta(days=180)

# CSV-файл
def log_to_csv(business_name, group_id, reason):
    """Функция для записи логов в CSV"""
    logging.info([datetime.now(), business_name, group_id, reason])

def vk_request(method, params):
    """для выполнения запросов к VK API"""
    params.update({"access_token": ACCESS_TOKEN, "v": API_VERSION})
    response = requests.get(BASE_URL + method, params=params)
    time.sleep(0.5)
    logging.info([datetime.now(), response])

    if response.status_code == 200:
        logging.info(f"Запрос к {method} выполнен успешно.")
    else:
        logging.error(f"Ошибка при запросе к {method}: {response.status_code} - {response.text}")

    return response.json()


for business_name in business_names:
    logging.info(f"Ищем группу для: {business_name}")

    search_response = vk_request("groups.search", {"q": business_name, "count": 5})

    group_found = 0
    members_count = 0
    group_activity = "Неизвестно"
    num_posts_last_6_months = 0
    avg_comments_per_post = 0
    avg_likes_per_post = 0
    avg_views_per_post = 0
    engagement_rate = 0

    best_match = None
    best_score = 0

    if "response" in search_response and search_response["response"]["count"] > 0:
        for group in search_response["response"]["items"]:
            group_id = group["id"]
            group_name = group["name"].lower().strip()
            match_score = fuzz.ratio(business_name.lower(), group_name)

            logging.info(f"Найдена группа: {group_name} (ID: {group_id}) | Совпадение: {match_score}%")


            if match_score > best_score and match_score >= 60:
                best_match = group
                best_score = match_score

    if best_match:
        group_id = best_match["id"]
        group_info_response = vk_request("groups.getById", {"group_id": group_id, "fields": "members_count,activity"})
        members_count = group_info_response["response"][0].get("members_count", 0)
        group_activity = group_info_response["response"][0].get("activity", "Неизвестно")

        if members_count > 10:
            group_found = 1
            logging.info(f"Выбрана группа: {best_match['name']} (ID: {group_id}) | Итоговое совпадение: {best_score}% | Тематика: {group_activity}")

            wall_response = vk_request("wall.get", {"owner_id": f"-{group_id}", "count": 100})
            posts = wall_response.get("response", {}).get("items", [])
            recent_posts = [post for post in posts if datetime.fromtimestamp(post["date"]) > six_months_ago]

            num_posts_last_6_months = len(recent_posts)

            total_comments = 0
            total_likes = 0
            total_views = 0

            for post in recent_posts:
                total_comments += post.get("comments", {}).get("count", 0)
                total_likes += post.get("likes", {}).get("count", 0)
                total_views += post.get("views", {}).get("count", 0)
            avg_comments_per_post = total_comments / num_posts_last_6_months if num_posts_last_6_months > 0 else 0
            avg_likes_per_post = total_likes / num_posts_last_6_months if num_posts_last_6_months > 0 else 0
            avg_views_per_post = total_views / num_posts_last_6_months if num_posts_last_6_months > 0 else 0

            total_engagement = total_likes + total_comments
            engagement_rate = (total_engagement / members_count) * 100 if members_count > 0 else 0

            log_to_csv(business_name, group_id, "Success")
        else:
            logging.warning(f"Группа {best_match['name']} (ID: {group_id}) отклонена: подписчиков < 10")
            group_found = 0

    else:
        logging.warning(f"Группа не найдена для: {business_name}")

    company_data = {
        "Название": business_name,
        "Группа найдена": group_found,
        "Тематика группы": group_activity,
        "Число подписчиков": members_count,
        "Число постов за 6 месяцев": num_posts_last_6_months,
        "Среднее число комментариев на пост": round(avg_comments_per_post, 2),
        "Среднее число лайков на пост": round(avg_likes_per_post, 2),
        "Среднее число просмотров на пост": round(avg_views_per_post, 2),
        "Процент вовлеченности (ER)": round(engagement_rate, 2)
    }

    aggregated_results.append(company_data)

    logging.info(f"Добавлена: {business_name} | Подписчики: {members_count} | Тематика: {group_activity} | Постов за 6 мес: {num_posts_last_6_months} | Лайков/пост: {round(avg_likes_per_post, 2)} | Просмотров/пост: {round(avg_views_per_post, 2)} | ER: {round(engagement_rate, 2)}%")

aggregated_df = pd.DataFrame(aggregated_results)
aggregated_df.to_csv(save_to_csv, index=False, encoding="utf-8-sig")
logging.info(f"Сохранено {len(df)} записей в {save_to_csv}")
