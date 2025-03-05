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

ACCESS_TOKEN = "vk1.a.K6PA5cy4Zu57p3JMs12lCyGZ8tS_BNBeaZbXlTf4RqQhkpwQDVkw-8xuUQDsHqC_uASSu-ZEJSRD-UrU2Dvqek5r16PrP9sNw52A91AXqbt0ULmYci2AmHdgaWORfcYXNgJLtccq41zxUmcF6CLfbhKUw0xyYQh0r4RJ-G9nHcg_XOntkb6R-fH018ACIT57"

API_VERSION = "5.131"
BASE_URL = "https://api.vk.com/method/"

file_path = "zoon_data_all_full_.csv"
log_file = "logs.csv"

counter = 0

df = pd.read_csv(file_path)
business_names = df["Название"].dropna().unique().tolist()

aggregated_results = []
six_months_ago = datetime.now() - timedelta(days=180)

def log_to_csv(business_name, group_id, reason):
    logging.info([datetime.now(), business_name, group_id, reason])

def vk_request(method, params):
    params.update({"access_token": ACCESS_TOKEN, "v": API_VERSION})
    response = requests.get(BASE_URL + method, params=params)
    time.sleep(0.5)
    logging.info([datetime.now(), response])

    if response.status_code == 200:
        logging.info(f"Запрос к {method} выполнен успешно.")
    else:
        logging.error(f"Ошибка при запросе к {method}: {response.status_code} - {response.text}")

    return response.json()

def get_gender_distribution(group_id):
    group_info = vk_request("groups.getById", {"group_id": group_id, "fields": "members_count"})
    total_members = group_info.get("response", [{}])[0].get("members_count", 0)

    min_members = min(1000, total_members)
    response = vk_request("groups.getMembers", {"group_id": group_id, "fields": "sex", "count": min_members})

    if "response" in response and "items" in response["response"]:
        members = response["response"]["items"]

        if not members:
            return 0, 0, 1

        male_count = sum(1 for member in members if member.get("sex") == 2)
        female_count = sum(1 for member in members if member.get("sex") == 1)
        sampled_count = len(members)

        percent_male = round((male_count / sampled_count) * 100, 2) if sampled_count > 0 else 0
        percent_female = 100 - percent_male if sampled_count > 0 else 0

        return percent_male, percent_female, 0

    return 0, 0, 1


def get_avg_news_mentions(query):
    end_time = int(datetime.now().timestamp())
    start_time = int((datetime.now() - timedelta(days=30)).timestamp())
    query = f'"{query}"'
    news_response = vk_request("newsfeed.search", {"q": query, "start_time": start_time, "end_time": end_time, "count": 100 })
    if "response" in news_response and isinstance(news_response["response"], dict):
        count = news_response["response"].get("count", 0)
        return count / 30.0
    return 0


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
    percent_male = 0
    percent_female = 0
    members_see = 0
    avg_news_mentions = 0

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

            percent_male, percent_female, members_see = get_gender_distribution(group_id)
            avg_news_mentions = get_avg_news_mentions(business_name)

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
        "Подписчики скрыты": members_see,
        "Тематика группы": group_activity,
        "Число подписчиков": members_count,
        "Среднее число упоминаний в новостях в день": round(avg_news_mentions, 2),
        "Процент мужчин": percent_male,
        "Процент женщин": percent_female,
        "Число постов за 6 месяцев": num_posts_last_6_months,
        "Среднее число комментариев на пост": round(avg_comments_per_post, 2),
        "Среднее число лайков на пост": round(avg_likes_per_post, 2),
        "Среднее число просмотров на пост": round(avg_views_per_post, 2),
        "Процент вовлеченности (ER)": round(engagement_rate, 2)
    }
    print(company_data)
    counter += 1
    print(counter)
    aggregated_results.append(company_data)

    logging.info(f"Добавлена: {business_name} | Подписчики: {members_count} | Тематика: {group_activity} | Постов за 6 мес: {num_posts_last_6_months} | Лайков/пост: {round(avg_likes_per_post, 2)} | Просмотров/пост: {round(avg_views_per_post, 2)} | ER: {round(engagement_rate, 2)}% | Подписчики скрыты: {members_see} | Процент мужчин: {percent_male} | Процент женщин: {percent_female} | Среднее число упоминаний в новостях в день: {round(avg_news_mentions, 2)}")

aggregated_df = pd.DataFrame(aggregated_results)
aggregated_df.to_csv(save_to_csv, index=False, encoding="utf-8-sig")
logging.info(f"Сохранено {len(df)} записей в {save_to_csv}")
