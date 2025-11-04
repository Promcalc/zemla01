import os
import re
import time
import json
import feedparser
import requests
from datetime import datetime
from html import unescape

import gspread
from google.oauth2.service_account import Credentials

# === Конфигурация ===
RSS_URL = "https://torgi.gov.ru/new/api/public/lotcards/rss?lotStatus=PUBLISHED,APPLICATIONS_SUBMISSION&matchPhrase=false&byFirstVersion=true"
MAP_URL = "https://nspd.gov.ru/map?thematic=PKK&zoom=14.022938145428002&coordinate_x=10153878.513581853&coordinate_y=7361695.523330088&baseLayerId=235&theme_id=1&is_copy_url=true"
GEO_API_BASE = "https://nspd.gov.ru/api/geoportal/v2/search/geoportal"
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 OPR/123.0.0.0 (Edition Yx 05)"
CADASTRAL_PATTERN = re.compile(r'\b\d{2}:\d{2}:\d{7}:\d+\b')

# === Вспомогательные функции ===

def clean_html(text):
    if not text:
        return ""
    clean = re.sub(r'<[^>]+>', '', text)
    return unescape(clean)

def normalize_field_name(name):
    name = name.strip()
    if name.endswith(':'):
        name = name[:-1].strip()
    name = re.sub(r'\s+', ' ', name)
    return name.capitalize()

def parse_description_to_dict(desc_html):
    desc_clean = clean_html(desc_html)
    lines = desc_clean.split('\n')
    fields = {}
    for line in lines:
        line = line.strip()
        if not line or ':' not in line:
            continue
        parts = line.split(':', 1)
        key = normalize_field_name(parts[0])
        value = parts[1].strip() if len(parts) > 1 else ""
        if key:
            fields[key] = value
    return fields

def get_session_with_cookies():
    session = requests.Session()
    session.headers.update({
        "user-agent": USER_AGENT,
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    })
    resp = session.get(MAP_URL, timeout=10)
    resp.raise_for_status()
    return session

def format_error_response(response):
    try:
        body = response.text[:10000]
    except:
        body = "<binary or unreadable>"
    return (
        f"Status: {response.status_code}\n"
        f"URL: {response.url}\n"
        f"Headers: {dict(response.headers)}\n"
        f"Body: {body}"
    )

def fetch_geoportal_data(session, cad_num):
    url = f"{GEO_API_BASE}?thematicSearchId=1&query={requests.utils.quote(cad_num)}"
    headers = {
        "accept": "*/*",
        "referer": MAP_URL,
        "sec-ch-ua": '"Not;A=Brand";v="99", "Opera";v="123", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "priority": "u=1, i",
    }
    try:
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return data, None
        else:
            return None, format_error_response(resp)
    except Exception as e:
        return None, f"Exception: {str(e)}"

def get_sheet():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    return client.open_by_key(SHEET_ID).sheet1

# === Основная логика ===

def main():
    sheet = get_sheet()
    sheet_data = sheet.get_all_values()
    
    is_first_run = len(sheet_data) == 0 or not any(sheet_data[0])
    special_cols = {"Кадастровый номер", "Geoportal данные", "nspd_error", "Unsorted"}

    if is_first_run:
        print("🆕 First run: collecting all fields from RSS items...")
        feed = feedparser.parse(RSS_URL)
        all_field_names = set(special_cols)

        for item in feed.entries:
            # Все строковые поля из item
            for key, value in item.items():
                if isinstance(value, str):
                    all_field_names.add(normalize_field_name(key))
            # Поля из description
            desc_fields = parse_description_to_dict(item.get("description", ""))
            all_field_names.update(desc_fields.keys())

        # Формируем заголовки: сначала обычные, потом Unsorted в конце
        header_row = sorted([f for f in all_field_names if f != "Unsorted"])
        header_row.append("Unsorted")

        print(f"📝 Creating header with {len(header_row)} columns")
        sheet.update('A1', [header_row])
        sheet_data = [header_row]

    headers = sheet_data[0]
    header_to_col = {name: i for i, name in enumerate(headers)}
    unsorted_col_idx = header_to_col.get("Unsorted", len(headers) - 1)

    # Проверка обязательных колонок
    required = ["Кадастровый номер", "Geoportal данные", "nspd_error"]
    for col in required:
        if col not in header_to_col:
            raise RuntimeError(f"Missing required column: {col}")

    session = get_session_with_cookies()

    # === Шаг 1: Повторная обработка ошибок ===
    cad_col_idx = header_to_col["Кадастровый номер"]
    error_col_idx = header_to_col["nspd_error"]
    geo_col_idx = header_to_col["Geoportal данные"]

    rows_to_update = []
    for row_idx, row in enumerate(sheet_data[1:], start=2):
        if len(row) <= cad_col_idx:
            continue
        cad_num = row[cad_col_idx].strip()
        if not cad_num or not CADASTRAL_PATTERN.match(cad_num):
            continue
        if len(row) > error_col_idx and row[error_col_idx].strip():
            print(f"🔁 Retrying failed request for {cad_num} (row {row_idx})")
            geo_data, error = fetch_geoportal_data(session, cad_num)
            if error is None:
                geo_str = json.dumps(geo_data, ensure_ascii=False)
                rows_to_update.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, geo_col_idx + 1),
                    "values": [[geo_str]]
                })
                rows_to_update.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, error_col_idx + 1),
                    "values": [[""]]
                })
            time.sleep(0.5)

    if rows_to_update:
        print(f"📤 Updating {len(rows_to_update)} fixed rows...")
        for update in rows_to_update:
            sheet.update(update["range"], update["values"])
        sheet_data = sheet.get_all_values()  # обновляем после изменений

    # === Шаг 2: Обработка новых лотов по pubDate ===
    print("🔍 Fetching RSS feed...")
    feed = feedparser.parse(RSS_URL)
    if not feed.entries:
        print("📭 No RSS entries")
        return

    # Собираем и сортируем по pubDate
    rss_items = []
    for item in feed.entries:
        pub_dt = None
        if hasattr(item, 'published_parsed') and item.published_parsed:
            try:
                pub_dt = datetime.fromtimestamp(time.mktime(item.published_parsed))
            except:
                pass
        rss_items.append((pub_dt, item))
    rss_items.sort(key=lambda x: x[0] or datetime.min)

    # Находим последнюю дату в таблице
    pubdate_col_name = normalize_field_name("pubDate")  # = "Pubdate"
    last_pub_date = None
    if pubdate_col_name in header_to_col:
        col_idx = header_to_col[pubdate_col_name]
        for row in reversed(sheet_data[1:]):
            if len(row) > col_idx and row[col_idx].strip():
                try:
                    last_pub_date = datetime.fromisoformat(row[col_idx].replace("Z", "+00:00"))
                    break
                except:
                    continue

    print(f"🕗 Last processed pubDate: {last_pub_date}")

    new_rows = []
    for pub_dt, item in rss_items:
        if pub_dt and last_pub_date and pub_dt <= last_pub_date:
            continue

        # Собираем все данные
        row_dict = {}

        # 1. Все строковые поля из item
        for key, value in item.items():
            if isinstance(value, str):
                field_name = normalize_field_name(key)
                row_dict[field_name] = value

        # 2. Поля из description
        desc_fields = parse_description_to_dict(item.get("description", ""))
        row_dict.update(desc_fields)

        # 3. Приводим pubDate к ISO (если есть)
        if pub_dt:
            row_dict[pubdate_col_name] = pub_dt.isoformat()

        # 4. Кадастровый номер
        cad_text = str(row_dict.get("Описание", "")) + " " + item.get("description", "")
        cad_match = CADASTRAL_PATTERN.search(cad_text)
        cad_num = cad_match.group(0) if cad_match else None

        if cad_num:
            row_dict["Кадастровый номер"] = cad_num
            geo_data, error = fetch_geoportal_data(session, cad_num)
            row_dict["Geoportal данные"] = json.dumps(geo_data, ensure_ascii=False) if error is None else ""
            row_dict["nspd_error"] = "" if error is None else error
        else:
            row_dict["Кадастровый номер"] = ""
            row_dict["Geoportal данные"] = ""
            row_dict["nspd_error"] = ""

        # 5. Формируем строку
        row = [""] * len(headers)
        unsorted_pairs = []
        for field_name, value in row_dict.items():
            if field_name in header_to_col:
                row[header_to_col[field_name]] = str(value) if value is not None else ""
            else:
                unsorted_pairs.append(f"{field_name}: {value}")

        if unsorted_pairs:
            row[unsorted_col_idx] = "\n".join(unsorted_pairs)

        new_rows.append(row)
        time.sleep(0.5)

    if new_rows:
        print(f"✅ Appending {len(new_rows)} new rows")
        sheet.append_rows(new_rows)
    else:
        print("📭 No new lots to add.")

if __name__ == "__main__":
    main()