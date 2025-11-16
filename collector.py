import os
import re
import time
import json
import feedparser
import requests
import urllib3
from datetime import datetime
from html import unescape

import gspread
from google.oauth2.service_account import Credentials

from dotenv import load_dotenv

# Отключаем SSL-предупреждения
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()  # загружает переменные из .env

# === Конфигурация ===
RSS_URL = "https://torgi.gov.ru/new/api/public/lotcards/rss?lotStatus=PUBLISHED,APPLICATIONS_SUBMISSION&catCode=2&byFirstVersion=true"
MAP_URL = "https://nspd.gov.ru/map?thematic=PKK&zoom=14.022938145428002&coordinate_x=10153878.513581853&coordinate_y=7361695.523330088&baseLayerId=235&theme_id=1&is_copy_url=true"
GEO_API_BASE = "https://nspd.gov.ru/api/geoportal/v2/search/geoportal"
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

# Новый заголовок
LOT_INFO_COL = "Lot_info"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 OPR/123.0.0.0 (Edition Yx 05)"

MAX_CELL_CHARS = 50000

# ✅ Исправленный регэксп: поддерживает 4–19 цифр в третьей части (квартал+участок)
CADASTRAL_PATTERN = re.compile(r'\b\d{2}:\d{2}:\d{4,19}:\d{1,6}\b')

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def validate_and_truncate_row(row: list, headers: list, row_index_in_batch: int, lot_id: str = "") -> list:
    """
    Проверяет каждую ячейку в строке на превышение лимита Google Sheets (50k символов).
    Если превышает — заменяет на пустую строку и выводит предупреждение.
    """
    validated_row = []
    for col_idx, cell_value in enumerate(row):
        cell_str = str(cell_value) if cell_value is not None else ""
        if len(cell_str) > MAX_CELL_CHARS:
            field_name = headers[col_idx] if col_idx < len(headers) else f"Column_{col_idx}"
            preview = cell_str.replace("\n", "\\n")
            print(f"⚠️ CELL TOO LONG (row {row_index_in_batch}, lot '{lot_id}')")
            print(f"   Field: {field_name}")
            print(f"   Length: {len(cell_str)} chars (max {MAX_CELL_CHARS})")
            print(f"   Preview: {preview}")
            validated_row.append("")  # сохраняем пусто, чтобы не сломать вставку
        else:
            validated_row.append(cell_value)
    return validated_row

# Извлекает ID лота из ссылки
def extract_lot_id_from_link(link: str) -> str:
    """Из 'https://torgi.gov.ru/.../23000030610000000997_1  ' → '23000030610000000997_1'"""
    if not link:
        return ""
    # Убираем пробелы в конце
    link = link.strip()
    # Берём часть после последнего '/'
    parts = link.rstrip('/').split('/')
    if parts:
        lot_id = parts[-1]
        # Оставляем только цифры, подчёркивания
        lot_id_clean = re.sub(r'[^0-9_]', '', lot_id)
        if lot_id_clean:
            return lot_id_clean
    return ""

# Получает данные лота по ID
def fetch_lot_info(lot_id: str, referer: str):
    """Запрашивает детали лота с torgi.gov.ru"""
    if not lot_id:
        return None

    url = f"https://torgi.gov.ru/new/api/public/lotcards/{lot_id}"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Referer": referer,
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": USER_AGENT,
        "branchId": "null",
        "organizationId": "null",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Opera";v="123", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "traceparent": "00-4028d76347b5b5ea5b4479f015343701-e346b4143d2840ea-01"
    }

    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"⚠️ Lot info error {resp.status_code} for {lot_id}")
            return None
    except Exception as e:
        print(f"💥 Lot info exception for {lot_id}: {e}")
        return None

def clean_html_tags(text: str) -> str:
    if not text:
        return ""
    clean = re.sub(r'<[^>]+>', '', text)
    clean = unescape(clean)
    return clean.strip()

def normalize_field_name(name: str) -> str:
    name = name.strip()
    if name.endswith(':'):
        name = name[:-1].strip()
    name = name.replace(':', '_')
    name = re.sub(r'\s+', ' ', name)
    return name.capitalize()

def parse_description_fields(description_html: str) -> dict:
    if not description_html:
        return {}
    partially_clean = unescape(description_html)
    parts = re.split(r'<br\s*/?>', partially_clean, flags=re.IGNORECASE)
    fields = {}
    for part in parts:
        clean_part = clean_html_tags(part)
        clean_part = clean_part.strip()
        if not clean_part or ':' not in clean_part:
            continue
        key_raw, value_raw = clean_part.split(':', 1)
        key_norm = normalize_field_name(key_raw)
        value_clean = value_raw.strip()
        if key_norm and value_clean:
            fields[key_norm] = value_clean
    return fields

def extract_item_raw_fields(item) -> dict:
    """Извлекает оригинальные поля из feedparser-элемента."""
    fields = {}
    # Стандартные поля
    if hasattr(item, 'title') and item.title:
        fields['title'] = item.title
    if hasattr(item, 'link') and item.link:
        fields['link'] = item.link
    if hasattr(item, 'description') and item.description:
        fields['description'] = item.description
#    if hasattr(item, 'published') and item.published:
#        fields['pubDate'] = item.published
    # ✅ Конвертируем pubDate в ISO
    if hasattr(item, 'published') and item.published and item.published_parsed:
        try:
            dt = datetime.fromtimestamp(time.mktime(item.published_parsed))
            fields['pubDate'] = dt.isoformat()  # ← ISO-формат!
        except:
            fields['pubDate'] = item.published  # fallback
    elif hasattr(item, 'published'):
        fields['pubDate'] = item.published

    if hasattr(item, 'id') and item.id:
        fields['guid'] = item.id

    # Поля из namespaces (dc:date и др.)
    if hasattr(item, 'dc_date') and item.dc_date:
        fields['dc:date'] = item.dc_date
    elif 'dc' in item and 'date' in item['dc']:
        dc_val = item['dc']['date']
        if isinstance(dc_val, list) and dc_val:
            fields['dc:date'] = dc_val[0]
        elif isinstance(dc_val, str):
            fields['dc:date'] = dc_val

    return fields

def extract_cadastral_number_from_item(item_fields: dict, desc_fields: dict) -> str:
    """Ищет кадастровый номер сначала в полях, потом в тексте."""
    # 1. В распарсенных полях description
    for key, value in desc_fields.items():
        if "кадастровый номер" in key.lower():
            if CADASTRAL_PATTERN.fullmatch(value.strip()):
                return value.strip()

    # 2. В полях item (на случай, если уже есть как отдельное поле)
    for key, value in item_fields.items():
        if "кадастровый номер" in key.lower():
            if CADASTRAL_PATTERN.fullmatch(str(value).strip()):
                return str(value).strip()

    # 3. В общем тексте (title + description)
    text = item_fields.get("title", "") + " " + item_fields.get("description", "")
    match = CADASTRAL_PATTERN.search(text)
    return match.group(0) if match else ""

def get_session_with_cookies():
    session = requests.Session()
    session.verify = False
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

def collect_all_field_names_from_items(items):
    field_set = set()
    for item in items:
        item_fields = extract_item_raw_fields(item)
        for key, value in item_fields.items():
            if isinstance(value, str):
                field_set.add(normalize_field_name(key))
        desc_fields = parse_description_fields(item_fields.get("description", ""))
        field_set.update(desc_fields.keys())
    special_fields = {"Кадастровый номер", "Nspd_data", "Nspd_error", "Unsorted", LOT_INFO_COL}
    field_set.update(special_fields)
    sorted_fields = sorted([f for f in field_set if f != "Unsorted"])
    sorted_fields.append("Unsorted")
    return sorted_fields

def build_row_for_sheet(item_fields, desc_fields, headers, cadastral_number="", nspd_data="", nspd_error=""):
    row_dict = {}
    for key, value in item_fields.items():
        if isinstance(value, str):
            field_name = normalize_field_name(key)
            row_dict[field_name] = value
    row_dict.update(desc_fields)
    row_dict["Кадастровый номер"] = cadastral_number
    row_dict["Nspd_data"] = nspd_data
    row_dict["Nspd_error"] = nspd_error

    header_to_index = {name: i for i, name in enumerate(headers)}
    row = [""] * len(headers)
    unsorted_pairs = []
    for field_name, value in row_dict.items():
        if field_name in header_to_index:
            row[header_to_index[field_name]] = str(value) if value is not None else ""
        else:
            unsorted_pairs.append(f"{field_name}: {value}")
    if "Unsorted" in header_to_index:
        row[header_to_index["Unsorted"]] = "\n".join(unsorted_pairs)
    return row

def find_last_filled_row_in_column(sheet, col_letter: str, max_rows_limit: int = 100000) -> int:
    """
    Находит номер последней непустой строки в указанной колонке Google Таблицы.
    
    Алгоритм:
      1. Экспоненциальный рост: проверяем строки 1, 2, 4, 8, 16, ..., пока не найдём пустую.
      2. Бинарный поиск между последней заполненной и первой пустой.
    
    Возвращает:
      - Номер строки (int), если найдена хотя бы одна непустая строка (начиная с 2, т.к. 1 — заголовки)
      - 0, если нет ни одной непустой строки после заголовков
    """
    low = 1  # первая строка — заголовки, нас интересует начиная со 2
    high = 1

    # Шаг 1: Экспоненциальный рост, пока не найдём пустую строку
    while high <= max_rows_limit:
        range_name = f"{col_letter}{high}:{col_letter}{high}"
        try:
#            print(range_name)
            values = sheet.get(range_name)
            if not values or not values[0] or not values[0][0].strip():
                # Нашли пустую строку → останавливаемся
                break
        except Exception:
            # Считаем пустой
            break
        low = high
        high *= 2

    # Ограничиваем сверху
    high = min(high, max_rows_limit)

    # Шаг 2: Бинарный поиск между low и high
    last_filled = 0
    while low <= high:
        mid = (low + high) // 2
        range_name = f"{col_letter}{mid}:{col_letter}{mid}"
        try:
            values = sheet.get(range_name)
            if values and values[0] and values[0][0].strip():
                last_filled = mid
                low = mid + 1
            else:
                high = mid - 1
        except Exception:
            high = mid - 1

    # Нас интересуют только строки после заголовков (>=2)
    return last_filled if last_filled >= 2 else 0

def parse_date_flexible(date_str: str):
    """Парсит дату в ISO или RFC-2822 формате."""
    if not date_str:
        return None
    # Сначала пробуем ISO
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except:
        pass
    # Потом RFC-2822
    try:
        import email.utils
        return datetime.fromtimestamp(email.utils.parsedate_to_datetime(date_str).timestamp())
    except:
        return None

# === ОСНОВНАЯ ЛОГИКА ===

def main():
    sheet = get_sheet()

    # ================================
    # 🔁 ОБРАБОТКА СУЩЕСТВУЮЩИХ ЗАПИСЕЙ (для первого прогона ПОСЛЕ добавления Lot_info)
    # После завершения — ЗАКОММЕНТИРУЙТЕ этот блок!
    # ================================
#    try:
#        first_row = sheet.row_values(1)
#        if LOT_INFO_COL in first_row:
#            print("🔧 Found existing table with Lot_info column. Processing old rows...")
#            link_col_idx = None
#            lot_info_col_idx = None
#            for i, name in enumerate(first_row):
#                if normalize_field_name("link") == normalize_field_name(name):
#                    link_col_idx = i
#                if name == LOT_INFO_COL:
#                    lot_info_col_idx = i
#
#        if link_col_idx is not None and lot_info_col_idx is not None:
#            last_row = find_last_filled_row_in_column(sheet, gspread.utils.rowcol_to_a1(1, link_col_idx + 1)[0])
#            if last_row > 0:
#                print(f"🧮 Processing rows 2 to {last_row} for Lot_info (in batches)...")
#                
#                # Определяем буквы колонок
#                link_col_letter = gspread.utils.rowcol_to_a1(1, link_col_idx + 1)[0]
#                lot_info_col_letter = gspread.utils.rowcol_to_a1(1, lot_info_col_idx + 1)[0]
#
#                batch_size = 30  # ≤30 — безопасно для лимитов
#                start_row = 2
#                while start_row <= last_row:
#                    end_row = min(start_row + batch_size - 1, last_row)
#                    print(f"  📥 Reading rows {start_row}–{end_row}...")
#
#                    # Пакетное чтение
#                    link_range = f"{link_col_letter}{start_row}:{link_col_letter}{end_row}"
#                    lot_info_range = f"{lot_info_col_letter}{start_row}:{lot_info_col_letter}{end_row}"
#                    
#                    try:
#                        batch_data = sheet.batch_get([link_range, lot_info_range])
#                        links_batch = batch_data[0] if len(batch_data) > 0 else []
#                        lot_info_batch = batch_data[1] if len(batch_data) > 1 else []
#                    except Exception as e:
#                        print(f"    ❌ Batch read error: {e}")
#                        time.sleep(10)
#                        start_row += batch_size
#                        continue
#
#                    # Обрабатываем пакет
#                    updates = []
#                    for i in range(len(links_batch)):
#                        row_num = start_row + i
#                        link_val = links_batch[i][0] if i < len(links_batch) and links_batch[i] else ""
#                        lot_info_val = lot_info_batch[i][0] if i < len(lot_info_batch) and lot_info_batch[i] else ""
#
#                        if link_val and (not lot_info_val or lot_info_val.strip() == ""):
#                            lot_id = extract_lot_id_from_link(link_val)
#                            if lot_id:
#                                print(f"    📥 Fetching lot info for {lot_id} (row {row_num})")
#                                lot_data = fetch_lot_info(lot_id, link_val)
#                                if lot_data:
#                                    cell_addr = gspread.utils.rowcol_to_a1(row_num, lot_info_col_idx + 1)
#                                    updates.append({
#                                        "range": cell_addr,
#                                        "values": [[json.dumps(lot_data, ensure_ascii=False)]]
#                                    })
#                                time.sleep(0.3)  # между запросами к torgi.gov.ru
#
#                    # Пакетная запись (если есть что обновлять)
#                    if updates:
#                        try:
#                            sheet.batch_update(updates)
#                            print(f"    ✅ Updated {len(updates)} rows")
#                        except Exception as e:
#                            print(f"    ❌ Batch update error: {e}")
#                            time.sleep(5)
#
#                    # Задержка перед следующим пакетом
#                    time.sleep(2.0)
#                    start_row += batch_size
#            else:
#                print("⚠️ Could not find Link or Lot_info columns")
#        else:
#            print("🆕 Lot_info column not present — skipping old rows processing")
#    except Exception as e:
#        print(f"⚠️ Old rows processing failed: {e}")

    # ================================
    # КОНЕЦ БЛОКА ДЛЯ ЗАКомМЕНТИРОВАНИЯ
    # ================================

    # === Первый запуск? (только первая строка) ===
    try:
        first_row = sheet.row_values(1)
        is_first_run = not any(cell.strip() for cell in first_row)
    except Exception:
        is_first_run = True

    if is_first_run:
        print("🆕 First run: downloading RSS to collect headers...")
        rss_resp = requests.get(RSS_URL, verify=False, timeout=15)
        rss_resp.raise_for_status()
        feed = feedparser.parse(rss_resp.content)
        if not feed.entries:
            print("📭 No entries in RSS")
            return
        headers = collect_all_field_names_from_items(feed.entries)
        print(f"📝 Creating header with {len(headers)} columns")
        sheet.update(range_name='A1', values=[headers])
        first_row = headers
    else:
        first_row = sheet.row_values(1)
        print(f"📝 Read first line Type: {type(first_row)} Value {first_row}")

    headers = first_row
    header_to_col = {name: i for i, name in enumerate(headers)}
    required_cols = ["Кадастровый номер", "Nspd_data", "Nspd_error", "Unsorted"]
    for col in required_cols:
        if col not in header_to_col:
            raise RuntimeError(f"Missing required column: {col}")


    # === Находим последнюю дату публикации (эффективно) ===
    pubdate_col_name = normalize_field_name("pubDate")
    last_pub_date = None
    if pubdate_col_name in header_to_col:
        col_idx = header_to_col[pubdate_col_name]
        col_letter = gspread.utils.rowcol_to_a1(1, col_idx + 1)[0]  # 'A', 'B', ...

        last_row = find_last_filled_row_in_column(sheet, col_letter)
        if last_row > 0:
            range_name = f"{col_letter}{last_row}:{col_letter}{last_row}"
            try:
                values = sheet.get(range_name)
                if values and values[0] and values[0][0].strip():
                    date_str = values[0][0].strip()
                    try:
#                        last_pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        last_pub_date = parse_date_flexible(date_str)
                    except Exception as e:
                        print(f"⚠️ Invalid date format in row {last_row}: {date_str} ({e})")
            except Exception as e:
                print(f"⚠️ Could not read date from row {last_row}: {e}")
        else:
            print("📭 No pubDate entries found in sheet")
    else:
        print("⚠️ Column 'Pubdate' not found in headers")

    print(f"🕗 Last processed pubDate: {last_pub_date}")

    # === Повторная обработка ошибок (последние 100 строк) ===
    cad_col_idx = header_to_col["Кадастровый номер"]
    error_col_idx = header_to_col["Nspd_error"]
    geo_col_idx = header_to_col["Nspd_data"]
    cad_col_letter = gspread.utils.rowcol_to_a1(1, cad_col_idx + 1)[0]
    error_col_letter = gspread.utils.rowcol_to_a1(1, error_col_idx + 1)[0]

    rows_to_update = []
    total_rows = sheet.row_count
    if total_rows >= 2:
        start_row = max(2, total_rows - 99)
        try:
            cad_vals = sheet.get(f"{cad_col_letter}{start_row}:{cad_col_letter}") or []
            err_vals = sheet.get(f"{error_col_letter}{start_row}:{error_col_letter}") or []
            session = get_session_with_cookies()
            for i in range(len(cad_vals)):
                row_num = start_row + i
                cad = cad_vals[i][0].strip() if i < len(cad_vals) and cad_vals[i] else ""
                err = err_vals[i][0].strip() if i < len(err_vals) and err_vals[i] else ""
                if cad and CADASTRAL_PATTERN.fullmatch(cad) and err:
                    print(f"🔁 Retrying {cad} (row {row_num})")
                    geo_data, error = fetch_geoportal_data(session, cad)
                    if error is None:
                        geo_str = json.dumps(geo_data, ensure_ascii=False)
                        rows_to_update.append({"range": gspread.utils.rowcol_to_a1(row_num, geo_col_idx + 1), "values": [[geo_str]]})
                        rows_to_update.append({"range": gspread.utils.rowcol_to_a1(row_num, error_col_idx + 1), "values": [[""]]})
                    time.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Error during retry: {e}")

    if rows_to_update:
        print(f"📤 Updating {len(rows_to_update)} rows")
        for upd in rows_to_update:
            sheet.update(range_name=upd["range"], values=upd["values"])

    # === Обработка новых лотов из RSS ===
    print("🔍 Fetching RSS for new lots...")
    rss_resp = requests.get(RSS_URL, verify=False, timeout=15)
    rss_resp.raise_for_status()
    feed = feedparser.parse(rss_resp.content)
    if not feed.entries:
        print("📭 No RSS entries")
        return

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

    new_rows = []
    for pub_dt, item in rss_items:
        if pub_dt and last_pub_date and pub_dt <= last_pub_date:
            continue

        item_fields = extract_item_raw_fields(item)
        desc_fields = parse_description_fields(item_fields.get("description", ""))
        cad_num = extract_cadastral_number_from_item(item_fields, desc_fields)

        # ✅ Получаем Lot_info
        lot_info = ""
        link_val = item_fields.get("link", "")
        if link_val:
            lot_id = extract_lot_id_from_link(link_val)
            if lot_id:
                lot_data = fetch_lot_info(lot_id, link_val)
                if lot_data:
                    lot_info = json.dumps(lot_data, ensure_ascii=False)

        nspd_data, nspd_error = "", ""
        if cad_num:
            session = get_session_with_cookies()  # или переиспользуйте сессию
            geo_data, error = fetch_geoportal_data(session, cad_num)
            if error is None:
                nspd_data = json.dumps(geo_data, ensure_ascii=False)
            else:
                nspd_error = error

        row = build_row_for_sheet(
            item_fields=item_fields,
            desc_fields=desc_fields,
            headers=headers,
            cadastral_number=cad_num,
            nspd_data=nspd_data,
            nspd_error=nspd_error
        )

        # Добавляем Lot_info вручную (т.к. его нет в item_fields)
        if LOT_INFO_COL in header_to_col:
            row[header_to_col[LOT_INFO_COL]] = lot_info

        new_rows.append(row)
        time.sleep(0.5)

#    if new_rows:
#        print(f"✅ Appending {len(new_rows)} new rows")
#        sheet.append_rows(new_rows)
    if new_rows:
        print(f"✅ Appending {len(new_rows)} new rows")
        validated_rows = []
        for i, row in enumerate(new_rows):
            # Попытаемся получить lot_id из строки (например, из колонки Link)
            lot_id = ""
            try:
                link_col_name = normalize_field_name("link")
                if link_col_name in header_to_col:
                    link_val = row[header_to_col[link_col_name]]
                    lot_id = extract_lot_id_from_link(link_val)
            except:
                pass
    
            validated_row = validate_and_truncate_row(
                row=row,
                headers=headers,
                row_index_in_batch=i + 1,  # нумерация с 1
                lot_id=lot_id
            )
            validated_rows.append(validated_row)
    
        sheet.append_rows(validated_rows)
    else:
        print("📭 No new lots.")

if __name__ == "__main__":
    main()