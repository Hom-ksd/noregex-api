from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import re
import html
import requests
import csv
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from ratelimit import limits, sleep_and_retry
import asyncio

app = FastAPI()

class ScrapingStatus(BaseModel):
    status: str
    total_links: int = 0
    processed_links: int = 0

scraping_status = ScrapingStatus(status="Not started")
nobel_prize_data = []

def create_session_with_retries():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

@sleep_and_retry
@limits(calls=50, period=2)  # 1 call every 2 seconds
def rate_limited_get(session, url, headers=None, timeout=30):
    return session.get(url, headers=headers, timeout=timeout)

def fetch_and_parse_link(session, link, headers, debug=True):
    row = [link]
    try:
        response = rate_limited_get(session, link, headers=headers)
        response.raise_for_status()
        page_content = html.unescape(response.text)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {link}: {e}")
        return row + [""] * 7  # Add empty fields for missing data

    content_pattern = r'<div class="content">(.+?)</div>'
    content = re.findall(content_pattern, page_content, re.DOTALL)

    if content:
        content = content[0].strip().replace('\n', '')
        
        # Extract name and prize
        p_pattern = r'<p>(.+?)</p>'
        p = re.findall(p_pattern, content)
        if p:
            p = p[0].strip()
            name_and_year_pattern = r'(.+?)<br>(.+)'
            match = re.search(name_and_year_pattern, p)
            if match:
                name = match.group(1)
                prize = match.group(2)
                row.append(name)

                # Extract category and year
                category_year_pattern = r'The Nobel Prize in ([A-Za-z\s]+?) (\d{4})|The Sveriges Riksbank Prize in ([A-Za-z\s]+?) in Memory of Alfred Nobel (\d{4})|The Nobel ([A-Za-z]+?) Prize (\d{4})'
                match = re.search(category_year_pattern, prize)
                if match:
                    if match.group(1):
                        category, year = match.group(1), match.group(2)
                    elif match.group(3):
                        category, year = match.group(3), match.group(4)
                    elif match.group(5):
                        category, year = match.group(5), match.group(6)
                    row.extend([category, year])
                else:
                    row.extend(["", ""])
            else:
                row.extend(["", "", ""])
        else:
            row.extend(["", "", ""])

        # Extract born info
        born_info_pattern = r'<p class="born-date">([^<]+)'
        born_info = re.findall(born_info_pattern, content)
        if born_info:
            born_info = born_info[0].strip()
            born_date_pattern = r'Born: ([^,]+?), (.+)|Born: (\d{4})|Founded: (\d{4}), (.+)|Founded: (\d{4})|Residence at the time of the award:(.+)'
            match = re.search(born_date_pattern, born_info)
            if match:
                if match.group(1):
                    row.extend([match.group(1), match.group(2)])
                elif match.group(3):
                    row.extend([match.group(3), ""])
                elif match.group(4):
                    row.extend([match.group(4), match.group(5)])
                elif match.group(6):
                    row.extend([match.group(6), ""])
                elif match.group(7):
                    row.extend(["", match.group(7).strip()])
            else:
                row.extend(["", ""])
        else:
            row.extend(["", ""])

        # Extract motivation
        motivation_pattern = r'<p>Prize motivation: (.+?)</p>'
        motivation = re.search(motivation_pattern, content)
        print(motivation)
        
        # Remove Special characters in motivation (")
        row.append(re.sub('\W+', ' ', motivation.group(1).strip()) if motivation else "")

    else:
        row.extend([""] * 6)

    # Extract image
    div_image_pattern = r'<div class="image">(.+?)</div>'
    div_image = re.findall(div_image_pattern, page_content, re.DOTALL)
    if div_image:
        no_script_pattern = r'<noscript>(.+?)</noscript>'
        no_script = re.findall(no_script_pattern, div_image[0])
        if no_script:
            src_img_pattern = r'src="([^"]+?)"'
            src_img = re.findall(src_img_pattern, no_script[0])
            row.append(src_img[0].strip() if src_img else "")
        else:
            row.append("")
    else:
        row.append("")

    if debug:
        print(f"Processed: {link}")

    return row

async def scrape_nobel_prizes():
    global scraping_status, nobel_prize_data
    url = 'https://www.nobelprize.org/prizes/lists/all-nobel-prizes/all/'
    session = create_session_with_retries()

    try:
        response = rate_limited_get(session, url)
        response.raise_for_status()
        page_content = response.text
    except requests.exceptions.RequestException as e:
        scraping_status.status = f"Error fetching main page: {e}"
        return

    link_pattern = r'<a class="card-prize--laureates--links--link" href="([^"]+)'
    links = re.findall(link_pattern, page_content)

    scraping_status.total_links = len(links)
    scraping_status.status = "In progress"

    data = [["Link", "Name", "Category", "Year", "Born date", "Born country", "Motivation", "Image"]]
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0'}

    data_lock = Lock()

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_link = {executor.submit(fetch_and_parse_link, session, link, headers): link for link in links}
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                print(link)
                row = future.result()
                with data_lock:
                    data.append(row)
                    scraping_status.processed_links += 1
            except Exception as e:
                print(f"Error processing {link}: {e}")

    # with open('output.csv', 'w', newline='', encoding='utf-8') as file:
    #     writer = csv.writer(file)
    #     writer.writerows(data)

    nobel_prize_data = [
        {
            "name": row[1],
            "category": row[2],
            "year": row[3],
            "born_date": row[4],
            "born_place": row[5],
            "motivation": row[6],
            "image": row[7]
        }
        for row in data[1:]  # header row
    ]

    scraping_status.status = "Completed"

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(scrape_nobel_prizes())

@app.get("/scraping-status")
async def get_scraping_status():
    return scraping_status

@app.get("/nobel-prizes")
async def get_all_nobel_prizes():
    print('Get All Data')
    if scraping_status.status != "Completed":
        return {"error": "Data scraping is not complete. Please try again later."}
    return nobel_prize_data

@app.get("/")
async def read_root():
    return {"Hello": "World"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)