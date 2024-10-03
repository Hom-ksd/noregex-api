
from fastapi import FastAPI, BackgroundTasks, Query
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
from typing import List, Optional

app = FastAPI()
class ScrapingStatus(BaseModel):
    status: str
    total_links: int = 0
    processed_links: int = 0

class NobelPrize(BaseModel):
    name: str
    category: str
    year: str
    born_date: str
    born_place: str
    motivation: str
    image: str

class Pagination(BaseModel):
    total_records: int
    current_page: int
    total_pages: int
    next_page: Optional[int]
    prev_page: Optional[int]

class NobelPrizeResponse(BaseModel):
    data: List[NobelPrize]
    pagination: Pagination


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

    with ThreadPoolExecutor(max_workers=32) as executor:
        future_to_link = {executor.submit(fetch_and_parse_link, session, link, headers): link for link in links}
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
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

def extract_year(date_string):
    """Extract year from a date string."""
    year_match = re.search(r'\b(\d{4})\b', date_string)
    return int(year_match.group(1)) if year_match else None

@app.get("/nobel-prizes", response_model=NobelPrizeResponse)
async def get_nobel_prizes(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(10, ge=1, le=100, description="Number of items per page"),
    name_filter: Optional[str] = Query(None, description="Regex pattern to filter names"),
    category_filter: Optional[str] = Query(None, description="Comma-separated list of regex patterns to filter categories"),
    country_filter: Optional[str] = Query(None, description="Comma-separated list of regex patterns to filter countries"),
    motivation_filter: Optional[str] = Query(None, description="Regex pattern to filter motivations"),
    birth_year_start: Optional[int] = Query(None, description="Start year for birth year range"),
    birth_year_end: Optional[int] = Query(None, description="End year for birth year range"),
    prize_year_start: Optional[int] = Query(None, description="Start year for prize year range"),
    prize_year_end: Optional[int] = Query(None, description="End year for prize year range")
):
    if scraping_status.status != "Completed":
        return {"error": "Data scraping is not complete. Please try again later."}

    filtered_data = nobel_prize_data

    # Filter by name
    if name_filter:
        try:
            name_regex = re.compile(name_filter, re.IGNORECASE)
            filtered_data = [prize for prize in filtered_data if name_regex.search(prize['name'])]
        except re.error:
            return {"error": "Invalid regex pattern for name filter"}

    # Filter by multiple categories using regex
    if category_filter:
        category_patterns = [cat.strip() for cat in category_filter.split(',')]
        try:
            category_regex = re.compile('|'.join(category_patterns), re.IGNORECASE)
            filtered_data = [prize for prize in filtered_data if category_regex.search(prize['category'])]
        except re.error:
            return {"error": "Invalid regex pattern for category filter"}

    # Filter by multiple countries using regex
    if country_filter:
        country_patterns = [country.strip() for country in country_filter.split(',')]
        try:
            country_regex = re.compile('|'.join(country_patterns), re.IGNORECASE)
            filtered_data = [prize for prize in filtered_data if country_regex.search(prize['born_place'])]
        except re.error:
            return {"error": "Invalid regex pattern for country filter"}

    # Filter by motivation
    if motivation_filter:
        try:
            motivation_regex = re.compile(motivation_filter, re.IGNORECASE)
            filtered_data = [prize for prize in filtered_data if motivation_regex.search(prize['motivation'])]
        except re.error:
            return {"error": "Invalid regex pattern for motivation filter"}

    # Filter by birth year range
    if birth_year_start is not None or birth_year_end is not None:
        filtered_data = [
            prize for prize in filtered_data 
            if prize['born_date'] and 
            (birth_year_start is None or 
             (extract_year(prize['born_date']) is not None and 
              extract_year(prize['born_date']) >= birth_year_start)) and
            (birth_year_end is None or 
             (extract_year(prize['born_date']) is not None and 
              extract_year(prize['born_date']) <= birth_year_end))
        ]

    # Filter by prize year range
    if prize_year_start is not None or prize_year_end is not None:
        filtered_data = [
            prize for prize in filtered_data 
            if (prize_year_start is None or int(prize['year']) >= prize_year_start) and
               (prize_year_end is None or int(prize['year']) <= prize_year_end)
        ]

    total_records = len(filtered_data)
    total_pages = (total_records + page_size - 1) // page_size

    # Ensure page is within valid range
    page = min(max(1, page), total_pages)

    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    paginated_data = filtered_data[start_index:end_index]

    next_page = page + 1 if page < total_pages else None
    prev_page = page - 1 if page > 1 else None

    return NobelPrizeResponse(
        data=paginated_data,
        pagination=Pagination(
            total_records=total_records,
            current_page=page,
            total_pages=total_pages,
            next_page=next_page,
            prev_page=prev_page
        )
    )


    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
