import streamlit as st
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, unquote, parse_qs
import os
import re
import zipfile
from io import BytesIO
import shutil
import requests  # Adding requests for synchronous fetching

file_types = [".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv"]

# Initialize stop flag and log container in Streamlit session state
if "stop_scraping" not in st.session_state:
    st.session_state.stop_scraping = False

if "log_messages" not in st.session_state:
    st.session_state.log_messages = []

if "download_ready" not in st.session_state:
    st.session_state.download_ready = False


def download_file_sync(url, download_folder, headers):
    try:
        response = requests.get(url, headers=headers, stream=True, allow_redirects=True)
        response.raise_for_status()

        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        filename = query_params.get('filename', [os.path.basename(parsed_url.path)])[0]
        filename = unquote(filename)
        filename = re.sub(r'[\\/*?:"<>|]', "_", filename)

        extension = os.path.splitext(filename)[1]
        if not extension:
            content_type = response.headers.get('Content-Type', '').lower()
            extension_mapping = {
                "application/pdf": ".pdf",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                "application/vnd.ms-excel": ".xls",
                "application/msword": ".doc",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                "text/csv": ".csv"
            }
            for content, ext in extension_mapping.items():
                if content in content_type:
                    extension = ext
                    break
            filename += extension

        if not any(filename.lower().endswith(ext) for ext in file_types):
            return

        filepath = os.path.join(download_folder, filename)
        with open(filepath, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        st.session_state.log_messages.append(f"Downloaded: {filepath}")

    except requests.exceptions.RequestException as e:
        st.session_state.log_messages.append(f"Failed to download {url}: {e}")
    except OSError as e:
        st.session_state.log_messages.append(f"Failed to save {url}: {e}")


async def download_file(session, url, download_folder, headers):
    if st.session_state.stop_scraping:
        return

    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()

            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            filename = query_params.get('filename', [os.path.basename(parsed_url.path)])[0]
            filename = unquote(filename)
            filename = re.sub(r'[\\/*?:"<>|]', "_", filename)

            extension = os.path.splitext(filename)[1]
            if not extension:
                content_type = response.headers.get('Content-Type', '').lower()
                extension_mapping = {
                    "application/pdf": ".pdf",
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
                    "application/vnd.ms-excel": ".xls",
                    "application/msword": ".doc",
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
                    "text/csv": ".csv"
                }
                extension = extension_mapping.get(content_type, "")
                filename += extension

            if not any(filename.lower().endswith(ext) for ext in file_types):
                return

            filepath = os.path.join(download_folder, filename)
            with open(filepath, 'wb') as file:
                while not st.session_state.stop_scraping:
                    chunk = await response.content.read(8192)
                    if not chunk:
                        break
                    file.write(chunk)
            if st.session_state.stop_scraping:
                st.session_state.log_messages.append("Stopping download in progress.")
    except asyncio.CancelledError:
        st.session_state.log_messages.append(f"Cancelled download for {url}")
    except Exception as e:
        st.session_state.log_messages.append(f"Failed to download {url}: {e}")


async def extract_links(soup, base_url, headers):
    links = set()

    for a_tag in soup.find_all('a', href=True):
        if st.session_state.stop_scraping:
            break
        href = a_tag['href']
        if href.startswith('javascript:'):
            continue  # Skip JavaScript links
        link = urljoin(base_url, href)
        links.add(link)

    for div_tag in soup.find_all('div', onclick=True):
        if st.session_state.stop_scraping:
            break
        onclick_attr = div_tag['onclick']
        match = re.search(r'SaveToDisk\("([^"]+)"', onclick_attr)
        if match:
            pdf_link = match.group(1)
            links.add(pdf_link)

    for table in soup.find_all('table'):
        if st.session_state.stop_scraping:
            break
        for a_tag in table.find_all('a', href=True):
            if st.session_state.stop_scraping:
                break
            href = a_tag['href']
            if href.startswith('javascript:'):
                continue  # Skip JavaScript links
            link = urljoin(base_url, href)
            links.add(link)

    for list_tag in soup.find_all(['ul', 'ol']):
        if st.session_state.stop_scraping:
            break
        for a_tag in list_tag.find_all('a', href=True):
            if st.session_state.stop_scraping:
                break
            href = a_tag['href']
            if href.startswith('javascript:'):
                continue  # Skip JavaScript links
            link = urljoin(base_url, href)
            links.add(link)

    for iframe in soup.find_all('iframe'):
        if st.session_state.stop_scraping:
            break
        src = iframe.get('src')
        if src:
            iframe_src = urljoin(base_url, src)
            try:
                response = requests.get(iframe_src, headers=headers, allow_redirects=True)
                if response.status_code == 200:
                    links.add(iframe_src)
            except Exception as e:
                st.session_state.log_messages.append(f"Failed to resolve iframe URL {iframe_src}: {e}")

    return links


def remove_unwanted_elements(soup):
    selectors_to_remove = ['nav', 'header', 'footer']
    for selector in selectors_to_remove:
        for element in soup.select(selector):
            element.decompose()
    return soup


def is_document_url(url):
    return any(url.lower().endswith(ext) for ext in file_types)


async def scrape_and_download(session, url, base_download_folder, visited_urls, base_domain, headers, level=1,
                              max_depth=3):
    if st.session_state.stop_scraping:
        return

    if level > max_depth:
        return

    st.session_state.log_messages.append(f"Processing URL: {url} at level {level}")
    try:
        if url in visited_urls:
            st.session_state.log_messages.append(f"Skipping already visited URL: {url}")
            return

        visited_urls.add(url)

        parsed_url = urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

        level_folder = os.path.join(base_download_folder, f"level_{level}")
        os.makedirs(level_folder, exist_ok=True)

        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), 'html.parser')
            cleaned_soup = remove_unwanted_elements(soup)
            links = await extract_links(cleaned_soup, base_url, headers)

        for link in links:
            if st.session_state.stop_scraping:
                st.session_state.log_messages.append("Stopping the scraping process.")
                break
            if is_document_url(link):
                await download_file(session, link, level_folder, headers)
            else:
                # Synchronously download if async fails
                download_file_sync(link, level_folder, headers)
            if st.session_state.stop_scraping:
                break

        for link in links:
            if st.session_state.stop_scraping:
                st.session_state.log_messages.append("Stopping the scraping process.")
                break
            link_domain = urlparse(link).netloc
            if not is_document_url(link) and link_domain == base_domain:
                await scrape_and_download(session, link, base_download_folder, visited_urls, base_domain, headers,
                                          level + 1, max_depth)
            if st.session_state.stop_scraping:
                break

    except asyncio.CancelledError:
        st.session_state.log_messages.append(f"Cancelled scraping for {url}")
    except Exception as e:
        st.session_state.log_messages.append(f"Error fetching the URL: {e}")


async def zip_folder(folder_path):
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path, folder_path))
    buffer.seek(0)
    return buffer


async def main_scraping(urls, headers, max_depth):
    visited_urls = set()
    base_download_folder = "downloads"

    # Create/clear download folder
    if os.path.exists(base_download_folder):
        shutil.rmtree(base_download_folder)
    os.makedirs(base_download_folder)

    async with aiohttp.ClientSession() as session:
        tasks = [
            scrape_and_download(session, url, os.path.join(base_download_folder, urlparse(url).netloc), visited_urls,
                                urlparse(url).netloc, headers, max_depth=max_depth) for url in urls]
        await asyncio.gather(*tasks, return_exceptions=True)

    st.session_state.log_messages.append("Scraping completed successfully!")
    st.session_state.download_ready = True


async def update_logs():
    while not st.session_state.stop_scraping and not st.session_state.download_ready:
        if st.session_state.log_messages:
            st.text("\n".join(st.session_state.log_messages))
        await asyncio.sleep(1)


async def run_app():
    st.title("Web Scraper")

    # Display the file types that can be downloaded
    st.subheader("This scraper can download the following file types:")
    st.write(", ".join([".pdf", ".docx", ".doc", ".xlsx", ".xls", ".csv"]))

    urls = st.text_area("Enter URLs (comma-separated)").split(',')
    urls = [url.strip() for url in urls if url.strip()]

    if not urls:
        st.warning("Please enter at least one URL.")
        return

    max_depth = st.selectbox("Select Scraping Depth", ["Level 1", "Level 2", "Level 3", "Level 4", "Level 5", "Max"],
                             index=5)

    depth_mapping = {
        "Level 1": 1,
        "Level 2": 2,
        "Level 3": 3,
        "Level 4": 4,
        "Level 5": 5,
        "Max": float('inf')
    }
    max_depth = depth_mapping[max_depth]

    col1, col2, col3 = st.columns([2, 1, 2])

    with col1:
        start_button = st.button("Start Scraping")

    with col2:
        stop_button = st.button("Stop Scraping")

    with col3:
        download_button = st.empty()

    log_container = st.empty()

    if start_button:
        st.session_state.stop_scraping = False
        st.session_state.log_messages = []
        st.session_state.download_ready = False

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }

        st.write("Scraping started...")

        await asyncio.gather(
            main_scraping(urls, headers, max_depth),
            update_logs()
        )

    if stop_button:
        st.session_state.stop_scraping = True
        st.session_state.download_ready = True
        st.warning("Scraping has been stopped.")

    if st.session_state.download_ready:
        if os.path.exists("downloads"):
            zip_buffer = await zip_folder("downloads")
            download_button.download_button(
                label="Download Scraped Files",
                data=zip_buffer,
                file_name="scraped_files.zip",
                mime="application/zip"
            )


if __name__ == "__main__":
    asyncio.run(run_app())
