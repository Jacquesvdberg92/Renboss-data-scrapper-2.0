import os
import asyncio
from tkinter import filedialog
from tkinter.ttk import Progressbar
from openpyxl import Workbook
from bs4 import BeautifulSoup
import requests
import re
from tqdm import tqdm
import time
import aiohttp

# Define the scraping speed (number of URLs to scrape concurrently)
scraping_speed = 15

# Ask the user for the file paths and names to save the Excel files
excel_output_file_path_tools = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], title="Save Tools Data Excel File", initialfile="tools_data.xlsx")
excel_output_file_path_prices = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")], title="Save Prices Excel File", initialfile="prices_data.xlsx")

# Create a new Excel workbook and get the active worksheet
toolsworkbook = Workbook()
toolsworksheet = toolsworkbook.active
pricesworkbook = Workbook()
pricesworksheet = pricesworkbook.active

# Define the cookie data as a dictionary
cookies = {
    'UserCountry_15': '276',
}

# Progress Bar
total_rows = 0
completed_rows = 0
progress_bar = tqdm(total=total_rows, desc="Processing URLs", unit="URL")

scraped_data = []
re_try_list = []


async def create_lists():
    csv_file_path = os.path.join(os.path.dirname(__file__), 'product_links.csv')
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError("CSV file does not exist.")
    
    with open(csv_file_path, 'r') as file:
        data = file.readlines()
        num_lists = scraping_speed
        total_rows = len(data)
        list_length = len(data)
        remainder = list_length % num_lists
        
        # Calculate the minimum number of URLs per list
        min_urls_per_list = list_length // num_lists
        
        # Initialize lists
        lists = [[] for _ in range(num_lists)]
        
        # Initialize a set to store unique URLs
        unique_urls = set()
        
        # Distribute URLs evenly among the lists
        for idx, row in enumerate(data):
            columns = row.strip().split(',')  # Assuming CSV file is comma-separated
            url = columns[1].strip()  # Extracting URL from the first column
            if url and url not in unique_urls:  # Check if URL is not empty and not a duplicate
                unique_urls.add(url)  # Add URL to the set of unique URLs
                list_idx = idx % num_lists
                lists[list_idx].append(url)
        
        for i, lst in enumerate(lists):
            print(f"The current list {i} has a total of: {len(lst)} URLs" + "\n")
        
        return lists
    
async def write_to_excel(sku, sDesc, weight, price):
    try:
        toolsdata_row = [
                    sku, sDesc, "", "SST","", "", "", "Y",
                    "Y", "Y", "", "",
                    "", "", "V0003", "",
                    "", "", "", "",
                    "", "", "", "",
                    "", "", "", "",
                    "No", "", "", "",
                    "", "", "", "",
                    "", "", "", "",
                    "", "", "",
                    "", "", "", "",
                    "", "", "", weight,
                    "Kg", "", "",
                    "", "", "",
                    "", "", "",
                    "", "", "",
                    "", weight, "Kg",
                    "", "", "", "",
                    "", "", "", "",
                    "", "", "",
                    "", "", "", "",
                    "", "", "", "",
                    "", "", "",
                    "", "", "",
                    "", "", "",
                    "", "", "",
                    "", "", "", "",
                    "", "", "",
                    "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "", "",
                    "", "", "", "",
                    "", "", "", "",
                    "", "", "",
                    "", "", "",
                    "", "", ""
                ]
        toolsworksheet.append(toolsdata_row)

        pricesdata_row = [
                    sku,"1",
                    price,"EUR","",
                    "","","",
                ]
        pricesworksheet.append(pricesdata_row)
        
        # Save the workbook to the specified Excel file paths
        toolsworkbook.save(excel_output_file_path_tools)
        pricesworkbook.save(excel_output_file_path_prices)
        
    except AttributeError as e:
         # Log the error and the URL that caused it in the error file
         print(f"Error writing to Excel" + "\n\n")
         

def write_errors_to_file(urls):
    csv_file_path = os.path.join(os.path.dirname(__file__), 'product_links.csv')
    error_file_path = "errors.txt"

    # Read the original data
    with open(csv_file_path, 'r') as file:
        data = file.readlines()

    # Remove the URLs with errors from the original data
    data = [line for line in data if line.strip().split(',')[1] not in urls]

    # Write the cleaned data back to the original file
    with open(csv_file_path, 'w') as file:
        file.writelines(data)

    # Write the URLs with errors to the error file
    with open(error_file_path, 'a') as file:
        for url in urls:
            file.write(url + "\n")     
    


async def scraper(url_list, sem):
    async with aiohttp.ClientSession() as session:
        async with sem:
            async with aiohttp.ClientSession(cookies=cookies) as session:
                for i, url in enumerate(url_list):
                    try:
                        #print(f"Scraping {url}")

                        # Preload the next URL
                        next_url_index = (i + 1) % len(url_list)
                        next_url = url_list[next_url_index]
                        next_task = asyncio.create_task(session.get(next_url))

                        async with session.get(url) as response:
                            if response.status != 200:
                                print(f"Failed to fetch {url}: {response.status}")
                                continue
                            
                            html = await response.text()
                            soup = BeautifulSoup(html, 'lxml')
                            ## Find SKU, sDesc, Price, Weight, Description, Image, and insert them into the 'scrapedTools' table
                            sku = soup.find(class_='itemName').get_text()
                            sDesc = soup.find(id='PlaceHolderMain_SrsItemDetailControl_lblItemName').get_text()
                            # Check if the Price element is found before getting its text
                            price_element = soup.find(id='PlaceHolderMain_SrsItemDetailControl_lblPrice')
                            price_text = price_element.get_text() if price_element else "N/A"
                            # Extract only the numeric portion from the price_text using regular expressions
                            price_numeric = re.search(r'[\d.,]+', re.sub(r'\s', '', price_text)).group().replace(',', '.')
                            # Convert the extracted numeric string to a floating-point number
                            price = price_numeric
                            # Check if the Weight element is found before getting its text
                            weight_element = soup.find(id='lblWeight')
                            if weight_element:
                                weight_text = weight_element.get_text()
                                weight_match = re.search(r'\d+(\.\d+)?', re.sub(r'\s', '', weight_text))
                                if weight_match:
                                    weight = float(weight_match.group())
                                else:
                                    weight = "N/A"
                            else:
                                weight = "N/A"
                            # Check if the Description element is found before getting its text
                            lDesc_element = soup.find(id='lblItemDescription')
                            lDesc = lDesc_element.get_text() if lDesc_element else "N/A"
                            imgSrc = soup.find(id='PlaceHolderMain_SrsItemDetailControl_rptImagesZoom_NewImgZoomhref_0')

                            # Write the scraped data to the Excel files
                            #await write_to_excel(sku, sDesc, weight, price)

                            # Append the scraped data to the 'scraped_data' list
                            scraped_data.append([sku, sDesc, price, weight, lDesc, imgSrc])

                        # Await the next URL to finish loading
                        await next_task

                    except Exception as e:
                        # Log the error and the URL that caused it in the error file
                        print(f"Error scraping {url}: {e} \n")
                        print(f"Adding {url} to the retry list" + "\n\n")
                        
                        # Append the URL to the retry list
                        re_try_list.append(url)
                        continue
                    # Update the progress bar
                    progress_bar.update(1)
    
    return completed_rows



async def main():
    start_time = time.time()
    lists = await create_lists()
    sem = asyncio.Semaphore(scraping_speed)  # Adjust the semaphore limit as needed

    tasks = []
    total_urls = sum(len(lst) for lst in lists)
    progress_bar.total = total_urls  # Update total URLs in the progress bar
    for url_list in lists:
        tasks.append(scraper(url_list, sem))
    
    await asyncio.gather(*tasks)
    write_errors_to_file(re_try_list)
    
    
    
    # Write the scraped data to the Excel files
    print(f"Writing scraped data to Excel files" + "\n\n")
    for data in scraped_data:
        await write_to_excel(data[0], data[1], data[3], data[2])

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")

if __name__ == "__main__":
    asyncio.run(main())