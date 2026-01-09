from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

from selenium.webdriver.chrome.service import Service

# Chrome for Testing paths
CHROMEDRIVER_PATH = "/Users/cybercs/chrome-for-testing/chromedriver-mac-arm64/chromedriver"
CHROME_BINARY_PATH = "/Users/cybercs/chrome-for-testing/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing"

producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
class WebCrawler:
    def __init__(self, url):
        self.url = url
        self.driver = None
        
    def initialize_driver(self):
        try:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            #chrome_options.add_argument("--headless=new")  # New headless Chrome
            
            # Use Chrome for Testing binary
            chrome_options.binary_location = CHROME_BINARY_PATH
            
            # Use ChromeDriver for Testing
            service = Service(CHROMEDRIVER_PATH)
            
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            return True
        except Exception as e:
            print(f"Failed to initialize driver: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def start_crawling(self):
        if not self.initialize_driver():
            return None
            
        try:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.driver.get(self.url)
                    break
                except WebDriverException as e:
                    if attempt == max_retries - 1:
                        raise e
                    time.sleep(2)

            today = datetime.now()
            all_data = {}
            first_day_flights = None  # Lưu dữ liệu flights của ngày đầu tiên có dữ liệu
            first_day_date = None  # Lưu ngày đầu tiên có dữ liệu
            
            for i in range(7):  
                current_date = today + timedelta(days=i)
                date_str = current_date.strftime('%Y-%m-%d')
                print(f"\nProcessing date: {date_str}")

                date_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.NAME, "flight_date"))
                )
                self.driver.execute_script(f"arguments[0].value = '{date_str}';", date_input)
                time.sleep(1)  
                
                search_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.btn-filter[type="submit"]'))
                )
                search_button.click()
                time.sleep(10)
                
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.TAG_NAME, 'tbody'))
                    )
                    
                    data = self.get_crawl_data()
                    
                    if data and data.get('data'):
                        valid_flights = 0
                        flights_to_send = []  # Lưu danh sách flights hợp lệ
                        
                        for idx, flight in enumerate(data['data']):
                            # Skip rows that don't have enough columns or contain "no flights" message
                            if len(flight) < 9:
                                if len(flight) == 1 and ('Không có lịch bay' in flight[0] or 'không có' in flight[0].lower()):
                                    print(f"No flights available for {date_str}")
                                continue
                            
                            # Validate that required fields are not empty
                            if not flight[0] or not flight[4]:  # scheduled_time and flight_id are required
                                continue
                            
                            try:
                                information = {
                                    'date':date_str,
                                    'scheduled_time':flight[0],
                                    'updated_time':flight[1] if len(flight) > 1 else '',
                                    'route':flight[2] if len(flight) > 2 else '',
                                    'flight_id':flight[4] if len(flight) > 4 else '',
                                    'counter':flight[5] if len(flight) > 5 else '',
                                    'gate':flight[6] if len(flight) > 6 else '',
                                    'status':flight[8] if len(flight) > 8 else ''
                                }
                                producer.send('flights',value=information)
                                flights_to_send.append(information)
                                valid_flights += 1
                            except (IndexError, KeyError) as e:
                                print(f"Error processing flight: {str(e)}")
                                continue
                        
                        if valid_flights > 0:
                            # Lưu dữ liệu của ngày đầu tiên có dữ liệu
                            if first_day_flights is None:
                                first_day_flights = flights_to_send
                                first_day_date = date_str
                                print(f"Successfully collected {valid_flights} flights for {date_str} (saved as reference)")
                            else:
                                print(f"Successfully collected {valid_flights} flights for {date_str}")
                            all_data[date_str] = data
                        else:
                            # Không có flights hợp lệ, sử dụng lại dữ liệu ngày đầu tiên
                            if first_day_flights is not None:
                                print(f"No valid flights for {date_str}, reusing data from {first_day_date}")
                                for flight_info in first_day_flights:
                                    # Cập nhật date cho ngày hiện tại
                                    flight_info_copy = flight_info.copy()
                                    flight_info_copy['date'] = date_str
                                    producer.send('flights', value=flight_info_copy)
                                print(f"Reused {len(first_day_flights)} flights from {first_day_date} for {date_str}")
                            else:
                                print(f"No flight data for {date_str} (no reference data available yet)")
                    else:
                        # Không có data, sử dụng lại dữ liệu ngày đầu tiên
                        if first_day_flights is not None:
                            print(f"No flight data for {date_str}, reusing data from {first_day_date}")
                            for flight_info in first_day_flights:
                                # Cập nhật date cho ngày hiện tại
                                flight_info_copy = flight_info.copy()
                                flight_info_copy['date'] = date_str
                                producer.send('flights', value=flight_info_copy)
                            print(f"Reused {len(first_day_flights)} flights from {first_day_date} for {date_str}")
                        else:
                            print(f"No flight data for {date_str} (no reference data available yet)")
                    
                except TimeoutException:
                    # Timeout, sử dụng lại dữ liệu ngày đầu tiên
                    if first_day_flights is not None:
                        print(f"No data found for date {date_str}, reusing data from {first_day_date}")
                        for flight_info in first_day_flights:
                            # Cập nhật date cho ngày hiện tại
                            flight_info_copy = flight_info.copy()
                            flight_info_copy['date'] = date_str
                            producer.send('flights', value=flight_info_copy)
                        print(f"Reused {len(first_day_flights)} flights from {first_day_date} for {date_str}")
                    else:
                        print(f"No data found for date {date_str} (no reference data available yet)")
                    continue

                time.sleep(2)  

            return all_data
            
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            self.cleanup()
    
    def get_crawl_data(self):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table.table-striped'))
                )
                
                headers = []
                header_elements = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'table.table.table-striped thead tr th'))
                )
                for header in header_elements:
                    try:
                        header_text = WebDriverWait(self.driver, 5).until(
                            EC.visibility_of(header)
                        ).text.strip()
                        headers.append(header_text)
                    except:
                        continue
                
                data_rows = []
                row_elements = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'table.table.table-striped tbody tr'))
                )
                
                for row in row_elements:
                    try:
                        cells = WebDriverWait(row, 5).until(
                            EC.presence_of_all_elements_located((By.TAG_NAME, 'td'))
                        )
                        
                        row_data = []
                        for cell in cells:
                            try:
                                cell_text = WebDriverWait(self.driver, 5).until(
                                    EC.visibility_of(cell)
                                ).text.strip()
                                row_data.append(cell_text)
                            except:
                                row_data.append("")  
                        
                        if row_data and any(row_data):  
                            data_rows.append(row_data)
                            
                    except Exception as row_error:
                        print(f"Error processing row: {str(row_error)}")
                        continue
                
                if headers and data_rows:  
                    return {
                        'headers': headers,
                        'data': data_rows
                    }
                else:
                    raise Exception("No data found in table")
                    
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(2)  
        return None
    
    def cleanup(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            print(f"Error during cleanup: {str(e)}")

url = "https://acv.vn/thong-tin-lich-bay"
crawler = WebCrawler(url)
result = crawler.start_crawling()


    
