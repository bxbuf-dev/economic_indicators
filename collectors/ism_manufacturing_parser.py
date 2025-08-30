# collectors/ism_manufacturing_parser.py
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import calendar

def determine_expected_report_month():
    """
    Determine which month report we expect to be available
    ISM Manufacturing PMI is usually published in first few days of next month
    """
    today = datetime.now()
    
    # If we're in first 5 days of month, previous month report might not be out yet
    if today.day <= 5:
        # Check previous month
        if today.month == 1:
            expected_month = 12
            expected_year = today.year - 1
        else:
            expected_month = today.month - 1
            expected_year = today.year
    else:
        # After 5th, definitely expect previous month report
        if today.month == 1:
            expected_month = 12
            expected_year = today.year - 1
        else:
            expected_month = today.month - 1
            expected_year = today.year
    
    month_name = calendar.month_name[expected_month].lower()
    return expected_year, expected_month, month_name

def build_ism_url(month_name):
    """
    Build ISM report URL for given month
    """
    base_url = "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/pmi"
    return f"{base_url}/{month_name}/"

def get_ism_manufacturing_data():
    """
    Parse ISM Manufacturing PMI data from official ISM website
    Returns: dict with current data or None if no new data
    """
    
    # Determine expected report
    expected_year, expected_month, month_name = determine_expected_report_month()
    expected_date = f"{expected_year}-{expected_month:02d}-01"
    
    print(f"Looking for ISM Manufacturing PMI report for: {calendar.month_name[expected_month]} {expected_year}")
    print(f"Expected date format: {expected_date}")
    
    # Try multiple month variations in case our logic is off
    month_variations = [month_name]
    
    # Add current month if we're early in the month
    if datetime.now().day <= 10:
        current_month_name = calendar.month_name[datetime.now().month].lower()
        if current_month_name not in month_variations:
            month_variations.append(current_month_name)
    
    # Try previous month as backup
    if expected_month == 1:
        prev_month_name = calendar.month_name[12].lower()
    else:
        prev_month_name = calendar.month_name[expected_month - 1].lower()
    
    if prev_month_name not in month_variations:
        month_variations.append(prev_month_name)
    
    print(f"Will try these month URLs: {month_variations}")
    
    for month_attempt in month_variations:
        url = build_ism_url(month_attempt)
        print(f"Trying URL: {url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 404:
                print(f"  → 404 Not Found for {month_attempt}")
                continue
                
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Get all text content
            all_text = soup.get_text()
            
            print(f"  → Successfully loaded page for {month_attempt}")
            
            # Extract the report date from page content
            # Look for patterns like "JULY 2025", "July 2025 Manufacturing ISM Report On Business"
            date_patterns = [
                r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s+(\d{4})',
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
                r'Manufacturing\s+ISM\s+Report.*?(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})'
            ]
            
            report_month = None
            report_year = None
            
            for pattern in date_patterns:
                match = re.search(pattern, all_text)
                if match:
                    if len(match.groups()) == 2:
                        report_month_name = match.group(1).capitalize()
                        report_year = int(match.group(2))
                    else:
                        report_month_name = match.group(3).capitalize()
                        report_year = int(match.group(4))
                    
                    month_map = {
                        'January': 1, 'February': 2, 'March': 3, 'April': 4,
                        'May': 5, 'June': 6, 'July': 7, 'August': 8,
                        'September': 9, 'October': 10, 'November': 11, 'December': 12
                    }
                    report_month = month_map.get(report_month_name)
                    break
            
            if not report_month or not report_year:
                print(f"  → Could not extract report date from {month_attempt}")
                continue
                
            report_date = f"{report_year}-{report_month:02d}-01"
            print(f"  → Found report for: {calendar.month_name[report_month]} {report_year} ({report_date})")
            
            # Extract PMI data
            extracted_data = {}
            
            # Define patterns for different PMI components
            data_patterns = {
                'headline': [
                    r'Manufacturing\s+PMI.*?(\d+\.?\d*)%?',
                    r'PMI.*?registered\s+(\d+\.?\d*)%?',
                    r'PMI.*?(\d+\.?\d*)%?\s+percent',
                    r'PMI\s*[:\-]?\s*(\d+\.?\d*)'
                ],
                'new_orders': [
                    r'New\s+Orders.*?(\d+\.?\d*)%?',
                    r'New\s+Orders\s+Index.*?(\d+\.?\d*)%?'
                ],
                'production': [
                    r'Production.*?(\d+\.?\d*)%?',
                    r'Production\s+Index.*?(\d+\.?\d*)%?'
                ],
                'employment': [
                    r'Employment.*?(\d+\.?\d*)%?',
                    r'Employment\s+Index.*?(\d+\.?\d*)%?'
                ],
                'supplier_deliveries': [
                    r'Supplier\s+Deliveries.*?(\d+\.?\d*)%?',
                    r'Deliveries.*?(\d+\.?\d*)%?'
                ],
                'inventories': [
                    r'Inventories.*?(\d+\.?\d*)%?',
                    r'Inventories\s+Index.*?(\d+\.?\d*)%?'
                ],
                'customers_inventories': [
                    r'Customers.*?Inventories.*?(\d+\.?\d*)%?',
                    r'Customer.*?Inventories.*?(\d+\.?\d*)%?'
                ],
                'prices_paid': [
                    r'Prices.*?(\d+\.?\d*)%?',
                    r'Prices\s+Paid.*?(\d+\.?\d*)%?'
                ],
                'order_backlog': [
                    r'Backlog\s+of\s+Orders.*?(\d+\.\d+)',
                    r'Order\s+Backlog.*?(\d+\.\d+)',
                    r'Backlog.*?(\d+\.\d+)'
                ],
                'exports': [
                    r'Exports.*?(\d+\.?\d*)%?',
                    r'New\s+Export\s+Orders.*?(\d+\.?\d*)%?'
                ],
                'imports': [
                    r'Imports.*?(\d+\.?\d*)%?'
                ]
            }
            
            # Try to extract each data point
            for category, patterns in data_patterns.items():
                for pattern in patterns:
                    match = re.search(pattern, all_text, re.IGNORECASE)
                    if match:
                        try:
                            value = float(match.group(1))
                            # Sanity check - PMI values should be between 0 and 100
                            if 0 <= value <= 100:
                                extracted_data[category] = value
                                print(f"  → Found {category}: {value}")
                                break
                        except (ValueError, IndexError):
                            continue
            
            if not extracted_data:
                print(f"  → No PMI data extracted from {month_attempt}")
                continue
            
            # If we found data, return it
            print(f"✅ Successfully extracted PMI data for {report_date}")
            print(f"   Found {len(extracted_data)} data points: {list(extracted_data.keys())}")
            
            return {
                'date': report_date,
                'values': extracted_data,
                'source_url': url
            }
            
        except requests.RequestException as e:
            print(f"  → Network error for {month_attempt}: {e}")
            continue
        except Exception as e:
            print(f"  → Parsing error for {month_attempt}: {e}")
            continue
    
    print("❌ Could not find or parse ISM Manufacturing PMI data from any URL")
    return None

def check_if_data_exists_in_db(dao, indicator_id, expected_date):
    """
    Check if data for expected_date already exists in database
    """
    try:
        latest_date = dao.get_latest_indicator_date(indicator_id)
        if not latest_date:
            print("No existing data in database")
            return False
        
        print(f"Latest data in DB: {latest_date}")
        print(f"Expected data date: {expected_date}")
        
        # Compare year-month only
        latest_year_month = latest_date[:7]  # "2025-07"
        expected_year_month = expected_date[:7]  # "2025-08"
        
        if latest_year_month >= expected_year_month:
            print(f"✅ Data for {expected_year_month} already exists (latest: {latest_year_month})")
            return True
        else:
            print(f"📥 Need to fetch data for {expected_year_month} (latest: {latest_year_month})")
            return False
            
    except Exception as e:
        print(f"Error checking database: {e}")
        return False