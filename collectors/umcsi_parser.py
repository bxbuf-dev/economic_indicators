# collectors/umcsi_parser_improved.py
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import pandas as pd

def get_umcsi_data():
    """
    Parse UMCSI data from official University of Michigan website
    Returns: dict with current data and text releases
    """
    url = "https://www.sca.isr.umich.edu/"
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Get all text content from the entire page
        all_text = soup.get_text()
        
        print(f"Parsing UMCSI data from {url}")
        
        # Extract date from the page title/header
        title_match = re.search(r'Final Results for (January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', all_text)
        
        if title_match:
            month_name = title_match.group(1)
            year = int(title_match.group(2))
        else:
            # Fallback: look for any month/year pattern
            date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', all_text)
            if date_match:
                month_name = date_match.group(1)
                year = int(date_match.group(2))
            else:
                # Last resort: use current date
                month_name = datetime.now().strftime("%B")
                year = datetime.now().year
        
        # Convert month name to number
        month_map = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        month = month_map.get(month_name, datetime.now().month)
        
        # Use day 15 as standard for monthly data
        release_date = f"{year}-{month:02d}-15"
        
        print(f"Detected data for: {month_name} {year} -> {release_date}")
        
        # Extract numerical data - look for the data table
        extracted_data = {}
        
        # Find all table cells and extract Index values
        table_cells = soup.find_all(['td', 'th'])
        
        # Look for specific patterns in table structure
        for i, cell in enumerate(table_cells):
            cell_text = cell.get_text().strip()
            
            # Look for the Index labels and their corresponding values
            if 'Index of Consumer Sentiment' in cell_text:
                # Look for value in next few cells
                for j in range(1, 4):
                    if i + j < len(table_cells):
                        next_cell = table_cells[i + j].get_text().strip()
                        if re.match(r'^\d+\.?\d*$', next_cell):
                            extracted_data['composite'] = float(next_cell)
                            print(f"Found composite: {next_cell}")
                            break
            
            elif 'Current Economic Conditions' in cell_text:
                # Look for value in next few cells
                for j in range(1, 4):
                    if i + j < len(table_cells):
                        next_cell = table_cells[i + j].get_text().strip()
                        if re.match(r'^\d+\.?\d*$', next_cell):
                            extracted_data['current'] = float(next_cell)
                            print(f"Found current: {next_cell}")
                            break
            
            elif 'Index of Consumer Expectations' in cell_text:
                # Look for value in next few cells
                for j in range(1, 4):
                    if i + j < len(table_cells):
                        next_cell = table_cells[i + j].get_text().strip()
                        if re.match(r'^\d+\.?\d*$', next_cell):
                            extracted_data['expectations'] = float(next_cell)
                            print(f"Found expectations: {next_cell}")
                            break
        
        # Fallback: use regex patterns on full text if table parsing failed
        if not extracted_data:
            print("Table parsing failed, using regex patterns...")
            table_patterns = {
                'composite': r'Index of Consumer Sentiment[|\s]*(\d+\.?\d*)',
                'current': r'Current Economic Conditions[|\s]*(\d+\.?\d*)', 
                'expectations': r'Index of Consumer Expectations[|\s]*(\d+\.?\d*)'
            }
            
            for category, pattern in table_patterns.items():
                match = re.search(pattern, all_text, re.IGNORECASE)
                if match:
                    extracted_data[category] = float(match.group(1))
                    print(f"Found {category} (regex): {match.group(1)}")
        
        # Extract commentary text after director statement
        # Look for the director's commentary section
        director_marker = "Surveys of Consumers Director Joanne Hsu"
        
        # Find all div elements that might contain commentary
        all_divs = soup.find_all('div')
        
        commentary_blocks = []
        found_director = False
        
        for div in all_divs:
            div_text = div.get_text().strip()
            
            # Skip if this is just the table data
            if len(div_text) < 200:
                continue
                
            # Skip navigation and structural elements
            if any(skip_word in div_text.lower() for skip_word in [
                'final results for', 'aug', 'jul', 'm-m', 'y-y',
                'home', 'tables', 'charts', 'reports', 'contact',
                'read our', 'next data release', 'fetchdoc'
            ]):
                continue
            
            # Look for director marker
            if director_marker in div_text:
                found_director = True
                # Extract the text after director name
                director_pos = div_text.find(director_marker)
                commentary_start = director_pos + len(director_marker)
                commentary = div_text[commentary_start:].strip()
                
                if len(commentary) > 100:  # Substantial commentary
                    commentary_blocks.append(commentary)
                    print(f"Found director commentary: {len(commentary)} chars")
                    print(f"Preview: {commentary[:150]}...")
                    break
        
        # If we didn't find commentary in the director div, look for separate commentary paragraphs
        if not commentary_blocks:
            print("No commentary found in director section, looking for separate paragraphs...")
            
            # Look for substantial text blocks that contain economic commentary
            for div in all_divs:
                div_text = div.get_text().strip()
                
                if len(div_text) < 300:  # Need substantial content
                    continue
                    
                # Skip structural elements
                if any(skip_word in div_text.lower() for skip_word in [
                    'final results', 'home', 'tables', 'charts', 'director joanne hsu'
                ]):
                    continue
                
                # Look for economic commentary keywords
                if any(keyword in div_text.lower() for keyword in [
                    'consumer sentiment', 'sentiment confirmed', 'inflation expectations',
                    'year-ahead inflation', 'long-run inflation', 'economic conditions'
                ]):
                    commentary_blocks.append(div_text)
                    print(f"Found commentary paragraph: {len(div_text)} chars")
                    print(f"Preview: {div_text[:150]}...")
        
        # Split commentary into expectations and inflation sections
        expectations_text = ""
        inflation_text = ""
        
        if commentary_blocks:
            full_commentary = " ".join(commentary_blocks)
            
            # Split by sentences and categorize
            sentences = full_commentary.split('.')
            
            expectations_sentences = []
            inflation_sentences = []
            current_section = "expectations"  # Default to expectations
            
            for sentence in sentences:
                sentence = sentence.strip()
                if len(sentence) < 20:
                    continue
                
                # Switch to inflation section when we see inflation keywords
                if any(keyword in sentence.lower() for keyword in [
                    'year-ahead inflation', 'inflation expectations', 'long-run inflation'
                ]):
                    current_section = "inflation"
                
                # Add to appropriate section
                if current_section == "inflation":
                    inflation_sentences.append(sentence)
                else:
                    expectations_sentences.append(sentence)
            
            # Join sentences back together
            if expectations_sentences:
                expectations_text = '. '.join(expectations_sentences).strip()
                if not expectations_text.endswith('.'):
                    expectations_text += '.'
            
            if inflation_sentences:
                inflation_text = '. '.join(inflation_sentences).strip() 
                if not inflation_text.endswith('.'):
                    inflation_text += '.'
            
            # If we couldn't split properly, use first half as expectations, second as inflation
            if not expectations_text and not inflation_text and commentary_blocks:
                full_text = commentary_blocks[0]
                mid_point = len(full_text) // 2
                
                # Find a good sentence break near the middle
                for i in range(mid_point - 100, mid_point + 100):
                    if i < len(full_text) and full_text[i:i+2] == '. ':
                        expectations_text = full_text[:i+1].strip()
                        inflation_text = full_text[i+2:].strip()
                        break
                
                if not expectations_text:
                    expectations_text = full_text
        
        print(f"\nFinal commentary extraction:")
        print(f"Expectations text: {len(expectations_text)} chars")
        if expectations_text:
            print(f"Preview: {expectations_text[:200]}...")
        print(f"Inflation text: {len(inflation_text)} chars")
        if inflation_text:
            print(f"Preview: {inflation_text[:200]}...")
        
        return {
            'date': release_date,
            'values': extracted_data,
            'text_releases': {
                'expectations': expectations_text,
                'inflation': inflation_text
            }
        }
        
    except requests.RequestException as e:
        print(f"Error fetching UMCSI data: {e}")
        return None
    except Exception as e:
        print(f"Error parsing UMCSI data: {e}")
        return None