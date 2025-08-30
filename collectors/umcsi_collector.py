# collectors/umcsi_collector.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dao_No_Debug import IndicatorDAO
from collectors.umcsi_parser import get_umcsi_data
import json

def collect_umcsi():
    """
    Collect current UMCSI data from official website
    """
    print("Starting UMCSI data collection...")
    
    dao = IndicatorDAO()
    
    try:
        # First, rename the old indicator if it exists
        rename_old_indicator(dao)
        
        # Get or create the US UMCSI indicator
        indicator_id = dao.add_indicator(
            name='us_umcsi',
            full_name='University of Michigan Consumer Sentiment Index',
            source='University of Michigan Surveys of Consumers',
            description='US Consumer Sentiment Index including composite, current conditions, and expectations components'
        )
        
        # Fetch current data
        umcsi_data = get_umcsi_data()
        
        if not umcsi_data:
            print("Failed to fetch UMCSI data")
            return
        
        date = umcsi_data['date']
        values = umcsi_data['values']
        text_releases = umcsi_data['text_releases']
        
        print(f"Processing UMCSI data for {date}")
        
        # Determine if this is preliminary or final data
        is_preliminary = any('preliminary' in text.lower() for text in text_releases.values())
        suffix = '_p' if is_preliminary else ''
        
        # Add numerical values to database
        records_added = 0
        for category, value in values.items():
            category_name = f"{category}{suffix}"
            
            # Check if this exact record already exists
            existing_records = dao.get_indicator_values_by_category(indicator_id, category_name)
            if not existing_records.empty and date in existing_records.index.strftime('%Y-%m-%d'):
                print(f"Data already exists for {category_name} on {date}, skipping...")
                continue
            
            dao.add_indicator_value(indicator_id, date, value, category_name)
            print(f"Added {category_name}: {value}")
            records_added += 1
        
        # Add text releases to database with duplicate checking
        print(f"\nProcessing text releases:")
        print(f"Expectations text length: {len(text_releases.get('expectations', ''))}")
        print(f"Inflation text length: {len(text_releases.get('inflation', ''))}")
        
        releases_added = 0
        for text_type, text_content in text_releases.items():
            print(f"\nProcessing {text_type}:")
            print(f"Content preview: {text_content[:100]}...")
            
            if text_content and text_content.strip():  # Only add non-empty text
                release_data = {
                    'type': text_type,
                    'content': text_content,
                    'is_preliminary': is_preliminary
                }
                
                print(f"Adding {text_type} release to database...")
                
                try:
                    success = dao.add_indicator_release(
                        indicator_id=indicator_id,
                        date=date,
                        release_data=release_data,
                        source_url="https://www.sca.isr.umich.edu/",
                        category=text_type
                    )
                    
                    if success:
                        print(f"✓ Added {text_type} text release")
                        releases_added += 1
                    else:
                        print(f"⚠ {text_type} release already existed")
                        
                except Exception as e:
                    print(f"✗ Error adding {text_type} release: {e}")
            else:
                print(f"✗ Skipping {text_type} - no content")
        
        print(f"\nUMCSI collection complete. Added {records_added} value records and {releases_added} text releases.")
        
    except Exception as e:
        print(f"Error in UMCSI collection: {e}")
        
    finally:
        dao.close()

def rename_old_indicator(dao):
    """
    Rename umcsi_michigan_full to us_umcsi if it exists
    """
    try:
        # Check if old indicator exists
        dao.cursor.execute("SELECT id FROM indicators WHERE name = 'umcsi_michigan_full'")
        result = dao.cursor.fetchone()
        
        if result:
            print("Found existing umcsi_michigan_full, renaming to us_umcsi...")
            dao.cursor.execute(
                "UPDATE indicators SET name = 'us_umcsi' WHERE name = 'umcsi_michigan_full'"
            )
            dao.conn.commit()
            print("Renamed successfully")
        
    except Exception as e:
        print(f"Error renaming indicator: {e}")

if __name__ == "__main__":
    collect_umcsi()