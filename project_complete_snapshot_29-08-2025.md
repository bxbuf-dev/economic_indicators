# Economic Indicators Project - Complete Technical Snapshot

## Project Overview
**Status:** Stage 2 (Data Collection) - Treasury collectors complete, FRED collectors operational  
**GitHub:** https://github.com/bxbuf-dev/economic_indicators.git  
**Last Updated:** 2025-08-29

## Development Environment Setup

### Virtual Environment & Dependencies
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install dependencies
pip install beautifulsoup4==4.13.4 certifi==2025.8.3 charset-normalizer==3.4.3 fredapi==0.5.2 idna==3.10 lxml==6.0.1 numpy==2.3.2 pandas==2.3.2 python-dateutil==2.9.0.post0 python-dotenv==1.1.1 pytz==2025.2 requests==2.32.5 six==1.17.0 soupsieve==2.7 typing_extensions==4.14.1 tzdata==2025.2 urllib3==2.5.0
```

### Environment Configuration
Create `.env` file in project root:
```
FRED_API_KEY=your_fred_api_key_here
```

## Database Architecture

**Location:** `~/Documents/economic_indicators.db` (synced via cloud storage)  
**DBMS:** SQLite with UTF-8 encoding

### Core Tables Schema

```sql
-- Main indicator registry
CREATE TABLE indicators (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    full_name TEXT,
    source TEXT, 
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Numeric time series data
CREATE TABLE indicator_values (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT '',
    value REAL NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (indicator_id) REFERENCES indicators (id),
    UNIQUE(indicator_id, date, category)
);

-- Structured text releases (JSON format)
CREATE TABLE indicator_releases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    category TEXT,
    release_data TEXT,
    source_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (indicator_id) REFERENCES indicators (id)
);

-- User comments for visualization
CREATE TABLE comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    indicator_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    comment_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (indicator_id) REFERENCES indicators (id)
);
```

### Setup Script
```python
# database_setup.py - Run once to initialize DB
import sqlite3
from pathlib import Path

home_dir = Path.home()
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'
# [Include full setup code from provided files]
```

## Data Access Layer (DAO)

### Core DAO Implementation
```python
# dao.py - Single source of truth for all database operations
import sqlite3
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

home_dir = Path.home()
DB_PATH = home_dir / 'Documents' / 'economic_indicators.db'

class IndicatorDAO:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    # WRITE METHODS
    def add_indicator(self, name, full_name, source, description):
        # Returns existing ID if indicator exists, creates new if not
    
    def add_indicator_value(self, indicator_id, date, value, category=''):
        # INSERT OR IGNORE for duplicate protection
    
    def add_indicator_release(self, indicator_id, date, release_data, source_url, category=None):
        # Converts dict to JSON automatically
    
    def add_comment(self, indicator_id, date, comment_text):
        # For user annotations on charts
    
    # READ METHODS  
    def get_indicator_values(self, indicator_id):
        # Returns pandas DataFrame with datetime index
    
    def get_latest_indicator_date(self, indicator_id):
        # For incremental data loading
    
    def close(self):
        # Always call when done
```

**Key Features:**
- Automatic duplicate handling via `INSERT OR IGNORE`
- JSON serialization for complex release data
- Pandas integration for easy data analysis
- Incremental loading support

## Data Collection Framework

### Treasury Data Collectors (COMPLETE)

**Shared Parser:** `collectors/treasury_parser.py`
```python
# Universal Treasury data fetcher
def get_treasury_history(rate_type: str, start_date: datetime.date) -> pd.DataFrame:
    # Handles month-by-month data fetching
    # Filters to Fridays only
    # Returns standardized DataFrame: ['date', 'value', 'category']
```

**Collectors:**
- `yield_curve_collector.py` - Nominal Treasury rates (`daily_treasury_yield_curve`)
- `real_yield_curve_collector.py` - Real Treasury rates (`daily_treasury_real_yield_curve`)

### FRED Data Collectors (OPERATIONAL)

**Shared Parser:** `collectors/fred_parser.py` 
```python
# Universal FRED API interface
def get_fred_series_history(series_id: str, start_date: str = None) -> pd.DataFrame:
    # Single series fetcher
    
def get_fred_calculated_series(series_configs: list, calculation_func, start_date: str = None) -> pd.DataFrame:
    # Multi-series calculator (e.g., Real M2 = M2SL/CPIAUCSL*100)
```

**Active Collectors:**
- `real_m2_collector.py` - Real M2 Money Stock (calculated from M2SL/CPIAUCSL)
- `building_permits_collector.py` - Building permits by unit type (PERMIT, PERMIT1, PERMIT24, PERMIT5)

**Historical Data Loader:**
- `michigan_historical_loader.py` - One-time CSV import for UMCSI historical data

## Utility Scripts

### Database Management
```python
# _db_inspector.py - Diagnostic tool
def inspect_db():
    # Shows unique categories and recent records for specific indicator

# single_record_deleter.py - Surgical data removal  
def delete_record_by_id(record_id):
    # Individual record deletion with confirmation
def delete_latest_records(indicator_id, count):
    # Bulk deletion of recent records

# delete_indicator_script.py - Full indicator removal
def delete_indicator_by_id(indicator_id):
    # Cascading deletion of entire indicator and all related data
```

## Current Data Coverage

### Completed Indicators
- **us_treasury_yield_curve** - Complete nominal yield curve history
- **us_treasury_real_yield_curve** - Complete real yield curve history  
- **real_m2_usd** - Real M2 money stock (calculated)
- **building_permits_us** - Building permits by unit type
- **umcsi_michigan_full** - Historical UMCSI data (composite, current, expectations)

### Missing Collectors (TODO)
- **ISM Manufacturing PMI** (FRED: NAPM + text release parsing)
- **ISM Services PMI** (FRED: ISMNS + text release parsing)
- **Current UMCSI** (FRED: UMCSENT + text release parsing)

## Development Principles

### Regression Testing Protocol
When modifying existing completed modules (especially dao.py), ALL public methods must be re-tested using `test_dao.py` to ensure no functionality breaks.

### Task Completion Workflow
Before considering any task complete:
1. Update dependencies: `pip freeze > requirements.txt`
2. Commit changes to Git with clear message
3. Update this technical snapshot

### Error Handling Standards
- All database operations wrapped in try/except blocks
- Graceful degradation when external APIs fail
- Duplicate protection via `INSERT OR IGNORE`
- Clear console logging for debugging

## Next Development Phase

### Immediate Tasks
1. Create ISM Manufacturing PMI collector
2. Create ISM Services PMI collector  
3. Create current UMCSI collector with text parsing
4. Build data visualization dashboard

### Text Release Parsing Requirements
ISM and UMCSI collectors must parse complex text releases into structured JSON matching this format:
```json
{
  "headline_index": {"name": "Services PMI", "value": 53.5},
  "summary": {"demand": "...", "supply": "..."},
  "sub_indices": {"business_activity": {"value": 56.0, "ranking": "..."}},
  "industry_summary": {"growing_industries_count": 14, "growing_industries_list": [...]},
  "commodities": {"up_in_price": [...], "down_in_price": [...]},
  "quotes": [{"industry": "...", "comment": "..."}]
}
```

## Project File Structure
```
economic_indicators/
├── .venv/                          # Python virtual environment
├── .env                            # API keys (not committed)
├── .gitignore                      # Excludes .env and __pycache__
├── requirements.txt                # Frozen dependencies
├── dao.py                          # Core data access layer
├── database_setup.py               # One-time DB initialization
├── test_dao.py                     # DAO regression test suite
├── single_record_deleter.py        # Surgical data removal tool
├── delete_indicator_script.py      # Full indicator removal tool
├── _db_inspector.py               # Database diagnostic tool
├── _db_maintenance_tool.py        # Bulk data cleanup
├── collectors/
│   ├── treasury_parser.py         # Shared Treasury.gov parser
│   ├── fred_parser.py             # Shared FRED API parser
│   ├── yield_curve_collector.py   # Nominal Treasury rates
│   ├── real_yield_curve_collector.py # Real Treasury rates
│   ├── real_m2_collector.py       # Real M2 calculated series
│   ├── building_permits_collector.py # Building permits by type
│   └── michigan_historical_loader.py # One-time UMCSI CSV import
└── data/
    └── michigan_historical.csv     # Historical UMCSI data
```

## Testing & Validation

### DAO Test Suite
Run `python test_dao.py` to validate all core database operations:
- Table creation and cleanup
- Data insertion across all tables  
- JSON serialization/deserialization
- Data retrieval and pandas integration

### Collector Testing Pattern
1. Use `single_record_deleter.py` to remove recent records
2. Run collector to verify incremental loading
3. Use `_db_inspector.py` to validate data structure and categories
4. Check for proper duplicate handling

This snapshot contains everything needed to continue development or hand off to another developer. All core infrastructure is operational and tested.