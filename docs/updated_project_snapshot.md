# Economic Indicators Project - Technical Snapshot

## Project Overview
**Status:** Stage 2 (Data Collection) - Treasury, FRED, and UMCSI collectors operational  
**GitHub:** https://github.com/bxbuf-dev/economic_indicators.git  
**Last Updated:** 2025-08-30

## Development Environment

### Dependencies
Standard Python data stack: pandas, numpy, requests, beautifulsoup4, fredapi, python-dotenv, sqlite3

### Configuration
- **Database:** `~/Documents/economic_indicators.db` (SQLite)
- **Environment:** `.env` file with FRED_API_KEY
- **Virtual Environment:** `.venv/` directory

## Database Schema

### Core Tables Structure
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

### JSON Release Data Formats

**UMCSI Text Releases:**
```json
{
    "type": "expectations",
    "content": "Consumer sentiment confirmed its early-month reading...",
    "is_preliminary": false
}
```

**Future ISM Release Format:**
```json
{
    "headline_index": {"name": "Manufacturing PMI", "value": 53.5},
    "summary": {"demand": "...", "supply": "..."},
    "sub_indices": {"new_orders": {"value": 56.0, "ranking": "..."}},
    "industry_summary": {"growing_industries_count": 14, "growing_industries_list": [...]},
    "commodities": {"up_in_price": [...], "down_in_price": [...]},
    "quotes": [{"industry": "...", "comment": "..."}]
}
```

### Key Features
- Automatic duplicate handling via INSERT OR IGNORE
- JSON serialization for complex release data
- Pandas integration with datetime indexing
- Foreign key relationships with cascading options

## Data Collection Framework

### Treasury Data (COMPLETE)
- **Collectors:** `yield_curve_collector.py`, `real_yield_curve_collector.py`
- **Parser:** `treasury_parser.py` - Universal Treasury.gov scraper
- **Data:** Complete historical yield curves (nominal and real rates)
- **Indicators:** `us_treasury_yield_curve`, `us_treasury_real_yield_curve`

### FRED Data (OPERATIONAL)
- **Collectors:** `real_m2_collector.py`, `building_permits_collector.py`
- **Parser:** `fred_parser.py` - FRED API interface with calculations
- **Data:** Real M2 Money Stock, Building Permits by unit type
- **Indicators:** `real_m2_usd`, `building_permits_us`

### University of Michigan (COMPLETE)
- **Collectors:** `umcsi_collector.py`, `michigan_historical_loader.py`
- **Parser:** `umcsi_parser.py` - Web scraper with text extraction
- **Data:** Consumer Sentiment with director commentary
- **Indicators:** `us_umcsi` (current + text releases)

## Utility Scripts

### Database Management
- **dao.py / dao_No_Debug.py** - Core data access layer
- **database_setup.py** - One-time DB initialization
- **test_dao.py** - Regression test suite

### Data Maintenance
- **_db_inspector.py** - Database diagnostic tool
- **universal_record_deleter.py** - Complete data management (by date/table/ID)
- **delete_indicator_script.py** - Full indicator removal
- **_db_maintenance_tool.py** - Bulk cleanup operations

## Current Data Coverage

### Active Indicators (5)
1. **us_treasury_yield_curve** - Daily nominal Treasury rates (1M-30Y)
2. **us_treasury_real_yield_curve** - Daily real Treasury rates (5Y-30Y)
3. **real_m2_usd** - Real M2 Money Stock (calculated: M2SL/CPIAUCSL*100)
4. **building_permits_us** - Building permits by unit type (1, 2-4, 5+ units)
5. **us_umcsi** - Consumer Sentiment (composite, current, expectations + text)

### Missing Collectors (2)
- **ISM Manufacturing PMI** - FRED series + text release parsing
- **ISM Services PMI** - FRED series + text release parsing

## Recent Improvements (August 30, 2025)

### UMCSI Enhancement
- Fixed parser to extract director commentary instead of table data
- Smart text categorization (expectations vs inflation sections)
- Comprehensive text releases stored as structured JSON
- Improved web scraping with fallback strategies

### Universal Data Management
- Complete record view grouped by date across all tables
- Precise deletion tools for records, dates, or entire indicators
- Enhanced statistics and diagnostic capabilities

## Development Principles

### Testing Protocol
- Regression testing required when modifying core modules (dao.py)
- Use `test_dao.py` to validate all public methods
- Incremental loading validation with duplicate protection

### Task Completion Workflow
1. Update requirements.txt with `pip freeze`
2. Test functionality with existing validation tools
3. Update technical snapshot
4. Commit with clear message

### Error Handling Standards
- All database operations in try/except blocks
- Graceful API failure degradation
- Console logging for debugging
- Duplicate protection via INSERT OR IGNORE

## Project Structure Summary
```
economic_indicators/
├── .venv/                          # Virtual environment
├── .env                            # API keys (gitignored)
├── requirements.txt                # Dependencies
├── dao.py / dao_No_Debug.py       # Data access layer
├── database_setup.py               # DB initialization
├── test_dao.py                     # Test suite
├── universal_record_deleter.py     # Data management
├── _db_inspector.py               # Diagnostics
├── collectors/                     # Data collection modules
│   ├── treasury_parser.py
│   ├── fred_parser.py
│   ├── umcsi_parser.py
│   └── [collector scripts]
└── data/                          # Static data files
```

## Development Status
All core infrastructure operational. Treasury and FRED collectors complete with historical data loaded. UMCSI collector fully functional with text release parsing. Database management tools comprehensive. Ready for ISM collector development and dashboard creation.

## Testing & Validation

### DAO Test Suite
Run `python test_dao.py` to validate all core database operations:
- Table creation and cleanup
- Data insertion across all tables  
- JSON serialization/deserialization
- Data retrieval and pandas integration

### Collector Testing Pattern
1. Use `universal_record_deleter.py` to remove recent records by date
2. Run collector to verify incremental loading
3. Use `_db_inspector.py` to validate data structure and categories
4. Check for proper duplicate handling

### UMCSI Validation
1. Run `python collectors/umcsi_collector.py`
2. Verify numerical values extracted correctly
3. Check text releases contain meaningful commentary (not table data)
4. Confirm proper separation of expectations vs inflation text

## Code Access
Full implementation code for any module can be provided on request. All scripts are documented and tested. Database schema and sample queries available in separate artifacts if needed.