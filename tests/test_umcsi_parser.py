# Тестовый скрипт
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.umcsi_parser import get_umcsi_data

if __name__ == "__main__":
    print("Testing improved UMCSI parser...")
    result = get_umcsi_data()
    
    if result:
        print("\n=== PARSING RESULTS ===")
        print(f"Date: {result['date']}")
        print(f"Values: {result['values']}")
        print(f"\nExpectations text length: {len(result['text_releases']['expectations'])}")
        print(f"Expectations preview: {result['text_releases']['expectations'][:300]}...")
        print(f"\nInflation text length: {len(result['text_releases']['inflation'])}")  
        print(f"Inflation preview: {result['text_releases']['inflation'][:300]}...")
    else:
        print("Failed to parse data")