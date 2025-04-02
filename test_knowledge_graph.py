# test_knowledge_graph_samples.py
import asyncio
import json
import os
from utils.knowledge_graph import DBKnowledgeGraph

async def test_knowledge_graph_samples():
    # Path to your SQLite database
    db_path = "sqlite_db/sqlite.db"
    cache_file = f"{os.path.splitext(db_path)[0]}_graph.json"
    
    # Force rebuild by removing cache file
    if os.path.exists(cache_file):
        print(f"Removing old cache file: {cache_file}")
        os.remove(cache_file)
    
    # Initialize the knowledge graph
    print("Initializing knowledge graph (collecting sample data)...")
    kg = DBKnowledgeGraph(db_path)
    await kg.initialize()
    
    # Test table info with sample data
    print("\n--- Testing Table Info with Sample Data ---")
    table_info = kg.get_table_info('sis_airports')
    
    # Print column information
    print("\nColumns:")
    for col in table_info.get("columns", [])[:5]:  # Just show first 5 columns
        print(f"- {col['name']} ({col['type']})")
    print("... (and more columns)")
    
    # Print sample data
    print("\nSample Data (first 2 rows):")
    if "sample_rows" in table_info:
        for i, row in enumerate(table_info.get("sample_rows", [])[:2]):
            # Print a subset of columns for readability
            print(f"Row {i+1}: {json.dumps({k: row[k] for k in ['AIRPORT_NAME', 'AIRPORT_COUNTRY_NAME', 'AIRPORT_IATA'] if k in row}, indent=2)}")
    else:
        print("No sample rows found in table_info")
    
    # Test column statistics
    print("\n--- Testing Column Statistics ---")
    column_stats = table_info.get("column_statistics", {})
    
    if "AIRPORT_COUNTRY_NAME" in column_stats:
        stats = column_stats["AIRPORT_COUNTRY_NAME"]
        print(f"Statistics for AIRPORT_COUNTRY_NAME:")
        print(f"- Distinct values: {stats.get('distinct_count')}")
        
        # Print common values if available
        common_values = stats.get("common_values", [])
        if common_values:
            print("- Most common countries:")
            for val in common_values[:3]:  # Just show top 3
                print(f"  - {val['value']} (appears {val['count']} times)")
    else:
        print("No column statistics found for AIRPORT_COUNTRY_NAME")
        print(f"Available keys in column_stats: {list(column_stats.keys())}")
    
    # Test specific column values method
    print("\n--- Testing Column Values Method ---")
    column_values = kg.get_column_values('sis_airports', 'AIRPORT_REGION')
    
    if "sample_values" in column_values:
        print(f"Sample values for AIRPORT_REGION:")
        print(", ".join(str(v) for v in column_values.get("sample_values", [])[:5]))
    else:
        print(f"No sample values found for AIRPORT_REGION: {column_values}")
    
    # Test with sis_wan table
    print("\n--- Testing sis_wan Table Sample Data ---")
    column_values = kg.get_column_values('sis_wan', 'WAN_ACCESS_TYPE_PRIMARY')
    
    if "sample_values" in column_values:
        print(f"Sample values for WAN_ACCESS_TYPE_PRIMARY:")
        print(", ".join(str(v) for v in column_values.get("sample_values", [])[:5]))
    else:
        print(f"No sample values found for WAN_ACCESS_TYPE_PRIMARY: {column_values}")
    
    print("\nKnowledge graph cache rebuilt with sample data.")

if __name__ == "__main__":
    asyncio.run(test_knowledge_graph_samples())