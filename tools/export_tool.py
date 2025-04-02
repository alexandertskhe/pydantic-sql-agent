import json
import csv
import os
import re
import time
from typing import Optional, Tuple

# Directory to store CSV exports - shared with the FastAPI server
EXPORT_DIR = "csv_exports"
# URL of the FastAPI server
SERVER_URL = "http://localhost:8000"

# Ensure export directory exists
os.makedirs(EXPORT_DIR, exist_ok=True)

async def query_to_csv_file(db, sql_query: str, limit: Optional[int] = None) -> tuple[str, str, int]:
    """
    Run an SQL query and save the results as a CSV file.
    
    Args:
        db: Database connection
        sql_query: SQL query to run
        limit: Optional limit on number of rows to return
        
    Returns:
        Tuple of (filename, download_url, row_count)
    """
    try:
        # Execute the query
        cursor = await db.execute(sql_query)
        
        # Fetch results
        rows = await cursor.fetchmany(limit) if limit else await cursor.fetchall()
        
        # Get column names from cursor description
        if not cursor.description:
            return "", "", 0
        
        column_names = [desc[0] for desc in cursor.description]
        
        # Try to extract table name from query
        table_match = re.search(r'FROM\s+([^\s,;]+)', sql_query, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else "query_result"
        
        # Clean up table name if it has quotes or brackets
        table_name = re.sub(r'["\[\]`\']', '', table_name)
        
        # Create filename with timestamp
        timestamp = int(time.time())
        filename = f"{table_name}_{timestamp}.csv"
        
        # Full path to save the file
        file_path = os.path.join(EXPORT_DIR, filename)
        
        # Convert rows to list of dicts for easier processing
        dict_rows = [dict(zip(column_names, row)) for row in rows]
        
        # Write to CSV file
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_names)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            writer.writerows(dict_rows)
        
        # Create download URL from FastAPI server
        download_url = f"{SERVER_URL}/download/{filename}"
        
        # Print debug information
        print(f"CSV Export successful: {filename} with {len(rows)} rows")
        
        # Return information
        return filename, download_url, len(rows)
        
    except Exception as e:
        # Enhanced error logging
        print(f"Error generating CSV: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return "", "", 0