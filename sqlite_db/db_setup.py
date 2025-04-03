import pandas as pd
import sqlite3
import os
from pathlib import Path

# Use relative paths
current_dir = os.path.dirname(os.path.abspath(__file__))
data_folder = Path("../data")
db_path = os.path.join(current_dir, "sqlite.db")

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Enable foreign keys
cursor.execute("PRAGMA foreign_keys = ON;")

# First, load all parquet files into tables
parquet_files = [f for f in os.listdir(data_folder) if f.endswith('.parquet')]
for file_name in parquet_files:
    file_path = os.path.join(data_folder, file_name)
    table_name = Path(file_name).stem
    
    print(f"Processing {file_name} into table {table_name}...")
    df = pd.read_parquet(file_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Successfully loaded {file_name}")

# Function to add primary key to a table
def add_primary_key(conn, table_name, primary_key_column):
    cursor = conn.cursor()
    
    # Get column info from the original table
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    
    # Create column definitions for the new table
    column_defs = []
    for col in columns:
        name = col[1]
        type_name = col[2]
        if name == primary_key_column:
            column_defs.append(f'"{name}" {type_name} PRIMARY KEY')
        else:
            column_defs.append(f'"{name}" {type_name}')
    
    # Create a new table with the primary key
    temp_table = f'"{table_name}_temp"'
    create_sql = f"CREATE TABLE {temp_table} ({', '.join(column_defs)});"
    cursor.execute(create_sql)
    
    # Get list of column names for the INSERT statement
    column_names = [f'"{col[1]}"' for col in columns]
    column_names_str = ', '.join(column_names)
    
    # Copy data from the original table to the new table
    insert_sql = f"INSERT INTO {temp_table} ({column_names_str}) SELECT {column_names_str} FROM \"{table_name}\";"
    cursor.execute(insert_sql)
    
    # Drop the original table
    cursor.execute(f'DROP TABLE "{table_name}";')
    
    # Rename the new table to the original name
    cursor.execute(f'ALTER TABLE {temp_table} RENAME TO "{table_name}";')
    
    conn.commit()
    print(f"Added primary key ({primary_key_column}) to {table_name}")
    return True

# Function to add a foreign key with orphan record handling
def add_foreign_key(conn, table_name, fk_column, ref_table, ref_column, orphan_handling='skip'):
    """
    Add a foreign key to a table with options for handling orphaned records.
    
    Parameters:
    - conn: SQLite connection
    - table_name: Name of the table to add foreign key to
    - fk_column: Column name to make a foreign key
    - ref_table: Referenced table name
    - ref_column: Referenced column name
    - orphan_handling: How to handle orphaned records
      - 'skip': Skip orphaned records (default)
      - 'null': Set foreign key to NULL for orphaned records
      - 'report': Just report orphaned records without changing anything
    """
    cursor = conn.cursor()
    
    # Check for orphaned records
    check_sql = f"""
    SELECT COUNT(*) FROM "{table_name}" 
    WHERE "{fk_column}" NOT IN (SELECT "{ref_column}" FROM "{ref_table}") 
    AND "{fk_column}" IS NOT NULL
    """
    cursor.execute(check_sql)
    orphan_count = cursor.fetchone()[0]
    
    if orphan_count > 0:
        print(f"WARNING: Found {orphan_count} orphaned records in {table_name}.{fk_column}")
        
        # List some example orphaned values
        sample_sql = f"""
        SELECT "{fk_column}", COUNT(*) as count FROM "{table_name}" 
        WHERE "{fk_column}" NOT IN (SELECT "{ref_column}" FROM "{ref_table}") 
        AND "{fk_column}" IS NOT NULL
        GROUP BY "{fk_column}"
        LIMIT 5
        """
        cursor.execute(sample_sql)
        samples = cursor.fetchall()
        print(f"Sample orphaned values: {samples}")
        
        if orphan_handling == 'report':
            print("Not creating foreign key - just reporting orphaned records")
            return False
        
    # Get column info from the original table
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    
    # Create column definitions for the new table
    column_defs = []
    for col in columns:
        col_name = col[1]
        col_type = col[2]
        column_defs.append(f'"{col_name}" {col_type}')
    
    # Add foreign key constraint
    constraint = f'FOREIGN KEY ("{fk_column}") REFERENCES "{ref_table}"("{ref_column}")'
    
    # Create a new table with the foreign key
    temp_table = f'"{table_name}_temp"'
    create_sql = f"CREATE TABLE {temp_table} ({', '.join(column_defs)}, {constraint});"
    cursor.execute(create_sql)
    
    # Get list of column names for the INSERT statement
    column_names = [f'"{col[1]}"' for col in columns]
    column_names_str = ', '.join(column_names)
    
    # Handle the data insertion based on orphan handling option
    if orphan_handling == 'skip' and orphan_count > 0:
        # Only insert non-orphaned records
        insert_sql = f"""
        INSERT INTO {temp_table} ({column_names_str})
        SELECT {column_names_str} FROM "{table_name}"
        WHERE "{fk_column}" IN (SELECT "{ref_column}" FROM "{ref_table}")
        OR "{fk_column}" IS NULL
        """
        cursor.execute(insert_sql)
        print(f"Skipped {orphan_count} orphaned records")
    
    elif orphan_handling == 'null' and orphan_count > 0:
        # Insert all records, but set orphaned foreign keys to NULL
        # First, insert the non-orphaned records
        insert_non_orphans_sql = f"""
        INSERT INTO {temp_table} ({column_names_str})
        SELECT {column_names_str} FROM "{table_name}"
        WHERE "{fk_column}" IN (SELECT "{ref_column}" FROM "{ref_table}")
        OR "{fk_column}" IS NULL
        """
        cursor.execute(insert_non_orphans_sql)
        
        # Now get the orphaned records and insert them with NULL in the foreign key column
        other_columns = [f'"{col[1]}"' for col in columns if col[1] != fk_column]
        other_columns_str = ', '.join(other_columns)
        
        # Build a SQL statement to insert orphaned records with NULL in the foreign key
        columns_for_orphan_insert = ', '.join([f'"{col[1]}"' for col in columns if col[1] != fk_column] + ['NULL'])
        select_columns_for_orphan = ', '.join([f'"{col[1]}"' for col in columns if col[1] != fk_column] + ['NULL'])
        
        insert_orphans_sql = f"""
        INSERT INTO {temp_table} ({column_names_str})
        SELECT {select_columns_for_orphan}
        FROM "{table_name}"
        WHERE "{fk_column}" NOT IN (SELECT "{ref_column}" FROM "{ref_table}")
        AND "{fk_column}" IS NOT NULL
        """
        cursor.execute(insert_orphans_sql)
        print(f"Set {orphan_count} orphaned foreign keys to NULL")
    
    else:
        # Just insert all records (for when there are no orphans)
        insert_sql = f"INSERT INTO {temp_table} ({column_names_str}) SELECT {column_names_str} FROM \"{table_name}\";"
        cursor.execute(insert_sql)
    
    # Drop the original table
    cursor.execute(f'DROP TABLE "{table_name}";')
    
    # Rename the new table to the original name
    cursor.execute(f'ALTER TABLE {temp_table} RENAME TO "{table_name}";')
    
    conn.commit()
    print(f"Added foreign key from {table_name}.{fk_column} to {ref_table}.{ref_column}")
    return True





############## Add tables to db ##############

# Add primary and foreign keys
add_primary_key(conn, "sis_airports", "APT_ID")
add_foreign_key(conn, "sis_wan", fk_column="APT_ID", ref_table="sis_airports", ref_column="APT_ID", orphan_handling='skip')

# add_primary_key(conn, "project_roadmaps", "key")
# add_foreign_key(conn, "project_plans", fk_column="key", ref_table="project_roadmaps", ref_column="key", orphan_handling='skip')

# Close the connection
conn.close()
print("Database setup complete with keys added")