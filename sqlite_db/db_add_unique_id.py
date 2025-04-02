import sqlite3
import os

# Use relative paths
current_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_dir, "sqlite.db")

# Connect to SQLite database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Function to add a new column with uniqueness constraint
def add_unique_column(conn, table_name, new_column, column_type="TEXT", use_rowid=True):
    """
    Add a new unique column to a table using the recreate table method
    
    Parameters:
    - conn: SQLite connection
    - table_name: Name of the table to modify
    - new_column: Name of the new column to add
    - column_type: Data type of the new column (default: TEXT)
    - use_rowid: Whether to use SQLite's rowid as the unique values (default: True)
    """
    cursor = conn.cursor()
    
    # Get column info from the original table
    cursor.execute(f'PRAGMA table_info("{table_name}");')
    columns = cursor.fetchall()
    
    # Check if column already exists
    if any(col[1] == new_column for col in columns):
        print(f"Column '{new_column}' already exists in table '{table_name}'")
        return False
    
    # Get foreign key info
    cursor.execute(f'PRAGMA foreign_key_list("{table_name}");')
    fk_info = cursor.fetchall()
    
    # Create column definitions for the new table
    column_defs = []
    for col in columns:
        col_name = col[1]
        col_type = col[2]
        column_defs.append(f'"{col_name}" {col_type}')
    
    # Add the new column definition
    column_defs.append(f'"{new_column}" {column_type} UNIQUE')
    
    # Add foreign key constraints if they exist
    fk_constraints = []
    for fk in fk_info:
        from_col = fk[3]
        to_table = fk[2]
        to_col = fk[4]
        fk_constraints.append(f'FOREIGN KEY ("{from_col}") REFERENCES "{to_table}"("{to_col}")')
    
    # Create a new table with the unique constraint
    temp_table = f'"{table_name}_temp"'
    constraints = ", ".join(fk_constraints) if fk_constraints else ""
    if constraints:
        create_sql = f'CREATE TABLE {temp_table} ({", ".join(column_defs)}, {constraints});'
    else:
        create_sql = f'CREATE TABLE {temp_table} ({", ".join(column_defs)});'
    
    print(f"Creating new table with column: {create_sql}")
    cursor.execute(create_sql)
    
    # Get list of column names for the INSERT statement (exclude the new column)
    old_column_names = [f'"{col[1]}"' for col in columns]
    old_column_names_str = ', '.join(old_column_names)
    
    if use_rowid:
        # Insert data with rowid as the unique identifier
        insert_sql = f'''
        INSERT INTO {temp_table} ({old_column_names_str}, "{new_column}")
        SELECT {old_column_names_str}, rowid
        FROM "{table_name}";
        '''
    else:
        # Insert data with NULL for the new column (requires manual update later)
        insert_sql = f'''
        INSERT INTO {temp_table} ({old_column_names_str}, "{new_column}")
        SELECT {old_column_names_str}, NULL
        FROM "{table_name}";
        '''
    
    print(f"Inserting data: {insert_sql}")
    cursor.execute(insert_sql)
    
    # Drop the original table
    cursor.execute(f'DROP TABLE "{table_name}";')
    
    # Rename the new table to the original name
    cursor.execute(f'ALTER TABLE {temp_table} RENAME TO "{table_name}";')
    
    conn.commit()
    print(f"Added new unique column '{new_column}' to table '{table_name}'")
    
    # Count rows for verification
    cursor.execute(f'SELECT COUNT(*) FROM "{table_name}";')
    total_rows = cursor.fetchone()[0]
    print(f"Total rows in {table_name}: {total_rows}")
    
    return True

# Add the wan_device_id column to sis_wan table
add_unique_column(conn, "sis_wan", "wan_device_id", "TEXT", use_rowid=True)

# Close the connection
conn.close()
print("Database column added successfully")