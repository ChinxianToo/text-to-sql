"""
Database Setup Utilities
Creates sample databases and provides schema information for the text-to-SQL system.
"""

import duckdb
import pandas as pd
from typing import Dict, Any


def create_sample_sales_database(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """
    Create a sample sales database with salesperson and timber_sales tables
    
    Args:
        db_path: Path to database file or ":memory:" for in-memory database
        
    Returns:
        DuckDB connection object
    """
    # Create connection
    conn = duckdb.connect(db_path)
    
    # Create salesperson table
    conn.execute("""
        CREATE TABLE salesperson (
            salesperson_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT NOT NULL
        )
    """)
    
    # Insert sample data into salesperson table
    salesperson_data = [
        (1, 'John Doe', 'North'),
        (2, 'Jane Smith', 'South')
    ]
    
    conn.executemany(
        "INSERT INTO salesperson (salesperson_id, name, region) VALUES (?, ?, ?)",
        salesperson_data
    )
    
    # Create timber_sales table
    conn.execute("""
        CREATE TABLE timber_sales (
            sales_id INTEGER PRIMARY KEY,
            salesperson_id INTEGER,
            volume REAL NOT NULL,
            sale_date DATE NOT NULL,
            FOREIGN KEY (salesperson_id) REFERENCES salesperson(salesperson_id)
        )
    """)
    
    # Insert sample data into timber_sales table
    timber_sales_data = [
        (1, 1, 120.0, '2021-01-01'),
        (2, 1, 150.0, '2021-02-01'),
        (3, 2, 180.0, '2021-01-01'),
        (4, 2, 200.0, '2021-02-01'),
        (5, 1, 175.0, '2021-03-01'),
        (6, 2, 195.0, '2021-03-01')
    ]
    
    conn.executemany(
        "INSERT INTO timber_sales (sales_id, salesperson_id, volume, sale_date) VALUES (?, ?, ?, ?)",
        timber_sales_data
    )
    
    print("✅ Sample sales database created successfully")
    return conn


def get_database_schema_info() -> str:
    """
    Get comprehensive database schema information for the LLM
    
    Returns:
        Formatted string with database schema information
    """
    schema_info = """
### Database Structure:

**Tables:**

1. **salesperson**
   - `salesperson_id` (INT, PRIMARY KEY) - Unique identifier for each salesperson
   - `name` (TEXT) - Full name of the salesperson  
   - `region` (TEXT) - Geographic region assigned to the salesperson

**Sample Data:**
| salesperson_id | name       | region |
|----------------|------------|--------|
| 1              | John Doe   | North  |
| 2              | Jane Smith | South  |

2. **timber_sales**
   - `sales_id` (INT, PRIMARY KEY) - Unique identifier for each sale
   - `salesperson_id` (INT, FOREIGN KEY) - References salesperson.salesperson_id
   - `volume` (REAL) - Volume of timber sold in units
   - `sale_date` (DATE) - Date when the sale occurred

**Sample Data:**
| sales_id | salesperson_id | volume | sale_date  |
|----------|----------------|--------|------------|
| 1        | 1              | 120    | 2021-01-01 |
| 2        | 1              | 150    | 2021-02-01 |
| 3        | 2              | 180    | 2021-01-01 |

### Key Points:
- **Relationships**: One-to-many between `salesperson` and `timber_sales` via `salesperson_id`
- **Indexes**: Likely primary keys on `salesperson_id` and `sales_id`

### Example Queries:
- **Total timber sold by each salesperson**:
```sql
SELECT s.name, SUM(t.volume) AS total_sales_volume
FROM salesperson s
JOIN timber_sales t ON s.salesperson_id = t.salesperson_id
GROUP BY s.salesperson_id, s.name;
```

- **Sales by region**:
```sql
SELECT s.region, SUM(t.volume) AS total_volume
FROM salesperson s
JOIN timber_sales t ON s.salesperson_id = t.salesperson_id
GROUP BY s.region;
```

- **Monthly sales trends**:
```sql
SELECT 
    strftime('%Y-%m', t.sale_date) AS month,
    SUM(t.volume) AS total_volume
FROM timber_sales t
GROUP BY strftime('%Y-%m', t.sale_date)
ORDER BY month;
```
"""
    return schema_info


def create_extended_sample_database(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """
    Create an extended sample database with more tables for comprehensive testing
    
    Args:
        db_path: Path to database file or ":memory:" for in-memory database
        
    Returns:
        DuckDB connection object
    """
    conn = create_sample_sales_database(db_path)
    
    # Add products table
    conn.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            unit_price REAL NOT NULL
        )
    """)
    
    # Insert sample product data
    product_data = [
        (1, 'Pine Logs', 'Softwood', 25.50),
        (2, 'Oak Logs', 'Hardwood', 45.00),
        (3, 'Maple Logs', 'Hardwood', 40.00),
        (4, 'Cedar Logs', 'Softwood', 35.00)
    ]
    
    conn.executemany(
        "INSERT INTO products (product_id, product_name, category, unit_price) VALUES (?, ?, ?, ?)",
        product_data
    )
    
    # Add product_id to timber_sales
    conn.execute("ALTER TABLE timber_sales ADD COLUMN product_id INTEGER")
    
    # Update existing sales with product associations
    updates = [
        (1, 1), (2, 2), (3, 3), (4, 4), (1, 5), (2, 6)
    ]
    
    for product_id, sales_id in updates:
        conn.execute(
            "UPDATE timber_sales SET product_id = ? WHERE sales_id = ?",
            (product_id, sales_id)
        )
    
    print("✅ Extended sample database created successfully")
    return conn


def get_extended_schema_info() -> str:
    """
    Get schema information for the extended database
    
    Returns:
        Formatted string with extended database schema information
    """
    base_schema = get_database_schema_info()
    
    extended_info = """

3. **products**
   - `product_id` (INT, PRIMARY KEY) - Unique identifier for each product
   - `product_name` (TEXT) - Name of the timber product
   - `category` (TEXT) - Product category (Softwood/Hardwood)
   - `unit_price` (REAL) - Price per unit volume

**Sample Data:**
| product_id | product_name | category  | unit_price |
|------------|--------------|-----------|------------|
| 1          | Pine Logs    | Softwood  | 25.50      |
| 2          | Oak Logs     | Hardwood  | 45.00      |
| 3          | Maple Logs   | Hardwood  | 40.00      |

### Additional Relationships:
- `timber_sales.product_id` references `products.product_id`

### Additional Example Queries:
- **Revenue by product category**:
```sql
SELECT p.category, SUM(t.volume * p.unit_price) AS total_revenue
FROM timber_sales t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.category;
```

- **Top performing products**:
```sql
SELECT p.product_name, SUM(t.volume) AS total_volume_sold
FROM timber_sales t
JOIN products p ON t.product_id = p.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_volume_sold DESC;
```
"""
    
    return base_schema + extended_info


def verify_database_setup(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Verify that the database is set up correctly
    
    Args:
        conn: DuckDB connection object
        
    Returns:
        True if verification passes, False otherwise
    """
    try:
        # Check if tables exist
        tables_result = conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables_result]
        
        required_tables = ['salesperson', 'timber_sales']
        for table in required_tables:
            if table not in table_names:
                print(f"❌ Missing required table: {table}")
                return False
        
        # Check row counts
        salesperson_count = conn.execute("SELECT COUNT(*) FROM salesperson").fetchone()[0]
        sales_count = conn.execute("SELECT COUNT(*) FROM timber_sales").fetchone()[0]
        
        if salesperson_count < 2:
            print(f"❌ Insufficient salesperson data: {salesperson_count} rows")
            return False
            
        if sales_count < 3:
            print(f"❌ Insufficient sales data: {sales_count} rows")
            return False
        
        print("✅ Database verification passed")
        print(f"   - Salesperson records: {salesperson_count}")
        print(f"   - Sales records: {sales_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database verification failed: {e}")
        return False


def display_sample_data(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Display sample data from all tables
    
    Args:
        conn: DuckDB connection object
    """
    print("\n" + "="*50)
    print("📊 SAMPLE DATA")
    print("="*50)
    
    # Show salesperson data
    print("\n👤 Salesperson Table:")
    salesperson_df = conn.execute("SELECT * FROM salesperson").df()
    print(salesperson_df.to_string(index=False))
    
    # Show timber_sales data
    print("\n🌲 Timber Sales Table:")
    sales_df = conn.execute("SELECT * FROM timber_sales").df()
    print(sales_df.to_string(index=False))
    
    # Show products table if it exists
    try:
        products_df = conn.execute("SELECT * FROM products").df()
        print("\n📦 Products Table:")
        print(products_df.to_string(index=False))
    except:
        pass  # Products table doesn't exist in basic setup 