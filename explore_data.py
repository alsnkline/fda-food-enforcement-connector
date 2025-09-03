#!/usr/bin/env python3
"""
Script to explore the FDA Food Enforcement data loaded by the connector
"""

import duckdb
import json

def explore_fda_data():
    """Explore the FDA data in the warehouse.db file"""
    
    # Connect to the DuckDB database
    conn = duckdb.connect('files/warehouse.db')
    
    print("🔍 FDA Food Enforcement Data Explorer")
    print("=" * 50)
    
    # Get table information
    print("\n📊 Table Information:")
    tables = conn.execute("SHOW TABLES").fetchall()
    for table in tables:
        print(f"  - {table[0]}")
    
    # Get column information
    print("\n📋 Column Information:")
    columns = conn.execute("DESCRIBE tester.food_enforcement_records").fetchall()
    print(f"  Total columns: {len(columns)}")
    print("\n  Key columns:")
    for col in columns[:20]:  # Show first 20 columns
        print(f"    - {col[0]} ({col[1]})")
    if len(columns) > 20:
        print(f"    ... and {len(columns) - 20} more columns")
    
    # Get record count
    print("\n📈 Record Count:")
    count = conn.execute("SELECT COUNT(*) FROM tester.food_enforcement_records").fetchone()[0]
    print(f"  Total records: {count}")
    
    # Show sample records
    print("\n🍎 Sample Records:")
    sample = conn.execute("SELECT * FROM tester.food_enforcement_records LIMIT 3").fetchall()
    columns = [desc[0] for desc in conn.description]
    
    for i, record in enumerate(sample, 1):
        print(f"\n  Record {i}:")
        for col, val in zip(columns, record):
            if val is not None and str(val).strip():
                # Truncate long values
                val_str = str(val)
                if len(val_str) > 100:
                    val_str = val_str[:100] + "..."
                print(f"    {col}: {val_str}")
    
    # Show some interesting statistics
    print("\n📊 Data Statistics:")
    
    # Classification distribution
    print("\n  Classification Distribution:")
    classifications = conn.execute("""
        SELECT classification, COUNT(*) as count 
        FROM tester.food_enforcement_records 
        WHERE classification IS NOT NULL 
        GROUP BY classification 
        ORDER BY count DESC
    """).fetchall()
    for class_type, count in classifications:
        print(f"    {class_type}: {count}")
    
    # Status distribution
    print("\n  Status Distribution:")
    statuses = conn.execute("""
        SELECT status, COUNT(*) as count 
        FROM tester.food_enforcement_records 
        WHERE status IS NOT NULL 
        GROUP BY status 
        ORDER BY count DESC
    """).fetchall()
    for status, count in statuses:
        print(f"    {status}: {count}")
    
    # Recent recalls
    print("\n  Recent Recalls (by report_date):")
    recent = conn.execute("""
        SELECT recall_number, recalling_firm, product_description, report_date, classification
        FROM tester.food_enforcement_records 
        WHERE report_date IS NOT NULL 
        ORDER BY report_date DESC 
        LIMIT 5
    """).fetchall()
    for recall in recent:
        print(f"    {recall[0]} - {recall[1]} - {recall[2][:50]}... - {recall[3]} - {recall[4]}")
    
    # Check for flattened openfda fields
    print("\n🔍 OpenFDA Fields (flattened):")
    openfda_cols = [col for col in columns if col.startswith('openfda_')]
    if openfda_cols:
        print(f"  Found {len(openfda_cols)} OpenFDA fields:")
        for col in openfda_cols[:10]:  # Show first 10
            print(f"    - {col}")
        if len(openfda_cols) > 10:
            print(f"    ... and {len(openfda_cols) - 10} more")
    else:
        print("  No OpenFDA fields found")
    
    conn.close()
    print("\n✅ Data exploration complete!")

if __name__ == "__main__":
    explore_fda_data()
