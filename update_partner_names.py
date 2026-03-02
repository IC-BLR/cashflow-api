#!/usr/bin/env python3
"""
Script to update partner names in the database to Partner 1, Partner 2, etc.
"""
import duckdb

DB_PATH = "payment_allocation.duckdb"

def update_partner_names():
    """Update partner names to Partner 1, Partner 2, etc."""
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Get distinct partner codes and names
        partners = conn.execute("""
            SELECT DISTINCT "Partner Code", "Partner Name"
            FROM payment_allocations
            ORDER BY "Partner Code"
        """).fetchall()
        
        print(f"Found {len(partners)} partners")
        
        # Update each partner name
        for idx, (partner_code, old_name) in enumerate(partners, 1):
            new_name = f"Partner {idx}"
            print(f"Updating {partner_code}: '{old_name}' -> '{new_name}'")
            
            conn.execute("""
                UPDATE payment_allocations
                SET "Partner Name" = ?
                WHERE "Partner Code" = ?
            """, [new_name, partner_code])
        
        # Commit changes
        conn.commit()
        print(f"\nSuccessfully updated {len(partners)} partners")
        
        # Verify the update
        print("\nVerifying update...")
        updated = conn.execute("""
            SELECT DISTINCT "Partner Code", "Partner Name"
            FROM payment_allocations
            ORDER BY "Partner Code"
        """).fetchall()
        
        for partner_code, partner_name in updated:
            print(f"  {partner_code}: {partner_name}")
            
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    update_partner_names()

