from weekly_app.services.inventory_etl import run_inventory_etl

if __name__ == "__main__":
    df = run_inventory_etl()
    print("\n✅ Inventory ETL completed successfully\n")
    print(df)
