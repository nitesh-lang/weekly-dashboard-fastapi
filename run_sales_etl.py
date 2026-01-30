from weekly_app.services.sales_etl import run_sales_etl

if __name__ == "__main__":
    df = run_sales_etl()
    print("\n✅ Sales ETL completed successfully\n")
    print(df)
