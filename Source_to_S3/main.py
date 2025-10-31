import os
import subprocess
from dotenv import load_dotenv
from pathlib import Path
from db_connection import get_connection

load_dotenv()
tables = os.getenv("TABLE_LIST")
tables = [t.strip().lower() for t in tables.split(",")]
print(tables)

def main():

    if not tables:
        print("No tables found in .env (TABLE_LIST).")
        return

    for table in tables:
        script_path = Path(f"Source_to_S3/{table.lower()}.py")
        if script_path.exists():
            try:
                subprocess.run(["python", str(script_path),table], check=True)
                print(f"Successfully executed {script_path.name}\n")
            except subprocess.CalledProcessError as e:
                print(f"Error while executing {script_path.name}: {e}\n")
        else:
            print(f"Script not found for table '{table}' ({script_path}).\n")

    print("All scripts executed successfully.")
    
if __name__ == "__main__":
    main()
