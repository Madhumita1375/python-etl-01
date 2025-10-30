import os
import subprocess
from dotenv import load_dotenv
from pathlib import Path
from db_connection import get_connection

def main():
    env_path = Path(r"C:\Users\madhumita.sundararaj\Desktop\python-etl-01\scripts\variables.env")
    load_dotenv(dotenv_path=env_path)
    tables = os.getenv("TABLE_LIST")
    print(f"TABLE_LIST: {tables}\n")

    if not tables:
        print("No tables found in .env (TABLE_LIST).")
        return
    tables = [t.strip().lower() for t in tables.split(",")]

    scripts_dir = Path(__file__).parent

    for table in tables:
        script_path = scripts_dir / f"{table.lower()}.py"
        if script_path.exists():
            try:
                subprocess.run(["python", str(script_path)], check=True)
                print(f"Successfully executed {script_path.name}\n")
            except subprocess.CalledProcessError as e:
                print(f"Error while executing {script_path.name}: {e}\n")
        else:
            print(f"Script not found for table '{table}' ({script_path}).\n")

    print("All scripts executed successfully.")
    
if __name__ == "__main__":
    main()
