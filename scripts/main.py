import os
import importlib
from dotenv import load_dotenv
from db_connection import get_connection

def main():
    load_dotenv()

    tables = os.getenv("TABLE_LIST")
    if not tables:
        return

    tables = [t.strip().lower() for t in tables.split(",")]
    conn = get_connection()

    for table in tables:
        module_name = f"scripts.extract_{table}"
        func_name = f"extract_{table}"

        try:
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            func(conn)

        except ModuleNotFoundError:
            print(f"Module not found for table '{table}'.")
        except AttributeError:
            print(f"Function '{func_name}' not found in module '{module_name}'.")
        except Exception as e:
            print(f"Error processing table {table}: {e}\n")

    conn.close()

if __name__ == "__main__":
    main()
