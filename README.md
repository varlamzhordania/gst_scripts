# 📝 Excel-to-MySQL Import Script

This Python script imports data from an Excel `.xls` or `.xlsx` file into a MySQL database based on predefined **sheet-to-table mappings**. It automatically:

* Renames and maps Excel columns to database columns
* Resolves foreign keys from other tables
* Handles `yes/no` → `1/0` flag conversion
* Groups rows into nested JSON structures for certain sheets
* Performs **upserts** (insert or update) based on unique keys
* Supports multi-value foreign keys and custom flight data parsing

---

## ⚙️ Prerequisites

* Python 3.8+
* MySQL database (schema already created)
* Install all dependencies from requirements.txt:

  ```bash
  pip install -r requirements.txt
  ```

---

## 🛠 Configuration

At the top of the script, set your database credentials:

```python
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = '<PASSWORD>'
DB_NAME = 'gst_scripts'
```

Make sure the database and tables exist with the required columns and primary keys (`id`).

---

## 📁 Excel File Structure

* Each sheet name in your Excel file must match a key in the `SHEET_MAPPINGS` dictionary.
* The `columns` mapping maps Excel column headers → database columns.
* The `foreign_key` section defines:

  * Which column is looked up from which table
  * Whether it supports multiple values (comma separated)
  * What format to store (CSV, JSON array, or nested array)

**Example:**

```python
"City": {
  "table": "city",
  "columns": {
    "City Name": "city_name",
    "City Country": "country_id"
  },
  "foreign_key": {
    "City Country": {
      "lookup_table": "country",
      "lookup_column": "country_name",
      "target_column": "id"
    }
  }
}
```

---

## 📦 How It Works

The script:

1. Opens the Excel file
2. Iterates through each sheet
3. Renames and cleans columns
4. Converts `yes/no` style flags to integers
5. Resolves foreign keys based on other table data
6. Handles custom formats like:

   * Grouped JSON data (e.g., itineraries)
   * Flights (departure/arrival airports per day)
7. Inserts or updates rows in MySQL

---

## 💻 Usage

```bash
python your_script.py /path/to/master_template.xlsx
```

**Example:**

```bash
python tour_data.py C:\Users\YourName\Documents\master_template.xls
```

* The script will print logs like:

  ```
  ⚠️ FK not found: 'France' in country.country_name
  Inserted 10 rows into hotel table
  ```

---

## 📌 Notes

* Make sure your Excel column names exactly match what is defined in `SHEET_MAPPINGS`.
* Foreign key values must already exist in their lookup tables (otherwise a warning is shown).
* The `UNIQUE_KEYS` mapping determines which column is used for upsert checks.
* `INT_COLUMNS` specifies which columns are treated as flags (converted to 0/1).

---

## 📂 Key Components

| Function              | Description                                     |
| --------------------- | ----------------------------------------------- |
| `resolve_fk()`        | Resolves single or multi foreign key references |
| `handle_json_group()` | Groups rows by a key and saves as nested JSON   |
| `upsert_row()`        | Performs insert-or-update by unique key         |
| `import_sheet()`      | Main logic to import a single sheet             |
| `get_db_connection()` | Creates and returns a MySQL connection          |

---

## 📧 Troubleshooting

* **Missing `xlrd` error**

  ```bash
  pip install xlrd>=2.0.1
  ```

* **Foreign key not found**
  Ensure the referenced row already exists in its table.

* **MySQL connection errors**
  Double-check your `DB_HOST`, `DB_USER`, `DB_PASS`, and `DB_NAME` settings.


