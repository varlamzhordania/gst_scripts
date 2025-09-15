import sys
import json
import pandas as pd
import mysql.connector

DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASS = '<PASSWORD>'
DB_NAME = 'gst_scripts'

# ----------------------------
# Sheet to table & column mappings
# ----------------------------
SHEET_MAPPINGS = {
    "Country": {
        "table": "country",
        "columns": {
            "Country Name": "country_name",
            "Time Zone*": "country_time_zone",
            "Country Code*": "country_area_code",
            "include in website": "include",
            "Website subtitle": "title",
            "Website description": "description",
            "country cover image": "image",
        }
    },
    "GTI Category": {
        "table": "gti_category",
        "columns": {
            "Category Name": "category_name",
            "Category Title": "title",
            "Category Description": "description",
            "Countries": "countries",
            "Cover Image": "image"
        },
        "foreign_key": {
            "Countries": {
                "lookup_table": "country",
                "lookup_column": "country_name",
                "target_column": "id",
                "multi": True,
                "sep": ",",
                "fk_return_type": "csv",
            }
        }
    },
    "Hotels": {
        "table": "hotel",
        "columns": {
            "Hotel Name*": "hotel_name",
            "Hotel Address*": "hotel_address",
            "Hotel City*": "hotel_city",
            "Hotel Country*": "hotel_country",
            "Hotel Phone*": "hotel_phone",
            "Hotel Email*": "hotel_email",
            "Password*": "password",
            "Hotel description*": "description",
            "Hotel features": "hotel_features",
            "Room Features": "room_features",
            "Hotel map*": "hotel_map",
            "single bedroom price*": "single",
            "double bedroom Price": "doubleBd",
            "triple bedroom Price": "triple",
            "Profile image*": "image"
        }
    },
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
    },
    "Guide": {
        "table": "guide",
        "columns": {
            "First Name*": "guide_first_n",
            "Middle Name*": "guide_m_name",
            "Last Name*": "guide_l_name",
            "Fees per Day*": "guide_fees_per_day",
            "Phone*": "guide_phone",
            "Email*": "guide_email",
            "City*": "guide_city",
            "Country*": "guide_country",
            "Password*": "password",
            "guide bio*": "guide_address",
            "Profile image*": "image"
        },
    },
    "transports suplier": {
        "table": "supplier",
        "columns": {
            "Supplier Name": "name",
            "City": "city",
            "Country": "country",
            "Phone": "phone",
            "Email": "email",
            "supplier type": "type",
            "Profile image": "image"
        }
    },
    "transports cost": {
        "table": "transport_cost",
        "columns": {
            "Route name*": "route_name",
            "Supplier": "supplier",
            "Transportation Cost*": "cost",
            "Max guests per service*": "max_persons"
        },
        "foreign_key": {
            "Supplier": {
                "lookup_table": "supplier",
                "lookup_column": "name",
                "target_column": "id"
            }
        }
    },
    "Sights": {
        "table": "sight",
        "columns": {
            "Sight Name*": "sight_name",
            "Country*": "country_id",
            "City*": "city_id",
            "Sight Email*": "sight_email",
            "Entrance Fees*": "sight_entrance_fees",
            "National Pass*": "sight_national_pass",
            "Sight Phone*": "sight_phone_number",
            "Description*": "sights_description"
        },
        "foreign_key": {
            "Country*": {
                "lookup_table": "country",
                "lookup_column": "country_name",
                "target_column": "id"
            },
            "City*": {
                "lookup_table": "city",
                "lookup_column": "city_name",
                "target_column": "id"
            },
        }
    },
    "Airport": {
        "table": "airports",
        "columns": {
            "Airport Name": "airport_name",
            "Airport IATA Name": "iata_code",
            "Country": "country_id"
        },
        "foreign_key": {
            "Country": {
                "lookup_table": "country",
                "lookup_column": "country_name",
                "target_column": "id"
            }
        }
    },
    "Air ticket": {
        "table": "airline_ticket",
        "columns": {
            "range start": "range_start",
            "range end*": "range_end",
            "Ticket total Price*": "ticket_price",
            "Airline": "airline",
            "Ticket title*": "ticket_title",
        },
        "custom_flights": {
            "Departure Airport": "departure_airport",
            "Arrival Airport": "arrival_airport",
            "Tour Day": "tour_day"
        }
    },
    "General Tour Itinerary": {
        "table": "gti",
        "columns": {
            "GTI Name*": "gti_name",
            "GTI Total Days*": "gti_total_days",
            "Meals included": "meals",
            "Tour Regular Price": "tour_price",
            "Tour Price for pastors": "tour_price_pastors",
            "Tour Image": "tour_image",
            "include in website": "include",
            "Tour description*": "description",
            "hotels": "hotels",
            "Guides": "guides",
            "Tour Map Image": "map",
            "Tour notes": "notes",
            "Category": "category",  # optional if you have categories
            "flights": "flights",
            "Sights": "sights",
            "Transport routes": "transport"
        },
        "foreign_key": {
            "Category": {
                "lookup_table": "gti_category",
                "lookup_column": "category_name",
                "target_column": "id",
                "multi": False,
                "sep": "",
                "fk_return_type": "csv"
            },
            "hotels": {
                "lookup_table": "hotel",
                "lookup_column": "hotel_name",
                "target_column": "id",
                "multi": True,
                "sep": ",",
                "fk_return_type": "nested_array"  # [["1","2"]]
            },
            "Guides": {
                "lookup_table": "guide",
                "lookup_column": "guide_email",
                "target_column": "id",
                "multi": True,
                "sep": ",",
                "fk_return_type": "nested_array"
            },
            "flights": {
                "lookup_table": "airline_ticket",
                "lookup_column": "ticket_title",
                "target_column": "id",
                "multi": True,
                "sep": ",",
                "fk_return_type": "csv"  # 1,2
            },
            "Sights": {
                "lookup_table": "sight",
                "lookup_column": "sight_name",
                "target_column": "id",
                "multi": True,
                "sep": ",",
                "fk_return_type": "csv"
            },
            "Transport routes": {
                "lookup_table": "transport_cost",
                "lookup_column": "route_name",
                "target_column": "id",
                "multi": True,
                "sep": ",",
                "fk_return_type": "csv"
            },
        }
    },
    "GTI itineraries": {
        "table": "gti_itinerary",
        "json_group": {
            "group_by": "gti_name",
            "fk_table": "gti",
            "fk_lookup_column": "gti_name",
            "fk_column": "gti_id",
            "json_column": "tour_itinerary",
            "columns": ["Activity Name", "Activity Country",
                        "Activity City", "Activity image (URL)",
                        "Activity Description"],
            "column_mapping": {
                "Activity Name": "title",
                "Activity Country": "country",
                "Activity City": "city",
                "Activity Description": "description",
                "Activity image (URL)": "image"
            }
        },
        "columns": {
            "GTI Name": "gti_name"
        }
    }
}

UNIQUE_KEYS = {
    "country": "country_name",
    "gti_category": "category_name",
    "hotel": "hotel_name",
    "city": "city_name",
    "guide": "guide_email",
    "supplier": "name",
    "transport_cost": "route_name",
    "sight": "sight_name",
    "airports": "iata_code",
    "airline_ticket": "ticket_title",
    "gti": "gti_name",
    "gti_itinerary": "gti_id",
}

INT_COLUMNS = {
    "country": ["include"],
    "gti": ["include"]
}


# ----------------------------
# Database connection
# ----------------------------
def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
    )


# ----------------------------
# Core Import Logic
# ----------------------------
def to_int_flag(value):
    if pd.isna(value):
        return 0
    if isinstance(value, str):
        value = value.strip().lower()
        if value in ("yes", "1", "true", "include"):
            return 1
        else:
            return 0
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def resolve_fk(
        cursor,
        lookup_table,
        lookup_column,
        target_column,
        value,
        multi=False,
        sep=",",
        return_type="json_array"
        # options: "csv", "json_array", "nested_array"
):
    """
    Resolve a foreign key or list of foreign keys.
    return_type controls the format:
      - "csv": 1,2,3
      - "json_array": [1,2,3]
      - "nested_array": [["1",0],["2",1],["3",2]]
    """
    if not value:
        return None

    if multi:
        ids = []
        for v in str(value).split(sep):
            v = v.strip()
            if not v:
                continue
            cursor.execute(
                f"SELECT {target_column} FROM {lookup_table} WHERE {lookup_column} = %s",
                (v,)
            )
            result = cursor.fetchone()
            if result:
                ids.append(result[0])
            else:
                print(
                    f"⚠️ FK not found: '{v}' in {lookup_table}.{lookup_column}"
                )

        if not ids:
            return None

        if return_type == "csv":
            return ",".join(str(i) for i in ids)
        elif return_type == "json_array":
            return json.dumps(ids)
        elif return_type == "nested_array":
            # convert to [["1", index], ...]
            nested = [[str(val), i] for i, val in enumerate(ids)]
            return json.dumps(nested)

        else:
            raise ValueError(f"Unknown return_type: {return_type}")

    else:
        # single value
        cursor.execute(
            f"SELECT {target_column} FROM {lookup_table} WHERE {lookup_column} = %s",
            (value,)
        )
        result = cursor.fetchone()
        return result[0] if result else None


def handle_json_group(sheet_name, df, cursor, db):
    mapping = SHEET_MAPPINGS[sheet_name]
    table = mapping["table"]
    group_cfg = mapping["json_group"]

    nested_grouped = {}

    for _, row in df.iterrows():
        key = row[group_cfg["group_by"]]  # e.g., GTI Name
        if not key:
            continue

        day_num = int(row["Day No"]) if row.get("Day No") else 1
        day_label = f"Day {day_num}"

        if key not in nested_grouped:
            nested_grouped[key] = {}
        if day_label not in nested_grouped[key]:
            nested_grouped[key][day_label] = []

        # Build JSON object per row
        item = {}
        for excel_col in group_cfg["columns"]:
            json_key = group_cfg.get("column_mapping", {}).get(
                excel_col,
                excel_col
            )
            value = row.get(excel_col)
            item[json_key] = value

        nested_grouped[key][day_label].append(item)

    # Insert JSON into database
    inserted_count = 0
    for key, day_dict in nested_grouped.items():
        fk_id = resolve_fk(
            cursor,
            group_cfg["fk_table"],
            group_cfg["fk_lookup_column"],
            "id",
            key
        )
        if not fk_id:
            print(f"⚠️ FK not found for key: {key}")
            continue

        # Sort days numerically and convert to list of lists
        nested_list = [day_dict[day] for day in sorted(
            day_dict.keys(),
            key=lambda x: int(x.split()[1])
        )]
        json_value = json.dumps(nested_list, ensure_ascii=False)

        cursor.execute(
            f"INSERT INTO {table} ({group_cfg['fk_column']}, {group_cfg['json_column']}) VALUES (%s, %s)",
            (fk_id, json_value)
        )
        inserted_count += 1

    db.commit()
    print(
        f"Inserted {inserted_count} nested JSON group(s) into {table} table"
    )


def upsert_row(cursor, table, unique_col, row_data):
    cols = list(row_data.keys())
    values = list(row_data.values())

    # Check if record exists
    cursor.execute(
        f"SELECT id FROM {table} WHERE {unique_col} = %s",
        (row_data[unique_col],)
    )
    existing = cursor.fetchone()

    if existing:
        # UPDATE
        set_clause = ", ".join([f"`{col}` = %s" for col in cols])
        sql = f"UPDATE {table} SET {set_clause} WHERE {unique_col} = %s"
        cursor.execute(sql, values + [row_data[unique_col]])
    else:
        # INSERT
        col_names = ", ".join([f"`{col}`" for col in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        cursor.execute(sql, values)


def import_sheet(sheet_name, df, cursor, db):
    mapping = SHEET_MAPPINGS[sheet_name]
    table = mapping["table"]

    # Clean column names and rename based on mapping
    df = df.rename(columns=mapping.get("columns", {}))
    df = df.rename(columns=lambda x: x.strip())
    df = df.where(pd.notnull(df), None)

    # Convert int/flag columns
    if table in INT_COLUMNS:
        for col in INT_COLUMNS[table]:
            if col in df.columns:
                df[col] = df[col].apply(to_int_flag)

    # 1️ Handle JSON group sheets
    if "json_group" in mapping:
        handle_json_group(sheet_name, df, cursor, db)
        return

    # 2️ Handle foreign keys (single/multi)
    if "foreign_key" in mapping:
        for orig_col, fk in mapping["foreign_key"].items():
            db_col = mapping["columns"][orig_col]
            return_type = fk.get(
                "fk_return_type",
                "json_array"
            )  # default to JSON array
            for i, val in df.iterrows():
                raw_value = val[db_col]
                df.at[i, db_col] = resolve_fk(
                    cursor,
                    fk["lookup_table"],
                    fk["lookup_column"],
                    fk["target_column"],
                    raw_value,
                    fk.get("multi", False),
                    fk.get("sep", ","),
                    return_type=return_type
                )

    # 3 Handle custom flights (Air ticket)
    if "custom_flights" in mapping:
        base_cols = list(mapping["columns"].values())
        flights_cols = mapping["custom_flights"]

        for _, row in df.iterrows():
            # Split all columns by comma
            departures = [x.strip() for x in
                          str(row["Departure Airport"]).split(",") if
                          x.strip()]
            arrivals = [x.strip() for x in
                        str(row["Arrival Airport"]).split(",") if
                        x.strip()]
            days = [x.strip() for x in str(row["Tour Day"]).split(",") if
                    x.strip()]

            flights = []
            # Loop over all three lists together
            for dep, arr, day in zip(departures, arrivals, days):
                departure_id = resolve_fk(
                    cursor,
                    "airports",
                    "airport_name",
                    "id",
                    dep
                )
                arrival_id = resolve_fk(
                    cursor,
                    "airports",
                    "airport_name",
                    "id",
                    arr
                )
                if departure_id and arrival_id:
                    flights.append([departure_id, arrival_id, day])
                else:
                    print(f"⚠️ Could not resolve flight: {dep} -> {arr}")

            # Insert row
            row_values = [row[col] for col in base_cols] + [
                json.dumps(flights)]
            insert_cols = base_cols + ["flights"]
            placeholders = ", ".join(["%s"] * len(insert_cols))
            col_names = ", ".join([f"`{col}`" for col in insert_cols])
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
            cursor.execute(sql, row_values)

        db.commit()
        print(f"Inserted {len(df)} rows into {table} table")
        return

    # 4️ Upsert rows
    unique_col = UNIQUE_KEYS.get(table)
    for _, row in df.iterrows():
        row_data = {col: row[col] for col in df.columns}
        if unique_col:
            upsert_row(cursor, table, unique_col, row_data)
        else:
            # fallback: pure insert if no unique key defined
            cols = list(row_data.keys())
            vals = list(row_data.values())
            col_names = ", ".join([f"`{c}`" for c in cols])
            placeholders = ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
            cursor.execute(sql, vals)

    db.commit()
    print(f"Inserted {len(df)} rows into {table} table")


def load_excel_sheets(file_path):
    """Return a dict of sheet_name -> DataFrame"""
    all_sheets = pd.read_excel(file_path, sheet_name=None)
    cleaned_sheets = {}
    for sheet_name, df in all_sheets.items():
        clean_name = sheet_name.strip()  # remove trailing spaces
        df.columns = df.columns.str.strip()  # remove spaces in headers
        cleaned_sheets[clean_name] = df
    return cleaned_sheets


# ----------------------------
# Main entry point
# ----------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python import_excel.py <excel_file_path>")
        sys.exit(1)

    excel_path = sys.argv[1]
    print(f"Loading Excel file: {excel_path}")

    all_sheets = load_excel_sheets(excel_path)
    db = get_db_connection()
    cursor = db.cursor(buffered=True)

    for sheet_name, df in all_sheets.items():
        if sheet_name not in SHEET_MAPPINGS:
            print(f"Skipping sheet '{sheet_name}' (no mapping defined)")
            continue

        print(f"Processing sheet: {sheet_name}")
        print(df.columns.tolist())
        import_sheet(
            sheet_name,
            df,
            cursor,
            db
        )

    cursor.close()
    db.close()
    print("All sheets processed.")


if __name__ == "__main__":
    main()
