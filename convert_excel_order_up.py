import pandas as pd
import json
import os

# --- Configuration ---
EXCEL_FILE_NAME = "OrderUpQuestionList.xlsx"  # Name of your Excel file
SHEET_NAME = "Sheet1"                         # Name of the sheet containing the questions
OUTPUT_JSON_FILE_NAME = "order_up_questions.json" # Name of the output JSON file

# Column names in your Excel file (adjust if different)
QUESTION_COLUMN = "Question"
ITEM_1_COLUMN = "Item1_Correct"
ITEM_2_COLUMN = "Item2_Correct"
ITEM_3_COLUMN = "Item3_Correct"
ITEM_4_COLUMN = "Item4_Correct"

def convert_excel_to_order_up_json():
    """
    Reads data from an Excel file and converts it into the JSON format
    for the "Order Up!" game round.
    """
    try:
        # Construct the full path to the Excel file
        excel_file_path = os.path.join(os.path.dirname(__file__), EXCEL_FILE_NAME)
        print(f"Attempting to read Excel file from: {excel_file_path}")

        # Read the specified sheet from the Excel file
        df = pd.read_excel(excel_file_path, sheet_name=SHEET_NAME)
        print(f"Successfully read {len(df)} rows from sheet '{SHEET_NAME}'.")

    except FileNotFoundError:
        print(f"ERROR: Excel file '{EXCEL_FILE_NAME}' not found at '{excel_file_path}'.")
        print("Please ensure the Excel file is in the same directory as this script.")
        return
    except Exception as e:
        print(f"ERROR: Could not read Excel file. Details: {e}")
        return

    all_questions_data = []

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        try:
            question = str(row[QUESTION_COLUMN]).strip()
            item1 = str(row[ITEM_1_COLUMN]).strip()
            item2 = str(row[ITEM_2_COLUMN]).strip()
            item3 = str(row[ITEM_3_COLUMN]).strip()
            item4 = str(row[ITEM_4_COLUMN]).strip()

            # Basic validation: ensure no essential fields are empty
            if not all([question, item1, item2, item3, item4]):
                print(f"WARNING: Skipping row {index + 2} due to missing data.")
                continue

            question_data = {
                "question": question,
                "items_in_correct_order": [
                    item1,
                    item2,
                    item3,
                    item4
                ]
            }
            all_questions_data.append(question_data)

        except KeyError as e:
            print(f"ERROR: Missing expected column in Excel: {e}. Please check column names.")
            print(f"Expected columns: '{QUESTION_COLUMN}', '{ITEM_1_COLUMN}', '{ITEM_2_COLUMN}', '{ITEM_3_COLUMN}', '{ITEM_4_COLUMN}'")
            return # Stop processing if columns are incorrect
        except Exception as e:
            print(f"WARNING: Skipping row {index + 2} due to an error: {e}")
            continue

    # Write the data to the JSON file
    try:
        output_json_path = os.path.join(os.path.dirname(__file__), OUTPUT_JSON_FILE_NAME)
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(all_questions_data, f, indent=2, ensure_ascii=False)
        print(f"\nSuccessfully converted {len(all_questions_data)} questions to '{output_json_path}'.")
        if not all_questions_data:
            print("WARNING: No data was written to the JSON file. Check Excel content and column names.")

    except Exception as e:
        print(f"ERROR: Could not write JSON file. Details: {e}")

if __name__ == "__main__":
    print("Starting Order Up! Excel to JSON conversion...")
    convert_excel_to_order_up_json()
    print("Conversion process finished.")