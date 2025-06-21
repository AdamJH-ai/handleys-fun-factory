# convert_excel_quick_pairs.py
import pandas as pd
import json
import os

# --- Configuration ---
# The name of your Excel file
EXCEL_FILENAME = 'QuickPairsQuestionList.xlsx'
# The name of the sheet within the Excel file to read from
SHEET_NAME = 'Sheet1'
# The name of the output JSON file
JSON_FILENAME = 'quick_pairs_questions.json'
# Expected number of pairs per question
NUM_PAIRS = 3

def convert_excel_to_json():
    """
    Reads a structured Excel file and converts it into a JSON file
    for the "Quick Pairs" game round.
    """
    print(f"--- Starting Quick Pairs Excel to JSON Conversion ---")

    # --- 1. Check if Excel file exists ---
    if not os.path.exists(EXCEL_FILENAME):
        print(f"ERROR: Excel file not found: '{EXCEL_FILENAME}'")
        print("Please make sure the file is in the same directory as this script.")
        return

    print(f"Reading from '{EXCEL_FILENAME}', sheet='{SHEET_NAME}'...")

    try:
        # --- 2. Read the Excel file using pandas ---
        # We specify dtype=str to prevent pandas from auto-formatting numbers (like years)
        df = pd.read_excel(EXCEL_FILENAME, sheet_name=SHEET_NAME, dtype=str)

        # --- 3. Clean up the data ---
        # Fill any empty cells (NaN) with empty strings for easier handling
        df.fillna('', inplace=True)
        # Strip whitespace from all cells
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        print(f"Found {len(df)} rows in the Excel file.")
        
        all_questions = []

        # --- 4. Iterate over each row in the DataFrame ---
        for index, row in df.iterrows():
            category_prompt = row.iloc[0]

            # --- Validation: Skip rows with no category prompt ---
            if not category_prompt:
                print(f"  - Skipping row {index + 2}: No category prompt found.")
                continue

            pairs = []
            is_row_valid = True
            
            # --- 5. Process the pairs for the current row ---
            for i in range(NUM_PAIRS):
                # Calculate column indices for the current pair
                # Pair 1: cols 1, 2. Pair 2: cols 3, 4. Pair 3: cols 5, 6
                item1_col_idx = 1 + (i * 2)
                item2_col_idx = 2 + (i * 2)

                # Check if columns exist in the dataframe
                if item1_col_idx >= len(df.columns) or item2_col_idx >= len(df.columns):
                    print(f"  - ERROR on row {index + 2}: Not enough columns for pair {i + 1}.")
                    is_row_valid = False
                    break
                
                item1 = row.iloc[item1_col_idx]
                item2 = row.iloc[item2_col_idx]

                # --- Validation: Skip pairs with empty items ---
                if not item1 or not item2:
                    print(f"  - ERROR on row {index + 2}: Incomplete pair #{i + 1}. Both items must have values. Skipping this entire row.")
                    is_row_valid = False
                    break
                
                pairs.append([item1, item2])
            
            if not is_row_valid:
                continue

            # --- 6. Assemble the question object ---
            question_obj = {
                "category_prompt": category_prompt,
                "pairs": pairs
            }
            all_questions.append(question_obj)
            print(f"  + Processed row {index + 2}: '{category_prompt[:50]}...'")

        # --- 7. Write the data to a JSON file ---
        print(f"\nSuccessfully processed {len(all_questions)} valid questions.")
        print(f"Writing to '{JSON_FILENAME}'...")

        with open(JSON_FILENAME, 'w', encoding='utf-8') as json_file:
            # Use indent=2 for pretty-printing, making the JSON human-readable
            json.dump(all_questions, json_file, indent=2, ensure_ascii=False)

        print(f"--- Conversion Complete! ---")
        print(f"'{JSON_FILENAME}' has been created/updated successfully.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        print("Please check your Excel file format and sheet name.")

# --- Run the conversion ---
if __name__ == '__main__':
    convert_excel_to_json()