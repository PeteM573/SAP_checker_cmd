import pandas as pd
import os
from collections import Counter 

def load_sap_data(excel_file_path: str) -> pd.DataFrame:
    """
    Loads data from the specified Excel file path into a pandas DataFrame.
    Assumes the relevant data is on the first sheet.
    """
    try:
        df = pd.read_excel(excel_file_path)
        print(f"Successfully loaded data from {excel_file_path}")
        # Optional: Add basic validation (e.g., check for required columns)
        required_cols = ['Repair Number', 'Movement Code']
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Excel file missing required columns: {required_cols}")
        print("Required columns found.")
        return df
    except FileNotFoundError:
        print(f"Error: File not found at {excel_file_path}")
        # In a real app, you might raise the error or return None/empty DataFrame
        # For the sprint, printing and returning an empty DF might be okay to avoid stopping flow
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while loading the Excel file: {e}")
        return pd.DataFrame()
        
def find_anomalous_repairs(data: pd.DataFrame) -> list[str]:
    # Return type changed
    """
    Identifies and categorizes anomalies for Repair Numbers.
    An anomaly occurs if a Repair Number does not have exactly one
    of each movement code (251, 161, 252) and a total count of 3 transactions.

    Args:
        data: Pandas DataFrame loaded from the SAP export.
              Requires 'Repair Number' and 'Movement Code' columns.

    Returns:
        A list of dictionaries. Each dictionary represents an anomalous
        repair and contains 'Repair Number', 'Reason', and 'Movement Codes Found'.
        Returns an empty list if no anomalies are found or data is invalid.
    """
    if data.empty or 'Repair Number' not in data.columns:
        print("Warning: Input data is empty or missing 'Repair Number' column. Cannot process.")
        return []
    anomalous_details = []
    required_codes = {251, 161, 252} # Define the expected codes
    # Group by 'Repair Number'
    grouped = data.groupby('Repair Number')
    print(f"--- Analyzing {len(grouped)} unique Repair Numbers ---")
    
    for repair_num, group_df in grouped:
        movement_codes = group_df['Movement Code'].tolist()
        total_count = len(movement_codes)
        code_counts = Counter(movement_codes) # Count occurrences of each code

        # Check conditions for a "perfect" repair
        is_perfect = (
            total_count == 3 and
            code_counts[251] == 1 and
            code_counts[161] == 1 and
            code_counts[252] == 1
        )

        if not is_perfect:
            # It's anomalous, now figure out why
            reasons = []
            if total_count != 3:
                reasons.append(f"Total count is {total_count} (expected 3)")

            # Check counts for specific required codes
            if code_counts[251] != 1:
                reasons.append(f"Count 251 is {code_counts.get(251, 0)} (expected 1)")
            if code_counts[161] != 1:
                reasons.append(f"Count 161 is {code_counts.get(161, 0)} (expected 1)")
            if code_counts[252] != 1:
                reasons.append(f"Count 252 is {code_counts.get(252, 0)} (expected 1)")

            # Check for unexpected codes (if total count was 3 but specific counts were wrong/extra codes)
            unexpected_codes = set(movement_codes) - required_codes
            if unexpected_codes:
                 reasons.append(f"Contains unexpected codes: {list(unexpected_codes)}")

            # Ensure we always have a reason if flagged
            if not reasons and total_count != 3 : # Fallback if specific checks didn't add reason but count was wrong
                 reasons.append("Transaction count != 3, specific required code counts may be correct but extras exist")
            elif not reasons: # Should ideally not happen if logic is sound
                 reasons.append("Unknown anomaly (check logic)")


            anomaly_info = {
                "Repair Number": repair_num,
                "Reason": ", ".join(reasons) if reasons else "Anomaly detected but reason unclear",
                "Movement Codes Found": dict(code_counts) # Store the actual counts found
            }
            anomalous_details.append(anomaly_info)

    print(f"Found {len(anomalous_details)} anomalous repair numbers with details.")
    return anomalous_details

def write_results_to_file(detailed_anomalies: list[dict], output_file_path: str = "flagged_repairs_detailed.txt") -> str: # Changed input type and default filename
    """
    Writes the list of detailed anomalies to a text file.
    Handles the case where the list is empty.
    Returns a status message.
    """
    try:
        output_dir = os.path.dirname(output_file_path)
        if output_dir and not os.path.exists(output_dir):
             os.makedirs(output_dir)

        with open(output_file_path, 'w') as f:
            if not detailed_anomalies:
                f.write("No anomalous repair numbers found based on the detailed criteria.\n")
                status_message = f"No anomalies found. Results summary written to {output_file_path}"
            else:
                f.write("Flagged Repair Numbers for Investigation (Detailed):\n")
                f.write("="*60 + "\n")
                for anomaly in detailed_anomalies:
                    f.write(f"Repair Number: {anomaly.get('Repair Number', 'N/A')}\n")
                    f.write(f"  Reason(s):   {anomaly.get('Reason', 'N/A')}\n")
                    # Convert counts dict to a more readable string
                    counts_str = ", ".join([f"Code {k}: {v}" for k, v in anomaly.get('Movement Codes Found', {}).items()])
                    f.write(f"  Codes Found: {counts_str}\n")
                    f.write("-"*60 + "\n") # Separator for readability
                f.write(f"\nTotal flagged: {len(detailed_anomalies)}\n")
                status_message = f"Found {len(detailed_anomalies)} anomalies with details. Results written to {output_file_path}"

        print(status_message)
        return status_message
    except Exception as e:
        error_message = f"Error writing detailed results to file {output_file_path}: {e}"
        print(error_message)
        return error_message


# --- Update the test section ---
if __name__ == "__main__":
    test_file = 'synthetic_sap_data.xlsx'
    print("--- Testing Data Loading ---")
    data = load_sap_data(test_file)
    flagged_list = [] # Initialize empty list

    if not data.empty:
        print("\n--- Data Loaded (First 5 Rows): ---")
        print(data.head())
        print("\n--- Testing Anomaly Detection ---")
        flagged_list = find_anomalous_repairs(data)
        print("\n--- Flagged Repair Numbers (from function): ---")
        print(flagged_list)
        print("\n--- Testing Output Writing ---")
        output_message = write_results_to_file(flagged_list, "test_output.txt") # Use a test filename
        print(f"Output function returned: {output_message}")
        # Simple check against our example ground truth
        expected_anomalies = ['R100024', 'R100058', 'R100327'] # Based on example data
        # Sort both lists for consistent comparison
        flagged_list.sort()
        expected_anomalies.sort()
        if flagged_list == expected_anomalies:
             print("\nSUCCESS: Output matches expected anomalies for the example data!")
        else:
             print(f"\nMISMATCH: Output was {flagged_list}, expected {expected_anomalies}")