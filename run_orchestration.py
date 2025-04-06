
import os
# Make sure pandas is imported if your functions return/accept DataFrames directly
# Although in the @tool definition we aim for basic types if possible
import pandas as pd
#from langchain_core.tools import tool
# Using RunnableLambda for simpler function wrapping if needed, but @tool is often cleaner
from langchain_core.tools import tool
import argparse
from dotenv import load_dotenv
from typing import List, Dict

# Import the functions from your logic file
# Ensure 'orchestration_logic.py' is in the same directory or Python path
try:
    from orchestration_logic import load_sap_data, find_anomalous_repairs, write_results_to_file
except ImportError:
    print("Error: Could not import functions from orchestration_logic.py.")
    print("Make sure orchestration_logic.py is in the same directory.")
    exit() # Stop if we can't import our core logic

load_dotenv() # Load environment variables if needed

# --- Define LangChain Tools ---
# Wrap your Python functions using the @tool decorator.
# This makes them recognizable as components LangChain can use.

@tool
def excel_data_loader(excel_file_path: str) -> dict:
    """Loads data from an Excel file specified by the path and returns it as a dictionary containing the DataFrame."""
    print(f"--- Tool: excel_data_loader running with input: {excel_file_path} ---")
    df = load_sap_data(excel_file_path)
    # LangChain tools often work best passing dictionaries between steps.
    # We package the DataFrame inside a dictionary.
    # Important: Ensure the key ('dataframe' here) is consistent with what the next tool expects.
    return {"dataframe": df}

@tool
def repair_anomaly_analyzer(dataframe: pd.DataFrame) -> dict: # Output is still dict, but content changed
    """Analyzes the DataFrame for anomalous repair numbers, providing detailed reasons.""" # Updated docstring
    print("--- Tool: repair_anomaly_analyzer running ---")
    if dataframe is None or not isinstance(dataframe, pd.DataFrame) or dataframe.empty:
        print("Analyzer received no valid DataFrame.")
        return {"flagged_details": []} # Change key name

    # Call the updated logic function which returns list[dict]
    flagged_details_list = find_anomalous_repairs(dataframe)

    # Pass the detailed results in a dictionary for the next step
    # Change the key to reflect the new content
    return {"flagged_details": flagged_details_list} # Use the new key

@tool
def results_outputter(flagged_details: List[Dict]) -> str: # Expect List[Dict], match key from previous tool
    """Writes the detailed flagged repair information to a file and returns a status message.""" # Updated docstring
    print("--- Tool: results_outputter running ---")

    # Define the final output file name (maybe change default)
    output_file = "flagged_repairs_detailed.txt"

    # Use the flagged_details input directly, which is now list[dict]
    status_message = write_results_to_file(flagged_details, output_file) # Call updated logic function

    return status_message

# --- Define the Orchestration Chain using LangChain Expression Language (LCEL) ---

# This defines the sequence: Loader -> Analyzer -> Outputter
# The pipe symbol (|) chains the tools together. LangChain automatically handles
# passing the output dictionary of one tool as the input dictionary to the next.
# The chain expects the initial input (the file path string) for the first tool.

chain = excel_data_loader | repair_anomaly_analyzer | results_outputter

# Alternative using RunnableSequence (does the same thing, sometimes more explicit):
# chain = RunnableSequence(first=excel_data_loader, middle=[repair_anomaly_analyzer], last=results_outputter)

# --- Run The Chain --
if __name__ == "__main__":
    # --- Argument Parsing Setup ---
    parser = argparse.ArgumentParser(description="Run SAP Repair Anomaly Detection Orchestration.")
    parser.add_argument("input_excel_file", # This is the name of the argument internally
                        metavar="INPUT_EXCEL_FILE", # How it's shown in help messages
                        type=str,
                        help="Path to the input Excel file (e.g., synthetic_sap_data.xlsx)")
    args = parser.parse_args()
    input_excel_file = args.input_excel_file

    print(f"\n--- Starting LangChain Orchestration for file: {input_excel_file} ---")
    final_result=chain.invoke(input_excel_file)

    # Basic Error Handling: Check if input file exists before running
    if not os.path.exists(input_excel_file):
        print(f"ERROR: Input file not found: {input_excel_file}")
        print("Please ensure the file path is correct.")
        # Optionally add: parser.print_help()

    else:
        try:
            # Invoke the chain. Input/Output handling between tools is managed by LCEL
            print("\n--- Invoking LangChain Chain ---")
            final_result = chain.invoke(input_excel_file)

            print("\n--- LangChain Orchestration Finished ---")
            print(f"Final Status: {final_result}")
            # Update the reminder message for the new output file name
            print(f"Check the output file: flagged_repairs_detailed.txt")

        except Exception as e:
            print(f"\n--- An error occurred during LangChain chain execution: ---")
            print(e)