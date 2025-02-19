import pandas as pd
from fuzzywuzzy import process, fuzz

# Load your datasets (replace the file paths with your actual file paths)
left_df = pd.read_csv('GIAS Standard Extract - 07-02-2024.csv')  # The left dataset you want to keep
right_df = pd.read_csv('dt051-table-1.csv')  # The right dataset you're joining with

# Clean text function (optional, but recommended for fuzzy matching)
def clean_text(text):
    if pd.isna(text):
        return ""
    text = text.lower().strip()  # Clean the text: lowercase and remove extra spaces
    return text

# Apply cleaning to the columns you want to fuzzy match on (example: 'Hospital')
left_df["Cleaned_Column"] = left_df["EstablishmentName"].apply(clean_text)
right_df["Cleaned_Column"] = right_df["HE provider"].apply(clean_text)

# Function for fuzzy matching
def find_best_match(value, choices, threshold=85):
    if pd.isna(value):
        return None
    match = process.extractOne(value, choices, scorer=fuzz.token_sort_ratio)
    if match and match[1] >= threshold:
        return match[0]  # return best match if it meets the threshold
    return None  # return None if no match or match is below the threshold

# Create a list of cleaned values from the right dataset
right_cleaned_list = right_df["Cleaned_Column"].dropna().tolist()

# Apply fuzzy matching to find the best match for each row in the left dataset
left_df["Matched_Column"] = left_df["Cleaned_Column"].apply(lambda x: find_best_match(x, right_cleaned_list))

# Merge the datasets, keeping only rows from the left dataset (left join)
merged_df = pd.merge(left_df, right_df, left_on="Matched_Column", right_on="Cleaned_Column", how="left", suffixes=("_left", "_right"))

# Optionally, drop the temporary matching columns (Cleaned_Column, Matched_Column) if you don't need them
merged_df.drop(columns=["Cleaned_Column_left", "Cleaned_Column_right", "Matched_Column"], inplace=True, errors="ignore")

# Export the result to a new CSV file
merged_df.to_csv('merged_output.csv', index=False)

print("Merge completed and saved as 'merged_output.csv'")
