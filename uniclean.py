import pandas as pd
from fuzzywuzzy import process, fuzz

# Load your datasets (replace the file paths with your actual file paths)
left_df = pd.read_csv('GIAS Standard Extract - 07-02-2024.csv', encoding_errors='ignore')  # The left dataset you want to keep
student_df = pd.read_csv('dt051-table-1-studentnumbers.csv',encoding_errors='ignore')  # The right dataset you're joining with
energy_df = pd.read_csv('dt042-table-2-energyuse.csv',encoding_errors='ignore')  # The right dataset you're joining with

right_df = pd.merge(energy_df, student_df, on="HE provider",how="outer")

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

merged_df['StudentNumbers'] = merged_df['Total'].str.replace(',', '', regex=False)  # Remove commas

# List of columns you want to apply the transformation to
columns_to_convert = ["StudentNumbers","Total energy consumption (kWh)","Total generation of electricity exported to grid (kWh)","Total renewable energy generated onsite or offsite (kWh)"]  # Replace with actual column names

# Iterate over each column and apply the transformations
for col in columns_to_convert:
    if col in merged_df.columns:
        # Remove commas and convert to numeric
        merged_df[col] = merged_df[col].str.replace(',', '', regex=False)  # Remove commas
        merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')  # Convert to numeric
        merged_df[col].fillna(0, inplace=True)  # Optionally fill NaN values with 0 (or any default value)
    else:
        print(f"Column {col} is missing in the dataframe.")
#choose relevant columns

#make text object
for col in merged_df.columns:
    # If the column is not numeric, convert it to string
    if not pd.api.types.is_numeric_dtype(merged_df[col]):
        merged_df[col] = merged_df[col].astype(str).str.replace(',', '', regex=False)
        merged_df[col] = merged_df[col].str.replace('"', '', regex=False)

        merged_df['StudentNumbers'] = merged_df['Total'].str.replace(',', '', regex=False)

minimum_df=merged_df[["LA (name)","EstablishmentName","HE provider","Postcode","StudentNumbers","Total energy consumption (kWh)","Total generation of electricity exported to grid (kWh)","Total renewable energy generated onsite or offsite (kWh)"]]
# Export the result to a new CSV file
minimum_df.to_csv('merged_output.csv', index=False)

print("Merge completed and saved as 'merged_output.csv'")
