import pandas as pd
from fuzzywuzzy import process, fuzz

# Load your datasets
left_df = pd.read_csv('GIAS Standard Extract - 07-02-2024.csv', encoding_errors='ignore')  
right_df = pd.read_csv('pivoted_hesa_data.csv', encoding_errors='ignore')  

# Clean text function for fuzzy matching
def clean_text(text):
    if pd.isna(text):
        return ""
    return text.lower().strip()

# Apply cleaning to the columns
left_df["Cleaned_Column"] = left_df["EstablishmentName"].apply(clean_text)
right_df["Cleaned_Column"] = right_df["HE Provider"].apply(clean_text)

# Function for fuzzy matching
def find_best_match(value, choices, threshold=85):
    if pd.isna(value):
        return None
    match = process.extractOne(value, choices, scorer=fuzz.token_sort_ratio)
    if match and match[1] >= threshold:
        return match[0]
    return None  

# List of cleaned values from right dataset
right_cleaned_list = right_df["Cleaned_Column"].dropna().tolist()

# Apply fuzzy matching
left_df["Matched_Column"] = left_df["Cleaned_Column"].apply(lambda x: find_best_match(x, right_cleaned_list))

# Merge while ensuring all Scottish universities are retained
merged_df = pd.merge(left_df, right_df, 
                     left_on="Matched_Column", 
                     right_on="Cleaned_Column", 
                     how="outer", 
                     suffixes=("_left", "_right"))

# Ensure all universities from right_df are included, even if they were not matched
merged_df["EstablishmentName"] = merged_df["EstablishmentName"].combine_first(merged_df["HE Provider"])

# Drop temporary columns
merged_df.drop(columns=["Cleaned_Column_left", "Cleaned_Column_right", "Matched_Column"], inplace=True, errors="ignore")

# Select relevant columns, ensuring no Scottish universities are excluded
minimum_df = merged_df[[col for col in [
    'LA (name)', 'EstablishmentName', 'Postcode', 'Region', 'HE Provider', 
    'Biofuels (kWh)', 'Biomass (kWh)', 'Carbon allowance (tonnes CO2e)', 
    'Carbon allowance bought or sold (tonnes CO2e)', 'Coal (industrial) (kWh)', 
    'Compressed natural gas (kWh)', 'Cost (£)', 'Electricity consumed from onsite CHP (kWh)', 
    'Fuel oil (kWh)', 'Gas oil (kWh)', 'Grid electricity (kWh)', 
    'Heat consumed from onsite CHP (kWh)', 'Liquefied natural gas (kWh)', 
    'Liquefied petroleum gas (kWh)', 'Lubricants (kWh)', 
    'Natural gas excluding that used as input for a CHP unit (kWh)', 
    'Natural gas used as input for a CHP unit (kWh)', 'Onsite photovoltaic (kWh)', 
    'Onsite wind (kWh)', 'Other onsite renewables (kWh)', 'Other petroleum gas (kWh)', 
    'Participation', 'Petroleum coke (kWh)', 'Research income (£)', 
    'Steam and hot water (kWh)', 'Teaching income (£)', 'Total energy consumption (kWh)', 
    'Total energy generated onsite by CHP (kWh)', 'Total expenditure (£)', 
    'Total gross internal area (m2)', 'Total income (£)', 
    'Total percentage of renewable energy purchased through green tariffs (%)', 
    'Total renewable energy generated onsite or offsite (kWh)'] if col in merged_df.columns]]

# Save output
minimum_df.to_csv('hesamerged_output.csv', index=False)

print("Merge completed and saved as 'hesamerged_output.csv'")
