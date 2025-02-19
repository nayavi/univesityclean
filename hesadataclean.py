import pandas as pd

hesa_df = pd.read_csv("HESA data all.csv")  # Columns: Hospital, NHS_Trust

print(hesa_df.columns)

# Use pivot_table to handle duplicate indices, and apply an aggregation function (e.g., 'sum', 'mean')
hesa_df_pivot = pd.pivot_table(hesa_df, 
                               index='HE Provider', 
                               columns='Category', 
                               values='Value', 
                               aggfunc='sum')  # You can change 'sum' to other functions like 'mean', etc.


hesa_df_pivot.to_csv("pivoted_hesa_data.csv")
