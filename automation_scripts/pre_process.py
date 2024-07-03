import pandas as pd
import re

def find_pattern_1(text):
    pattern = r'([a-z0-9]+)([A-Z])'
    corrected_text = re.sub(pattern, r'\1. \2', text)
    return corrected_text

def find_pattern_2(text):
    pattern = r'([a-z])\.([A-Z])'
    corrected_text = re.sub(pattern, r'\1. \2', text)
    return corrected_text

def remove_quotes(text):
    pattern = r'[“”"‘’„‟❛❜❝❞\']'
    return re.sub(pattern, '', text)




df = pd.read_excel('combined.xlsx')
df = df.dropna(subset=['content'])

for index,row in df.iterrows():
    text = df.loc[index, 'content']
    if pd.isna(text):
        continue

    text = remove_quotes(text)
    text = find_pattern_1(text)
    text = find_pattern_2(text)
    df.loc[index, 'content'] = text

df.to_excel("combined_modeling.xlsx", index=False)