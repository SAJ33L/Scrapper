import csv
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font
from openpyxl.utils import get_column_letter
import re

def clean_price(val):
    if not val or val == 'N/A' or val == '-' or val == 'Not listed on competitor':
        return None
    # Extract numeric part
    m = re.search(r'(\d[\d,]*\.\d{2})', val)
    if m:
        try:
            return float(m.group(1).replace(',', ''))
        except ValueError:
            return None
    return None

def main():
    input_file = '/home/saj33l/Hashed/Scrapper/output_top_300.csv'
    output_file = '/home/saj33l/Hashed/Scrapper/output_top_300_formatted.xlsx'

    wb = Workbook()
    ws = wb.active
    ws.title = "Benchmarking Data"

    # Define styles
    teal_fill = PatternFill(start_color="008080", end_color="008080", fill_type="solid")
    red_fill = PatternFill(start_color="FF0000", end_color="FF0000", fill_type="solid")
    white_font = Font(color="FFFFFF")

    with open(input_file, 'r', encoding='utf-8-sig') as f:
        reader = list(csv.reader(f))
        
    if not reader:
        print("Empty CSV")
        return

    headers = reader[0]
    ws.append(headers)

    # find column indices (1-based for openpyxl)
    def get_idx(name):
        try:
            return headers.index(name) + 1
        except ValueError:
            return None

    dmi_eu_idx = get_idx('DMI Sales Price (€)')
    dmi_uk_idx = get_idx('DMI Sales Price (£)')
    
    comp_eu_cols = []
    for c in ['Dontalia Sales Price (€)', 'Henry Schein Sales Price (€)']:
        idx = get_idx(c)
        if idx: comp_eu_cols.append(idx)
        
    comp_uk_cols = []
    for c in ['DentalSky Sales Price (£)']:
        idx = get_idx(c)
        if idx: comp_uk_cols.append(idx)

    # Write data
    for r_idx, row in enumerate(reader[1:], start=2):
        ws.append(row)
        
        # We need to process the formatting based on parsed values
        # Let's get reference values
        dmi_eu_val = clean_price(row[dmi_eu_idx - 1]) if dmi_eu_idx else None
        dmi_uk_val = clean_price(row[dmi_uk_idx - 1]) if dmi_uk_idx else None
        
        # Apply formatting for EU competitors
        if dmi_eu_val is not None:
            for c_idx in comp_eu_cols:
                comp_val = clean_price(row[c_idx - 1])
                if comp_val is not None:
                    cell = ws.cell(row=r_idx, column=c_idx)
                    if comp_val > dmi_eu_val:
                        cell.fill = teal_fill
                        cell.font = white_font
                    elif comp_val < dmi_eu_val:
                        cell.fill = red_fill
                        cell.font = white_font

        # Apply formatting for UK competitors
        if dmi_uk_val is not None:
            for c_idx in comp_uk_cols:
                comp_val = clean_price(row[c_idx - 1])
                if comp_val is not None:
                    cell = ws.cell(row=r_idx, column=c_idx)
                    if comp_val > dmi_uk_val:
                        cell.fill = teal_fill
                        cell.font = white_font
                    elif comp_val < dmi_uk_val:
                        cell.fill = red_fill
                        cell.font = white_font

    # Autofit columns
    for col in ws.columns:
        max_length = 0
        column = get_column_letter(col[0].column)
        for cell in col:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except:
                pass
        adjusted_width = (max_length + 2)
        ws.column_dimensions[column].width = min(adjusted_width, 50) # Cap at 50

    wb.save(output_file)
    print(f"Saved formatted excel to {output_file}")

if __name__ == "__main__":
    main()
