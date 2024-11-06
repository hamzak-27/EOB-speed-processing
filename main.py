import streamlit as st
import os
import pandas as pd
import nest_asyncio
from llama_parse import LlamaParse
import re
from datetime import datetime
import tempfile
from parallel_utils import parallel_process_pdfs

# Apply nest_asyncio
nest_asyncio.apply()

# Set up LlamaParse

llama_parser = LlamaParse(api_key=st.secrets["LLAMA_API_KEY"], result_type="text")


def extract_payment_info(text, paid_amount):
    if float(paid_amount) > 0:
        eft_pattern = r"EFT\s*NUMBER[:\s]*([A-Z0-9\-]+)[\s\S]*?EFT\s*DATE[:\s]*([0-9/]+)[\s\S]*?EFT\s*AMOUNT[:\s]*\$?([0-9,]+\.[0-9]{2})"
        match = re.search(eft_pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            eft_number = match.group(1).strip()
            eft_date = match.group(2).strip()
            eft_amount = match.group(3).strip().replace(',', '')
            return eft_number, eft_date, eft_amount
        else:
            return None, None, None
    else:
        return "N/A", "N/A", "N/A"

def extract_claim_number(text):
    pattern = r"Claim Number\s*[\s\S]*?(\d+)"
    match = re.search(pattern, text, re.DOTALL)
    return match.group(1) if match else None

def extract_grand_totals(text):
    pattern = r'Grand Totals:\s*Other Patient\s*Line Charge\s*Allowed\s*QPA\s*Contractual\s*Payer Initiated\s*OA\s*Copay\s*Deductible\s*Coinsurance\s*Responsibility\s*Withhold\s*Paid\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})\s*(\$\d+\.\d{2})'
    match = re.search(pattern, text)

    categories = ["Line Charge", "Allowed", "QPA", "Contractual", "Payer Initiated", "OA", "Copay",
                  "Deductible", "Coinsurance", "Responsibility", "Withhold", "Paid"]

    if match:
        return dict(zip(categories, match.groups()))
    else:
        return dict(zip(categories, ["$0.00"] * 12))

def extract_corrected_patient_name(text):
    pattern = r"Corrected Patient Name:\s+([A-Za-z,\s]+)"
    matches = re.findall(pattern, text)
    return matches[0] if matches else None

def extract_date_of_service(text):
    date_pattern = r'(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})-'
    matches = re.findall(date_pattern, text)
    return matches[0] if matches else (None, None)

def extract_pdf_data(pdf_path):
    document = llama_parser.load_data(pdf_path)

    if len(document) > 0:
        text = document[0].text
    else:
        print(f"Skipping: No content found in {pdf_path}")
        return None

    claim_number = extract_claim_number(text)
    grand_totals = extract_grand_totals(text)
    start_date, end_date = extract_date_of_service(text)

    grand_totals_mapped = {key: float(value.strip('$')) for key, value in grand_totals.items()}
    total_ptr = grand_totals_mapped["Copay"] + grand_totals_mapped["Coinsurance"] + grand_totals_mapped["Deductible"]
    check_eft_number, eft_date, eft_amount = extract_payment_info(text, grand_totals_mapped["Paid"])
    claim_number_str = str(claim_number) if claim_number else "N/A"
    corrected_patient_name = extract_corrected_patient_name(text)

    data = {
        'Patient Name': corrected_patient_name,
        'Date of Service': start_date if start_date else "N/A",
        'Line Charge': grand_totals_mapped["Line Charge"],
        'Allowed': grand_totals_mapped["Allowed"],
        'Contractual': grand_totals_mapped["Contractual"],
        'Copay': grand_totals_mapped["Copay"],
        'Deductible': grand_totals_mapped["Deductible"],
        'Coinsurance': grand_totals_mapped["Coinsurance"],
        'Paid': grand_totals_mapped["Paid"],
        'Total PTR': total_ptr,
        'Check/EFT Number': check_eft_number,
        'EFT Amount': eft_amount if eft_amount != "N/A" else 0,
        'EFT Date': eft_date,
        'Processed/Denial Date': eft_date,
        'Payer Claim Number': claim_number_str
    }

    return data

def format_currency(value):
    """Format numbers as currency with dollar sign if greater than zero"""
    try:
        if pd.notnull(value) and float(value) > 0:
            return f"${value:,.2f}"
        return f"{value:,.2f}" if pd.notnull(value) else value
    except:
        return value

# Streamlit app UI
st.title('PDF-EOB Processor')

# File uploader
uploaded_files = st.file_uploader(
    "Choose PDF files",
    accept_multiple_files=True,
    type="pdf"
)

if uploaded_files:
    st.write(f"Uploaded {len(uploaded_files)} file(s)")
    
    if st.button('Process PDFs'):
        with st.spinner('Processing PDFs...'):
            # Use parallel processing
            all_data = parallel_process_pdfs(uploaded_files, extract_pdf_data)
            
            if all_data:
                result_df = pd.DataFrame(all_data)
                
                # Process DataFrame
                result_df['Date of Service'] = pd.to_datetime(
                    result_df['Date of Service'],
                    errors='coerce',
                    format='%m/%d/%Y'
                )
                result_df['Date of Service'] = result_df['Date of Service'].dt.date
                
                # Format currency columns
                currency_columns = [
                    'Line Charge', 'Allowed', 'Contractual', 'Copay',
                    'Deductible', 'Coinsurance', 'Paid', 'Total PTR', 'EFT Amount'
                ]
                
                for col in currency_columns:
                    result_df[col] = result_df[col].apply(format_currency)
                
                # Sort and format other columns
                result_df_sorted = result_df.sort_values(by='Date of Service')
                result_df_sorted['Payer Claim Number'] = result_df_sorted['Payer Claim Number'].apply(
                    lambda x: f'{int(x):d}' if pd.notnull(x) and x != "N/A" else x
                )

                st.success('PDFs processed successfully!')
                st.dataframe(result_df_sorted)

                # Generate Excel file
                output = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
                with pd.ExcelWriter(output.name, engine='xlsxwriter') as writer:
                    result_df_sorted.to_excel(writer, sheet_name='Sheet1', index=False)
                    workbook = writer.book
                    worksheet = writer.sheets['Sheet1']
                    
                    # Format headers
                    header_format = workbook.add_format({
                        'bold': True,
                        'bg_color': '#CCFFCC',
                        'border': 1
                    })
                    
                    # Apply formats
                    for col_num, value in enumerate(result_df_sorted.columns.values):
                        worksheet.write(0, col_num, value, header_format)

                # Offer download link
                with open(output.name, "rb") as file:
                    btn = st.download_button(
                        label="Download Excel file",
                        data=file,
                        file_name="processed_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

                # Safely remove the temporary file
                try:
                    os.unlink(output.name)
                except PermissionError:
                    pass
            else:
                st.error("No valid data extracted from PDFs.")
else:
    st.info('Please upload PDF files to process.')

# Sidebar information
st.sidebar.header('About')
st.sidebar.info('This app processes PDF files and extracts relevant information into an Excel file.')