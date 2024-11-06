import concurrent.futures
import os
import tempfile
from typing import List
import streamlit as st

def process_single_pdf(pdf_file, extract_pdf_data_func):
    """Process a single PDF file using the existing extract_pdf_data function"""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_file:
        temp_file.write(pdf_file.getvalue())
        temp_file_path = temp_file.name

    try:
        # Use the existing extract_pdf_data function
        pdf_data = extract_pdf_data_func(temp_file_path)
        return pdf_data
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_file_path)
        except (PermissionError, FileNotFoundError):
            pass

def parallel_process_pdfs(pdf_files: List, extract_pdf_data_func, max_workers=None):
    """
    Process multiple PDFs in parallel using ProcessPoolExecutor
    """
    # Create a progress bar
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Calculate number of workers (use CPU count - 1 to leave one core free)
    if max_workers is None:
        max_workers = max(1, os.cpu_count() - 1)
    
    all_data = []
    total_files = len(pdf_files)
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all PDF processing tasks
        future_to_pdf = {executor.submit(process_single_pdf, pdf_file, extract_pdf_data_func): pdf_file 
                        for pdf_file in pdf_files}
        
        # Process completed tasks as they finish
        for i, future in enumerate(concurrent.futures.as_completed(future_to_pdf)):
            pdf_file = future_to_pdf[future]
            try:
                pdf_data = future.result()
                if pdf_data:
                    all_data.append(pdf_data)
                else:
                    st.warning(f"No data extracted from {pdf_file.name}")
            except Exception as e:
                st.error(f"Error processing {pdf_file.name}: {str(e)}")
            
            # Update progress
            progress = (i + 1) / total_files
            progress_bar.progress(progress)
            status_text.text(f"Processing PDFs: {i+1}/{total_files}")
    
    # Clear progress bar and status text
    progress_bar.empty()
    status_text.empty()
    
    return all_data