import re
import time
import os
import boto3
from dotenv import load_dotenv

def extract_pii_simple(text):
    # Regex patterns for PII
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    phone_pattern = r'\b(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})\b'
    credit_card_pattern = r'\b(?:\d[ -]*?){13,16}\b'
    
    # Find all matches in the text
    emails = re.findall(email_pattern, text)
    phones = re.findall(phone_pattern, text)
    credit_cards = re.findall(credit_card_pattern, text)
    
    return emails, phones, credit_cards

def read_file_from_s3(s3, bucket_name, file_key):
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    return obj['Body'].read().decode('utf-8')

def get_file_size_from_s3(s3, bucket_name, file_key):
    obj = s3.head_object(Bucket=bucket_name, Key=file_key)
    return obj['ContentLength']

def list_files_in_bucket(s3, bucket_name):
    objects = s3.list_objects_v2(Bucket=bucket_name)
    return [obj['Key'] for obj in objects.get('Contents', [])]

def save_results_to_s3(s3, bucket_name, file_key, content):
    s3.put_object(Bucket=bucket_name, Key=file_key, Body=content)

def main():
    start_time_init = time.time()

    source_bucket_name = "random-pii-text"
    destination_bucket_name = "pii-scan-output"

    # Load environment variables from .env file
    load_dotenv()
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    
    file_keys = list_files_in_bucket(s3, source_bucket_name)
    
    for file_key in file_keys:
        start_time = time.time()
        text = read_file_from_s3(s3, source_bucket_name, file_key)
        
        # Get the total file size
        file_size = get_file_size_from_s3(s3, source_bucket_name, file_key)
        
        emails_found, phones_found, credit_cards_found = extract_pii_simple(text)
        
        # Calculate the size of the extracted PII data
        pii_data_size = sum(len(item) for item in emails_found + phones_found + credit_cards_found)
        
        extract_time = time.time() - start_time
        
        # Create the results content
        results_content = (
            f"Emails: {emails_found}\n"
            f"Phone Numbers: {phones_found}\n"
            f"Credit Card Numbers: {credit_cards_found}\n"
            f"Total file size: {file_size} bytes\n"
            f"Size of extracted PII data: {pii_data_size} bytes\n"
            f"Time taken to extract PII: {extract_time:.4f} seconds\n"
        )
        
        # Remove the .txt extension and create the result file key
        base_filename = os.path.splitext(os.path.basename(file_key))[0]
        result_file_key = f"{base_filename}_pii_results.txt"
        
        # Save the results to the destination bucket
        save_results_to_s3(s3, destination_bucket_name, result_file_key, results_content)
        print(f"scan result uploaded for {base_filename}")
        print(f"time taken: {(time.time() - start_time):.4f} seconds")

    print(f"total time taken: {(time.time() - start_time_init):.4f} seconds")

if __name__ == "__main__":
    main()