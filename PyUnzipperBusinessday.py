import datetime
import zipfile
import os
import sys

def get_previous_business_day():
    today = datetime.date.today()
    offset = 1 if today.weekday() > 0 else 3  # Monday or after: 1 day back, Sunday: 3 days back
    return today - datetime.timedelta(days=offset)

def unzip_files_for_date(date, source_directory, destination_directory):
    date_str = date.strftime('%Y-%m-%d')
    zip_file_name = f"{date_str}.zip"
    zip_file_path = os.path.join(source_directory, zip_file_name)

if os.path.exists(zip_file_path):
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(destination_directory)
        print(f"Unzipped files from {zip_file_name} into {destination_directory}")
    else:
        print(f"No zip file found for {date_str}")

def unzip_files_for_date_range(start_date, end_date, source_directory, destination_directory):
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday to Friday
            unzip_files_for_date(current_date, source_directory, destination_directory)
        current_date += datetime.timedelta(days=1)

if __name__ == "__main__":
    source_dir = '/path/to/your/zipfiles'
    destination_dir = '/path/to/unzip/to'

    # Check for command line arguments for specific dates or date range
    if len(sys.argv) == 2:
        # Unzip for previous business day if no date is provided
        previous_business_day = get_previous_business_day()
        unzip_files_for_date(previous_business_day, source_dir, destination_dir)
    elif len(sys.argv) == 3:
        # Unzip for a specific date range
        start_date = datetime.datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
        end_date = datetime.datetime.strptime(sys.argv[2], '%Y-%m-%d').date()
        unzip_files_for_date_range(start_date, end_date, source_dir, destination_dir)
    else:
        print("Usage: python script.py [start_date] [end_date]")
        print("Dates should be in YYYY-MM-DD format.")