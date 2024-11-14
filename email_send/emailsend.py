# send_welcome_emails_gmail.py

import os
import sys
import logging
import argparse
from pymongo import MongoClient
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
from dotenv import load_dotenv
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Configure logging with RotatingFileHandler to prevent log files from growing indefinitely
handler = RotatingFileHandler("send_welcome_emails.log", maxBytes=5*1024*1024, backupCount=5)  # 5MB per file, 5 backups
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        handler,
        logging.StreamHandler(sys.stdout)
    ]
)

# Retrieve environment variables
MONGODB_URI = os.getenv('MONGODB_URI')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE')  # Optional, if not in URI
GMAIL_USER = os.getenv('GMAIL_USER')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')
FROM_NAME = os.getenv('FROM_NAME', 'Your App')

# Validate essential environment variables
if not MONGODB_URI:
    logging.error("MONGODB_URI is not set. Please set it in the .env file.")
    sys.exit(1)

if not GMAIL_USER or not GMAIL_APP_PASSWORD:
    logging.error("GMAIL_USER and/or GMAIL_APP_PASSWORD are not set. Please set them in the .env file.")
    sys.exit(1)

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), 'templates', 'welcome_email_template.html')

def load_email_template():
    """Load the HTML email template from a file."""
    try:
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"Email template file '{TEMPLATE_PATH}' not found.")
        sys.exit(1)


def send_welcome_email(to_email, first_name):
    """Send a personalized welcome email to the user via Gmail SMTP."""
    subject = "Welcome to WildVision!"
    year = datetime.now().year
    html_template = load_email_template()
    html_content = html_template.format(first_name=first_name, year=year)
    
    # Create a MIME message
    message = MIMEMultipart()
    # Properly encode the 'From' header
    message['From'] = formataddr((str(Header(FROM_NAME, 'utf-8')), GMAIL_USER))
    # Properly encode the 'Subject' header
    message['Subject'] = Header(subject, 'utf-8')
    message['To'] = to_email
    message.attach(MIMEText(html_content, 'html', 'utf-8'))

    try:
        # Connect to Gmail's SMTP server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, to_email, message.as_string())
        logging.info(f"Email sent to {to_email} successfully.")
        return True
    except Exception as e:
        logging.error(f"Failed to send email to {to_email}: {e}")
        return False

def send_test_email(test_email):
    """Send a test email to the specified email address."""
    logging.info(f"Sending test email to {test_email}...")
    success = send_welcome_email(test_email, "Test User")
    if success:
        logging.info(f"Test email sent to {test_email} successfully.")
    else:
        logging.error(f"Failed to send test email to {test_email}.")

def process_users(users_collection):
    """Process and send welcome emails to new users."""
    # Define the query to find users who haven't been emailed yet
    # Assumes there is a field 'welcome_email_sent' set to False or missing
    query = {'$or': [{'welcome_email_sent': {'$exists': False}}, {'welcome_email_sent': False}]}

    try:
        # Count the number of users matching the query directly from the collection
        count = users_collection.count_documents(query)
        if count == 0:
            logging.info("No new users to send emails to.")
            return
        logging.info(f"Found {count} users to send welcome emails to.")

        # Fetch the users to email
        users_to_email = users_collection.find(query)
    except Exception as e:
        logging.error(f"Error querying users: {e}")
        sys.exit(1)

    for user in users_to_email:
        email = user.get('email')
        first_name = user.get('firstName', 'User')  # Default to 'User' if firstName is missing

        if not email:
            logging.warning(f"User with ID {user.get('userId', 'N/A')} does not have an email address. Skipping.")
            continue

        success = send_welcome_email(email, first_name)

        if success:
            try:
                # Update the user document to mark the email as sent
                users_collection.update_one(
                    {'_id': user['_id']},
                    {'$set': {'welcome_email_sent': True}}
                )
                logging.info(f"Marked user {email} as emailed.")
            except Exception as e:
                logging.error(f"Failed to update user {email} as emailed: {e}")
        else:
            logging.error(f"Failed to send email to {email}. Will retry in next run.")


def main():
    """Main function to handle command-line arguments and execute appropriate actions."""
    parser = argparse.ArgumentParser(description="Send welcome emails to new users or a test email.")
    parser.add_argument('--test', action='store_true', help='Send a test email to a specified address.')
    parser.add_argument('--test-email', type=str, help='The email address to send the test email to.')
    parser.add_argument('--interval', type=int, default=60, help='Interval in seconds between checks (default: 60)')
    args = parser.parse_args()

    if args.test:
        if not args.test_email:
            logging.error("Please provide an email address for the test using the --test-email argument.")
            sys.exit(1)
        send_test_email(args.test_email)
    else:
        # Initialize MongoDB client only if not in test mode
        try:
            client = MongoClient(MONGODB_URI)
            if MONGODB_DATABASE:
                db = client[MONGODB_DATABASE]
            else:
                db = client.get_default_database()
            users_collection = db['users']  # Replace 'users' with your actual collection name
            logging.info("Connected to MongoDB successfully.")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            sys.exit(1)

        # Continuous loop to check for new users every 'interval' seconds
        logging.info(f"Starting continuous check for new users every {args.interval} seconds.")
        try:
            while True:
                process_users(users_collection)
                logging.info(f"Sleeping for {args.interval} seconds before next check.")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            logging.info("Script interrupted by user. Exiting gracefully.")
            sys.exit(0)
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main()
