import logging
import os
import requests
import re
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the retry session function
def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Define the email extraction function
def extract_email(text):
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(pattern, text)
    if matches:
        return matches[0].strip('\"<>[]()')
    return None

# Define the GitHub API handler class
class GitHubApiHandler:
    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.current_key_index = 0
        self.request_count = 0
        self.max_requests_per_key = 3650
        self.failed_attempts = 0

    def get_headers(self):
        return {'Authorization': f'token {self.api_keys[self.current_key_index]}'}

    def check_and_switch_key(self):
        remaining_requests = self.get_remaining_requests()
        logger.info(f"Remaining requests for current key: {remaining_requests}")
        if remaining_requests < 10:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            self.request_count = 0
            self.failed_attempts += 1
            logger.info(f"Switched to new API key: {self.current_key_index + 1}")
            if self.failed_attempts >= 18:
                logger.info("API rate limit hit for all keys. Waiting for 1 hour and 5 minutes.")
                time.sleep(3900)  # Wait for 1 hour and 5 minutes
                self.failed_attempts = 0

    def get_remaining_requests(self):
        headers = self.get_headers()
        url = 'https://api.github.com/rate_limit'
        response = requests_retry_session().get(url, headers=headers)
        if response.status_code == 200:
            rate_limit_data = response.json()
            remaining = rate_limit_data['rate']['remaining']
            return remaining
        return 0

    def get_user_info_from_github_api(self, username_or_url):
        self.check_and_switch_key()
        headers = self.get_headers()
        self.request_count += 1
        if username_or_url.startswith('https://github.com/'):
            username = username_or_url.split('/')[-1]
        else:
            username = username_or_url
        url = f'https://api.github.com/users/{username}'
        response = requests_retry_session().get(url, headers=headers)
        if response.status_code != 200:
            logger.info(f"Failed to fetch user info for {username_or_url}, status code: {response.status_code}")
            return None
        user_data = response.json()
        email = user_data.get('email', '') or self.get_email_from_readme(username, headers)
        return email

    def get_email_from_readme(self, username, headers):
        url = f'https://raw.githubusercontent.com/{username}/{username}/main/README.md'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return extract_email(response.text)
        return None

# Define the main function
def main():
    try:
        input_csv_path = 'input2.csv'
        start_time = datetime.now()
        max_runtime = timedelta(hours=5, minutes=50)  # Set maximum runtime

        api_keys = os.environ['MY_GITHUB_API_KEYS'].split(',')

        github_api_handler = GitHubApiHandler(api_keys)

        logger.info("Reading input CSV file...")
        input_df = pd.read_csv(input_csv_path)

        # Add the 'Status' and 'Email' columns if they do not exist
        if 'Status' not in input_df.columns:
            input_df['Status'] = ''
        if 'Email' not in input_df.columns:
            input_df['Email'] = ''

        for index, row in input_df.iterrows():
            # Check if the maximum runtime has been reached
            if datetime.now() - start_time > max_runtime:
                logger.info("Maximum runtime reached. Saving progress and exiting...")
                input_df.to_csv(input_csv_path, index=False)
                return

            if row['Status'] == 'Done':
                continue

            profile_url = row['Profile URL']
            username = row['Username']
            user_id = row['User ID']
            logger.info(f"Processing {username} ({profile_url})")

            try:
                email = github_api_handler.get_user_info_from_github_api(profile_url)
                if email:
                    input_df.at[index, 'Email'] = email
                    input_df.at[index, 'Status'] = 'Done'  # Mark as done
                    logger.info(f"Appended email for {username}: {email}")
                else:
                    logger.info(f"No email found for {username}")

                # Save the progress immediately after each row
                input_df.to_csv(input_csv_path, index=False)

            except Exception as e:
                logger.error(f"An error occurred while processing {profile_url}: {e}")
                continue

        # Final save after loop completion
        logger.info(f"Processing complete. Final data saved to {input_csv_path}")
        input_df.to_csv(input_csv_path, index=False)

    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")

if __name__ == "__main__":
    main()
