from os import path

import argparse
import gspread
import json
import logging
import requests
import sys
import time

DRY_RUN = False

# YNAB
YNAB_FETCH_RETRIES = 3
YNAB_TOKEN_KEY = "YNAB_TOKEN"  # json key for token value
YNAB_BUDGET_ID_KEY = "YNAB_BUDGET"  # json key for budget id value
YNAB_START_YEAR = '2021-01-01'

YNAB_BASE_URL = 'https://api.youneedabudget.com/v1'

YNAB_TXNS_APPROVED_ONLY = True  # only approved transactions

# Google Sheets
GSHEET_SPREADSHEET_NAME = 'Budget 2021'
GSHEET_TXNS_SHEET_NAME = 'YNAB-Transactions'

# YNAB -> Google Sheets mapping
YNAB_COLUMNS = ['date', 'account_name', 'payee_name', 'memo', 'category_name', 'amount']
GSHEETS_COLUMNS = ['Date', 'Account', 'Payee', 'Memo', 'Category', 'Amount']

class AuthData:
    gsheet_file = None
    ynab_file_dict = None

    def __init__(self, gsheet_file: str, ynab_file: str):
        # validate the files
        if not path.exists(gsheet_file):
            raise FileNotFoundError

        if not path.exists(ynab_file):
            raise FileNotFoundError

        f = open(ynab_file)

        self.gsheet_file = gsheet_file
        self.ynab_file_dict = json.load(f)

    def get_gsheet_file(self):
        return self.gsheet_file

    def get_ynab_token(self):
        if YNAB_TOKEN_KEY not in self.ynab_file_dict:
            raise KeyError
        else:
            return self.ynab_file_dict[YNAB_TOKEN_KEY]

    def get_ynab_budget(self):
        if YNAB_BUDGET_ID_KEY not in self.ynab_file_dict:
            raise KeyError
        else:
            return self.ynab_file_dict[YNAB_BUDGET_ID_KEY]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gsheet_file",
        default=None,
        required=True,
        help='file with the google service account key',
        type=str)
    parser.add_argument(
        "--ynab_file",
        default=None,
        required=True,
        help='file with the ynab account key json',
        type=str)
    parser.add_argument(
        "--log_file",
        nargs='?',
        default=None,
        help='file to log to. default: none',
        type=str)
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=logging.WARNING, filemode='a')
    if args.log_file:
        logging.getLogger('').addHandler(logging.FileHandler(args.log_file))

    auth = AuthData(args.gsheet_file, args.ynab_file)

    # YNAB
    raw_txns = get_ynab_transactions(auth)
    formatted_txns = format_ynab_transactions(auth, raw_txns)

    # GSHEETS
    post_gsheets_transactions(auth, formatted_txns)


def get_ynab_transactions(auth: AuthData) -> list:
    txns_list = []
    for i in range(YNAB_FETCH_RETRIES):
        endpoint = '%s/budgets/%s/transactions' % (YNAB_BASE_URL, auth.get_ynab_budget())

        response = requests.get(
            endpoint,
            headers={'Authorization': 'Bearer %s' % auth.get_ynab_token()},
            params={'since_date': YNAB_START_YEAR},
        )
        if response.status_code != 200:
            logging.error(
                'got response %d when fetching YNAB transactions: %s' % (response.status_code, response.json()))

        txns_list = response.json()['data']['transactions']

    return txns_list


def format_ynab_transactions(auth: AuthData, raw_txns: list) -> list:
    header = GSHEETS_COLUMNS
    header.append('Time Updated: %s' % time.ctime())

    formatted_txns = [header]
    for transaction in raw_txns:
        if YNAB_TXNS_APPROVED_ONLY and 'approved' in transaction and not transaction['approved']:
            continue

        relevant_txn_columns = []
        for column in YNAB_COLUMNS:
            if column == 'amount':
                transaction[column] = convert_milliunits_to_dollar_amount(transaction[column])

            relevant_txn_columns.append(str(transaction[column]))
        formatted_txns.append(relevant_txn_columns)

    return formatted_txns


def post_gsheets_transactions(auth: AuthData, transaction_sheet: list) -> None:
    gc = gspread.service_account(filename=auth.get_gsheet_file())
    sh = gc.open(GSHEET_SPREADSHEET_NAME)
    ynab_worksheet = sh.worksheet(GSHEET_TXNS_SHEET_NAME)

    sheet_range = "A:Z"
    ynab_worksheet.update(sheet_range, transaction_sheet)


def convert_milliunits_to_dollar_amount(val: int) -> float:
    """
	'amount' from YNAB returns money in milliunits (e.g. $1 = 100), so convert it back to dollar amount.
	:rtype: float
	"""
    return float(val / 1000)


def pretty_print_json(json_object: dict) -> str:
    return json.dumps(json_object, indent=2)


if __name__ == "__main__":
    main()
