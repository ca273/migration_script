import os
import sys
import argparse
import enum
import datetime as dt
import concurrent.futures
import time
from pathlib import Path
import csv
import logging

import psycopg2


# -------------------------------------------------------------------------------------------------------------------- #
#                                                Script Headers                                                        #
# -------------------------------------------------------------------------------------------------------------------- #
class EnvTypes(enum.Enum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


parser = argparse.ArgumentParser(description="[Wed/July.17.2024] Migration from csv to PostgreSQL.")
parser.add_argument("environment",
                    type=str,
                    choices=[e.value for e in EnvTypes],
                    help="Specify settings env: (dev: dev) | (test: 2) | (prod: 3)"
                    )
args = parser.parse_args()


# -------------------------------------------------------------------------------------------------------------------- #
#                                                 Script Setup                                                         #
# -------------------------------------------------------------------------------------------------------------------- #

BASE_DIR = os.path.abspath(os.path.join(__file__))
sys.path.append(BASE_DIR)

# -------------------------------------------------------------------------------------------------------------------- #
#                                                  Script                                                              #
# -------------------------------------------------------------------------------------------------------------------- #

from settings import config
from repositories import UserRepository, DBManagerBase

logger = logging.getLogger(__name__)


class MigrationHandler:
    THREADS_COUNT = 2
    PAGE_SIZE = 2000

    @staticmethod
    def log_success(msg):
        with open("./success.log", "a") as f:
            f.write(msg)
        logger.info(msg)

    @staticmethod
    def log_exception(msg):
        with open("./exception.log", "a") as f:
            f.write(msg)
        logger.exception(msg)

    def __read_csv_in_chunks(self):
        """ To save memory we are using generators """

        BASE_DIR = Path(__file__).resolve().parent
        file_name = "users.csv"
        file_path = BASE_DIR / "data_to_migrate" / file_name

        with open(file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            chunk = []
            for i, row in enumerate(reader):
                chunk.append((row["first name"], row["last name"], row["address"], row["age"]))
                if (i + 1) % self.PAGE_SIZE == 0:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

    def process_bulk_insert(self, chunk, cursor, users_repository, db_connection):
        """ bulk insert of 3 retries """

        attempts = 3
        attempt = 0
        while attempt < attempts:
            try:
                users_repository.bulk_insert(cursor, chunk)
                break  # Break out of the retry loop if successful
            except psycopg2.DatabaseError as exc:
                attempt += 1
                self.log_exception(f"\n{dt.datetime.utcnow()} | Attempt {attempt} - Transaction error from {chunk[0]} To {chunk[-1]}. Exc: {exc}")
                if attempt < attempts:
                    time.sleep(1)  # Wait for 1 second before retrying
                else:
                    self.log_exception(f"\n{dt.datetime.utcnow()} | Failed after {attempt} attempts. Last exception: {exc}")
                    db_connection.rollback()

    def process_thread(self, chunk, db_manager, users_repository):
        try:
            if chunk:
                report_msg = f"\n{dt.datetime.utcnow()} | Started migration from {chunk[0]} To {chunk[-1]}. keys length: {len(chunk)}"
                report_success_msg = f"\n{dt.datetime.utcnow()} | migration from {chunk[0]} To {chunk[-1]} has finished. keys length: {len(chunk)}"

                self.log_success(report_msg)

                db_connection = db_manager.create_conn()
                with db_connection.cursor() as cursor:
                    try:
                        if not users_repository.users_table_exists(cursor, config.USERS_TABLE_NAME):
                            users_repository.create_users_table(cursor)
                        # users_repository.bulk_insert(cursor, chunk)
                        self.process_bulk_insert(chunk, cursor, users_repository, db_connection)
                    except psycopg2.DatabaseError as exc:
                        self.log_exception(f"\n{dt.datetime.utcnow()} | Transaction error from {chunk[0]} To {chunk[-1]}. Exc: {exc}")
                        db_connection.rollback()

                db_connection.commit()
                db_connection.close()

                self.log_success(f"\n{report_success_msg}")
        except Exception as exc:
            self.log_exception(f"\n{dt.datetime.utcnow()} | Exception in thread: exc: {exc}")

    def process(self):
        try:
            db_manager = DBManagerBase(db_user=config.DB_USER, password=config.DB_PASSWORD, host=config.DB_HOST,
                                       port=config.DB_PORT, db_name=config.DB_NAME)

            users_repository = UserRepository()
            for chunk in self.__read_csv_in_chunks():
                chunk_size = (self.PAGE_SIZE + self.THREADS_COUNT - 1) // self.THREADS_COUNT
                chunks = [chunk[i:i + chunk_size] for i in range(0, self.PAGE_SIZE, chunk_size)]
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.THREADS_COUNT) as executor:
                    futures = [executor.submit(self.process_thread, chunk, db_manager, users_repository) for chunk in chunks]

                    self.log_success(f"\n{dt.datetime.utcnow()} | {len(futures)} have been initiated")
                    # Wait for all futures to complete
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            result = future.result()
                        except Exception as exc:
                            self.log_exception(f'\n{dt.datetime.utcnow()} | Exception in future: {exc}')

        except Exception as exc:
            self.log_exception(f"\n{dt.datetime.utcnow()} | Exception in processing thread. exc: {exc}")


def main():
    handler = MigrationHandler()

    start_msg = f"\n{dt.datetime.utcnow()} | Running users migration script"
    handler.log_success(start_msg)
    print(start_msg)
    try:
        handler.process()
        end_msg = f"\n{dt.datetime.utcnow()} | users migration script has finished"
        handler.log_success(end_msg)
        print(end_msg)
    except Exception as exc:
        exc_msg = f"\n{dt.datetime.utcnow()} | exc in main(): {exc}"
        print(exc_msg)
        handler.log_exception(exc_msg)


if __name__ == '__main__':
    main()
