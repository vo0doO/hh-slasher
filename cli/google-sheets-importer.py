import logging
from datetime import datetime

import gspread
import pandas as pd
import pytz
from spidermon.contrib.actions.telegram import SimplyTelegramClient  # type: ignore
from tenacity import retry, stop_after_attempt, wait_exponential

from hh import settings


class Logger:
    def __init__(self, logger_name):
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger


logger = Logger(__name__).get_logger()


class GoogleSheetImporter:
    def __init__(self, chunk_size=100) -> None:
        self.chunk_size = chunk_size
        self._setup_google_sheet()

    def _setup_google_sheet(self) -> None:
        try:
            self.google_client = gspread.service_account(  # type: ignore
                filename=settings.IDATICA_GOOGLE_SERVICE_ACCOUNT_CREDENTAILS_FILE_PATH
            )
            self.google_spreadsheet = self.google_client.open_by_key(
                settings.GOOGLE_SPREADSHEET_KEY
            )
            self.google_worksheet = self.google_spreadsheet.worksheet(
                settings.GOOGLE_WORKSHEET_NAME
            )
            logger.info("Создан экземпляр GoogleSheetImporter")
        except Exception as error:
            self._handle_error("Ошибка при формировании df из таблицы гугл", error)

    def make_google_worksheet_df(self) -> None:
        try:
            all_records = self.google_worksheet.get_all_records(
                head=settings.GOOGLE_WORKSHEET_META["head"]  # type: ignore
            )
            self.google_worksheet_df = pd.DataFrame(all_records)
            logger.info("Сформирована df из таблицы гугл")
        except Exception as error:
            self._handle_error("Ошибка при формировании df из таблицы гугл", error)

    def get_google_worksheet_df(self) -> pd.DataFrame:  # type: ignore
        try:
            logger.info("Возвращена df из таблицы гугл")
            return self.google_worksheet_df
        except Exception as error:
            self._handle_error("Ошибка при возврате df из таблицы гугл", error)

    def make_spider_result_df(self) -> None:
        try:
            self.spider_result_df = pd.read_csv(
                settings.FEED_EXPORT_FILE_PATH, sep="\t"
            )
            logger.info("Сформирована df из результатов паука")
        except Exception as error:
            self._handle_error("Ошибка при формировании df из результатов паука", error)

    def make_new_rows_df(self) -> None:
        try:
            self.new_rows_df = self.spider_result_df[  # type: ignore
                ~self.spider_result_df["vacancy_id"].isin(
                    self.google_worksheet_df["vacancy_id"].to_list()
                )
            ]
            self.new_rows_df = self.new_rows_df.fillna("").astype(str)
            logger.info("Создана df с новыми вакансиями")
        except Exception as error:
            self._handle_error("Ошибка при определении новых строки(вакансии)", error)

    def high_level_update_google_worksheet(self) -> None:
        try:
            meta: dict = settings.GOOGLE_WORKSHEET_META

            # Обновления методами экземпляра листа
            update_update_date_gspread_request = {
                "range": meta["update_date"]["cell"],
                "values": [[self._get_current_date()]],
            }
            update_all_rows_count_gspread_request = {
                "range": meta["all_rows_count"]["cell"],
                "values": [[len(self.new_rows_df) + len(self.google_worksheet_df)]],
            }
            update_new_rows_count_gspread_request = {
                "range": meta["new_rows_count"]["cell"],
                "values": [[len(self.new_rows_df)]],
            }

            requests = [
                update_update_date_gspread_request,
                update_all_rows_count_gspread_request,
                update_new_rows_count_gspread_request,
            ]

            self.append_result = self._append_all_new_rows_chunks()
            self.update_result = self.google_worksheet.batch_update(requests)

            logger.info("Таблица обновлена")
        except Exception as error:
            self._handle_error("Ошибка при обновлении таблицы", error)

    def _split_new_rows_into_chunks(self):
        """Разделяет DataFrame на чанки заданного размера."""
        for i in range(0, len(self.new_rows_df), self.chunk_size):
            yield self.new_rows_df.iloc[i : i + self.chunk_size].values.tolist()

    @retry(
        stop=stop_after_attempt(7), wait=wait_exponential(multiplier=1, min=4, max=10)
    )  # noqa
    def _append_new_rows_chunk(self, chunk):
        """Добавляет один чанк данных с повтором при ошибке."""
        return self.google_worksheet.append_rows(values=chunk)

    def _append_all_new_rows_chunks(self):
        """Добавляет все данные чанками."""
        append_results = []
        for chunk in self._split_new_rows_into_chunks():
            append_results.append(self._append_new_rows_chunk(chunk))
            logger.info(f"Успешно загружен фрагмент из {len(chunk)} строк")
        updated_rows = sum(
            entry.get("updates", {}).get("updatedRows", 0)
            for entry in append_results
            if isinstance(entry, dict)
        )
        logger.info(f"Всего загружено {updated_rows} строк")
        return {"updatedRows": updated_rows}

    def send_success_report(self) -> None:
        try:
            logger.info("Начата отправка отчета")
            message = self._make_success_report_message()
            self.send_telegram_message(message)  # type: ignore
            logger.info("Отправка отчета успешно завершена")
        except Exception as error:
            self._handle_error("Ошибка в процессе отправки отчета", error)

    def _make_success_report_message(self) -> str:  # type: ignore
        try:
            message = f"""🎉 `'hh.cli.google-sheets-importer'` module finished!
            - Обновлено {self.update_result['totalUpdatedColumns']} ячейки с метаданными
            - Добавлено {self.append_result.get('updatedRows', 0)} новых вакансий
            """  # noqa
            logger.info(f"Сообщение отчета об успехе создано\n{message}")
            return message
        except Exception as error:
            self._handle_error("Ошибка при создании сообщения отчета об упехе", error)

    @staticmethod
    def send_telegram_message(message: str) -> None:
        try:
            client = SimplyTelegramClient(settings.SPIDERMON_TELEGRAM_SENDER_TOKEN)
            for recipient in settings.SPIDERMON_TELEGRAM_RECIPIENTS:
                client.send_message(message, recipient)
                logger.info("Сообщение в телеграм отправлено")
        except Exception as error:
            GoogleSheetImporter._handle_error(
                "Ошибка в процессе отправки сообщения в телеграм", error
            )

    @staticmethod
    def _handle_error(message: str, error: Exception) -> Exception:
        error_message = f"{message}: {error}"
        raise type(error)(error_message).with_traceback(error.__traceback__)

    @staticmethod
    def _get_current_date() -> str:
        now = datetime.now(tz=pytz.timezone("Europe/Moscow"))
        return now.strftime("%Y-%m-%d")


if __name__ == "__main__":
    try:
        importer = GoogleSheetImporter()
        importer.make_google_worksheet_df()
        importer.make_spider_result_df()
        importer.make_new_rows_df()
        importer.high_level_update_google_worksheet()
        importer.send_success_report()
    except Exception as error:
        telegram_message = f"💀 `'hh.cli.google-sheets-importer'` module finished with error! : {error}"  # noqa
        GoogleSheetImporter.send_telegram_message(telegram_message)
        logger.error(error)
        raise error
