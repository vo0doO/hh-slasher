from scrapy.exceptions import NotConfigured


def get(spider) -> set[str]:
    try:
        importer_module = __import__("cli.google-sheets-importer")
        importer_module = getattr(importer_module, "google-sheets-importer")

        spider.logger.manager.loggerDict["cli.google-sheets-importer"].disabled = True

        importer = importer_module.GoogleSheetImporter()
        importer.make_google_worksheet_df()

        google_worksheet_df = importer.get_google_worksheet_df().astype(str)
        spider.log("Сформирован кеш из id ваканий")

        return set(google_worksheet_df["vacancy_id"].tolist())
    except Exception as error:
        raise NotConfigured("Ошибка при получении кеша", error)
