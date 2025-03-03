project_name = hh-dev
ssh_server ?= root@browser.idatica.com
remote_run = ssh -i ~/.ssh/whoosh_rsa -t $(ssh_server)
project_dir = /root/projects/$(project_name)

install:
	$(remote_run) "mkdir -p $(project_dir) $(project_dir)/logs"
	make _upload_code
	$(remote_run) "cd $(project_dir) && uv sync"

upload_code:
	rsync -av -e "ssh -i ~/.ssh/whoosh_rsa" --timeout=60 --exclude='.scrapy' --exclude='.venv' --exclude='.DS_Store' --exclude='.git' --exclude='result/' --exclude='logs/' --exclude='.mypy_cache' --exclude='.vscode' --exclude='*.png' --exclude='*.pyc' . $(ssh_server):$(project_dir)

download_result:
	scp -i ~/.ssh/whoosh_rsa ${ssh_server}:${project_dir}/result/hh.csv ./result/hh.csv

download_logs:
	scp -r -i "~/.ssh/whoosh_rsa" ${ssh_server}:${project_dir}/logs/*.log ./logs/  

scrape:
	uv run scrapy crawl hh --logfile ./logs/hh.log

import_results_in_google_spreadsheet:
	uv run python -m cli.google-sheets-importer > ./logs/google-sheets-importer.log 2>&1

all:
	make scrape
	make import_results_in_google_spreadsheet