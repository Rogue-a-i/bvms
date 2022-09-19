# -*- coding: utf-8 -*-
from pathlib import Path
import pdftotext
import logging
import boto3
import os

# Initiate session
s3 = boto3.client('s3')


logger = logging.getLogger('bvms')

PDFS_DIR = Path('pdfs')


class BvmsPipeline:

  def process_item(self, item, spider):
    try:
      file_name, ext = os.path.splitext(item['path'])
      os.makedirs(PDFS_DIR, exist_ok=True)
      os.makedirs('./control', exist_ok=True)

      file_bytes = ''

      with open(f'{PDFS_DIR}/{item["path"]}', 'wb') as f:
        f.write(item['body'])
        f.close()

      with open(f'{PDFS_DIR}/{item["path"]}', 'rb') as f:
        pdf = pdftotext.PDF(f)
        for page in pdf:
          file_bytes += page

      txt_filename = os.path.splitext(f'{PDFS_DIR}/{item["mis"]}')[0]+'.txt'
      file_txt = open(txt_filename, 'w')
      file_txt.write(file_bytes)
      file_txt.close()

      # upload object here
      constrol_file = Path('./control/file_control.txt')
      f_control = open(constrol_file, 'a')
      f_control.write(f"{item['mis']},")
      f_control.close()

      logger.info(f'{PDFS_DIR}/{item["path"]} has been processed')
      os.remove(f'{PDFS_DIR}/{item["path"]}')
    except Exception as e:
      logging.error(e)

    return item
