from bvms.items import BvmsItem
from scrapy import Selector
import logging
import scrapy
import os

logger = logging.getLogger('bvms')


class BvmsSpider(scrapy.Spider):
  name = "bvms"

  def start_requests(self):

    to_delete = False
    if to_delete & os.path.isfile('./control/file_control.txt'):
      os.remove('./control/file_control.txt')

    os.makedirs('./control', exist_ok=True)
    open('./control/file_control.txt', 'w')

    parts = 3124
    urls = ['https://pesquisa.bvsalud.org/bvsms/?output=site&lang=pt&from=1&sort=&format=summary&count=100&fb=&page=1&filter%5Bla%5D%5B%5D=pt&filter%5Bfulltext%5D%5B%5D=1&skfp=true&index=&q=']
    for i in range(2, parts):
      from_ = (100 * i + 1) - 100
      urls.append(
        f'https://pesquisa.bvsalud.org/bvsms/?output=site&lang=pt&from={from_}&sort=&format=summary&count=100&fb=&page={i}&filter%5Bla%5D%5B%5D=pt&filter%5Bfulltext%5D%5B%5D=1&skfp=true&index=&q=',
      )
    logger.info(f'urls to scrap: {len(urls)}')
    for url in urls:
      logger.info(f'scrapping {url}')
      yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

  def parse(self, response):
    page_selector = Selector(text=response.body)
    pub_headlines = page_selector.xpath(
      '//div[@class="box1"]/div[@class="textArt"]/div[@class="titleArt"]/a/@href'
    ).getall()

    ids = []
    for url in pub_headlines:
      _id = url.split('/')[-1]
      # if _id.startswith('mis'):
      ids.append(_id)
    file_control = open('./control/file_control.txt', 'r+')
    processed_mis = file_control.read().split(',')

    for p_mis in processed_mis:
      if p_mis in ids:
        ids.remove(p_mis)

    for mis in ids:
      url = f'https://pesquisa.bvsalud.org/bvsms/resource/pt/{mis}'
      yield scrapy.Request(url=url, callback=self.parse_inner_page, meta={'mis': mis})

  def parse_inner_page(self, response):
    page_selector = Selector(text=response.body)
    pdf_link = page_selector.xpath('//a[@title="Texto completo"]/@href').get()
    id_doc = response.meta['mis']
    if pdf_link is None or pdf_link == '':
      logger.warning(f'pdf of {id_doc} not found. skipping...')
      yield None
    else:
      yield scrapy.Request(url=response.urljoin(pdf_link), callback=self.save_pdf, meta=response.meta)

  def save_pdf(self, response):
    path = response.url.split('/')[-1]
    return BvmsItem(path=path, body=response.body, mis=response.meta['mis'])
