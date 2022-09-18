from scrapy import Selector
from bvms.items import BvmsItem
import scrapy
import os


class BvmsSpider(scrapy.Spider):
  name = "bvms"

  def start_requests(self):

    to_delete = False
    if to_delete & os.path.isfile('./control/file_control.txt'):
      os.remove('./control/file_control.txt')

    os.makedirs('./control', exist_ok=True)
    open('./control/file_control.txt', 'w')

    parts = 20
    urls = ['https://pesquisa.bvsalud.org/bvsms/?u_filter%5B%5D=fulltext&u_filter%5B%5D=db&u_filter%5B%5D=mj_cluster&u_filter%5B%5D=collection_bvsms&u_filter%5B%5D=la&u_filter%5B%5D=year_cluster&u_filter%5B%5D=type&fb=&lang=pt&skfp=true&where=&filter%5Bla%5D%5B%5D=pt&range_year_start=&range_year_end=']
    for i in range(2, parts):
      from_ = (100 * i + 1) - 100
      urls.append(
        f'https://pesquisa.bvsalud.org/bvsms/?output=site&lang=pt&from={from_}&sort=&format=summary&count=100&fb=&page={i}&q=tombo%3A10001%24+and+collection_bvsms%3A"TXTC"&index=&where=ALL',
      )
    for url in urls:
      yield scrapy.Request(url=url, callback=self.parse)

  def parse(self, response):
    page_selector = Selector(text=response.body)
    # '//div[@class="totalResults"]/strong[2]/text()'
    pub_headlines = page_selector.xpath('//div[@class="resultSet"]/div/@id').getall()

    file_control = open('./control/file_control.txt', 'r+')
    processed_mis = file_control.read().split(',')

    for p_mis in processed_mis:
      if p_mis in pub_headlines:
        pub_headlines.remove(p_mis)

    for mis in pub_headlines:
      url = f'https://pesquisa.bvsalud.org/bvsms/resource/pt/{mis}'
      yield scrapy.Request(url=url, callback=self.parse_inner_page, meta={'mis': mis})

  def parse_inner_page(self, response):
    page_selector = Selector(text=response.body)
    pdf_link = page_selector.xpath('//a[@title="Texto completo"]/@href').get()
    yield scrapy.Request(url=response.urljoin(pdf_link), callback=self.save_pdf, meta=response.meta)

  def save_pdf(self, response):
    path = response.url.split('/')[-1]
    bvmsItem = BvmsItem(path=path, body=response.body, mis=response.meta['mis'])
    return bvmsItem
