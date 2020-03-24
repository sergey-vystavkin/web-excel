import logging
from multiprocessing.pool import ThreadPool
from urllib.parse import urlencode, urljoin
import pandas as pd
import requests
from bs4 import BeautifulSoup
import tqdm
from source.tools import retry


logger = logging.getLogger(__name__)


class Scrapper:

    def __init__(self, config, mapping):
        self.config = config
        self._mapping = mapping
        self._mapping_form_tags = None
        self._pool = config.getint('settings', 'pool')
        self._search_names = []
        self.success_names = []
        self._pagination_hrefs = []
        self._form_hrefs = []
        self._forms = []
        self.forms_df = pd.DataFrame(columns=mapping.keys())
        self._soup_parser = None
        self._start_page = config.get('settings', 'search_HREF')

    def _set_soup_parser(self):
        html = '<!DOCTYPE html> <html> <body>  <h1>My First Heading</h1>  <p>My first paragraph.</p>  </body> </html>'
        try:
            BeautifulSoup(html, features='lxml')
            self._soup_parser = 'lxml'
        except Exception:
            BeautifulSoup(html, features='html.parser')
            self._soup_parser = 'html.parser'

    @retry()
    def _get_page_soup(self, href):
        try:
            r = requests.get(href)
        except Exception:
            logger.error(self.config.get('error', 'fpds_site_not_response'))
            raise Exception
        soup = BeautifulSoup(r.text, features=self._soup_parser)
        return soup

    def _site_validation(self, href):
        soup = self._get_page_soup(href)
        if soup.find('form', {'name': 'search_awardfull'}):
            return None
        logger.error(self.config.get('error', 'wrong_fpds_href'))
        raise Exception

    def _scrape_forms_hrefs(self, page, not_soup=True):
        if not_soup:
            soup = self._get_page_soup(page)
        else:
            soup = page
        hrefs = [self._build_href(self._start_page, href['href'], 'FORM') for href in soup.find_all(title='View')]
        self._form_hrefs.extend(hrefs)
        return None

    def _scrape_search_pages(self, search_name):
        if not search_name.strip():
            return ''
        search_href = '{0}?{1}'.format(self._start_page, urlencode({'q': search_name.strip()}))
        soup = self._get_page_soup(search_href)
        try:
            b_items = soup.find('span', {'class': 'results_heading'}).find_all_next('b')
            page_index = int(b_items[1].text)
            last_index = int(b_items[2].text)
            if last_index == 0:
                return ''
            elif last_index > page_index:
                items_iter = range(0, last_index, page_index)
                pagination = ['{0}&{1}'.format(search_href, urlencode({'start': i})) for i in items_iter]
                self._pagination_hrefs.extend(pagination)
            else:
                self._scrape_forms_hrefs(soup, False)
            return search_name
        except Exception:
            logger.error('Unable to found "results_heading". \n HREF: {0}'.format(search_href))
            raise Exception

    @staticmethod
    def _build_href(base_href, sub, href_type):
        if href_type == 'FORM':
            return urljoin(base_href, sub.split("'")[1])
        elif href_type == 'PAGN':
            return
        elif href_type == 'SEARCH':
            return '{0}?{1}'.format(base_href, urlencode({'q': sub.strip()}))

    def _scrape_form(self, href):
        result = {k: None for k in self._mapping.values()}
        soup = self._get_page_soup(href)
        for k, v in self._mapping_form_tags.items():
            tag = soup.find(id=k)
            if tag:
                if v == 'input':
                    result[k] = tag.attrs.get('value')
                elif v == 'checkbox':
                    result[k] = bool(tag.attrs.get('checked'))
                elif v == 'td' or v == 'text':
                    result[k] = tag.text
                elif v == 'select':
                    option = tag.find_next('option', {'selected': 'true'})
                    if option:
                        result[k] = option.text
        form = [result.get(self._mapping.get(k, None)) for k in self._mapping.keys()]
        self._forms.append(form)

    def run(self, search_names, message):

        try:
            logger.info(self.config.get('info', 'scrapping'))
            self._set_soup_parser()
            self._mapping_form_tags = {value[0]: value[1] for value in self._mapping.values() if value[0]}
            self._mapping = {key: value[0] for key, value in self._mapping.items()}
            self._search_names = search_names
            self._site_validation(self.config.get('settings', 'search_HREF'))
            logger.info(self.config.get('info', 'parse_search_names').format(len(search_names)))
            with ThreadPool(self._pool) as p:
                searched_names = list(tqdm.tqdm(p.imap(self._scrape_search_pages, list(set(self._search_names))),
                                                total=len(set(self._search_names))))
            self.success_names = [(True if (name in searched_names and name.strip() != '') else False)
                                  for name in self._search_names]
            with ThreadPool(self._pool) as p:
                list(tqdm.tqdm(p.imap(self._scrape_forms_hrefs, self._pagination_hrefs),
                               total=len(self._pagination_hrefs)))
            logger.info(self.config.get('info', 'parse_forms').format(len(self._form_hrefs)))
            with ThreadPool(self._pool) as p:
                list(tqdm.tqdm(p.imap(self._scrape_form, self._form_hrefs), total=len(self._form_hrefs)))
            if self._forms:
                self.forms_df = pd.DataFrame.from_records(self._forms)
                self.forms_df.columns = self._mapping.keys()
        except Exception:
            logger.error(self.config.get('error', 'fpds_site'), exc_info=True)
            message.send_fail_to_admin(letter=3)
            raise Exception('*Handled_error')
