import logging
import requests
import time
import csv
import re
import random
import zipfile

import os.path
import transliterate
# import ipdb

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from lxml.html import fromstring
from python_rucaptcha import ImageCaptcha

from .settings import RUCAPTCHA_KEY, PROXIES, PROXY_PORT, PROXY_LOGIN, PROXY_PASSWORD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class YandexMarketParser(object):
    '''
    Класс для получения данных с фндекс маркета
    '''
    origin = 'market.yandex.ru'

    def get_chromedriver(self, use_proxy=False, user_agent=None, proxy_host=None):
        manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "<all_urls>",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                },
                "minimum_chrome_version":"22.0.0"
            }
            """

        background_js = """
            var config = {
                    mode: "fixed_servers",
                    rules: {
                      singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                      },
                      bypassList: ["localhost"]
                    }
                  };

            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }

            chrome.webRequest.onAuthRequired.addListener(
                        callbackFn,
                        {urls: ["<all_urls>"]},
                        ['blocking']
            );
            """ % (proxy_host, PROXY_PORT, PROXY_LOGIN, PROXY_PASSWORD)

        chrome_options = Options()
        if use_proxy:
            pluginfile = 'proxy_auth_plugin.zip'

            with zipfile.ZipFile(pluginfile, 'w') as zp:
                zp.writestr("manifest.json", manifest_json)
                zp.writestr("background.js", background_js)
            chrome_options.add_extension(pluginfile)
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        driver = webdriver.Chrome(chrome_options=chrome_options)
        logger.info('Браузер создан')

        return driver

    def get_categories_data(self, categories_links=None):
        try:
            categories = self.get_categories()
            for category in categories:
                self.get_prices_for_category(category_hook=category.get('url'), category_name=category.get('name'))
            return
        except Exception as e:
            logger.error(e)

    def get_categories(self):
        """
        Получаем ссылки на все категории
        Записываем в файл, потом при необходимости можно будет их обновлять
        Сейчас проверяется, что файл с категориями существует, если его нет, то категории получаются снова
        :return:
        """
        cats = []

        proxy = random.choice(PROXIES)
        driver = self.get_chromedriver(proxy_host=proxy, use_proxy=True)
        logger.info("Вернули обратно объект драйвера")

        if os.path.exists('files/categories.csv') is False or os.path.getsize('files/categories.csv') == 0:
            content = None
            for i in range(10):
                driver.get('https://market.yandex.ru')
                try:
                    content = fromstring(driver.page_source)
                    if len(content.xpath(f'//article[@data-autotest-id="product-snippet"]')) > 0 or len(
                            content.xpath('//div[@data-zone-name="category-link"]//a/@href')) > 0:
                        break
                except Exception as e:
                    logger.info(e)
            if content is not None:
                # проверяем есть ли капча
                new_hook = content.xpath('//input[@name="retpath"]/@value')
                if len(new_hook) > 0:
                    content = self._solve_captcha(driver, content, new_hook)

                if len(content.xpath('//*[contains(.,"Краснодар")]')) == 0:
                    time.sleep(3)
                    button = driver.find_element_by_xpath('//div[@data-apiary-widget-name="@MarketNode/HeaderRegionPopup"]//button')
                    logger.info(button)
                    button.click()
                    form = driver.find_element_by_xpath('//input[@placeholder="Укажите другой регион"]//ancestor::form')
                    logger.info(form)
                    form.find_element_by_xpath('//input[@placeholder="Укажите другой регион"]').send_keys('Краснодар')
                    time.sleep(2)
                    form.find_element_by_xpath('//b[.="Краснодар"]/ancestor::a').click()
                    form.submit()
                    time.sleep(5)

                categories = content.xpath('//div[@data-zone-name="category-link"]//a/@href')[1:4]

                logger.info(f"Родительские категории: {categories}")

                for category in categories:
                    if 'http' not in category:
                        category = f'https://market.yandex.ru{category}'
                    cats.extend(self.get_childres(category, driver))

                self.write_categories_to_csv(categories=cats)
            else:
                logger.info(f"Не удалось получить содержимое страницы для дальнейшего поиска категорий.")
        else:
            with open('files/categories.csv', "r") as f_obj:
                reader = csv.DictReader(f_obj)
                for row in reader:
                    cats.append({'url': row.get('url'), 'name': row.get('name')})

        driver.close()
        return cats

    def get_childres(self, category_url, driver):
        '''
        Получаем потомков отдельной категории
        :param category_url: ссылка на радительскую категорию
        :return: возвращается словарь с поомками категории
        '''
        # засыпаем, чтобы меньше была вероятность получить капчу от яндекса
        time.sleep(5)
        children = []

        # ipdb.set_trace()
        driver.get(category_url)
        spans = driver.find_elements_by_xpath("//div[@data-zone-name='link']/span")
        for span in spans:
            try:
                span.click()
            except Exception as e:
                logger.error(e)
        content = fromstring(driver.page_source)
        logger.info(content)

        # проверяем есть ли капча
        new_hook = content.xpath('//input[@name="retpath"]/@value')
        if len(new_hook) > 0:
            content = self._solve_captcha(driver, content, new_hook)

        childs = content.xpath('//div[@data-zone-name="link"]//a')
        # ipdb.set_trace()
        for ch in childs:
            url1 = ch.xpath('.//@href')[0]
            name1 = ch.xpath('.//text()')[0]
            if 'http' not in url1:
                url1 = f'https://market.yandex.ru{url1}'
            time.sleep(5)
            driver.get(url1)
            content = fromstring(driver.page_source)

            logger.info(f'{url1}, {content}')

            # проверяем есть ли капча
            new_hook = content.xpath('//input[@name="retpath"]/@value')
            if len(new_hook) > 0:
                content = self._solve_captcha(driver, content, new_hook)
            childs = content.xpath('//div[@data-zone-name="link"]//a')
            for ch in childs:
                name = ch.xpath('.//text()')[0]
                url = ch.xpath('.//@href')[0]
                if 'http' not in url:
                    url = f'https://market.yandex.ru{url}'
                children.append({
                    'name': name,
                    'url': url
                })
            if len(childs) == 0:
                children.append({
                    'name': name1,
                    'url': url1
                })

        logger.info(children)

        return children

    def get_prices_for_category(self, category_hook, category_name):
        '''
        Получение цен и другой доступной информации для товаров определенной категории
        :param category_url: ссылка на категорию
        :return: при успешном получении данных возвращается True.
        '''
        proxy = random.choice(PROXIES)
        driver = self.get_chromedriver(use_proxy=True, proxy_host=proxy)
        content = None
        for i in range(10):
            driver.get(category_hook)
            content = fromstring(driver.page_source)
            if len(content.xpath(f'//article[@data-autotest-id="product-snippet"]')) > 0 or len(content.xpath('//input[@name="retpath"]/@value')) > 0:
                break
        if content is not None:
            # проверяем есть ли капча
            new_hook = content.xpath('//input[@name="retpath"]/@value')
            if len(new_hook) > 0:
                content = self._solve_captcha(driver, content, new_hook)

            price_list = self.get_prices_to_dict(content)

            page = 2
            while True:
                time.sleep(7)
                page_url = f'{category_hook}&page={page}'

                logger.info(f'page_url: {page_url}')

                driver.get(page_url)
                content = fromstring(driver.page_source)
                # проверяем есть ли капча
                new_hook = content.xpath('//input[@name="retpath"]/@value')
                if len(new_hook) > 0:
                    content = self._solve_captcha(driver, content, new_hook)

                if len(content.xpath(f'//article[@data-autotest-id="product-snippet"]')) == 0:
                    break
                price_list_page = self.get_prices_to_dict(content)
                price_list.extend(price_list_page)
                page += 1

            self.write_prices_to_csv(price_list, category_name)
        else:
            logger.error(f"Не удалось получить содержимое страницы ля сбора информации о ценах. Категория: {category_name}, {category_hook}")
            return
        driver.close()
        return True

    def get_prices_to_dict(self, content):
        '''
        Сбор данных о ценах
        :param content: контент страницы
        :return: словарь с данными.
        '''
        price_list = []
        goods = content.xpath(f'//article[@data-autotest-id="product-snippet"]')
        for good in goods:
            id = good.xpath('./@data-zone-data')[0]
            id = re.findall(r'"id":(\d*\w*),', id)[0]
            good_name = good.xpath('.//h3//text()')[0]
            try:
                vendor_name = good.xpath('.//h3/ancestor::div[1]/div[2]/text()')[0]
            except IndexError:
                vendor_name = ''
            try:
                nominal_price = good.xpath('.//div[@data-zone-name="price"]/a/div/span/span/text()')[0].replace(' ', '')
                if nominal_price.isdigit() is False:
                    nominal_price = good.xpath('.//div[@data-zone-name="price"]/a/div/span/span/text()')[1]
            except:
                continue
            description = '; '.join(good.xpath('.//ul/li/text()'))

            price_list.append([vendor_name, good_name, id, nominal_price, description])

        return price_list

    def _solve_captcha(self, rw, content, new_hook):
        """
        Проходим капчу и возвращаем страницу с результатом
        :param rw: драйвер
        :param content: контент страницы
        :param new_hook: ссылка
        :return: None или контент страницы после решения капчи
        """
        count = 0
        # тридцать попыток решить капчу
        while count < 30:
            key = content.xpath('//input[@name="key"]/@value')[0]
            try:
                img_url = content.xpath('//div[@class="captcha__image"]/img/@src')[0]
            except IndexError:
                return None

            rw.get(img_url)
            imgs = fromstring(rw.page_source)
            img_url = imgs.xpath('//img/@src')[0]
            rw.get(img_url)
            logger.info(f'Пытаемся получить итоговую картинку капчи и скачать ее {img_url}')

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"
            }
            s = requests.session()
            s.headers.update(headers)
            for cookie in rw.get_cookies():
                c = {cookie['name']: cookie['value']}
                s.cookies.update(c)

            r = s.get(img_url, allow_redirects=True)
            open('captcha' + '.jpeg', 'wb').write(r.content)

            answer = ImageCaptcha.ImageCaptcha(rucaptcha_key=RUCAPTCHA_KEY, sleep_time=15, save_format='temp').captcha_handler(captcha_file='captcha.jpeg')
            if answer is None:
                answer = ''
            answer = answer.get('captchaSolve')
            new_hook[0] = new_hook[0].replace(':', '%3A').replace('?', '%3F').replace('=', '%3D').replace('&', '%26')
            url = f'https://{self.origin}/checkcaptcha?key={key}&retpath={new_hook[0]}&rep={answer}'

            rw.get(url)
            content = fromstring(rw.page_source)
            if content is not None: # бывает когда content == None
                new_hook = content.xpath('//input[@name="retpath"]/@value')
                logger.info(f'new_hook 1: {new_hook}')
                if len(new_hook) == 0:
                    return content
            count += 1

    def write_prices_to_csv(self, price_list, category_name):
        '''
        Полученные данные запишем в файл
        :return:
        '''
        file_name = transliterate.translit(category_name.replace(' ', '_'), reversed=True)
        try:
            with open(f'files/{file_name}.csv', 'w', newline='') as csvfile:
                pricewriter = csv.writer(csvfile, delimiter='|',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                for price in price_list:
                    pricewriter.writerow([item for item in price])

                csvfile.close()
            return True
        except Exception as e:
            logger.error(e)
            return False

    def write_categories_to_csv(self, categories):
        '''
        Полученные данные запишем в файл
        :return:
        '''
        try:
            with open(f'files/categories.csv', 'w', newline='') as csvfile:
                pricewriter = csv.writer(csvfile, delimiter='|',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                pricewriter.writerow(['url', 'name'])
                for cat in  categories:
                    pricewriter.writerow([cat.get('url'), cat.get('name')])

                csvfile.close()
            return True
        except Exception as e:
            logger.error(e)
            return False