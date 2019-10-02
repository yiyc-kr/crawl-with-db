#!/usr/bin/env python
# coding: utf-8

import argparse
import json
import time

args_list = ["config_file", "db_url", "keyword", "request_url", "stage"]


def user_input():
    config = argparse.ArgumentParser()
    config.add_argument('-cf', '--config_file', help='config file name', default='', type=str, required=False)
    config_file_check = config.parse_known_args()
    object_check = vars(config_file_check[0])

    records = []
    if object_check['config_file'] != '':
        json_file = json.load(open(config_file_check[0].config_file))
        db_url = json_file['db_url']
        for record in range(0, len(json_file['Records'])):
            arguments = {}
            for i in args_list:
                arguments[i] = None
            for key, value in json_file['Records'][record].items():
                arguments[key] = value
            records.append(arguments)
    else:
        # Taking command line arguments from users
        parser = argparse.ArgumentParser()
        parser.add_argument('-db', '--db_url', help='db_url', type=str, required=True)
        parser.add_argument('-k', '--keyword', help='keyword', type=str, required=True)
        parser.add_argument('-ru', '--request_url',
                            help='prefer to type request url in db', type=str, required=True)
        parser.add_argument('-st', '--stage',
                            help='1. search result list of keyword\
                             2. select one of them and catch it', type=str, required=True)

        parser.add_argument('-gd', '--get_data', help='just get data from site', type=bool, nargs='?',
                            const=True, required=False)

        args = parser.parse_args()
        arguments = vars(args)
        records.append(arguments)
        db_url = records[0].pop('db_url')
    return db_url, records


class CrawlerWithDb:
    def __init__(self):
        pass

    def build_search_url(self, search_term):
        url = 'https://www.google.com/search?q=' + quote(search_term.encode('utf-8')) + \
              '&espv=2&biw=1366&bih=667&site=webhp&source=lnms&tbm=isch&sa=X&ei=XosDVaCXD8TasATItgE&ved=0CAcQ_AUoAg'

        return url

    def download_page(self, url):
        try:
            headers = {}
            headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 " \
                                    "(KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
            req = urllib.request.Request(url, headers=headers)
            resp = urllib.request.urlopen(req)
            respData = str(resp.read())
            return respData

        except Exception as e:
            print("Could not open URL. Please check your internet connection and/or ssl settings \n"
                  "If you are using proxy, make sure your proxy settings is configured correctly")
            sys.exit()

    def _get_next_item(self, s):
        start_line = s.find('rg_meta notranslate')
        if start_line == -1:  # If no links are found then give an error!
            end_quote = 0
            link = "no_links"
            return link, end_quote
        else:
            start_line = s.find('class="rg_meta notranslate">')
            start_object = s.find('{', start_line + 1)
            end_object = s.find('</div>', start_object + 1)
            object_raw = str(s[start_object:end_object])
            # remove escape characters based on python version
            version = (3, 0)
            cur_version = sys.version_info
            if cur_version >= version:  # python3
                try:
                    object_decode = bytes(object_raw, "utf-8").decode("unicode_escape")
                    final_object = json.loads(object_decode)
                except:
                    final_object = ""
            else:  # python2
                try:
                    final_object = (json.loads(self.repair(object_raw)))
                except:
                    final_object = ""
            return final_object, end_object

    # Format the object in readable format
    def format_object(self, object):
        formatted_object = {}
        formatted_object['image_format'] = object['ity']
        formatted_object['image_height'] = object['oh']
        formatted_object['image_width'] = object['ow']
        formatted_object['image_link'] = object['ou']
        formatted_object['image_description'] = object['pt']
        formatted_object['image_host'] = object['rh']
        formatted_object['image_source'] = object['ru']
        formatted_object['image_thumbnail_url'] = object['tu']
        return formatted_object

    # TODO: Arrange parameters
    def download_image(self, image_url, image_format, main_directory, dir_name, count, print_urls,
                        socket_timeout, prefix, print_size, no_numbering, no_download, save_source, img_src, img_host,
                        silent_mode, thumbnail_only, format, ignore_urls, search_term):
        if not silent_mode:
            if print_urls or no_download:
                print("Image URL: " + image_url)
        if ignore_urls:
            if any(url in img_host for url in ignore_urls):
                return "fail", "Image ignored due to blacklist", None, image_url
        if thumbnail_only:
            return "success", "Skipping image download...", str(image_url[(image_url.rfind('/')) + 1:]), image_url
        if no_download:
            return "success", "Printed url without downloading", None, image_url
        try:
            req = Request(image_url, headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"})
            try:
                # timeout time to download an image
                if socket_timeout:
                    timeout = float(socket_timeout)
                else:
                    timeout = 10

                response = urlopen(req, None, timeout)
                data = response.read()
                response.close()

                extensions = [".jpg", ".jpeg", ".gif", ".png", ".bmp", ".svg", ".webp", ".ico"]
                # keep everything after the last '/'
                image_name = str(image_url[(image_url.rfind('/')) + 1:])
                if format:
                    if not image_format or image_format != format:
                        download_status = 'fail'
                        download_message = "Wrong image format returned. Skipping..."
                        return_image_name = ''
                        absolute_path = ''
                        return download_status, download_message, return_image_name, absolute_path

                if image_format == "" or not image_format or "." + image_format not in extensions:
                    download_status = 'fail'
                    download_message = "Invalid or missing image format. Skipping..."
                    return_image_name = ''
                    absolute_path = ''
                    return download_status, download_message, return_image_name, absolute_path
                elif image_name.lower().find("." + image_format) < 0:
                    image_name = image_name + "." + image_format
                else:
                    image_name = search_term + image_name[image_name.lower().rfind("." + image_format):]
                    image_name = image_name[:image_name.lower().find("." + image_format) + (len(image_format) + 1)]

                # prefix name in image
                if prefix:
                    prefix = prefix + " "
                else:
                    prefix = ''

                if no_numbering:
                    path = main_directory + "/" + dir_name + "/" + prefix + image_name
                else:
                    image_name = image_name[:image_name.lower().find("." + image_format)] + "#" + str(count)\
                                 + image_name[image_name.lower().find("." + image_format):]
                    path = main_directory + "/" + dir_name + "/" + image_name

                try:
                    if not os.path.isdir(os.path.join(main_directory, dir_name)):
                        os.makedirs(os.path.join(main_directory, dir_name))
                    output_file = open(path, 'wb')
                    output_file.write(data)
                    output_file.close()
                    if save_source:
                        list_path = main_directory + "/" + save_source + ".txt"
                        list_file = open(list_path, 'a')
                        list_file.write(path + '\t' + img_src + '\n')
                        list_file.close()
                    absolute_path = os.path.abspath(path)
                except OSError as e:
                    download_status = 'fail'
                    download_message = "OSError on an image...trying next one..." + " Error: " + str(e)
                    return_image_name = ''
                    absolute_path = ''
                    return download_status, download_message, return_image_name, absolute_path

                # return image name back to calling method to use it for thumbnail downloads
                download_status = 'success'
                download_message = "Completed Image ====> " + prefix + image_name
                return_image_name = prefix + image_name

                # image size parameter
                if not silent_mode:
                    if print_size:
                        print("Image Size: " + str(self.file_size(path)))

            except UnicodeEncodeError as e:
                download_status = 'fail'
                download_message = "UnicodeEncodeError on an image...trying next one..." + " Error: " + str(e)
                return_image_name = ''
                absolute_path = ''

            except URLError as e:
                download_status = 'fail'
                download_message = "URLError on an image...trying next one..." + " Error: " + str(e)
                return_image_name = ''
                absolute_path = ''

            except BadStatusLine as e:
                download_status = 'fail'
                download_message = "BadStatusLine on an image...trying next one..." + " Error: " + str(e)
                return_image_name = ''
                absolute_path = ''

        except HTTPError as e:  # If there is any HTTPError
            download_status = 'fail'
            download_message = "HTTPError on an image...trying next one..." + " Error: " + str(e)
            return_image_name = ''
            absolute_path = ''

        except URLError as e:
            download_status = 'fail'
            download_message = "URLError on an image...trying next one..." + " Error: " + str(e)
            return_image_name = ''
            absolute_path = ''

        except ssl.CertificateError as e:
            download_status = 'fail'
            download_message = "CertificateError on an image...trying next one..." + " Error: " + str(e)
            return_image_name = ''
            absolute_path = ''

        except IOError as e:  # If there is any IOError
            download_status = 'fail'
            download_message = "IOError on an image...trying next one..." + " Error: " + str(e)
            return_image_name = ''
            absolute_path = ''

        except IncompleteRead as e:
            download_status = 'fail'
            download_message = "IncompleteReadError on an image...trying next one..." + " Error: " + str(e)
            return_image_name = ''
            absolute_path = ''

        return download_status, download_message, return_image_name, absolute_path

    def _get_all_items(self, page, main_directory, dir_name, limit, blacklist, search_term, arguments):
        items = []
        abs_path = []
        errorCount = 0
        i = 0
        count = 1

        while count < limit + 1:
            object, end_content = self._get_next_item(page)

            if object == "no_links":
                break
            elif object == "":
                page = page[end_content:]
            else:
                # format the item for readability
                object = self.format_object(object)
                if arguments['metadata']:
                    if not arguments["silent_mode"]:
                        print("\nImage Metadata: " + str(object))

                # download the images
                download_status, download_message, return_image_name, absolute_path = self.download_image(
                    object['image_link'], object['image_format'], main_directory, dir_name, count,
                    arguments['print_urls'], arguments['socket_timeout'], arguments['prefix'], arguments['print_size'],
                    arguments['no_numbering'], arguments['no_download'], arguments['save_source'],
                    object['image_source'], object['image_host'], arguments["silent_mode"], arguments["thumbnail_only"], arguments['format'],
                    blacklist, search_term)
                if not arguments["silent_mode"]:
                    print(download_message)
                if download_status == "success":

                    # download image_thumbnails
                    if arguments['thumbnail'] or arguments["thumbnail_only"]:
                        download_status, download_message_thumbnail = self.download_image_thumbnail(
                            object['image_thumbnail_url'], main_directory, dir_name, return_image_name,
                            arguments['print_urls'], arguments['socket_timeout'], arguments['print_size'],
                            arguments['no_download'], arguments['save_source'], object['image_source'],
                            arguments['ignore_urls'])
                        if not arguments["silent_mode"]:
                            print(download_message_thumbnail)

                    count += 1
                    object['image_filename'] = return_image_name
                    items.append(object)  # Append all the links in the list named 'Links'
                    abs_path.append(absolute_path)
                else:
                    errorCount += 1

                # delay param
                if arguments['delay']:
                    time.sleep(int(arguments['delay']))

                page = page[end_content:]
            i += 1
        if count < limit:
            print("\n\nUnfortunately all " + str(
                limit) + " could not be downloaded because some images were not downloadable. " + str(
                count - 1) + " is all we got for this search filter!")
        return items, errorCount, abs_path

    def get_data(self, db_url, arguments):


        for i in args_list:
            if i not in arguments:
                arguments[i] = None

        paths = {}
        errorCount = None

        if arguments['keyword']:
            keyword = str(arguments['keyword'])
        else:
            raise ValueError('keyword is a required argument!')

        if arguments['blacklist']:
            blacklist = [str(item).strip() for item in arguments['blacklist'].split(',')]

        if arguments['limit']:
            limit = int(arguments['limit'])
        else:
            limit = 100

        if arguments['output_directory']:
            main_directory = arguments['output_directory']
        else:
            main_directory = "images"

        if arguments['id']:
            id = str(arguments['id'])
        else:
            id = keyword

        if arguments['word_slice']:
            word_slice = int(arguments['word_slice'])
            if len(keyword.split()) < word_slice:
                raise ValueError("word_slice should be lower than keyword's word counts("
                                 + str(len(keyword.split())) + ")")
            else:
                keywords = []
                for i in range(word_slice, len(keyword.split())+1):
                    keywords += [' '.join(keyword.split()[item:item+i]) for item in range(len(keyword.split())-i+1)]
        else:
            keywords = [keyword]

        for search_term in keywords:
            url = self.build_search_url(search_term)

            if limit < 101:
                raw_html = self.download_page(url)
            else:
                print("TODO")   # TODO: download_page using selenium

            if not arguments["silent_mode"]:
                print("[ " + keyword + " ] Starting Download... [ " + search_term + " ]")

            items, errorCount, abs_path = self._get_all_items(raw_html, main_directory, id, limit, blacklist,
                                search_term, arguments)

        return abs_path, errorCount


def main():
    db_url, records = user_input()
    total_errors = 0
    t0 = time.time()  # start the timer

    for arguments in records:

        crawler = CrawlerWithDb()
        result_data, errors = crawler.get_data(db_url, arguments)
        total_errors = total_errors + errors


if __name__ == "__main__":
    main()