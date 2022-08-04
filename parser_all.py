from time import time

import requests
import json
from tqdm import tqdm
import concurrent.futures
from bs4 import BeautifulSoup

class Parser:
    def page_parser_rosrid(self, date):
        PARAMS = {"search_query":None,
        "sort_by":"Дата регистрации",
        "critical_technologies":[],
        "dissertations":True,
        "full_text_available":False,
        "ikrbses":True,
        "nioktrs":True,
        "organization":[],
        "page":1,
        "priority_directions":[],
        "rids":True,
        "rubrics":[],
        "search_area":"Во всех полях",
        "open_license":False,
        "free_licenses":False,
        "expert_estimation_exist":False,
        "end_date": f"{date}",
        "start_date": f"{date}"
        }    
        response_data = []
        for page in range(1, 1001):
            PARAMS['page'] = page                
            r = requests.post(url = 'https://www.rosrid.ru/api/base/search', data=PARAMS)
            if r.ok:
                response = r.json()
                response = response['hits']['hits']
                if not response:
                    break
                response_data.extend(response)               
        return response_data


    def parse_rnf_thread(self, link):
        link = f'https://rscf.ru/prjcard_int?{link}'

        r = requests.get(link) 
        if r.ok:
            soup = BeautifulSoup(r.text, 'html.parser')

            parsed = []
            for i in soup.find_all('p'):
                if i.find('span'):
                    s = i.find('span')     
                    if 'Прежний руководитель' not in s:
                        new = i.text.replace(s.text, '')              
                        new = new.replace('\t', '')
                        new = new.replace('\n', '')
                        new = new.replace('\xa0', '')
                        new = new.replace("'", '')
                        if new != '':
                            parsed.append(new)        

            reg_num = parsed[0]
            name = parsed[1]
            annotation = parsed[9]
            region = parsed[3][parsed[3].rfind(',')+2:]
            date = parsed[4][:4]

            author = parsed[2].split(',')
            author_name = author[0].split()[1]
            author_surname = author[0].split()[0]
            
            try:
                author_patronymic = author[0].split()[2]
            except:
                author_patronymic = 'None'
            
            position = author[1]

            org_name = parsed[3][:parsed[3].rfind(',')]
            org_short_name = None

            area_of_knowledge = parsed[6]

            period_of_execution = parsed[4]
            contest = parsed[5]

            keywords = [word for word in parsed[7].split(', ')]

            rubrics_codes = [code for code in parsed[8].split(', ')]

            expected_results = parsed[10]
            if len(parsed) > 11:
                reporting_materials = parsed[11]
            else:
                reporting_materials = None


            data = {'_index': 'rnfs',
            '_source': {'name': f'{name}',
            'annotation': f'{annotation}',
            'keyword_list': [{'name':key} for key in keywords],
            'work_supervisor': {'name': f'{author_name}',
            'surname': f'{author_surname}',
            'patronymic': f'{author_patronymic}',
            'position': f'{position}'},
            'organization_supervisor': {'name': 'None',
            'surname': 'None',
            'patronymic': 'None',
            'position': 'None'},
            'executor': {'name': f'{org_name}',
            'short_name': f'{org_short_name}',
            'region': {'name': f'{region}'}},
            'customer': {'name': 'None',
            'short_name': 'None'},
            'rubrics': [{'code' : rubric, 'udk' : 'None'} for rubric in rubrics_codes],
            'oecds': [],
            'last_status': {'created_date': f'{date}',
            'registration_number': f'{reg_num}'},
            'period_of_execution' : f'{period_of_execution}',
            'contest': f'{contest}',
            'area_of_knowledge': f'{area_of_knowledge}',
            'expected_results' : f'{expected_results}',
            'reporting_materials' : f'{reporting_materials}'}}

        return data




    def parse_rnf(self, year):
        data = []
        link = f'https://rscf.ru/extfilter?cmd=*WHERE%20(a.enf_year_beginw%20%3D%20%3F)&args[]={year}'
        r = requests.get(link)
        soup = BeautifulSoup(r.text, 'html.parser')

        links = set()
        for l in soup.find_all('a'):    
            res = l.get('href')
            if res != None:
                links.add(res[-11:])

        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = executor.map(self.parse_rnf_thread, links) 
        
        for result in results:
            data.append(result)
        return data



    def get_all(self):    
        all_reg_nums = set()
        for i in tqdm(range(2022, 2010, -1)):
            if i % 4 == 0:
                leap_year = 30
            else:
                leap_year = 29
            all_data = []
            if i == 2022:
                kalendari = [
                        [f'{i}-01-{j}' for j in range(1, 32)],
                        [f'{i}-02-{j}' for j in range(1, leap_year)],
                        [f'{i}-03-{j}' for j in range(1, 32)],
                        [f'{i}-04-{j}' for j in range(1, 31)],
                        [f'{i}-04-{j}' for j in range(1, 7)]
                    ]
            else:
                kalendari = [
                        [f'{i}-01-{j}' for j in range(1, 32)],
                        [f'{i}-02-{j}' for j in range(1, leap_year)],
                        [f'{i}-03-{j}' for j in range(1, 32)],
                        [f'{i}-04-{j}' for j in range(1, 31)],
                        [f'{i}-05-{j}' for j in range(1, 32)],
                        [f'{i}-06-{j}' for j in range(1, 31)],
                        [f'{i}-07-{j}' for j in range(1, 32)],
                        [f'{i}-08-{j}' for j in range(1, 32)],
                        [f'{i}-09-{j}' for j in range(1, 31)],
                        [f'{i}-10-{j}' for j in range(1, 32)],
                        [f'{i}-11-{j}' for j in range(1, 31)],
                        [f'{i}-12-{j}' for j in range(1, 32)]
                    ]
            if i != 2022:            
                break
            print(' Парсим РОСРИД')
            
            for _, mounth in enumerate(tqdm(kalendari)):
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    data = executor.map(self.page_parser_rosrid, mounth) 
                                             
                for block in data:
                    if block:
                        sub_block = []
                        for work in block:
                            if work['_source']['last_status']['registration_number'] not in all_reg_nums:
                                all_reg_nums.add(work['_source']['last_status']['registration_number'])
                                sub_block.append(work)
                        all_data.extend(sub_block)                         
            print(' Парсим РНФ')
            if i > 2013:
                works = self.parse_rnf(i)
                for work in works:
                    if work['_source']['last_status']['registration_number'] not in all_reg_nums:
                        all_reg_nums.add(work['_source']['last_status']['registration_number'])
                        all_data.append(work)
            
            file_name = f'Debug/data_{i}.json'
            with open(file_name, 'w', encoding='UTF-8') as outfile:
                json.dump(all_data, outfile, ensure_ascii=False)


if __name__ == '__main__':
    start = time()
    parser = Parser()
    parser.get_all()

    print(round((time()-start)/60, 2), 'Минут прошло')

