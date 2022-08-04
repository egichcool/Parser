import requests
import json
from tqdm import tqdm
import concurrent.futures
from bs4 import BeautifulSoup
import datetime
from sqlite3 import connect as sqlite3_connect
from sqlite3 import Error as sqlite3_Error

class Parser:
    def __init__(self) -> None:
        pass        


    def create_connection(self, path='DB/DB_V1.sqlite'):
        connection = None
        try:
            connection = sqlite3_connect(path)
        except sqlite3_Error as e:
            print(f'Ошибка {e}')
        return connection
        
    def execute_query(self, connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except sqlite3_Error as e:
            print(f"The error '{e}' occurred")

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
            request = f'''
            SELECT EXISTS(SELECT reg_num FROM main WHERE reg_num="{reg_num}")
            '''
            conn = self.create_connection()
            result = self.execute_query(conn, request)
            conn.close()
            if result[0][0] == 0:                        
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
        




    def parse_rnf(self, year=2022):
        data = []
        link = f'https://rscf.ru/extfilter?cmd=*WHERE%20(a.enf_year_beginw%20%3D%20%3F)&args[]={year}'
        r = requests.get(link)
        soup = BeautifulSoup(r.text, 'html.parser')

        links = set()
        conn = self.create_connection()   
        for l in soup.find_all('a'):    
            res = l.get('href')            
            if res != None:
                reg_num = res[-11:]
                request = f'''
                SELECT EXISTS(SELECT reg_num FROM main WHERE reg_num="{reg_num}")
                '''
                result = self.execute_query(conn, request)
                if result[0][0] == 0:
                    links.add(reg_num)
        conn.close()
        with concurrent.futures.ProcessPoolExecutor() as executor:
            results = executor.map(self.parse_rnf_thread, links) 
                                
        for result in results:     
            if result != None:       
                data.append(result)
            
        file_name = f'Data/temp.json'
        with open(file_name, 'w', encoding='UTF-8') as outfile:
            json.dump(data, outfile, ensure_ascii=False)
        if data != []:
            return True
        else:
            return False



    def rosrid_parser(self, year=2022):    
        all_reg_nums = set()        

        conn = self.create_connection()
        requets = '''SELECT reg_num FROM main WHERE not _index="rnfs" ORDER BY reg_date DESC LIMIT 1'''
        reg_num = self.execute_query(conn, requets)   
        reg_num = reg_num[0][0]
        conn.close()

        if year % 4 == 0:
            leap_year = 29
        else:
            leap_year = 28
        all_data = []
        kalendari = [
                [f'{year}-12-{j}' for j in range(31, 0, -1)],
                [f'{year}-11-{j}' for j in range(leap_year, 0, -1)],
                [f'{year}-10-{j}' for j in range(31, 0, -1)],
                [f'{year}-09-{j}' for j in range(30, 0, -1)],
                [f'{year}-08-{j}' for j in range(31, 0, -1)],
                [f'{year}-07-{j}' for j in range(30, 0, -1)],
                [f'{year}-06-{j}' for j in range(31, 0, -1)],
                [f'{year}-05-{j}' for j in range(31, 0, -1)],
                [f'{year}-04-{j}' for j in range(30, 0, -1)],
                [f'{year}-03-{j}' for j in range(31, 0, -1)],
                [f'{year}-02-{j}' for j in range(30, 0, -1)],
                [f'{year}-01-{j}' for j in range(31, 0, -1)]
            ]
        for _, month in enumerate(tqdm(kalendari)):
            if int(month[0][5:7]) > datetime.datetime.now().month:
                continue
            elif int(month[0][5:7]) == datetime.datetime.now().month:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    data = executor.map(self.page_parser_rosrid, month[len(month)-datetime.datetime.now().day:])
                for block in data:
                    if block:
                        sub_block = []
                        for work in block:
                            if work['_source']['last_status']['registration_number'] == reg_num:
                                return all_data
                            if work['_source']['last_status']['registration_number'] not in all_reg_nums:
                                all_reg_nums.add(work['_source']['last_status']['registration_number'])
                                sub_block.append(work)
                        all_data.extend(sub_block)  
            elif int(month[0][5:7]) < datetime.datetime.now().month:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    data = executor.map(self.page_parser_rosrid, month) 
                                            
                for block in data:
                    if block:
                        sub_block = []
                        for work in block:
                            if work['_source']['last_status']['registration_number'] == reg_num:
                                return all_data
                            if work['_source']['last_status']['registration_number'] not in all_reg_nums:
                                all_reg_nums.add(work['_source']['last_status']['registration_number'])
                                sub_block.append(work)
                        all_data.extend(sub_block)                                                


    def parse_rosrid(self):
        all_data = self.rosrid_parser()        
        file_name = f'Data/temp.json'
        with open(file_name, 'w', encoding='UTF-8') as outfile:
            json.dump(all_data, outfile, ensure_ascii=False)        
        if all_data != []:
            return True
        else:
            return False
