import json
from sqlite3 import connect as sqlite3_connect
from sqlite3 import Error as sqlite3_Error
from pymorphy2 import MorphAnalyzer
from tqdm import tqdm
from flashtext import KeywordProcessor
import re
from openpyxl import load_workbook
import concurrent.futures
from time import time


class Semantic_module():
    def __init__(self, db_name='DB/DB_V1.sqlite') -> None:    
        self.db_name = db_name 
        with open('Data/predlogi.txt', 'r', encoding='utf-8') as f:
            predlogi = f.readline()
        predlogi = predlogi.split(',')
        self.punctuation_processor = KeywordProcessor()
        for word in predlogi:
            self.punctuation_processor.add_keyword(word, ' ')

        self.morph = MorphAnalyzer()
        
        self.dict_keywords = {}
        self.dict_processors = {}
        self.dict_counters = {}
        obj = load_workbook('Data/Ключевые слова по областям.xlsx')
        sheet = obj.active
        main_state = ''
        sub_state = ''
        line = 2


        while True:
            line += 1
            if sheet[f'A{line}'].value == None:
                break
            if sheet[f'B{line}'].value != None:
                main_state = sheet[f'B{line}'].value
                self.dict_keywords.update({main_state: {}})
            if sheet[f'C{line}'].value != None:
                sub_state = sheet[f'C{line}'].value
                self.dict_keywords[main_state][sub_state] = []
            if sheet[f'F{line}'].value != None:
                f_cell = sheet[f'F{line}'].value.split(', ')
                g_cell = sheet[f'G{line}'].value.split(', ')

                f_cell_corrected = []
                for block in f_cell:
                    corrected_block = self.punctuation_processor.replace_keywords(block)
                    new_f = ''
                    for word in corrected_block.split():
                        new_f = new_f + ' ' + self.morph.parse(word)[0].normal_form
                    f_cell_corrected.extend([new_f[1:]])
                self.dict_keywords[main_state][sub_state].extend(f_cell_corrected)
                self.dict_keywords[main_state][sub_state].extend(g_cell)
        
        for _, (main_key, main_diction) in enumerate(self.dict_keywords.items()):
            self.dict_processors.update({main_key : {}})
            self.dict_counters.update({main_key : {}})
            for j, (sub_key, sub_diction) in enumerate(main_diction.items()):
                self.dict_processors[main_key][sub_key] = KeywordProcessor()
                for keyword in sub_diction:
                    self.dict_processors[main_key][sub_key].add_keyword(keyword)
                self.dict_counters[main_key][sub_key] = 0


    def create_connection(self):
        connection = None
        try:
            connection = sqlite3_connect(self.db_name)
        except sqlite3_Error as e:
            print(f'Ошибка {e}')
        return connection
        
    def execute_query(self, connection, query):
        cursor = connection.cursor()
        try:
            cursor.execute(query)
            connection.commit()
        except sqlite3_Error as e:            
            if repr(e) != "OperationalError('incomplete input')":
                with open(f'Debug/sqlerrors/{e}_{int(time())}.txt', 'w', encoding='UTF-8') as f:
                    f.write(query)
    
    def execute_read_query(self, connection, query):
        cursor = connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except sqlite3_Error as e:
            print(f"The error '{e}' occurred")
    

    def get_all_scrap_simbols(self):
        with open('Data/punctuation.txt', 'r', encoding='UTF-8') as f:
            trash_simbols = f.read()
        return f'[{trash_simbols}]'

    def replase_scrap_and_pretext(self, input):
        replased = re.sub(self.get_all_scrap_simbols(), ' ', input)
        return self.punctuation_processor.replace_keywords(replased)
    
    def replase_scrap(self, text):
        with open('Data/trash_simbols.txt', 'r', encoding='UTF-8') as f:
            trash_simbols = f.read()        
        return re.sub(f'[{trash_simbols}]', ' ', text)

    def find_priority_thread(self, block):
        result = {}
        name = block['_source']['name']
        keywords = [word['name'] for word in block['_source']['keyword_list']]
        if block['_index'] in ['nioktrs', 'rnfs']:
            annotation = block['_source']['annotation']
        elif block['_index'] in ['ikrbses', 'dissertations', 'rids']:
            annotation = block['_source']['abstract']
        registration_number = block['_source']['last_status']['registration_number']                                   
        all_text = f'{name} {annotation} {" ".join(keywords)}'
        all_text = self.replase_scrap_and_pretext(all_text)

        best_value = 0
        best_main_priority = 'Другие (20Я)'
        best_sub_priority = 'Другие'
        for i, (main_key, main_diction) in enumerate(self.dict_processors.items()):
            for j, (sub_key, proccessor) in enumerate(main_diction.items()):
                value = len(set(proccessor.extract_keywords(all_text)))
                if value > best_value:
                    best_value = value
                    best_main_priority = main_key
                    best_sub_priority = sub_key

        result['best_main_priority'] = best_main_priority[-2:-1]
        result['best_sub_priority'] = best_sub_priority
        result['all_text'] = all_text
        result['registration_number'] = registration_number

        return result


    def make_keywords(self):    #Основа                  
        all_words = {}
       
        file_name = f'Data/temp'
        with open(f'{file_name}.json', 'r', encoding='UTF-8') as f:
            data = json.load(f)  


        print(' Обработка и поиск приоритетных направлений')            
        with concurrent.futures.ProcessPoolExecutor() as executor:
            result = list(tqdm(executor.map(self.find_priority_thread, data), total=len(data)))

        print(' Добавление в базу новых ключей и обновление json с добавлением полного текста') 
        for i, block in enumerate(tqdm(result)):
            data[i]['_source']['full_text'] = block['all_text']
            data[i]['_source']['main_priority'] = block['best_main_priority']
            data[i]['_source']['sub_priority'] = block['best_sub_priority']
            temp_words = set(block['all_text'].split())
            for word in temp_words:
                if len(word) > 2:
                    word = self.morph.parse(word)[0].normal_form
                    conn = self.create_connection()
                    request = f'''
                    SELECT EXISTS(SELECT word FROM keysearch WHERE word="{word}")
                    '''
                    result = self.execute_read_query(conn, request)                    
                    if result[0][0] == 0:      
                        insert_keysearch = f'''
                        INSERT INTO keysearch (word) VALUES ("{word}")
                        '''
                        insert_keywords = f'''
                        INSERT INTO keysearch_words(word, reg_num)
                        VALUES (("{word}"), ("{block['registration_number']}"))
                        '''
                        self.execute_query(conn, insert_keysearch)
                        self.execute_query(conn, insert_keywords)
                    conn.close()

        with open(f'{file_name}_full.json', 'w', encoding='UTF-8') as f:
            json.dump(data, f, ensure_ascii=False)              

    def fill_tread(self, data):
        result = {}        

        if data['_index'] in ['nioktrs', 'rnfs']:
            annotation = data['_source']['annotation']
        elif data['_index'] in ['ikrbses', 'dissertations', 'rids']:
            annotation = data['_source']['abstract']  
        if annotation != None:
            annotation = annotation.replace("'", '')
        name = data['_source']['name']
        if name != None:
            name = name.replace("'", '')
        
        if data['_index'] in ['rnfs']:
            date = f"{data['_source']['last_status']['created_date']}-01-01"
        elif data['_index'] in ['ikrbses', 'dissertations', 'rids', 'nioktrs']:
            date = data['_source']['last_status']['created_date']
        
        if data['_index'] in ['nioktrs', 'ikrbses', 'rids', 'rnfs']:
            try:
                author_name = data['_source']['work_supervisor']['name']
            except KeyError:
                author_name = None
            try:
                author_surname = data['_source']['work_supervisor']['surname']
            except KeyError:
                author_surname = None
            try:
                author_patronymic = data['_source']['work_supervisor']['patronymic']
            except KeyError:
                author_patronymic = None
            try:
                author_position = data['_source']['work_supervisor']['position']
            except KeyError:
                author_position = None

            try:
                other_org_name = data['_source']['customer']['name']
                other_org_short_name = data['_source']['customer']['short_name']
            except KeyError:
                other_org_name = None
                other_org_short_name = None
            if data['_index'] in ['nioktrs', 'ikrbses', 'rnfs']:
                try:
                    region = data['_source']['executor']['region']['name']
                except KeyError:
                    region = None
                try:
                    org_name = data['_source']['executor']['name']
                except KeyError:
                    org_name = None
                try:
                    org_short_name = data['_source']['executor']['short_name']
                except KeyError:
                    org_short_name = None
            elif data['_index'] in ['rids']:
                try:
                    region = data['_source']['executors'][0]['region']['name']
                except KeyError:
                    region = None
                except IndexError:
                    region = None
                try:
                    org_name = data['_source']['executors'][0]['name']
                    org_short_name = data['_source']['executors'][0]['short_name']
                except KeyError:
                    org_name = None
                    org_short_name = None
                except IndexError:
                    org_name = None
                    org_short_name = None
        else:
            try:
                region = data['_source']['author_organization']['region']['name']
            except KeyError:
                region = None
            try:
                org_name = data['_source']['author_organization']['name']
            except KeyError:
                org_name = None
            try:
                org_short_name = data['_source']['author_organization']['short_name']
            except KeyError:
                org_short_name = None
            try:
                author_name = data['_source']['author_name']
                author_surname = data['_source']['author_surname']
                author_patronymic = data['_source']['author_patronymic']
            except KeyError:
                author_name = None
                author_surname = None
                author_patronymic = None
            author_position = None    
            try:
                other_org_name = data['_source']['protection_organization']['name']
                other_org_short_name = data['_source']['protection_organization']['short_name']
            except KeyError:
                other_org_name = None
                other_org_short_name = None
        
        try:
            org_supervisor_name = data['_source']['organization_supervisor']['name']
        except KeyError:
            org_supervisor_name = None
        try:
            org_supervisor_surname = data['_source']['organization_supervisor']['surname']
        except KeyError:
            org_supervisor_surname = None
        try:
            org_supervisor_patronymic = data['_source']['organization_supervisor']['patronymic']
        except KeyError:
            org_supervisor_patronymic = None
        try:
            org_supervisor_position = data['_source']['organization_supervisor']['position']
        except KeyError:
            org_supervisor_position = None

        result['main'] =  f"""(('{data['_source']['last_status']['registration_number']}'),
        ('{data['_index']}'), ('{name}'), ('{annotation}'), ('{data['_source']['full_text']}'), 
        ('{region}'), ('{date}'), ('{date[:10]}'), ('{author_name}'), ('{author_surname}'), 
        ('{author_patronymic}'), ('{author_position}'), ('{org_name}'), ('{org_short_name}'), 
        ('{org_supervisor_name}'), ('{org_supervisor_surname}'), 
        ('{org_supervisor_patronymic}'), ('{org_supervisor_position}'),
        ('{other_org_name}'),('{other_org_short_name}'),('{data['_source']['main_priority']}'),('{data['_source']['sub_priority']}')),"""
              

        result['keywords'] = ''
        for key in data['_source']['keyword_list']:
            keyword = key['name'].replace("'", '')
            result['keywords'] += f"(('{data['_source']['last_status']['registration_number']}'), ('{keyword}')),"            

        result['oecds'] = ''
        for oecds in data['_source']['oecds']:
            name = oecds['name'].replace("'", '')
            code = oecds['code'].replace("'", '')
            result['oecds'] += f"""(('{data['_source']['last_status']['registration_number']}'),
            ('{name}'), ('{code}')),"""
        
        result['rubrics'] = ''
        for rubric in data['_source']['rubrics']:
            result['rubrics'] += f"""(('{data['_source']['last_status']['registration_number']}'),
            ('{rubric['code']}'), ('{rubric['udk']}')),"""

        result['dissertations'] = ''
        result['supervisors'] = ''
        result['opponents'] = ''
        result['authors'] = ''
        result['rids'] = ''
        result['ikrbses'] = ''
        result['nioktrs'] = ''
        result['nioktr_budgets'] = ''
        result['nioktr_technology'] = ''
        result['nioktr_priority'] = ''
        result['nioktr_coexecutors'] = ''
        result['rnfs'] = ''
        if data['_index'] == 'dissertations':
            try:
                dissertation_type = data['_source']['dissertation_type']['name']
            except KeyError:
                dissertation_type = None
            try:
                dissertation_report_type = data['_source']['dissertation_report_type']['name']
            except KeyError:
                dissertation_report_type = None
            try:
                degree_pursued = data['_source']['degree_pursued']['name']
            except KeyError:
                degree_pursued = None
            try:
                chairman_dissertation_council_name = data['_source']['chairman_dissertation_council']['name']
            except KeyError:
                chairman_dissertation_council_name = None
            try:
                chairman_dissertation_council_surname = data['_source']['chairman_dissertation_council']['surname']
            except KeyError:
                chairman_dissertation_council_surname = None
            try:
                chairman_dissertation_council_patronymic = data['_source']['chairman_dissertation_council']['patronymic']
            except KeyError:
                chairman_dissertation_council_patronymic = None
            try:
                chairman_dissertation_council_position = data['_source']['chairman_dissertation_council']['position']
            except KeyError:
                chairman_dissertation_council_position = None
            try:
                chairman_dissertation_council_degree = data['_source']['chairman_dissertation_council']['degree']['name']
            except KeyError:
                chairman_dissertation_council_degree = None
            try:
                speciality_code = data['_source']['speciality_codes'][0]['code']
            except KeyError:
                speciality_code = None
            except IndexError:
                speciality_code = None
            try:
                speciality_name = data['_source']['speciality_codes'][0]['name']
            except KeyError:
                speciality_name = None
            except IndexError:
                speciality_name = None
            
            result['dissertations'] += f"""(('{data['_source']['last_status']['registration_number']}'),
            ('{dissertation_type}'),('{dissertation_report_type}'),
            ('{degree_pursued}'),('{chairman_dissertation_council_name}'),
            ('{chairman_dissertation_council_surname}'),('{chairman_dissertation_council_patronymic}'),
            ('{chairman_dissertation_council_position}'),('{chairman_dissertation_council_degree}'),
            ('{speciality_code} : {speciality_name}'),
            ('{data['_source']['protection_date']}'),('{data['_source']['tables_count']}'),('{data['_source']['pictures_count']}'),
            ('{data['_source']['bibliography']}'),('{data['_source']['applications_count']}'),('{data['_source']['pages_count']}'),
            ('{data['_source']['sources_count']}'),('{data['_source']['books_count']}')),"""
            
            try:
                for block in data['_source']['supervisors']:
                    try:
                        fio = block['fio'].replace("'", '')
                    except:
                        fio = None
                    try:
                        scientific_degree = block['scientific_degree']['name'].replace("'", '')
                    except:
                        scientific_degree = None
                    try:
                        speciality_code = block['speciality_code']['code'].replace("'", '')
                    except:
                        speciality_code = None
                    try:
                        speciality_name = block['speciality_code']['name'].replace("'", '')
                    except:
                        speciality_name = None
                    
                    result['supervisors'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                    ('{fio}'),('{scientific_degree}'),('{speciality_name}'),
                    ('{speciality_code}')),"""
            except KeyError:
                pass
            
            try:
                for block in data['_source']['opponents']:
                    result['opponents'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                    ('{block['fio']}'),('{block['scientific_degree']['name']}'),('{block['speciality_code']['name']}'),
                    ('{block['speciality_code']['code']}')),"""
            except KeyError:
                pass
        
        elif data['_index'] in ['rids', 'ikrbses']:
            for block in data['_source']['authors']:
                if block['description'] != None:
                    description = block['description'].replace("'", '')
                else:
                    description = None
                patronymic = block['patronymic']
                if patronymic != None:
                    patronymic = patronymic.replace("'", '')
                name = block['name']
                if name != None:
                    name = name.replace("'", '')
                surname = block['surname']
                if surname != None:
                    surname = surname.replace("'", '')
                result['authors'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                ('{name}'),('{surname}'),
                ('{patronymic}'),('{description}')),"""

            if data['_index'] == 'rids':
                try:
                    nioktr = data['_source']['nioktr']['name']
                except KeyError:
                    nioktr = None
                try:
                    using_ways = data['_source']['using_ways'].replace("'", '')
                except KeyError:
                    using_ways = None
                except AttributeError:
                    using_ways = None
                
                try:
                    rid_type = data['_source']['rid_type']['name']
                except KeyError:
                    rid_type = None
                try:
                    expected = data['_source']['expected']['name']
                except KeyError:
                    expected = None

                result['rids'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                ('{rid_type}'),('{expected}'),
                ('{nioktr}'),('{using_ways}')),"""            
            else:
                
                try:
                    nioktr = data['_source']['nioktr']['name'].replace("'", '')
                except KeyError:
                    nioktr = None
                except IndexError:
                    nioktr = None
                except AttributeError:
                    nioktr = None
                result['ikrbses'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                ('{nioktr}'),('{data['_source']['approve_date']}'),('{data['_source']['applications_count']}'),
                ('{data['_source']['books_count']}'),('{data['_source']['pages_count']}'),('{data['_source']['tables_count']}'),
                ('{data['_source']['pictures_count']}'),('{data['_source']['publication_count']}'),('{data['_source']['bibliography']}')),"""
        
        elif data['_index'] == 'nioktrs':            
            try:
                bases = data['_source']['nioktr_bases']['name']
            except KeyError:
                bases = None
            try:
                types = data['_source']['nioktr_types'][0]['name']
            except KeyError:
                types = None
            except IndexError:
                types = None
            result['nioktrs'] += f"""(('{data['_source']['last_status']['registration_number']}'),
            ('{bases}'),('{types}'),('{data['_source']['start_date']}'),('{data['_source']['end_date']}'),
            ('{data['_source']['contract_number']}'),('{data['_source']['contract_date']}')),"""

            
            if data['_source']['budgets'] != []:
                for block in data['_source']['budgets']:
                    result['nioktr_budgets'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                    ('{block['budget_type']['name']}'),('{block['funds']}')),"""
            else:
                result['nioktr_budgets'] = f"(('{data['_source']['last_status']['registration_number']}'),('None'),('None')),"
            
            if data['_source']['critical_technologies'] != []:
                for name in data['_source']['critical_technologies']:
                    result['nioktr_technology'] += f"(('{data['_source']['last_status']['registration_number']}'),('{name['name']}')),"                
            else:
                result['nioktr_technology'] = f"(('{data['_source']['last_status']['registration_number']}'),('None')),"



            if data['_source']['priority_directions'] != []:
                for name in data['_source']['priority_directions']:
                    result['nioktr_priority'] += f"(('{data['_source']['last_status']['registration_number']}'),('{name['name']}')),"
            else:
                result['nioktr_priority'] = f"(('{data['_source']['last_status']['registration_number']}'),('None')),"

            
            if data['_source']['coexecutors'] != []:
                for block in data['_source']['coexecutors']:
                    result['nioktr_coexecutors'] += f"""(('{data['_source']['last_status']['registration_number']}'),
                    ('{block['name']}'),('{block['short_name']}')),"""
            else:
                result['nioktr_coexecutors'] = f"(('{data['_source']['last_status']['registration_number']}'),('None'),('None')),"
        
        elif data['_index'] == 'rnfs': 
            result['rnfs'] += f"""(('{data['_source']['last_status']['registration_number']}'),
            ('{data['_source']['period_of_execution']}'),('{data['_source']['contest']}'),
            ('{data['_source']['area_of_knowledge']}'),('{data['_source']['expected_results']}'),
            ('{data['_source']['reporting_materials']}')),"""            


        return result


    def fill_DB(self):
        self.make_keywords()
        

        print('Заполняем общую массу')

        file_name = f'Data/temp_full.json'
        with open(file_name, 'r', encoding='UTF-8') as f:
            data = json.load(f)  

        data_groups = [i for i in range(len(data)) if i % 100 == 0]
        data_groups.append(len(data))
        past_edge = 0

        for edge in tqdm(data_groups):  
            if past_edge != edge:
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    result = executor.map(self.fill_tread, data[past_edge : edge])

                    insert_main_table = f'''
                    INSERT INTO main (reg_num, _index, name, annotation, full_text, region, reg_date, search_date, 
                    author_name, author_surname, author_patronymic, position, 
                    org_name, org_short_name, 
                    org_supervisor_name, org_supervisor_surname, org_supervisor_patronymic, org_supervisor_position,        
                    other_org_name, other_org_short_name, 
                    priority, priority_sub) VALUES 
                    '''                        

                    insert_keywords_table = '''
                    INSERT INTO keywords (reg_num, keyword) VALUES 
                    '''

                    insert_oecds_table = '''
                    INSERT INTO oecds (reg_num, name, code) VALUES 
                    '''

                    insert_rubrics_table = '''
                    INSERT INTO rubrics (reg_num, code, udk) VALUES 
                    '''

                    insert_dissertations_table = '''
                    INSERT INTO dissertations (reg_num, type, report_type, degree_pursued,
                    chairman_name, chairman_surname, chairman_patronymic, chairman_position, chairman_degree,
                    speciality_codes, protection_date, tables_count, pictures_count, bibliography, applications_count, pages_count, sources_count, books_count) VALUES 
                    '''
                    insert_supervisors_table = '''
                    INSERT INTO supervisors (reg_num, fio, degree, speciality_name, speciality_code) VALUES 
                    '''
                    insert_opponents_table = '''
                    INSERT INTO opponents (reg_num, fio, degree, speciality_name, speciality_code) VALUES 
                    '''

                    insert_rids_table = '''
                    INSERT INTO rids (reg_num, rid_type, expected, nioktr, using_ways) VALUES 
                    '''

                    insert_ikrbses_table = '''
                    INSERT INTO ikrbses (reg_num, nioktr, approve_date, 
                    applications_count, books_count, pages_count, tables_count, pictures_count, publication_count, bibliography) VALUES 
                    '''

                    insert_authors_table = '''
                    INSERT INTO authors (reg_num, name, surname, patronymic, description) VALUES 
                    '''

                    insert_nioktrs_table = '''
                    INSERT INTO nioktrs (reg_num, bases, types, start_date, end_date, contract_number, contract_date) VALUES 
                    '''

                    insert_nioktr_budgets_table = '''
                    INSERT INTO nioktr_budgets (reg_num, name, funds) VALUES 
                    '''

                    insert_nioktr_technology_table = '''
                    INSERT INTO nioktr_technology (reg_num, name) VALUES 
                    '''

                    insert_nioktr_priority_table = '''
                    INSERT INTO nioktr_priority (reg_num, name) VALUES 
                    '''

                    insert_nioktr_coexecutors_table = '''
                    INSERT INTO nioktr_coexecutors (reg_num, name, short_name) VALUES 
                    '''

                    insert_rnfs_table = '''
                    INSERT INTO rnfs (reg_num, period_of_execution, contest,
                    area_of_knowledge, expected_results, reporting_materials) VALUES 
                    '''

                    for block in result:
                        insert_main_table += block['main']
                        insert_keywords_table += block['keywords']
                        insert_oecds_table += block['oecds']
                        insert_rubrics_table += block['rubrics']
                        insert_dissertations_table += block['dissertations']
                        insert_supervisors_table += block['supervisors']
                        insert_opponents_table += block['opponents']
                        insert_rids_table += block['rids']
                        insert_ikrbses_table += block['ikrbses']
                        insert_authors_table += block['authors']
                        insert_nioktrs_table += block['nioktrs']
                        insert_nioktr_budgets_table += block['nioktr_budgets']
                        insert_nioktr_technology_table += block['nioktr_technology']
                        insert_nioktr_priority_table += block['nioktr_priority']
                        insert_nioktr_coexecutors_table += block['nioktr_coexecutors']
                        insert_rnfs_table += block['rnfs']


                    connection = self.create_connection()
                    self.execute_query(connection, insert_main_table[:-1])
                    self.execute_query(connection, insert_keywords_table[:-1])
                    self.execute_query(connection, insert_oecds_table[:-1])
                    self.execute_query(connection, insert_rubrics_table[:-1])
                    self.execute_query(connection, insert_dissertations_table[:-1])
                    self.execute_query(connection, insert_supervisors_table[:-1])
                    self.execute_query(connection, insert_opponents_table[:-1])
                    self.execute_query(connection, insert_rids_table[:-1])
                    self.execute_query(connection, insert_ikrbses_table[:-1])
                    self.execute_query(connection, insert_authors_table[:-1])
                    self.execute_query(connection, insert_nioktrs_table[:-1])
                    self.execute_query(connection, insert_nioktr_budgets_table[:-1])
                    self.execute_query(connection, insert_nioktr_technology_table[:-1])
                    self.execute_query(connection, insert_nioktr_priority_table[:-1])
                    self.execute_query(connection, insert_nioktr_coexecutors_table[:-1])   
                    self.execute_query(connection, insert_rnfs_table[:-1])  
                    connection.close()                                           

            past_edge = edge

