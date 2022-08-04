import pickle
import openpyxl
import string
from tqdm import tqdm

from flashtext import KeywordProcessor

import pymorphy2

morph = pymorphy2.MorphAnalyzer()

obj = openpyxl.load_workbook('C:/Users/Егорка/Desktop/11/Диплом/Ключевые слова по областям.xlsx')

dict_keywords = {'Индустрия (20А)' : [], 'Энергетика (20Б)' : [], 'Медицина (20В)' : [], 'Продовольствие (20Г)' : [], 
'Безопасность (20Д)' : [], 'Пространство (20Е)' : [], 'Общество (20Ж)' : []}

sheet = obj.active

with open('predlogi.txt', 'r', encoding='utf-8') as f:
    predlogi = f.read()
predlogi = predlogi.split(',')

for ch in string.punctuation:
    predlogi.append(ch)

punctuation_processor = KeywordProcessor()

for word in predlogi:
    punctuation_processor.add_keyword(word, ' ')

point = 'F6'
main_point = 'B1'
state = ''
for i in tqdm(range(1, 307)):  
    if sheet[f'B{i}'].value in dict_keywords.keys():        
        state = sheet[f'B{i}'].value        
    if state != '':
        if sheet[f'F{i}'].value != None:            
            temp = dict_keywords[state]
            f_cell = sheet[f'F{i}'].value.split(', ')
            g_cell = sheet[f'G{i}'].value.split(', ')


            f_cell_new = []
            for block in f_cell:
                corrected_block = punctuation_processor.replace_keywords(block)
                new_f = ''

                for word in corrected_block.split():                    
                    new_f = new_f + ' ' + morph.parse(word)[0].normal_form                
                new_f = new_f[1:]                
                f_cell_new.extend([new_f])                
            

            temp.extend(f_cell_new)            
            temp.extend(g_cell)
            dict_keywords.update({state : temp})

print(dict_keywords)

with open('keywords_norm_testing', 'wb') as f:
    pickle.dump(list(dict_keywords.items()), f)

