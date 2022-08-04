from PyQt5.QtWidgets import QApplication, QMainWindow
from PyQt5 import QtWidgets

import sys
from sqlite3 import connect as sqlite3_connect
from sqlite3 import Error as sqlite3_Error

from Ui_window import Ui_MainWindow
from Parser import Parser
from Semantic_module import Semantic_module

from MplCanvas import MplCanvas

from pymorphy2 import MorphAnalyzer

import datetime


class MainWindow(QMainWindow, Ui_MainWindow):
    def __init__(self) -> None:
        super(MainWindow, self).__init__()        
        self.fullsearch_status = 0                
        self.setupUi(self)
        self.groupBox.hide()
        self.allWorkTable.move(10, 50)
        self.morph = MorphAnalyzer()        

        self.prefill_diargams()
        self.fill_combo_boxes()  
        self.prefill_global_search()  
        self.showMaximized()

        output_text = 'Скачивание и обработка работ может занять какое то время\n'
        self.dlOutTextBrowser.setText(output_text)
        
        self.priority_combo.currentIndexChanged.connect(self.update_priority)

        self.fullSearchButton.clicked.connect(self.hide_show_fullsearch)
        self.searchButton.clicked.connect(self.global_search)

        self.radioButton.toggled.connect(self.get_years_diagrams)
        self.radioButton_2.toggled.connect(self.get_months_diagrams)

        self.downloadButton.clicked.connect(self.download)


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

    def update_priority(self, index):
        self.priority_sub_combo.clear()
        subs = self.priority_combo.itemData(index)
        if subs:
            self.priority_sub_combo.addItems(subs)

    
    def prefill_diargams(self):
        conn = self.create_connection()
        
        self.year_budgets = []
        self.year_priority = []
        for year in range(2018, datetime.datetime.now().year+1):
            years_request = f'''
            SELECT nioktr_budgets.name, SUM(nioktr_budgets.funds) FROM year INNER JOIN mounth ON year.year=mounth.year
            INNER JOIN day ON mounth.mounth=day.mounth
            INNER JOIN main ON day.date=main.search_date
            INNER JOIN nioktr_budgets ON main.reg_num=nioktr_budgets.reg_num
            INNER JOIN nioktr_budget_types ON nioktr_budget_types.name=nioktr_budgets.name
            WHERE not nioktr_budget_types.name = "None" AND year.year = "{year}"
            GROUP BY nioktr_budget_types.name
            '''
            result = self.execute_query(conn, years_request)
            if result != []:
                self.year_budgets.append([year, result])

            years_request = f'''
            SELECT priority.word, COUNT(main.priority) FROM year LEFT JOIN mounth ON year.year=mounth.year
            LEFT JOIN day ON mounth.mounth=day.mounth
            LEFT JOIN main ON day.date=main.search_date
            LEFT JOIN priority ON main.priority=priority.letter
            WHERE year.year = "{year}" AND not priority.letter="Я"
            GROUP BY priority.word 
            '''
            result = self.execute_query(conn, years_request)
            if result != []:
                self.year_priority.append([year, result])

        
        month_list = []
        month = datetime.datetime.now().month
        year = datetime.datetime.now().year
        for i in range(12):    
            month_list.append(f'{year}-{month}')
            month -= 1
            if month == 0:
                month = 12
                year -= 1

        self.month_budgets = []
        self.month_priority = []
        for month in month_list[::-1]:
            month_request = f'''
            SELECT nioktr_budgets.name, SUM(nioktr_budgets.funds) FROM mounth
            INNER JOIN day ON mounth.mounth=day.mounth
            INNER JOIN main ON day.date=main.search_date
            INNER JOIN nioktr_budgets ON main.reg_num=nioktr_budgets.reg_num
            INNER JOIN nioktr_budget_types ON nioktr_budget_types.name=nioktr_budgets.name
            WHERE not nioktr_budget_types.name = "None" AND mounth.mounth = "{month}"
            GROUP BY nioktr_budget_types.name
            '''
            result = self.execute_query(conn, month_request)
            if result != []:
                self.month_budgets.append([int(month[5:]), result])
            """
            month_request = f'''
            SELECT priority.word, COUNT(main.priority) FROM mounth
            LEFT JOIN day ON mounth.mounth=day.mounth
            LEFT JOIN main ON day.date=main.search_date
            LEFT JOIN priority ON main.priority=priority.letter
            WHERE mounth.mounth = "{month}" AND not priority.letter="Я"
            GROUP BY priority.word 
            '''
            """
            month_request = f'''
            SELECT priority.word, COUNT(main.priority) FROM priority
            LEFT OUTER JOIN main ON main.priority=priority.letter
            LEFT OUTER JOIN day ON day.date=main.search_date
            LEFT OUTER JOIN mounth ON mounth.mounth=day.mounth
            WHERE mounth.mounth = "{month}" AND not priority.letter="Я"
            GROUP BY priority.word 
            '''
            result = self.execute_query(conn, month_request)
            if result != []:
                '''
                count = len(result)
                while count < 7:
                    result.append(['', 0])
                    count += 1
                '''
                corrector = 0
                corrected_result = [['Безопасность', 0], ['Индустрия', 0], ['Медицина', 0], ['Общество', 0], ['Продовольствие', 0], ['Пространство', 0], ['Энергетика', 0]]
                for i, res in enumerate(result):
                    if res[0] == corrected_result[i+corrector][0]:
                        corrected_result[i+corrector][1] = res[1]
                    else:
                        corrector += 1
                self.month_priority.append([int(month[5:]), corrected_result])

        base = datetime.datetime(2018, 1, 1, 0, 0, 1)
        self.year_time = [base + datetime.timedelta(days=x*365) for x in range(5)]
        base = datetime.datetime(datetime.datetime.now().year-1, datetime.datetime.now().month+1, 1, 0, 0, 1)        
        self.month_time = [base + datetime.timedelta(days=x*31) for x in range(12)]

        conn.close()
        self.get_years_diagrams()

    
    def get_years_diagrams(self):
        budgets = self.year_budgets
        dates = self.year_time
        priority = self.year_priority

        self.set_diagrams(budgets, dates, priority)
    
    def get_months_diagrams(self):
        budgets = self.month_budgets
        dates = self.month_time[:len(self.month_priority)]
        priority = self.month_priority
        self.set_diagrams(budgets, dates, priority)

    def set_diagrams(self, budgets, dates, priority):
        ys_budgets = [[], [], [], [], [], [], [], []]
        labels_budget = []
        xs = dates
        for j, block in enumerate(budgets):
            for i, val in enumerate(block[1]):      
                ys_budgets[i].append(int(val[1]))  
                if j == 0:              
                    labels_budget.append(val[0])

        ys_priority = [[], [], [], [], [], [], []]
        labels_priority = []
        for j, block in enumerate(priority):
            for i, val in enumerate(block[1]):      
                ys_priority[i].append(int(val[1]))        
                if j == 0:                      
                    labels_priority.append(val[0])
        
        self.fill_diargams(xs, ys_budgets, labels_budget, ys_priority, labels_priority)


    def fill_diargams(self, xs, ys_budgets, labels_budget, ys_priority, labels_priority):        
        try:
            for i in reversed(range(self.gridLayout.count())): 
                self.gridLayout.itemAt(i).widget().deleteLater()
        except:
            pass
        self.sc = MplCanvas(self, width=5, height=4, dpi=100)
        self.sc2 = MplCanvas(self, width=5, height=4, dpi=100)
        self.gridLayout.addWidget(self.sc, 0, 0)
        self.gridLayout.addWidget(self.sc2, 0, 1)   


        for i in range(len(ys_priority)):
            self.sc.axes.set_title('Распределение по приоритетам')
            self.sc.axes.plot(xs, ys_priority[i], '-o')     
            self.sc.axes.tick_params(axis='x', rotation=70)
        self.sc.axes.legend(labels_priority, loc='upper right')
        self.sc.axes.grid()
        
        for i in range(len(ys_budgets)):            
            self.sc2.axes.set_title('Распределение бюджета')            
            self.sc2.axes.plot(xs, ys_budgets[i], '-o')     
            self.sc2.axes.tick_params(axis='x', rotation=70)
        self.sc2.axes.legend(labels_budget, loc='upper right')
        self.sc2.axes.grid()          


    def fill_combo_boxes(self):
        conn = self.create_connection()        
        get_priority = '''SELECT * from priority_sub'''
        priorities = self.execute_query(conn, get_priority)
        self.priority = []
        current = ''         
        for priority in priorities:
            if current != priority[1]:                
                current = priority[1]
                self.priority.append([current, ['*', priority[2]]])
            else:
                self.priority[-1][1].append(priority[2])
        for combo in self.priority:
            self.priority_combo.addItem(*combo)
        
        get_region = '''SELECT region from region'''
        regions_parse = self.execute_query(conn, get_region)
        regions = []
        for region in regions_parse:
            if region[0] != 'None':
                regions.append(region[0])
        regions = sorted(regions)
        self.region_combo.addItems(regions)

        get_rubrics = '''SELECT rubrics_lvl3, name from rubrics_lvl3'''
        rubrics_parse = self.execute_query(conn, get_rubrics)
        rubrics = []
        for region in rubrics_parse:
            if len(region[1]) > 53:
                rubrics.append(f'{region[0]} {region[1][:50]}...')
            else:
                rubrics.append(f'{region[0]} {region[1]}')
        rubrics = sorted(rubrics)
        self.rubrics_combo.addItems(rubrics)    
        
        conn.close()

    def hide_show_fullsearch(self):
        if self.fullsearch_status == 0:
            self.groupBox.show()
            self.allWorkTable.move(10, 160)
            self.fullsearch_status = 1
        else:
            self.groupBox.hide()
            self.allWorkTable.move(10, 50)
            self.fullsearch_status = 0

    
    def prefill_global_search(self):
        limit = 50
        global_search = f'''
        SELECT _index, name, priority_sub, author_surname, author_name,
        author_patronymic, org_short_name from main
        ORDER BY main.reg_date DESC
        LIMIT {limit}
        '''
        conn = self.create_connection()
        cursor = conn.cursor()
        posts = cursor.execute(global_search).fetchall()
        self.allWorkTable.setRowCount(limit)
        for i, post in enumerate(posts):
            index = post[0]
            if index == 'rids':
                index = 'РИД'
            if index == 'nioktrs':
                index = 'НИОКТР'
            if index == 'dissertations':
                index = 'Диссертация'
            if index == 'ikrbses':
                index = 'Научный отчет'
            if index == 'rnfs':
                index = 'РНФ'
            name = post[1]          
            author = ''
            if post[3] != 'None':
                author += f'{post[3]} '
            if post[4] != 'None':
                author += f'{post[4]} '
            if post[5] != 'None':
                author += f'{post[5]}'
            if post[6] != 'None':
                author += f', {post[6]}'
            self.allWorkTable.setColumnWidth(0, 50)
            self.allWorkTable.setColumnWidth(1, 350)
            self.allWorkTable.setColumnWidth(2, 170)
            
            self.allWorkTable.setItem(i, 0, QtWidgets.QTableWidgetItem(index))
            self.allWorkTable.setItem(i, 1, QtWidgets.QTableWidgetItem(name))
            self.allWorkTable.setItem(i, 2, QtWidgets.QTableWidgetItem(post[2]))
            self.allWorkTable.setItem(i, 3, QtWidgets.QTableWidgetItem(author))




    def global_search(self):
        limit = 50        
        checks = ''
        if not self.rid_check.isChecked():
            checks += f'AND not main._index="rids"'
        if not self.rnf_check.isChecked():
            checks += f'AND not main._index="rnfs"'
        if not self.nioktr_check.isChecked():
            checks += f'AND not main._index="nioktrs"'
        if not self.ikrbses_check.isChecked():
            checks += f'AND not main._index="ikrbses"'
        if not self.dissertation_check.isChecked():
            checks += f'AND not main._index="dissertations"'

        
        keywords_search = 'main '
        year_search = ''
        region_search = ''
        priority_search = ''
        rubrics_search = ''
        search_resque = self.search_text.toPlainText()
        if search_resque != '':
            keywords_search = '''keysearch INNER JOIN keysearch_words ON keysearch.word=keysearch_words.word 
            INNER JOIN main ON keysearch_words.reg_num=main.reg_num WHERE '''
            for i, word in enumerate(search_resque.split(' ')):
                word = self.morph.parse(word)[0].normal_form
                if len(word) > 2:
                    if i > 0:
                        keywords_search += f'''INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                                    main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM 
                                    keysearch INNER JOIN keysearch_words ON keysearch.word=keysearch_words.word 
                                    INNER JOIN main ON keysearch_words.reg_num=main.reg_num WHERE
                                    keysearch.word = "{word}" {checks} 
                                    '''
                    else:
                        keywords_search += f'keysearch.word = "{word}" {checks} ' 


        if self.year_combo.currentText() != '-':
            year_search = f'''
            INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM 
                year INNER JOIN mounth ON year.year=mounth.year
                INNER JOIN day ON mounth.mounth=day.mounth
                INNER JOIN main on day.date=main.search_date
                WHERE year.year = "{self.year_combo.currentText()}" {checks} 
            ''' 
        
        if self.region_combo.currentText() != '-':
            region_search = f'''
            INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM
                region INNER JOIN main ON region.region=main.region
                WHERE region.region = "{self.region_combo.currentText()}" {checks} 
            '''

        if self.priority_combo.currentText() != '-':
            if self.priority_sub_combo.currentText() == '*':
                priority_search = f'''
                INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                    main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM
                    priority INNER JOIN priority_sub ON priority.word=priority_sub.word
                    INNER JOIN main ON priority_sub.sub_word=main.priority_sub
                    WHERE priority.word = "{self.priority_combo.currentText()}" {checks} 
                '''
            else:
                priority_search = f'''
                INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                    main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM
                    priority_sub INNER JOIN main ON priority_sub.sub_word=main.priority_sub
                    WHERE priority_sub.sub_word = "{self.priority_sub_combo.currentText()}" {checks} 
                '''
        
        if self.rubrics_combo.currentText() != '-':
            rubric = self.rubrics_combo.currentText()[:8]
            if rubric[6:] == '00':
                if rubric[3:5] == '00':
                    rubrics_search = f'''
                        INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                        main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM
                        rubrics_lvl1 INNER JOIN rubrics_lvl2 ON rubrics_lvl1.rubrics_lvl1=rubrics_lvl2.rubrics_lvl1
                        INNER JOIN rubrics_lvl3 ON rubrics_lvl2.rubrics_lvl2=rubrics_lvl3.rubrics_lvl2
                        INNER JOIN rubrics ON rubrics_lvl3.rubrics_lvl3=rubrics.code
                        INNER JOIN main ON rubrics.reg_num=main.reg_num
                        WHERE rubrics_lvl1.rubrics_lvl1 = "{rubric[:2]}" {checks} 
                        '''
                else:
                    rubrics_search = f'''
                        INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                        main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM
                        rubrics_lvl2 INNER JOIN rubrics_lvl3 ON rubrics_lvl2.rubrics_lvl2=rubrics_lvl3.rubrics_lvl2
                        INNER JOIN rubrics ON rubrics_lvl3.rubrics_lvl3=rubrics.code
                        INNER JOIN main ON rubrics.reg_num=main.reg_num
                        WHERE rubrics_lvl2.rubrics_lvl2 = "{rubric[:5]}" {checks} 
                '''
            else:
                rubrics_search = f'''
                INTERSECT SELECT main._index, main.name, main.priority_sub, main.author_surname, 
                        main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM
                        rubrics_lvl3 INNER JOIN rubrics ON rubrics_lvl3.rubrics_lvl3=rubrics.code
                        INNER JOIN main ON rubrics.reg_num=main.reg_num
                        WHERE rubrics_lvl3.rubrics_lvl3 = "{rubric}" {checks} 
                '''

        if keywords_search == 'main ' and checks != '':
            keywords_search = f'main WHERE {checks[4:]}'

        search = f'''
        SELECT main._index, main.name, main.priority_sub, main.author_surname, 
        main.author_name, main.author_patronymic, main.org_short_name, main.reg_date FROM 
        {keywords_search} {region_search} {priority_search} {rubrics_search} {year_search}
        ORDER BY main.reg_date DESC
        LIMIT {limit}
        '''
        conn = self.create_connection()        
        result = self.execute_query(conn, search)
        self.allWorkTable.setRowCount(0)
        if result:
            self.allWorkTable.setRowCount(len(result))
            for i, post in enumerate(result):
                index = post[0]
                if index == 'rids':
                    index = 'РИД'
                if index == 'nioktrs':
                    index = 'НИОКТР'
                if index == 'dissertations':
                    index = 'Диссертация'
                if index == 'ikrbses':
                    index = 'Научный отчет'
                if index == 'rnfs':
                    index = 'РНФ'
                name = post[1]          
                author = ''
                if post[3] != 'None':
                    author += f'{post[3]} '
                if post[4] != 'None':
                    author += f'{post[4]} '
                if post[5] != 'None':
                    author += f'{post[5]}'
                if post[6] != 'None':
                    author += f', {post[6]}'
                self.allWorkTable.setItem(i, 0, QtWidgets.QTableWidgetItem(index))
                self.allWorkTable.setItem(i, 1, QtWidgets.QTableWidgetItem(name))
                self.allWorkTable.setItem(i, 2, QtWidgets.QTableWidgetItem(post[2])) #2
                self.allWorkTable.setItem(i, 3, QtWidgets.QTableWidgetItem(author))

        conn.close()

    
    def view_full_work(self, item):
        print(1)
        if item.column() == 1:
            print(item.data(), item.column(), item.row())        
    



    def download(self):
        self.downloadButton.setEnabled(False)
        parser = Parser()
        source = self.sourceComboBox.currentText()        
        if source == 'ЕГИСУ НИОКТР':
            result = parser.parse_rosrid()
        elif source == 'РНФ':
            result = parser.parse_rnf()            

        if result:
            output_text = 'Скачивание завершено, производится обработка и пополнение базы\n'
            self.dlOutTextBrowser.setText(output_text)
            semantic_module = Semantic_module()        
            semantic_module.fill_DB()
            output_text = 'Пополнение базы выполнено успешно'
            self.dlOutTextBrowser.setText(output_text)
        else:
            output_text = 'Новых работ обнаружено не было'
            self.dlOutTextBrowser.setText(output_text)
        self.downloadButton.setEnabled(True)
        

            




     




if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec_())