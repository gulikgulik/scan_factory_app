import sqlite3

def fill_rules_python_set(db_file: str) -> None:
    with sqlite3.connect(db_file) as con:
        cur = con.cursor()
        res = cur.execute('SELECT d.name, d.project_id FROM domains AS d')
        regex_set = set((r'\.'.join(['', *k.split('.')[1:]]) + '$', v)
                        for k, v in res.fetchall() if not k.split('.')[0].
                        replace('-', '').replace('_', '').isalpha())
        cur.executemany('INSERT INTO rules(project_id, regexp) SELECT :project_id,'
                        ':regexp WHERE NOT EXISTS (SELECT * FROM rules AS r WHERE '
                        'r.project_id = :project_id AND r.regexp = :regexp)',
                        ({'project_id': v, 'regexp': k} for k, v in regex_set))

def fill_rules_sqlite_func(db_file: str) -> None:
    with sqlite3.connect(db_file) as con:
        cur = con.cursor()
        con.create_function('check_if_garbage', 1, lambda x: not x.split('.')[0].
                            replace('-', '').replace('_', '').isalpha())
        con.create_function('gen_regex', 1,
                            lambda x: r'\.'.join(['',*x.split('.')[1:]]) + '$')
        res = cur.execute('SELECT DISTINCT gen_regex(d.name), d.project_id FROM'
                          ' domains AS d WHERE check_if_garbage(d.name)')
        cur.executemany('INSERT INTO rules(project_id, regexp) SELECT :project_id,'
                        ':regexp WHERE NOT EXISTS (SELECT * FROM rules AS r WHERE'
                        ' r.project_id = :project_id AND r.regexp = :regexp)',
                        ({'project_id': v, 'regexp': k} for k, v in res.fetchall()))

if __name__ == '__main__':
    '''Два варианта реализации. Предполагаю вариант с 'fill_rules_sqlite_func' быстрее
    и эффективнее по памяти должен отрабатывать из-за наложения ограничения в момент
    выборки данных.
    Если "ns1.vis.xxx.com" не должен попадать в исключения, то я не понял как
    определить критерии, чтобы получить регулярки, отсеивающие домены вида
    *.sub.yyy.com и *static.developer.xxx.com, как это написано в задании для примера
    данных в файле БД. Т.е в моем варианте в исключения попадут и по *.vis.xxx.com,
    т.к. по данным в таблице единственным критерием я определил - это наличие цифр
    в последнем уровне.
    '''
    # fill_rules_python_set('domains.db')
    fill_rules_sqlite_func('domains.db')