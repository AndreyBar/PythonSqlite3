import sqlite3
import os


def delete(file):
    if os.path.isfile(file):
        os.remove(file)


def connect(name):
    connection = sqlite3.connect(name)
    cursor = connection.cursor()
    return connection, cursor


def save(connection):
    connection.commit()
    connection.close()


def database_create():
    connection, cursor = connect('bank.db')

    # Создаем таблицу работников
    cursor.execute('''CREATE TABLE IF NOT EXISTS employees
                        (employee_id INTEGER, name TEXT, gender TEXT, address TEXT, passport TEXT, position_id INTEGER)''')

    # Создаем таблицу должностей
    cursor.execute('''CREATE TABLE IF NOT EXISTS positions
                    (position_id INTEGER, position TEXT, salary INTEGER, obligations TEXT, requirements TEXT)''')

    # Создаем таблицу валюты
    cursor.execute('''CREATE TABLE IF NOT EXISTS currency
                    (currency_id INTEGER, currency_name TEXT, exchange_rate REAL)''')

    # Создаем таблицу вкладчиков
    cursor.execute('''CREATE TABLE IF NOT EXISTS investors
                    (investor_name TEXT, address TEXT, phone TEXT, passport TEXT, date_in TEXT, date_out TEXT,
                    deposit_id INTEGER, deposit_in INTEGER, deposit_out INTEGER, deposit_returned BOOLEAN,
                    employee_id INTEGER, currency_id INTEGER)''')

    # Сохраняем изменения и закрываем соединение
    save(connection)


def database_fill():
    connection, cursor = connect('bank.db')

    # Заполняем таблицу работников
    employees = [(1, "John Doe", "male", "8785 Windfall St.", "141939132", 2),
                 (2, "Garth Muccio", "male", "1258, Fern Acres Ln", "752609650", 4),
                 (3, "Leanora Vonminden", "female", "1 N. Cactus Ave.", "386572171", 1),
                 (4, "Jake Ry", "male", "711 Old York Drive ", "948372032", 1),
                 (5, "Malina Kreiger", "female", "77 Winchester Lane", "288036398", 3),
                 (6, "Amy Pauken", "female", "665 Clinton Lane", "692864097", 1),
                 (7, "Aaron Buchser", "male", "787 Lakeview St.", "229311609", 4),
                 (8, "Loyd Nicastri", "male", "9041 Poor House Dr.", "861077626", 5),
                 (9, "Wilkie Stepro", "male", "1268 Chukker Ct", "839228925", 2),
                 (10, "Joi Morvan", "female", "1121 Quicksburg Rd", "230942622", 3)]
    cursor.executemany("INSERT INTO employees VALUES (?,?,?,?,?,?)", employees)

    # Заполняем таблицу должностей
    positions = [(1, "owner", 1, "observe the work flow", "not become bankrupt"),
                 (2, "deputy", 5000, "help the owner", "be smart"),
                 (3, "creditor", 4000, "give credits to customers", "get the best percentage rate"),
                 (4, "programmer", 4000, "maintenance banking system", "to know python and sql"),
                 (5, "operator", 2000, "to help customers through the phone", "fluent English")]
    cursor.executemany("INSERT INTO positions VALUES (?,?,?,?,?)", positions)

    # Заполняем таблицу валюты
    currency = [(1, "USD", 1),
                (2, "EUR", 0.9435),
                (3, "GBP", 0.8049)]
    cursor.executemany("INSERT INTO currency VALUES (?,?,?)", currency)

    # Заполняем таблицу вкладчиков
    investors = [("Caelie Bitts", "1155 Addison St", "+1248 080 8839", "582180798", "2015-03-21", "2017-03-21", 1, 5000,
                  7000, 0, 1, 2),
                 ("Antwan Pitcox", "1216 Anderson Rd", "+1440 295 8818", "346434289", "2015-06-12", "2018-06-12", 2,
                  10000, 15000, 0, 2, 1),
                 ("Eliza Toncray", "1085 Mccauley St", "+1220 660 5182", "526900208", "2013-01-29", "2015-01-29", 3,
                  1000, 2000, 1, 3, 2),
                 (
                 "Eden Marz", "1246 Washington St", "+1935 102 0853", "736219367", "2011-10-12", "2015-10-12", 4, 10000,
                 17000, 1, 4, 3),
                 ("Carita Murken", "1282 Adamston Rd", "+1463 957 5058", "925100703", "2017-01-10", "2017-07-10", 5,
                  5000, 5500, 0, 5, 1),
                 ("Bridger Gongwer", "1210 New Main St", "+1199 166 3186", "230594702", "2016-02-15", "2017-02-15", 6,
                  10000, 12000, 1, 6, 2),
                 ("Lorayne Laveau", "1149, Ransom St", "+1553 553 9361", "187530207", "2010-06-10", "2015-06-10", 7,
                  5000, 10000, 1, 9, 3),
                 ("Chile Willet", "1258 Us Highway 287", "+1253 740 3315", "203283517", "2012-12-11", "2013-12-11", 8,
                  7500, 9500, 1, 8, 1),
                 ("Latisha Tondo", "1033 15th St, Apalachicola", "+1670 995 4767", "800033944", "2015-09-14",
                  "2015-12-14", 9, 3000, 3300, 1, 10, 2),
                 ("Pip Disalvatore", "1294 Hawaii Belt Rd", "+1236 936 9538", "970365481", "2014-11-25", "2018-11-25",
                  10, 20000, 30000, 0, 7, 1)]
    cursor.executemany("INSERT INTO investors VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", investors)

    save(connection)


def database_view():
    connection, cursor = connect('bank.db')

    # Создаем отдел кадров
    cursor.execute('''CREATE VIEW IF NOT EXISTS human_res_dept AS
                      SELECT employees.employee_id, employees.name, positions.position
                      FROM employees
                      INNER JOIN positions ON employees.position_id = positions.position_id''')

    # Создаем вклады
    cursor.execute('''CREATE VIEW IF NOT EXISTS deposits_v AS
                      SELECT investors.deposit_id, investors.investor_name, investors.deposit_in, investors.deposit_out,
                      investors.deposit_returned, currency.currency_name
                      FROM investors
                      INNER JOIN currency ON investors.currency_id = currency.currency_id''')

    save(connection)


def database_position_filter(position):
    connection, cursor = connect('bank.db')
    cursor.execute("CREATE VIEW IF NOT EXISTS some_positions AS SELECT * FROM human_res_dept WHERE position = " + position)
    save(connection)


def database_currency_filter(currency):
    connection, cursor = connect('bank.db')
    cursor.execute("CREATE VIEW IF NOT EXISTS some_currency AS SELECT * FROM deposits_v WHERE currency_name = " + currency)
    save(connection)


def database_deposit_return_filter(is_returned):
    connection, cursor = connect('bank.db')
    cursor.execute("CREATE VIEW IF NOT EXISTS returned_deposits AS SELECT * FROM deposits_v WHERE deposit_returned = " + is_returned)
    save(connection)


def database_exact_deposit_filter(deposit_value):
    connection, cursor = connect('bank.db')
    cursor.execute("CREATE VIEW IF NOT EXISTS exact_deposit AS SELECT * FROM deposits_v WHERE deposit_in = " + deposit_value)
    save(connection)


def main():
    delete('bank.db')
    database_create()
    database_fill()
    database_view()
    database_position_filter('"owner"')
    database_currency_filter('"GBP"')
    database_deposit_return_filter("1")
    database_exact_deposit_filter("7500")


if __name__ == '__main__':
    main()
