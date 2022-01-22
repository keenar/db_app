from NRDB import NRDB, NRErr
import config as CONFIG

# import ConfigParser


GLOBALS = {}

dbms_var = CONFIG.sqlite.dbms
database_var = CONFIG.sqlite.database


def connect():
    try:
        db = NRDB(dbms=dbms_var, database=database_var)
        print(f'NRDB version {db.version()}')
        print(f'dmbs is {db.dbms}')
    except NRErr as err:
        db = None
        print(f'Error: {err}')
        exit(1)

    GLOBALS['db'] = db
    return db


def do_menu():
    while True:
        menu = (
            "A) Insert row",
            "F) Find row",
            "FR) Find rows",
            "E) Edit row",
            "L) List row",
            "D) Delete row",
            "X) Drop table",
            "Q) Quit"
        )
        print()
        for item in menu:
            print(item)
        response = input("Select an action or Q to quit > ").upper()
        if len(response) == 0:
            print("\nInput empty")
            continue
        elif response in "AFRELDXQ":
            break
        else:
            print("\nInvalid response")
            continue
    return response


def jump(response):
    if response == 'A':
        add_rec()
    elif response == 'F':
        find_rec()
    elif response == 'FR':
        find_recs()
    elif response == 'E':
        edit_rec()
    elif response == 'L':
        list_recs()
    elif response == 'D':
        delete_rec()
    elif response == 'X':
        drop_table()
    else:
        print('jump: invalid argument')


def add_rec():
    print("Insert one row")
    db = GLOBALS['db']
    numrows = db.add_row(["pandas", "www.pandas.org", 10000])
    row_id = db.lastrowid()
    print(f"{numrows} row added (row {row_id})")
    print(db.get_row(row_id))


def find_rec(**kwargs):
    if 'noprompt' not in kwargs:
        print("Find record")
    db = GLOBALS['db']
    if db is None:
        raise NRErr("find_rec: no db object")
    shortname = input("Short name > ")
    if len(shortname) == 0:
        return
    row_id = db.find_row('shortURL', shortname)
    print(f'row id is {row_id}')
    if row_id:
        row = db.get_row(row_id)
        print(f'found: {row}')
        return row_id
    else:
        print("row not found.")
        return None


def find_recs(**kwargs):
    db = GLOBALS['db']
    print("find more than one row (%s%)")
    search_string = input("Enter search string > ")
    row_ids = db.find_rows("shortURL", f"%{search_string}%")
    print(f"found {len(row_ids)} rows")
    for row_id in row_ids:
        print(db.get_row(row_id))


def edit_rec():
    db = GLOBALS['db']
    if db is None:
        raise NRErr("find_rec: no db object")
    shortname = input("Short name > ")
    if len(shortname) == 0:
        return
    row_id = find_rec(noprompt=True)
    if row_id is None:
        return
    target_url = input("Target URL (leave blank to cancel) > ")
    if len(target_url) == 0:
        print("Canceled.")
        return
    else:
        db.update_row(row_id, {'targetURL': target_url})
        row = db.get_row(row_id)
        print(f'Updated row is {row}')


def list_recs():
    print("List records")
    db = GLOBALS['db']
    if db is None:
        raise NRErr("list_recs: no db object")
    for row in db.get_rows():
        print(row)


def delete_rec():
    print("Delete record")
    db = GLOBALS['db']
    if db is None:
        raise NRErr("delete_rec: no db object")
    row_id = find_rec(noprompt=True)
    if row_id:
        yn = input(f'Delete row: (Y/N) >').upper()
        if yn == 'Y':
            db.del_row(row_id)
            print("Deleted.")
        else:
            print("Not deleted")


def drop_table():
    db = GLOBALS['db']
    print("cleanup: drop table temp")
    db.sql_do("DROP TABLE IF EXISTS temp")
    print("done.")


def main():
    connect()
    db = GLOBALS['db']
    print(f'have cursor {db.have_cursor()}')

    # start clean
    db.sql_do("DROP TABLE IF EXISTS temp")

    print(f"have table {db.have_table('temp')}")

    # create table
    print("create a table")
    if db.dbms == "sqlite":
        create_table = """
            CREATE TABLE IF NOT EXISTS temp (
                id INTEGER PRIMARY KEY,
                shortURL TEXT NOT NULL,
                targetURL TEXT, 
                users INT
            )
        """
    elif db.dbms == "mysql":
        create_table = """
            CREATE TABLE IF NOT EXISTS temp (
                id INTEGER AUTO_INCREMENT PRIMARY KEY,
                item VARCHAR(128) NOT NULL,
                description VARCHAR(128)
            )
        """
    else:
        raise NRErr("create table: unknown dbms")

    # create and set the table
    db.sql_do(create_table)
    db.table = "temp"
    print(f"have table {db.have_table('temp')}")
    print(f'table columns: {db.column_names()}\n')

    print("populate table")
    insert_rows = (
        ("so", "www.stackoverflow.com", 100),
        ("ggl", "www.google.com", 500),
        ("git", "www.github.com", 1000),
        ("apple", "www.apple.com", 2000),
        ("pandas", "www.pandas.org", 3000),
        ("pytorch", "www.pytorch.org", 4000)
    )

    # add rows
    db.begin_transaction()
    for row in insert_rows:
        db.add_row_nocommit(row)
    db.commit()
    print(f'Added {len(insert_rows)} rows')

    print(f'there are {db.count_rows()} rows')

    for row in db.get_rows():
        print(row)

    while True:
        response = do_menu()
        if response == 'Q':
            print('Quitting.')
            exit(0)
        else:
            print()  # blank line
            jump(response)


if __name__ == "__main__":
    main()
