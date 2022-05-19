class DatabaseManager:

    title = []
    material = []

    def __init__(self):
        self.connection = None

    def dbExistence(self):
        '''Check if the database exists'''
        try:
            print(f'Checking the existence of {"part_1p01.db"}')
            uri = 'file:{}?mode=rw'.format(pathname2url("part_1p01.db"))
            connection = sqlite3.connect(uri, uri=True)
            print(f'Database exists: Connected to {"part_1p01.db"}')

        except sqlite3.OperationalError as err:
            print('Database does not exist')
            print(err)
            DatabaseManager.dbCreate(self)

    def remove_duplicates_part(self):
        connection = sqlite3.connect("part_1p01.db")
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM part")
        print(
            f"@50 Number of rows in part_1p01.db duplicates removal = {len(cursor.fetchall())}")

        delete_query = '''DELETE FROM part
            WHERE ROWID NOT IN
            (SELECT MIN(ROWID)
            FROM part
            GROUP BY partTitle, partMatID
            )'''
        cursor.execute(delete_query)
        connection.commit()

    def remove_duplicates_function(self):
        connection = sqlite3.connect("part_1p01.db")
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM function")
        print(
            f"@50 Number of rows in part_1p01.db duplicates removal = {len(cursor.fetchall())}")

        delete_query = '''DELETE FROM function
            WHERE ROWID NOT IN
            (SELECT MIN(ROWID)
            FROM function
            GROUP BY functionID, functionTitle
            )'''
        cursor.execute(delete_query)
        connection.commit()

    def remove_duplicates_material(self):
        connection = sqlite3.connect("part_1p01.db")
        cursor = connection.cursor()

        cursor.execute("SELECT * FROM material")
        print(
            f"@50 Number of rows in part_1p01.db duplicates removal = {len(cursor.fetchall())}")

        delete_query = '''DELETE FROM material
            WHERE ROWID NOT IN
            (SELECT MIN(ROWID)
            FROM material
            GROUP BY matID, functionID
            )'''
        cursor.execute(delete_query)
        connection.commit()

    def dbCreate(self):
        connection = sqlite3.connect("part_1p01.db")
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE MAT_HONEYCOMB (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E11 REAL,
                            E22 REAL,
                            E33 REAL,
                            G12 REAL,
                            G23 REAL,
                            G31 REAL,
                            fun_ID11 INTEGER,
                            fun_ID22 INTEGER,
                            fun_ID33 INTEGER,
                            Eps_max11 REAL,
                            Eps_max22 REAL,
                            Eps_max33 REAL,
                            fun_ID12 INTEGER,
                            fun_ID23 INTEGER,
                            fun_ID31 INTEGER,
                            Eps_max12 REAL,
                            Eps_max23 REAL,
                            Eps_max31 REAL)
                        ''')

        cursor.execute('''CREATE TABLE SM_HONEYCOMB (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E11 REAL,
                            E22 REAL,
                            E33 REAL,
                            G12 REAL,
                            G23 REAL,
                            G31 REAL,
                            fun_ID11 INTEGER,
                            fun_ID22 INTEGER,
                            fun_ID33 INTEGER,
                            Eps_max11 REAL,
                            Eps_max22 REAL,
                            Eps_max33 REAL,
                            fun_ID12 INTEGER,
                            fun_ID23 INTEGER,
                            fun_ID31 INTEGER,
                            Eps_max12 REAL,
                            Eps_max23 REAL,
                            Eps_max31 REAL)
                        ''')

        cursor.execute('''CREATE TABLE MAT_FABRI (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E11 REAL,
                            E22 REAL,
                            NU12 REAL,
                            G12 REAL,
                            G23 REAL,
                            G31 REAL)
                        ''')

        cursor.execute('''CREATE TABLE SM_FABRI (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E11 REAL,
                            E22 REAL,
                            NU12 REAL,
                            G12 REAL,
                            G23 REAL,
                            G31 REAL)
                        ''')

        cursor.execute('''CREATE TABLE MAT_LAW70 (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E REAL,
                            NU REAL,
                            Emax REAL,
                            EPSmax REAL,
                            Nload INTEGER,
                            Nunload INTEGER)
                        ''')

        cursor.execute('''CREATE TABLE SM_LAW70 (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E REAL,
                            NU REAL,
                            Emax REAL,
                            EPSmax REAL,
                            Nload INTEGER,
                            Nunload INTEGER)
                        ''')

        cursor.execute('''CREATE TABLE part (
                            partAlias TEXT,
                            partTitle TEXT,
                            partMatID INTEGER)
                        ''')

        cursor.execute('''CREATE TABLE Mat_PLAS_TAB (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            functionID INTEGER)
                        ''')

        cursor.execute('''CREATE TABLE SM_PLAS_TAB (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            functionID INTEGER)
                        ''')

        cursor.execute('''CREATE TABLE Mat_PLAS_JOHNS (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E REAL,
                            NU REAL)
                        ''')

        cursor.execute('''CREATE TABLE SM_PLAS_JOHNS (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E REAL,
                            Nu REAL)
                        ''')

        cursor.execute('''CREATE TABLE Mat_VOID (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E REAL,
                            NU REAL)
                        ''')

        cursor.execute('''CREATE TABLE SM_VOID (
                            matID INTEGER,
                            law TEXT,
                            matTitle TEXT,
                            RHO REAL,
                            E REAL,
                            NU REAL)
                        ''')

        cursor.execute('''CREATE TABLE MAT_function (
                            functionID INTEGER,
                            functionTitle TEXT)
                        ''')

        cursor.execute('''CREATE TABLE SM_function (
                            functionID INTEGER,
                            functionTitle TEXT)
                        ''')

        cursor.execute('''CREATE TABLE Material (
                            matID INTEGER,
                            law TEXT,
                            matTitle INTEGER,
                            functionID INTEGER)
                        ''')

        cursor.execute('''CREATE TABLE Function (
                            functionID INTEGER,
                            functionTitle TEXT)
                        ''')

        cursor.execute(
            '''CREATE TABLE seat_reference (seat TEXT)''')  # table title reference(s) is not acceptable
        connection.commit()
        print("part_1p01.db is created")

    # Reads a reliable seat model with material assigned and
    # write the PART-block to part_1p01.db

    def dbAppend(self, inFile):

        seatParse(inFile)

        print("part_db/seat_dbAppend complete")

        # The reliable seats that are used in creation of database are recorded in database in table seat_references
        inFile = inFile.split("/")[-1]
        inFile = inFile[:-9]

        connection = sqlite3.connect("part_1p01.db")
        cursor = connection.cursor()
        cursor.execute("INSERT INTO seat_reference VALUES (?)", (inFile,))
        connection.commit()
        connection.close()
