import sqlite3

CREATE_TABLE_REGIAO = "CREATE TABLE IF NOT EXISTS regiao \
                            (_id INTEGER PRIMARY KEY, \
                            nome TEXT, \
                            ano TEXT, \
                            link TEXT \
                        );"

CREATE_TABLE_BAIRRO = "CREATE TABLE IF NOT EXISTS bairro ( \
                            _id INTEGER PRIMARY KEY, \
                            nome TEXT, \
                            ano TEXT, \
                            link TEXT, \
                            regiao_id INTEGER, \
                            FOREIGN KEY(regiao_id) REFERENCES regiao(_id) \
                        );"

CREATE_TABLE_AREA = "CREATE TABLE IF NOT EXISTS area ( \
                        _id INTEGER PRIMARY KEY, \
                        codigo TEXT, \
                        ano TEXT, \
                        link TEXT, \
                        bairro_id INTEGER, \
                        FOREIGN KEY(bairro_id) REFERENCES bairro(_id) \
                    );"

CREATE_TABLE_SUMARIO_AREA = "CREATE TABLE IF NOT EXISTS sumario_area ( \
                                _id INTEGER PRIMARY KEY, \
                                total_residentes INTEGER, \
                                total_homens INTEGER, \
                                total_mulheres INTEGER, \
                                FOREIGN KEY id_area REFERENCES area(_id) \
                            );"

CREATE_TABLE_RESIDENTES_AREA_IDATE = "CREATE TABLE residentes_idade_area ( \
                                         _id INTEGER PRIMARY KEY, \
                                         sexo INTEGER NOT NULL, \
                                         menos_1_ano INTEGER, \
                                         1_4_anos INTEGER, \
                                         5_anos INTEGER, \
                                         6_9_anos INTEGER, \
                                         10_19 anos INTEGER, \
                                         20_24_anos INTEGER, \
                                         25_49_anos INTEGER, \
                                         50_59_anos INTEGER, \
                                         60_64_anos INTEGER, \
                                         65_69_anos INTEGER, \
                                         mais_70_anos INTEGER, \
                                         FOREIGN KEY id_area REFERENCES area(_id) \
                                     );"


class DbManager:
    conn = None
    cur = None

    def __init__(self, delete_all=False):
        self.conn = sqlite3.connect('dados_prefeitura.db')
        self.cur = self.conn.cursor()

        if delete_all:
            self.cur.execute('DROP TABLE IF EXISTS regiao')
            self.cur.execute('DROP TABLE IF EXISTS bairro')
            self.cur.execute('DROP TABLE IF EXISTS area')
            self.cur.execute('DROP TABLE IF EXISTS sumario_area')
            self.cur.execute('DROP TABLE IF EXISTS residentes_idade_area')

        self.create_tables()

    def create_tables(self):
        self.cur.execute(CREATE_TABLE_REGIAO)
        self.cur.execute(CREATE_TABLE_BAIRRO)
        self.cur.execute(CREATE_TABLE_AREA)
        self.cursor.execute(CREATE_TABLE_SUMARIO_AREA)
        self.cursor.execute(CREATE_TABLE_RESIDENTES_AREA_IDATE)

    def close(self):
        self.conn.close()

    def update_regiao(self, nome, ano, link):
        self.cur.execute("UPDATE regiao SET nome=?, ano=?, \
                                link=? WHERE nome=?", (nome, ano, link, nome))
        if self.cur.rowcount == 0:
            self.cur.execute("INSERT INTO regiao (nome, ano, link) \
                    VALUES (?, ?, ?)", (nome, ano, link))

        self.conn.commit()

    def update_bairro(self, nome, ano, link, regiao_nome):
        # pegar a regiao do bairro
        self.cur.execute("SELECT _id FROM regiao WHERE \
                            NOME=?", (regiao_nome, ))
        regiao_id = self.cur.fetchone()[0]

        self.cur.execute("UPDATE bairro set nome=?, ano=?, link=?, \
            regiao_id=? WHERE nome=?", (nome, ano, link, regiao_id, regiao_nome))

        if self.cur.rowcount == 0:
            self.cur.execute("INSERT INTO bairro (nome, ano, link, \
                regiao_id) VALUES (?, ?, ?, ?)", (nome, ano, link, regiao_id))

        self.conn.commit()

    def update_area(self, codigo, ano, link, bairro_nome):
        #
        if codigo.isdigit():
            self.cur.execute("SELECT _id FROM bairro WHERE \
                                nome=?", (bairro_nome, ))
            bairro_id = self.cur.fetchone()[0]

            self.cur.execute("UPDATE area set codigo=?, ano=?, link=?, bairro_id=?\
                            WHERE codigo=?", (codigo, ano, link, bairro_id, codigo))

            if self.cur.rowcount == 0:
                self.cur.execute("INSERT INTO area (codigo, ano, link, bairro_id) \
                            VALUES (?, ?, ?, ?)", (codigo, ano, link, bairro_id))

            self.conn.commit()

    def get_regiao(self, regiao):
        print "get_regiao"

        self.cur.execute("SELECT * FROM regiao WHERE nome=?", (regiao, ))
        print "rowcount: ", self.cur.rowcount
        if self.cur.rowcount > 0:
            print "parent: ", self.cur.fetchone()
        print "parent: ", self.cur.fetchone()

    def print_regiao(self):
        self.cur.execute("SELECT * FROM regiao")

        regioes = self.cur.fetchall()
        for regiao in regioes:
            print regiao

    def print_bairro(self):
        self.cur.execute("SELECT * FROM bairro")

        bairros = self.cur.fetchall()
        for bairro in bairros:
            print bairro

    def print_area(self):
        self.cur.execute("SELECT * FROM area")

        areas = self.cur.fetchall()
        for area in areas:
            print area

    def select_area(self):
        self.cur.execute("SELECT * FROM area")

        return self.cur.fetchall()

    def select_bairro(self, id_bairro):
        self.cur.execute("SELECT * FROM bairro WHERE _id=?", (id_bairro, ))

        return self.cur.fetchone()
