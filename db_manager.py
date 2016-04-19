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
                                codigo TEXT, \
                                total_residentes INTEGER, \
                                total_homens INTEGER, \
                                total_mulheres INTEGER, \
                                id_area INTEGER, \
                                FOREIGN KEY(id_area) REFERENCES area(_id) \
                            );"

CREATE_TABLE_RESIDENTES_AREA_IDADE = "CREATE TABLE residentes_idade_area ( \
                                         _id INTEGER PRIMARY KEY, \
                                         sexo INTEGER NOT NULL, \
                                         menos_1_ano INTEGER, \
                                         anos_1_4 INTEGER, \
                                         anos_5 INTEGER, \
                                         anos_6_9 INTEGER, \
                                         anos_10_19 INTEGER, \
                                         anos_20_24 INTEGER, \
                                         anos_25_49 INTEGER, \
                                         anos_50_59 INTEGER, \
                                         anos_60_64 INTEGER, \
                                         anos_65_69 INTEGER, \
                                         mais_70_anos INTEGER, \
                                         id_area INTEGER, \
                                         FOREIGN KEY(id_area) REFERENCES area(_id) \
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
        self.cur.execute(CREATE_TABLE_SUMARIO_AREA)
        self.cur.execute(CREATE_TABLE_RESIDENTES_AREA_IDADE)

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
                            WHERE codigo=?", (codigo, ano, link, bairro_id,
                                              codigo))

            if self.cur.rowcount == 0:
                self.cur.execute("INSERT INTO area (codigo, ano, link, bairro_id) \
                            VALUES (?, ?, ?, ?)", (codigo, ano, link, bairro_id))

            self.conn.commit()

    def update_sumario_area(self, codigo, total_residentes,
                            total_homens, total_mulheres, cod_area):

        self.cur.execute("SELECT * FROM sumario_area WHERE \
                          codigo=?", (codigo, ))

        if self.cur.rowcount == 0:
            self.cur.execute('INSERT INTO sumario_area (codigo, total_residentes, \
                             total_homens, total_mulheres, id_area) VALUES (?, ?, ?, ?),', 
                             (cod_area, total_residentes, total_homens, total_mulheres, cod_area))
        self.cur.commit()

    def get_regiao_por_codigo(self, regiao_codigo):
        self.cur.execute('SELECT * FROM regiao WHERE \
                          codigo=?', (regiao_codigo, ))
        return self.cur.fetchone()

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
